package service

// YeaGrow execution plane (T2.1): idempotent async image task submission.
// Origin: finefu/sub2api branch yeagrow/execution-plane (base Wei-Shaw/sub2api ee58c0013).
//
// Storage: the Postgres idempotency_records table through the existing
// IdempotencyRepository (no new migration). The Redis task store remains the
// task record of truth; Redis SET NX on the task ID is the final arbiter of
// which request starts execution.
//
// Order per request:
//  1. derive task_id deterministically from (API key ID, Idempotency-Key);
//  2. atomically claim the key in Postgres (INSERT ... ON CONFLICT DO NOTHING),
//     storing the request fingerprint;
//  3. create the task with SET NX on that task_id; only the creator executes;
//  4. mark the claim succeeded.
// Because the task_id is a pure function of the key, a claim never points at a
// different task, and a crash between 2 and 3 is repaired by the next retry
// (after the claim lease) without any chance of two executions: SET NX lets
// exactly one request create the task.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"mime"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"go.uber.org/zap"
)

const (
	imageTaskIdempotencyScopePrefix = "image_task_async:api_key:"
	// imageTaskIdempotencyLease bounds how long a claim may stay unfinished
	// before another request may take it over (claim → SET NX takes ms).
	imageTaskIdempotencyLease    = 30 * time.Second
	imageTaskIdempotencyPollStep = 20 * time.Millisecond
	// ImageTaskStatusUnknown is returned on a replay whose task record is no
	// longer in Redis (expired or lost). The task is never recreated.
	ImageTaskStatusUnknown = "unknown"
)

// imageTaskIdempotencyWait is how long a concurrent duplicate waits for the
// owner to create the task before answering 409 in-progress (var for tests).
var imageTaskIdempotencyWait = 3 * time.Second

// ImageTaskStoreCreateIfAbsent is implemented by stores that can create a task
// record atomically (Redis SET NX). Required for idempotent submission.
type ImageTaskStoreCreateIfAbsent interface {
	SaveIfAbsent(ctx context.Context, task *ImageTaskRecord, ttl time.Duration) (bool, error)
}

// ImageTaskSubmission is the result of an idempotent create.
type ImageTaskSubmission struct {
	Task *ImageTask
	// Created is true only for the request that created the task; that request
	// alone must start execution. Replays return Created=false.
	Created bool
}

// SetIdempotencyRepository injects the repository (tests). Production falls
// back to the repository of the default IdempotencyCoordinator.
func (s *ImageTaskService) SetIdempotencyRepository(repo IdempotencyRepository) {
	if s != nil {
		s.idempotencyRepo = repo
	}
}

func (s *ImageTaskService) resolveIdempotencyRepository() IdempotencyRepository {
	if s.idempotencyRepo != nil {
		return s.idempotencyRepo
	}
	if coordinator := DefaultIdempotencyCoordinator(); coordinator != nil {
		return coordinator.repo
	}
	return nil
}

// idempotencyTTL keeps the claim strictly longer than any task record created
// under it can live (task TTL is renewed once on completion, which happens
// within the execution timeout plus upstream drain).
func (s *ImageTaskService) idempotencyTTL() time.Duration {
	return 2*s.ttl + s.ExecutionTimeout()
}

// ImageTaskIdempotencyFingerprint fingerprints a submission: endpoint (with or
// without the /v1 prefix), media type and the exact request body bytes.
func ImageTaskIdempotencyFingerprint(path, contentType string, body []byte) string {
	endpoint := strings.TrimPrefix(strings.TrimSpace(path), "/v1")
	mediaType := strings.ToLower(strings.TrimSpace(contentType))
	if parsed, _, err := mime.ParseMediaType(contentType); err == nil {
		mediaType = parsed
	}
	bodySum := sha256.Sum256(body)
	sum := sha256.Sum256([]byte("image_task_async\n" + endpoint + "\n" + mediaType + "\n" + hex.EncodeToString(bodySum[:])))
	return hex.EncodeToString(sum[:])
}

// imageTaskIDForIdempotencyKey derives the task ID from the API key and the
// normalized Idempotency-Key, so every request with the same key targets the
// same task.
func imageTaskIDForIdempotencyKey(apiKeyID int64, key string) string {
	sum := sha256.Sum256([]byte("yeagrow-image-task\x00" + strconv.FormatInt(apiKeyID, 10) + "\x00" + key))
	return "imgtask_" + hex.EncodeToString(sum[:16])
}

func imageTaskIdempotencyScope(apiKeyID int64) string {
	return imageTaskIdempotencyScopePrefix + strconv.FormatInt(apiKeyID, 10)
}

// CreateIdempotent creates the task for an Idempotency-Key or returns the task
// already bound to it. Errors: ErrIdempotencyKeyInvalid (400),
// ErrIdempotencyKeyConflict (409, different fingerprint, nothing created),
// ErrIdempotencyInProgress (409 + retry_after, owner still creating),
// ErrIdempotencyStoreUnavail / ErrImageTaskUnavailable (503).
func (s *ImageTaskService) CreateIdempotent(ctx context.Context, owner ImageTaskOwner, rawKey, fingerprint string) (*ImageTaskSubmission, error) {
	if s == nil || s.store == nil {
		return nil, ErrImageTaskUnavailable
	}
	key, err := NormalizeIdempotencyKey(rawKey)
	if err != nil {
		return nil, err
	}
	if key == "" {
		task, err := s.Create(ctx, owner)
		if err != nil {
			return nil, err
		}
		return &ImageTaskSubmission{Task: task, Created: true}, nil
	}
	repo := s.resolveIdempotencyRepository()
	if repo == nil {
		return nil, ErrIdempotencyStoreUnavail
	}
	creator, ok := s.store.(ImageTaskStoreCreateIfAbsent)
	if !ok {
		return nil, ErrImageTaskUnavailable
	}

	scope := imageTaskIdempotencyScope(owner.APIKeyID)
	keyHash := HashIdempotencyKey(key)
	taskID := imageTaskIDForIdempotencyKey(owner.APIKeyID, key)
	idemTTL := s.idempotencyTTL()
	deadline := time.Now().Add(imageTaskIdempotencyWait)

	for {
		now := time.Now()
		lockedUntil := now.Add(imageTaskIdempotencyLease)
		claim := &IdempotencyRecord{
			Scope:              scope,
			IdempotencyKeyHash: keyHash,
			RequestFingerprint: fingerprint,
			Status:             IdempotencyStatusProcessing,
			LockedUntil:        &lockedUntil,
			ExpiresAt:          now.Add(idemTTL),
		}
		claimed, err := repo.CreateProcessing(ctx, claim)
		if err != nil {
			return nil, ErrIdempotencyStoreUnavail.WithCause(err)
		}
		if claimed {
			return s.createClaimedTask(ctx, repo, creator, claim.ID, owner, taskID, idemTTL)
		}

		existing, err := repo.GetByScopeAndKeyHash(ctx, scope, keyHash)
		if err != nil {
			return nil, ErrIdempotencyStoreUnavail.WithCause(err)
		}
		if existing != nil {
			if existing.RequestFingerprint != fingerprint {
				return nil, ErrIdempotencyKeyConflict
			}
			stored, err := s.store.Get(ctx, taskID)
			switch {
			case err == nil:
				if stored.UserID != owner.UserID || stored.APIKeyID != owner.APIKeyID {
					return nil, ErrIdempotencyKeyConflict
				}
				return &ImageTaskSubmission{Task: imageTaskToPublic(stored)}, nil
			case !errors.Is(err, ErrImageTaskNotFound):
				return nil, ErrImageTaskUnavailable.WithCause(err)
			}

			// Same fingerprint, task record absent.
			expired := !existing.ExpiresAt.After(now)
			if existing.Status == IdempotencyStatusSucceeded && !expired {
				// The task was created under this claim and its record is gone
				// (expired or lost in Redis). Never create it again.
				return &ImageTaskSubmission{Task: &ImageTask{
					ID: taskID, TaskID: taskID, Object: "image.generation.task", Status: ImageTaskStatusUnknown,
				}}, nil
			}
			leaseOver := existing.LockedUntil == nil || !existing.LockedUntil.After(now)
			if expired || leaseOver {
				taken, err := repo.TryReclaim(ctx, existing.ID, existing.Status, now, lockedUntil, now.Add(idemTTL))
				if err != nil {
					return nil, ErrIdempotencyStoreUnavail.WithCause(err)
				}
				if taken {
					return s.createClaimedTask(ctx, repo, creator, existing.ID, owner, taskID, idemTTL)
				}
			}
		}
		// Claimed by a concurrent request that has not created the task yet,
		// or deleted by cleanup in between: wait briefly and look again.
		if !time.Now().Before(deadline) {
			return nil, ErrIdempotencyInProgress.WithMetadata(map[string]string{"retry_after": "1"})
		}
		select {
		case <-ctx.Done():
			return nil, ErrIdempotencyInProgress.WithMetadata(map[string]string{"retry_after": "1"})
		case <-time.After(imageTaskIdempotencyPollStep):
		}
	}
}

// createClaimedTask runs after this request holds the claim.
func (s *ImageTaskService) createClaimedTask(
	ctx context.Context,
	repo IdempotencyRepository,
	creator ImageTaskStoreCreateIfAbsent,
	claimID int64,
	owner ImageTaskOwner,
	taskID string,
	idemTTL time.Duration,
) (*ImageTaskSubmission, error) {
	now := time.Now().UTC()
	record := &ImageTaskRecord{
		ID:        taskID,
		UserID:    owner.UserID,
		APIKeyID:  owner.APIKeyID,
		Status:    ImageTaskStatusProcessing,
		CreatedAt: now.Unix(),
		ExpiresAt: now.Add(s.ttl).Unix(),
	}
	created, err := creator.SaveIfAbsent(ctx, record, s.ttl)
	if err != nil {
		// Nothing was created: release the claim for an immediate retry.
		if markErr := repo.MarkFailedRetryable(context.WithoutCancel(ctx), claimID, "IMAGE_TASK_STORE_UNAVAILABLE", time.Now(), time.Now().Add(idemTTL)); markErr != nil {
			logger.L().Warn("image_task.idempotency_release_failed", zap.Int64("claim_id", claimID), zap.Error(markErr))
		}
		return nil, ErrImageTaskUnavailable.WithCause(err)
	}
	result := &ImageTaskSubmission{Task: imageTaskToPublic(record), Created: created}
	if !created {
		// A previous holder created the task and stopped before marking the
		// claim; return that task and do not execute again.
		stored, err := s.store.Get(ctx, taskID)
		if err != nil {
			return nil, ErrImageTaskUnavailable.WithCause(err)
		}
		if stored.UserID != owner.UserID || stored.APIKeyID != owner.APIKeyID {
			return nil, ErrIdempotencyKeyConflict
		}
		result.Task = imageTaskToPublic(stored)
	}
	body, _ := json.Marshal(map[string]string{"task_id": taskID})
	if err := repo.MarkSucceeded(context.WithoutCancel(ctx), claimID, 202, string(body), time.Now().Add(idemTTL)); err != nil {
		// The task exists; retries find it through the task store, so this
		// only delays marking. Execution proceeds.
		logger.L().Warn("image_task.idempotency_mark_failed", zap.Int64("claim_id", claimID), zap.String("task_id", taskID), zap.Error(err))
	}
	return result, nil
}
