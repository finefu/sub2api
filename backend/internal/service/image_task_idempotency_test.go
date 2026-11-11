package service

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/stretchr/testify/require"
)

// idemTaskStore is an in-memory ImageTaskStore with an atomic SaveIfAbsent.
type idemTaskStore struct {
	mu       sync.Mutex
	tasks    map[string]*ImageTaskRecord
	creates  int
	failNext error
}

func newIdemTaskStore() *idemTaskStore {
	return &idemTaskStore{tasks: map[string]*ImageTaskRecord{}}
}

func (s *idemTaskStore) Save(_ context.Context, task *ImageTaskRecord, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	copy := *task
	s.tasks[task.ID] = &copy
	return nil
}

func (s *idemTaskStore) Get(_ context.Context, id string) (*ImageTaskRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	task, ok := s.tasks[id]
	if !ok {
		return nil, ErrImageTaskNotFound
	}
	copy := *task
	return &copy, nil
}

func (s *idemTaskStore) SaveIfAbsent(_ context.Context, task *ImageTaskRecord, _ time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failNext != nil {
		err := s.failNext
		s.failNext = nil
		return false, err
	}
	if _, ok := s.tasks[task.ID]; ok {
		return false, nil
	}
	copy := *task
	s.tasks[task.ID] = &copy
	s.creates++
	return true, nil
}

func (s *idemTaskStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.tasks)
}

func newIdemService(t *testing.T) (*ImageTaskService, *idemTaskStore, *inMemoryIdempotencyRepo) {
	t.Helper()
	store := newIdemTaskStore()
	repo := newInMemoryIdempotencyRepo()
	svc := NewImageTaskServiceWithUploader(store, nil, time.Hour, time.Minute)
	svc.SetIdempotencyRepository(repo)
	return svc, store, repo
}

var idemOwner = ImageTaskOwner{UserID: 7, APIKeyID: 9}

func requireAppError(t *testing.T, err error, status int, reason string) {
	t.Helper()
	require.Error(t, err)
	require.Equal(t, status, infraerrors.Code(err))
	require.Equal(t, reason, infraerrors.Reason(err))
}

func TestImageTaskIdempotency_SameKeySameFingerprintReturnsSameTask(t *testing.T) {
	svc, store, _ := newIdemService(t)
	ctx := context.Background()

	first, err := svc.CreateIdempotent(ctx, idemOwner, "run-1", "fp-a")
	require.NoError(t, err)
	require.True(t, first.Created)
	require.Equal(t, imageTaskIDForIdempotencyKey(idemOwner.APIKeyID, "run-1"), first.Task.ID)
	require.Equal(t, ImageTaskStatusProcessing, first.Task.Status)

	second, err := svc.CreateIdempotent(ctx, idemOwner, "  run-1 ", "fp-a")
	require.NoError(t, err)
	require.False(t, second.Created)
	require.Equal(t, first.Task.ID, second.Task.ID)
	require.Equal(t, 1, store.count())
}

func TestImageTaskIdempotency_DifferentFingerprintConflictsWithoutCreating(t *testing.T) {
	svc, store, _ := newIdemService(t)
	ctx := context.Background()
	_, err := svc.CreateIdempotent(ctx, idemOwner, "run-1", "fp-a")
	require.NoError(t, err)

	got, err := svc.CreateIdempotent(ctx, idemOwner, "run-1", "fp-b")
	require.Nil(t, got)
	requireAppError(t, err, 409, "IDEMPOTENCY_KEY_CONFLICT")
	require.Equal(t, 1, store.count())
	require.Equal(t, 1, store.creates)
}

func TestImageTaskIdempotency_APIKeysAreIsolated(t *testing.T) {
	svc, store, _ := newIdemService(t)
	ctx := context.Background()

	a, err := svc.CreateIdempotent(ctx, ImageTaskOwner{UserID: 7, APIKeyID: 9}, "run-1", "fp-a")
	require.NoError(t, err)
	b, err := svc.CreateIdempotent(ctx, ImageTaskOwner{UserID: 7, APIKeyID: 10}, "run-1", "fp-other")
	require.NoError(t, err)
	require.True(t, a.Created)
	require.True(t, b.Created, "same key under another API key is independent, even with another body")
	require.NotEqual(t, a.Task.ID, b.Task.ID)
	require.Equal(t, 2, store.count())
}

func TestImageTaskIdempotency_ConcurrentSameKeyCreatesOneTask(t *testing.T) {
	svc, store, _ := newIdemService(t)
	const n = 10
	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make([]*ImageTaskSubmission, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], errs[i] = svc.CreateIdempotent(context.Background(), idemOwner, "run-concurrent", "fp-a")
		}(i)
	}
	close(start)
	wg.Wait()

	created := 0
	for i := 0; i < n; i++ {
		require.NoError(t, errs[i])
		require.Equal(t, results[0].Task.ID, results[i].Task.ID)
		if results[i].Created {
			created++
		}
	}
	require.Equal(t, 1, created, "exactly one request may start execution")
	require.Equal(t, 1, store.count())
	require.Equal(t, 1, store.creates)
}

func TestImageTaskIdempotency_ClaimWithoutTaskIsRepairedAfterLease(t *testing.T) {
	svc, store, repo := newIdemService(t)
	ctx := context.Background()
	// Simulate a crash between claiming the key and creating the task.
	past := time.Now().Add(-time.Second)
	require.NoError(t, claimForTest(repo, "run-1", "fp-a", IdempotencyStatusProcessing, &past, time.Now().Add(time.Hour)))

	got, err := svc.CreateIdempotent(ctx, idemOwner, "run-1", "fp-a")
	require.NoError(t, err)
	require.True(t, got.Created)
	require.Equal(t, imageTaskIDForIdempotencyKey(idemOwner.APIKeyID, "run-1"), got.Task.ID)
	require.Equal(t, 1, store.count())
	rec, _ := repo.GetByScopeAndKeyHash(ctx, imageTaskIdempotencyScope(idemOwner.APIKeyID), HashIdempotencyKey("run-1"))
	require.Equal(t, IdempotencyStatusSucceeded, rec.Status)
}

func TestImageTaskIdempotency_ClaimWithinLeaseAnswersInProgress(t *testing.T) {
	old := imageTaskIdempotencyWait
	imageTaskIdempotencyWait = 100 * time.Millisecond
	t.Cleanup(func() { imageTaskIdempotencyWait = old })

	svc, store, repo := newIdemService(t)
	future := time.Now().Add(time.Minute)
	require.NoError(t, claimForTest(repo, "run-1", "fp-a", IdempotencyStatusProcessing, &future, time.Now().Add(time.Hour)))

	got, err := svc.CreateIdempotent(context.Background(), idemOwner, "run-1", "fp-a")
	require.Nil(t, got)
	requireAppError(t, err, 409, "IDEMPOTENCY_IN_PROGRESS")
	require.Equal(t, 1, RetryAfterSecondsFromError(err))
	require.Zero(t, store.count())
}

func TestImageTaskIdempotency_TaskCreatedButClaimNotMarked(t *testing.T) {
	svc, store, repo := newIdemService(t)
	ctx := context.Background()
	future := time.Now().Add(time.Minute)
	require.NoError(t, claimForTest(repo, "run-1", "fp-a", IdempotencyStatusProcessing, &future, time.Now().Add(time.Hour)))
	taskID := imageTaskIDForIdempotencyKey(idemOwner.APIKeyID, "run-1")
	require.NoError(t, store.Save(ctx, &ImageTaskRecord{ID: taskID, UserID: 7, APIKeyID: 9, Status: ImageTaskStatusCompleted}, time.Hour))

	got, err := svc.CreateIdempotent(ctx, idemOwner, "run-1", "fp-a")
	require.NoError(t, err)
	require.False(t, got.Created)
	require.Equal(t, ImageTaskStatusCompleted, got.Task.Status)
	require.Zero(t, store.creates)
}

func TestImageTaskIdempotency_LostTaskIsNeverRecreated(t *testing.T) {
	svc, store, repo := newIdemService(t)
	ctx := context.Background()
	first, err := svc.CreateIdempotent(ctx, idemOwner, "run-1", "fp-a")
	require.NoError(t, err)
	// Redis loses the task record (no AOF, eviction, or TTL) while the PG claim remains.
	store.mu.Lock()
	delete(store.tasks, first.Task.ID)
	store.mu.Unlock()

	got, err := svc.CreateIdempotent(ctx, idemOwner, "run-1", "fp-a")
	require.NoError(t, err)
	require.False(t, got.Created)
	require.Equal(t, first.Task.ID, got.Task.ID)
	require.Equal(t, ImageTaskStatusUnknown, got.Task.Status)
	require.Zero(t, store.count())
	require.Equal(t, 1, store.creates)
	rec, _ := repo.GetByScopeAndKeyHash(ctx, imageTaskIdempotencyScope(idemOwner.APIKeyID), HashIdempotencyKey("run-1"))
	require.Equal(t, IdempotencyStatusSucceeded, rec.Status)
}

func TestImageTaskIdempotency_ExpiredClaimIsReclaimed(t *testing.T) {
	svc, store, repo := newIdemService(t)
	require.NoError(t, claimForTest(repo, "run-1", "fp-a", IdempotencyStatusSucceeded, nil, time.Now().Add(-time.Second)))

	got, err := svc.CreateIdempotent(context.Background(), idemOwner, "run-1", "fp-a")
	require.NoError(t, err)
	require.True(t, got.Created)
	require.Equal(t, 1, store.count())
}

func TestImageTaskIdempotency_StoreFailureReleasesClaim(t *testing.T) {
	svc, store, repo := newIdemService(t)
	ctx := context.Background()
	store.failNext = errors.New("redis down")

	got, err := svc.CreateIdempotent(ctx, idemOwner, "run-1", "fp-a")
	require.Nil(t, got)
	requireAppError(t, err, 503, "IMAGE_TASK_UNAVAILABLE")
	rec, _ := repo.GetByScopeAndKeyHash(ctx, imageTaskIdempotencyScope(idemOwner.APIKeyID), HashIdempotencyKey("run-1"))
	require.Equal(t, IdempotencyStatusFailedRetryable, rec.Status)

	retry, err := svc.CreateIdempotent(ctx, idemOwner, "run-1", "fp-a")
	require.NoError(t, err)
	require.True(t, retry.Created)
	require.Equal(t, 1, store.count())
}

func TestImageTaskIdempotency_KeyValidationAndNoKey(t *testing.T) {
	svc, store, _ := newIdemService(t)
	ctx := context.Background()

	_, err := svc.CreateIdempotent(ctx, idemOwner, strings.Repeat("k", 129), "fp-a")
	requireAppError(t, err, 400, "IDEMPOTENCY_KEY_INVALID")
	_, err = svc.CreateIdempotent(ctx, idemOwner, "has space", "fp-a")
	requireAppError(t, err, 400, "IDEMPOTENCY_KEY_INVALID")
	require.Zero(t, store.count())

	plain, err := svc.CreateIdempotent(ctx, idemOwner, "", "fp-a")
	require.NoError(t, err)
	require.True(t, plain.Created)
	require.Len(t, plain.Task.ID, len("imgtask_")+32)
}

func TestImageTaskIdempotency_StoreWithoutCreateIfAbsentIsUnavailable(t *testing.T) {
	svc := NewImageTaskServiceWithUploader(&executionPlaneMemoryStore{tasks: map[string]*ImageTaskRecord{}}, nil, time.Hour, time.Minute)
	svc.SetIdempotencyRepository(newInMemoryIdempotencyRepo())
	_, err := svc.CreateIdempotent(context.Background(), idemOwner, "run-1", "fp-a")
	requireAppError(t, err, 503, "IMAGE_TASK_UNAVAILABLE")
}

func TestImageTaskIdempotencyFingerprint(t *testing.T) {
	body := []byte(`{"model":"gpt-image-2-low","prompt":"cat"}`)
	base := ImageTaskIdempotencyFingerprint("/v1/images/generations/async", "application/json", body)
	require.Equal(t, base, ImageTaskIdempotencyFingerprint("/images/generations/async", "application/json; charset=utf-8", body))
	require.NotEqual(t, base, ImageTaskIdempotencyFingerprint("/v1/images/edits/async", "application/json", body))
	require.NotEqual(t, base, ImageTaskIdempotencyFingerprint("/v1/images/generations/async", "application/json", []byte(`{"model":"gpt-image-2-low","prompt":"dog"}`)))
	require.NotEqual(t, base, ImageTaskIdempotencyFingerprint("/v1/images/generations/async", "multipart/form-data; boundary=x", body))
}

func TestImageTaskIDForIdempotencyKey(t *testing.T) {
	a := imageTaskIDForIdempotencyKey(9, "run-1")
	require.Equal(t, a, imageTaskIDForIdempotencyKey(9, "run-1"))
	require.NotEqual(t, a, imageTaskIDForIdempotencyKey(10, "run-1"))
	require.NotEqual(t, a, imageTaskIDForIdempotencyKey(9, "run-2"))
	require.True(t, strings.HasPrefix(a, "imgtask_"))
	require.Len(t, a, len("imgtask_")+32)
}

func claimForTest(repo *inMemoryIdempotencyRepo, key, fingerprint, status string, lockedUntil *time.Time, expiresAt time.Time) error {
	rec := &IdempotencyRecord{
		Scope:              imageTaskIdempotencyScope(idemOwner.APIKeyID),
		IdempotencyKeyHash: HashIdempotencyKey(key),
		RequestFingerprint: fingerprint,
		Status:             status,
		LockedUntil:        lockedUntil,
		ExpiresAt:          expiresAt,
	}
	ok, err := repo.CreateProcessing(context.Background(), rec)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("claim exists")
	}
	return nil
}
