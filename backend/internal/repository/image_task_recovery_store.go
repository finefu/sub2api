package repository

// YeaGrow execution plane (T2.3 / P3): Redis persistence for restart recovery.
// Origin: finefu/sub2api branch yeagrow/execution-plane (base Wei-Shaw/sub2api ee58c0013).
//
// Layout:
//   image_task:inflight:<instance>                    ZSET task_id -> created (unix ms);
//                                                     removed by the finish CAS script
//   image_task:instance:<instance>:lease              STRING owner process prefix, PX lease
//   image_task:instance:<instance>:process_prefixes   SET request-ID prefixes of this
//                                                     instance's processes (slot cleanup)

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

const (
	imageTaskInflightKeyPrefix = "image_task:inflight:"
	imageTaskInstanceKeyPrefix = "image_task:instance:"
)

var _ service.ImageTaskRecoveryStore = (*imageTaskStore)(nil)

func imageTaskInflightKey(instanceID string) string {
	return imageTaskInflightKeyPrefix + instanceID
}

func imageTaskInstanceLeaseKey(instanceID string) string {
	return imageTaskInstanceKeyPrefix + instanceID + ":lease"
}

func imageTaskInstancePrefixesKey(instanceID string) string {
	return imageTaskInstanceKeyPrefix + instanceID + ":process_prefixes"
}

// AddInflight implements service.ImageTaskRecoveryStore.
func (s *imageTaskStore) AddInflight(ctx context.Context, instanceID, taskID string, createdAt time.Time) error {
	return s.rdb.ZAdd(ctx, imageTaskInflightKey(instanceID), redis.Z{Score: float64(createdAt.UnixMilli()), Member: taskID}).Err()
}

// RemoveInflight implements service.ImageTaskRecoveryStore.
func (s *imageTaskStore) RemoveInflight(ctx context.Context, instanceID, taskID string) error {
	return s.rdb.ZRem(ctx, imageTaskInflightKey(instanceID), taskID).Err()
}

// ListInflight implements service.ImageTaskRecoveryStore.
func (s *imageTaskStore) ListInflight(ctx context.Context, instanceID string) ([]string, error) {
	return s.rdb.ZRange(ctx, imageTaskInflightKey(instanceID), 0, -1).Result()
}

// MarkDispatched implements service.ImageTaskRecoveryStore.
func (s *imageTaskStore) MarkDispatched(ctx context.Context, taskID string, at time.Time) error {
	return s.markStage(ctx, taskID, at, func(t *service.ImageTaskRecord) **int64 { return &t.DispatchedAt })
}

// MarkStoring implements service.ImageTaskRecoveryStore.
func (s *imageTaskStore) MarkStoring(ctx context.Context, taskID string, at time.Time) error {
	return s.markStage(ctx, taskID, at, func(t *service.ImageTaskRecord) **int64 { return &t.StoringAt })
}

// markStage sets a stage timestamp with an optimistic WATCH/MULTI CAS: the
// record is rewritten (TTL kept) only if it is unchanged and still processing.
// An already-set timestamp is kept.
func (s *imageTaskStore) markStage(ctx context.Context, taskID string, at time.Time, field func(*service.ImageTaskRecord) **int64) error {
	key := imageTaskKey(taskID)
	for attempt := 0; attempt < 5; attempt++ {
		err := s.rdb.Watch(ctx, func(tx *redis.Tx) error {
			data, err := tx.Get(ctx, key).Bytes()
			if err != nil {
				if errors.Is(err, redis.Nil) {
					return service.ErrImageTaskNotFound
				}
				return err
			}
			var task service.ImageTaskRecord
			if err := json.Unmarshal(data, &task); err != nil {
				return err
			}
			if task.Status != service.ImageTaskStatusProcessing {
				return service.ErrImageTaskDispatchRejected
			}
			slot := field(&task)
			if *slot != nil {
				return nil
			}
			ms := at.UnixMilli()
			*slot = &ms
			next, err := json.Marshal(&task)
			if err != nil {
				return err
			}
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.SetArgs(ctx, key, next, redis.SetArgs{KeepTTL: true})
				return nil
			})
			return err
		}, key)
		if errors.Is(err, redis.TxFailedErr) {
			continue
		}
		return err
	}
	return redis.TxFailedErr
}

var imageTaskLeaseRefreshScript = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
if current == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
if not current then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
  return 1
end
return 0
`)

var imageTaskLeaseReleaseScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`)

// AcquireInstanceLease implements service.ImageTaskRecoveryStore.
func (s *imageTaskStore) AcquireInstanceLease(ctx context.Context, instanceID, owner string, ttl time.Duration) (bool, error) {
	ok, err := s.rdb.SetNX(ctx, imageTaskInstanceLeaseKey(instanceID), owner, ttl).Result()
	if err != nil || ok {
		return ok, err
	}
	current, err := s.rdb.Get(ctx, imageTaskInstanceLeaseKey(instanceID)).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return false, err
	}
	return current == owner, nil
}

// RefreshInstanceLease implements service.ImageTaskRecoveryStore.
func (s *imageTaskStore) RefreshInstanceLease(ctx context.Context, instanceID, owner string, ttl time.Duration) (bool, error) {
	n, err := imageTaskLeaseRefreshScript.Run(ctx, s.rdb, []string{imageTaskInstanceLeaseKey(instanceID)}, owner, ttl.Milliseconds()).Int()
	return n == 1, err
}

// ReleaseInstanceLease implements service.ImageTaskRecoveryStore.
func (s *imageTaskStore) ReleaseInstanceLease(ctx context.Context, instanceID, owner string) error {
	return imageTaskLeaseReleaseScript.Run(ctx, s.rdb, []string{imageTaskInstanceLeaseKey(instanceID)}, owner).Err()
}

// RegisterProcessPrefix implements service.ImageTaskRecoveryStore.
func (s *imageTaskStore) RegisterProcessPrefix(ctx context.Context, instanceID, prefix string) ([]string, error) {
	key := imageTaskInstancePrefixesKey(instanceID)
	members, err := s.rdb.SMembers(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	if err := s.rdb.SAdd(ctx, key, prefix).Err(); err != nil {
		return nil, err
	}
	previous := make([]string, 0, len(members))
	for _, m := range members {
		if m != prefix {
			previous = append(previous, m)
		}
	}
	return previous, nil
}

// ForgetProcessPrefixes implements service.ImageTaskRecoveryStore.
func (s *imageTaskStore) ForgetProcessPrefixes(ctx context.Context, instanceID string, prefixes []string) error {
	if len(prefixes) == 0 {
		return nil
	}
	members := make([]any, len(prefixes))
	for i, p := range prefixes {
		members[i] = p
	}
	return s.rdb.SRem(ctx, imageTaskInstancePrefixesKey(instanceID), members...).Err()
}

// Confirmed concurrency slot key families whose members are request IDs
// (generateRequestID: "<process prefix>-<seq>"). Index keys
// (concurrency:*:active_index), wait counters and WebSocket ingress leases are
// never touched: they are not attributable to a process.
var (
	imageTaskSlotKeyScanPatterns = []string{
		accountSlotKeyPrefix + "*",
		userSlotKeyPrefix + "*",
		apiKeySlotKeyPrefix + "*",
		liveAccountSlotKeyPrefix + "*",
		liveUserSlotKeyPrefix + "*",
		liveAPIKeySlotKeyPrefix + "*",
	}
	imageTaskConfirmedSlotKey = regexp.MustCompile(`^concurrency:(live:)?(account|user|api_key):[0-9]+$`)
	imageTaskSlotPrefixFormat = regexp.MustCompile(`^r[0-9a-z]{1,20}$`)
)

// imageTaskSlotCleanupScript removes members starting with one of the given
// "<prefix>-" strings from a ZSET. Non-ZSET keys are left alone.
var imageTaskSlotCleanupScript = redis.NewScript(`
local t = redis.call('TYPE', KEYS[1])
if type(t) == 'table' then t = t['ok'] end
if t ~= 'zset' then
  return 0
end
local removed = 0
local members = redis.call('ZRANGE', KEYS[1], 0, -1)
for _, m in ipairs(members) do
  for i = 1, #ARGV do
    local p = ARGV[i]
    if string.sub(m, 1, string.len(p)) == p then
      removed = removed + redis.call('ZREM', KEYS[1], m)
      break
    end
  end
end
return removed
`)

// CleanupSlotsForPrefixes implements service.ImageTaskRecoveryStore.
func (s *imageTaskStore) CleanupSlotsForPrefixes(ctx context.Context, prefixes []string) (int, error) {
	args := make([]any, 0, len(prefixes))
	for _, p := range prefixes {
		p = strings.TrimSpace(p)
		if !imageTaskSlotPrefixFormat.MatchString(p) {
			continue // never run with an empty or malformed prefix
		}
		args = append(args, p+"-")
	}
	if len(args) == 0 {
		return 0, nil
	}
	total := 0
	seen := make(map[string]struct{})
	for _, pattern := range imageTaskSlotKeyScanPatterns {
		var cursor uint64
		for {
			keys, next, err := s.rdb.Scan(ctx, cursor, pattern, 500).Result()
			if err != nil {
				return total, err
			}
			for _, key := range keys {
				if _, dup := seen[key]; dup || !imageTaskConfirmedSlotKey.MatchString(key) {
					continue
				}
				seen[key] = struct{}{}
				n, err := imageTaskSlotCleanupScript.Run(ctx, s.rdb, []string{key}, args...).Int()
				if err != nil {
					return total, err
				}
				total += n
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
	}
	return total, nil
}
