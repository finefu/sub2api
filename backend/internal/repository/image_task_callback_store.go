package repository

// YeaGrow execution plane (T2.2): atomic finish + durable callback queue in Redis.
// Origin: finefu/sub2api branch yeagrow/execution-plane (base Wei-Shaw/sub2api ee58c0013).
//
// Layout:
//   image_task:<task_id>                      task record (JSON string)
//   image_task:callback:due                   ZSET event_id -> next attempt (unix ms); a claim moves
//                                             the score to now+lease, so a crashed worker's events
//                                             become due again after the lease (restart-safe)
//   image_task:callback:event:<event_id>      HASH body, api_key_id, created_ms, failures, last_error
//   image_task:callback:dead                  ZSET abandoned event_id -> abandon time (unix ms)

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

const (
	imageTaskCallbackDueKey         = "image_task:callback:due"
	imageTaskCallbackDeadKey        = "image_task:callback:dead"
	imageTaskCallbackEventKeyPrefix = "image_task:callback:event:"
)

var (
	_ service.ImageTaskFinishStore   = (*imageTaskStore)(nil)
	_ service.ImageTaskCallbackQueue = (*imageTaskStore)(nil)
)

func imageTaskCallbackEventKey(eventID string) string {
	return imageTaskCallbackEventKeyPrefix + eventID
}

// imageTaskFinishScript: CAS processing -> terminal and enqueue the event in one step.
// KEYS: task, due zset, event hash. ARGV: new record, ttl ms, has event, event id,
// body, api key id, created ms, event ttl ms.
var imageTaskFinishScript = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
if not current then
  return 'missing'
end
local ok, decoded = pcall(cjson.decode, current)
if not ok or type(decoded) ~= 'table' or decoded['status'] ~= 'processing' then
  return 'not_processing'
end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
if ARGV[3] == '1' then
  redis.call('DEL', KEYS[3])
  redis.call('HSET', KEYS[3], 'body', ARGV[5], 'api_key_id', ARGV[6], 'created_ms', ARGV[7], 'failures', '0')
  redis.call('PEXPIRE', KEYS[3], ARGV[8])
  redis.call('ZADD', KEYS[2], ARGV[7], ARGV[4])
end
return 'applied'
`)

// imageTaskClaimCallbacksScript leases due events. KEYS: due zset. ARGV: now ms, lease-until ms, limit.
var imageTaskClaimCallbacksScript = redis.NewScript(`
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[3]))
for _, id in ipairs(ids) do
  redis.call('ZADD', KEYS[1], ARGV[2], id)
end
return ids
`)

// FinishProcessing implements service.ImageTaskFinishStore.
func (s *imageTaskStore) FinishProcessing(ctx context.Context, next *service.ImageTaskRecord, ttl time.Duration, event *service.ImageTaskCallbackEnvelope) (service.ImageTaskFinishResult, error) {
	data, err := json.Marshal(next)
	if err != nil {
		return 0, err
	}
	hasEvent, eventID, body, apiKeyID, createdMS := "0", "", "", "0", "0"
	eventKey := imageTaskCallbackEventKeyPrefix
	if event != nil {
		hasEvent = "1"
		eventID = event.EventID
		body = string(event.Body)
		apiKeyID = strconv.FormatInt(event.APIKeyID, 10)
		createdMS = strconv.FormatInt(event.CreatedAt.UnixMilli(), 10)
		eventKey = imageTaskCallbackEventKey(eventID)
	}
	eventTTL := service.ImageTaskCallbackGiveUpAfter + service.ImageTaskCallbackRetention
	res, err := imageTaskFinishScript.Run(ctx, s.rdb,
		[]string{imageTaskKey(next.ID), imageTaskCallbackDueKey, eventKey},
		string(data), ttl.Milliseconds(), hasEvent, eventID, body, apiKeyID, createdMS, eventTTL.Milliseconds(),
	).Text()
	if err != nil {
		return 0, err
	}
	switch res {
	case "applied":
		return service.ImageTaskFinishApplied, nil
	case "missing":
		return service.ImageTaskFinishMissing, nil
	case "not_processing":
		return service.ImageTaskFinishNotProcessing, nil
	default:
		return 0, errors.New("unexpected finish script result: " + res)
	}
}

// ClaimDueCallbacks implements service.ImageTaskCallbackQueue.
func (s *imageTaskStore) ClaimDueCallbacks(ctx context.Context, now time.Time, lease time.Duration, limit int) ([]service.ImageTaskCallbackEnvelope, error) {
	ids, err := imageTaskClaimCallbacksScript.Run(ctx, s.rdb, []string{imageTaskCallbackDueKey},
		now.UnixMilli(), now.Add(lease).UnixMilli(), limit).StringSlice()
	if err != nil || len(ids) == 0 {
		return nil, err
	}
	pipe := s.rdb.Pipeline()
	cmds := make([]*redis.SliceCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.HMGet(ctx, imageTaskCallbackEventKey(id), "body", "api_key_id", "created_ms", "failures")
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	out := make([]service.ImageTaskCallbackEnvelope, 0, len(ids))
	for i, id := range ids {
		vals := cmds[i].Val()
		body, okBody := vals[0].(string)
		if !okBody || len(vals) < 4 {
			// Event hash expired: drop the dangling schedule entry.
			_ = s.rdb.ZRem(ctx, imageTaskCallbackDueKey, id).Err()
			continue
		}
		apiKeyID, _ := strconv.ParseInt(stringValue(vals[1]), 10, 64)
		createdMS, _ := strconv.ParseInt(stringValue(vals[2]), 10, 64)
		failures, _ := strconv.Atoi(stringValue(vals[3]))
		out = append(out, service.ImageTaskCallbackEnvelope{
			EventID:   id,
			APIKeyID:  apiKeyID,
			Body:      []byte(body),
			CreatedAt: time.UnixMilli(createdMS),
			Failures:  failures,
		})
	}
	return out, nil
}

func stringValue(v any) string {
	s, _ := v.(string)
	return s
}

// AckCallback implements service.ImageTaskCallbackQueue.
func (s *imageTaskStore) AckCallback(ctx context.Context, eventID string) error {
	_, err := s.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.ZRem(ctx, imageTaskCallbackDueKey, eventID)
		pipe.Del(ctx, imageTaskCallbackEventKey(eventID))
		return nil
	})
	return err
}

// RescheduleCallback implements service.ImageTaskCallbackQueue.
func (s *imageTaskStore) RescheduleCallback(ctx context.Context, eventID string, failures int, next time.Time, lastError string) error {
	_, err := s.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, imageTaskCallbackEventKey(eventID), "failures", failures, "last_error", lastError)
		pipe.ZAdd(ctx, imageTaskCallbackDueKey, redis.Z{Score: float64(next.UnixMilli()), Member: eventID})
		return nil
	})
	return err
}

// AbandonCallback implements service.ImageTaskCallbackQueue.
func (s *imageTaskStore) AbandonCallback(ctx context.Context, eventID string, failures int, reason string, now time.Time) error {
	_, err := s.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.ZRem(ctx, imageTaskCallbackDueKey, eventID)
		pipe.ZAdd(ctx, imageTaskCallbackDeadKey, redis.Z{Score: float64(now.UnixMilli()), Member: eventID})
		pipe.HSet(ctx, imageTaskCallbackEventKey(eventID), "failures", failures, "last_error", reason, "abandoned_ms", now.UnixMilli())
		pipe.Expire(ctx, imageTaskCallbackEventKey(eventID), service.ImageTaskCallbackRetention)
		return nil
	})
	return err
}
