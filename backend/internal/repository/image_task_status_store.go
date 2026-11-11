package repository

// YeaGrow execution plane: aggregate counts for GET /v1/execution-plane/status.
// Origin: finefu/sub2api branch yeagrow/execution-plane (base Wei-Shaw/sub2api ee58c0013).

import (
	"context"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

var _ service.ImageTaskStatusStore = (*imageTaskStore)(nil)

// ExecutionPlaneCounts reads ZCARD of the due and dead callback zsets and of this
// instance's in-flight index in one pipeline. Any Redis error is returned as is.
func (s *imageTaskStore) ExecutionPlaneCounts(ctx context.Context, instanceID string) (service.ImageTaskExecutionPlaneCounts, error) {
	var due, dead, inflight *redis.IntCmd
	_, err := s.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		due = pipe.ZCard(ctx, imageTaskCallbackDueKey)
		dead = pipe.ZCard(ctx, imageTaskCallbackDeadKey)
		if instanceID != "" {
			inflight = pipe.ZCard(ctx, imageTaskInflightKey(instanceID))
		}
		return nil
	})
	if err != nil {
		return service.ImageTaskExecutionPlaneCounts{}, err
	}
	counts := service.ImageTaskExecutionPlaneCounts{CallbackDue: due.Val(), CallbackDead: dead.Val()}
	if inflight != nil {
		counts.Inflight = inflight.Val()
	}
	return counts, nil
}
