package repository

// YeaGrow execution plane: status counts against the real key layout (miniredis).

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestExecutionPlaneCountsReadsDueDeadAndThisInstanceInflight(t *testing.T) {
	mr, rdb, store := newCallbackTestStore(t)
	ctx := context.Background()
	require.NoError(t, rdb.ZAdd(ctx, imageTaskCallbackDueKey, redis.Z{Score: 1, Member: "evt_a"}, redis.Z{Score: 2, Member: "evt_b"}, redis.Z{Score: 3, Member: "evt_c"}).Err())
	require.NoError(t, rdb.ZAdd(ctx, imageTaskCallbackDeadKey, redis.Z{Score: 1, Member: "evt_d"}).Err())
	require.NoError(t, store.AddInflight(ctx, "inst-a", "task_1", time.Now()))
	require.NoError(t, store.AddInflight(ctx, "inst-a", "task_2", time.Now()))
	require.NoError(t, store.AddInflight(ctx, "inst-b", "task_3", time.Now()))

	counts, err := store.ExecutionPlaneCounts(ctx, "inst-a")
	require.NoError(t, err)
	require.Equal(t, service.ImageTaskExecutionPlaneCounts{CallbackDue: 3, CallbackDead: 1, Inflight: 2}, counts)

	// Execution plane off: no instance, in-flight is not read.
	counts, err = store.ExecutionPlaneCounts(ctx, "")
	require.NoError(t, err)
	require.Equal(t, service.ImageTaskExecutionPlaneCounts{CallbackDue: 3, CallbackDead: 1}, counts)

	// Empty keys are zero, not errors.
	mr.FlushAll()
	counts, err = store.ExecutionPlaneCounts(ctx, "inst-a")
	require.NoError(t, err)
	require.Equal(t, service.ImageTaskExecutionPlaneCounts{}, counts)
}

func TestExecutionPlaneCountsReturnsRedisErrors(t *testing.T) {
	mr, rdb, store := newCallbackTestStore(t)
	ctx := context.Background()
	// Wrong type on a counted key is a real Redis error, not a zero.
	require.NoError(t, rdb.Set(ctx, imageTaskCallbackDueKey, "x", 0).Err())
	_, err := store.ExecutionPlaneCounts(ctx, "inst-a")
	require.Error(t, err)

	mr.Close()
	_, err = store.ExecutionPlaneCounts(ctx, "inst-a")
	require.Error(t, err)
}

func TestExecutionPlaneStatusThroughServiceUsesTheExecutionPlaneInstance(t *testing.T) {
	_, rdb, store := newCallbackTestStore(t)
	ctx := context.Background()
	require.NoError(t, store.AddInflight(ctx, recoveryTestInstance, "task_1", time.Now()))
	require.NoError(t, rdb.ZAdd(ctx, imageTaskCallbackDueKey, redis.Z{Score: 1, Member: "evt_a"}).Err())

	off := service.NewImageTaskService(store)
	counts, err := off.ExecutionPlaneStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, service.ImageTaskExecutionPlaneCounts{CallbackDue: 1}, counts)

	on := newRecoveryService(t, store, "rstat")
	counts, err = on.ExecutionPlaneStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, service.ImageTaskExecutionPlaneCounts{CallbackDue: 1, Inflight: 1}, counts)
}
