package repository

// YeaGrow T2.3 restart recovery against the real Redis scripts (miniredis).

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

const recoveryTestInstance = "inst-1"

func newRecoveryService(t *testing.T, store *imageTaskStore, process string) *service.ImageTaskService {
	t.Helper()
	svc := service.NewImageTaskService(store)
	svc.SetCallbackConfig(callbackConfigFor(t, "http://127.0.0.1:1/cb"))
	require.NoError(t, svc.EnableExecutionPlane(store, recoveryTestInstance, process, 0))
	return svc
}

// createRunTask creates a task through the service (in-flight index + owner
// process) and gives it a run_id so a callback event can be built.
func createRunTask(t *testing.T, svc *service.ImageTaskService, store *imageTaskStore) string {
	t.Helper()
	ctx := context.Background()
	task, err := svc.Create(ctx, service.ImageTaskOwner{UserID: 7, APIKeyID: 9})
	require.NoError(t, err)
	rec, err := store.Get(ctx, task.ID)
	require.NoError(t, err)
	rec.RunID = "run_" + task.ID
	require.NoError(t, store.Save(ctx, rec, time.Hour))
	return task.ID
}

func inflight(t *testing.T, rdb *redis.Client) []string {
	t.Helper()
	ids, err := rdb.ZRange(context.Background(), imageTaskInflightKey(recoveryTestInstance), 0, -1).Result()
	require.NoError(t, err)
	return ids
}

func dueEvents(t *testing.T, rdb *redis.Client) []string {
	t.Helper()
	ids, err := rdb.ZRange(context.Background(), imageTaskCallbackDueKey, 0, -1).Result()
	require.NoError(t, err)
	return ids
}

func TestImageTaskRecoveryClassifiesDispatchedAndQueuedTasks(t *testing.T) {
	_, rdb, store := newCallbackTestStore(t)
	ctx := context.Background()
	old := newRecoveryService(t, store, "raaa")

	dispatched := createRunTask(t, old, store)
	queued := createRunTask(t, old, store)
	require.NoError(t, store.MarkDispatched(ctx, dispatched, time.Now()))
	rec, err := store.Get(ctx, dispatched)
	require.NoError(t, err)
	require.NotNil(t, rec.DispatchedAt)
	require.Equal(t, "raaa", rec.OwnerProcess)
	require.Equal(t, recoveryTestInstance, rec.InstanceID)
	require.ElementsMatch(t, []string{dispatched, queued}, inflight(t, rdb))

	// The record of a task whose index entry was written but whose record never was.
	require.NoError(t, store.AddInflight(ctx, recoveryTestInstance, "imgtask_never_saved", time.Now()))

	// New process of the same instance; it already has a live task of its own.
	fresh := newRecoveryService(t, store, "rbbb")
	live := createRunTask(t, fresh, store)

	stats, err := fresh.RecoverOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, stats.Uncertain)
	require.Equal(t, 1, stats.NotSent)
	require.Equal(t, 1, stats.SkippedLive)
	require.Equal(t, 1, stats.Missing)

	got, err := store.Get(ctx, dispatched)
	require.NoError(t, err)
	require.Equal(t, service.ImageTaskStatusFailed, got.Status)
	require.Equal(t, 502, got.HTTPStatus)
	require.Equal(t, service.ImageTaskOutcomeUncertain, got.Execution.Outcome)
	require.Equal(t, service.ImageTaskReasonInstanceRestarted, got.Execution.Reason)
	require.True(t, got.Execution.Dispatched)

	got, err = store.Get(ctx, queued)
	require.NoError(t, err)
	require.Equal(t, service.ImageTaskStatusFailed, got.Status)
	require.Equal(t, 503, got.HTTPStatus)
	require.Equal(t, service.ImageTaskOutcomeNotSent, got.Execution.Outcome)
	require.False(t, got.Execution.Dispatched)

	got, err = store.Get(ctx, live)
	require.NoError(t, err)
	require.Equal(t, service.ImageTaskStatusProcessing, got.Status, "a task of the current process must not be recovered")

	require.Equal(t, []string{live}, inflight(t, rdb))
	require.ElementsMatch(t, []string{
		service.ImageTaskCallbackEventID(dispatched), service.ImageTaskCallbackEventID(queued),
	}, dueEvents(t, rdb), "exactly one callback event per recovered task")

	var event service.ImageTaskCallbackEvent
	body, err := rdb.HGet(ctx, imageTaskCallbackEventKey(service.ImageTaskCallbackEventID(queued)), "body").Result()
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal([]byte(body), &event))
	require.Equal(t, service.ImageTaskOutcomeNotSent, event.Status)
	require.Equal(t, "run_"+queued, event.RunID)

	// A second pass is a no-op (no second terminal state, no second event).
	stats, err = fresh.RecoverOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, stats.Uncertain+stats.NotSent)
	require.Len(t, dueEvents(t, rdb), 2)
}

func TestImageTaskLateCompletionAfterRecoveryIsRejected(t *testing.T) {
	_, rdb, store := newCallbackTestStore(t)
	ctx := context.Background()
	old := newRecoveryService(t, store, "raaa")
	id := createRunTask(t, old, store)
	require.NoError(t, store.MarkDispatched(ctx, id, time.Now()))

	fresh := newRecoveryService(t, store, "rbbb")
	_, err := fresh.RecoverOnce(ctx)
	require.NoError(t, err)

	result := json.RawMessage(`{"created":1,"data":[{"b64_json":"AA=="}]}`)
	err = old.CompleteWithExecution(ctx, id, 200, result, &service.ImageTaskExecution{
		Outcome: service.ImageTaskOutcomeSucceeded, Dispatched: true, UpstreamCalls: 1, LastUpstreamStatus: 200,
	})
	require.ErrorIs(t, err, service.ErrImageTaskAlreadyFinished)

	got, err := store.Get(ctx, id)
	require.NoError(t, err)
	require.Equal(t, service.ImageTaskStatusFailed, got.Status)
	require.Equal(t, service.ImageTaskOutcomeUncertain, got.Execution.Outcome)
	require.Len(t, dueEvents(t, rdb), 1)

	// A late dispatch marker for a finished task is refused as well.
	require.ErrorIs(t, store.MarkDispatched(ctx, id, time.Now()), service.ErrImageTaskDispatchRejected)
	require.ErrorIs(t, store.MarkDispatched(ctx, "imgtask_missing", time.Now()), service.ErrImageTaskNotFound)
}

func TestImageTaskFinishRemovesInflightEntry(t *testing.T) {
	_, rdb, store := newCallbackTestStore(t)
	ctx := context.Background()
	svc := newRecoveryService(t, store, "raaa")
	id := createRunTask(t, svc, store)
	require.Equal(t, []string{id}, inflight(t, rdb))
	require.NoError(t, svc.FailWithExecution(ctx, id, 503, json.RawMessage(`{"type":"api_error","message":"x"}`),
		&service.ImageTaskExecution{Outcome: service.ImageTaskOutcomeNotSent, Reason: service.ImageTaskReasonNoAccount}))
	require.Empty(t, inflight(t, rdb))
}

func TestImageTaskRecoveryWaitsForInstanceLease(t *testing.T) {
	_, _, store := newCallbackTestStore(t)
	ctx := context.Background()
	first := newRecoveryService(t, store, "raaa")
	_, err := first.RecoverOnce(ctx)
	require.NoError(t, err)

	second := newRecoveryService(t, store, "rbbb")
	_, err = second.RecoverOnce(ctx)
	require.ErrorIs(t, err, service.ErrImageTaskInstanceLeaseHeld)

	require.Equal(t, 0, first.Drain(0)) // graceful shutdown releases the lease
	_, err = second.RecoverOnce(ctx)
	require.NoError(t, err)
}

func TestImageTaskRecoveryLeaseExpiresAfterCrash(t *testing.T) {
	mr, _, store := newCallbackTestStore(t)
	ctx := context.Background()
	crashed := newRecoveryService(t, store, "raaa")
	_, err := crashed.RecoverOnce(ctx)
	require.NoError(t, err)

	next := newRecoveryService(t, store, "rbbb")
	_, err = next.RecoverOnce(ctx)
	require.ErrorIs(t, err, service.ErrImageTaskInstanceLeaseHeld)
	mr.FastForward(31 * time.Second)
	_, err = next.RecoverOnce(ctx)
	require.NoError(t, err)
}

func seedZSet(t *testing.T, mr *miniredis.Miniredis, key string, members ...string) {
	t.Helper()
	for i, m := range members {
		_, err := mr.ZAdd(key, float64(i+1), m)
		require.NoError(t, err)
	}
}

func TestImageTaskRecoveryCleansOnlyThisInstancesSlots(t *testing.T) {
	mr, _, store := newCallbackTestStore(t)
	ctx := context.Background()

	previous := newRecoveryService(t, store, "raaa")
	_, err := previous.RecoverOnce(ctx) // records prefix "raaa" for the instance
	require.NoError(t, err)
	// "raaa" process dies without draining; its lease expires.
	mr.FastForward(31 * time.Second)

	seedZSet(t, mr, "concurrency:account:1", "raaa-1", "rzzz-1", "rbbb-1")
	seedZSet(t, mr, "concurrency:user:7", "raaa-2", "raaa-3")
	seedZSet(t, mr, "concurrency:api_key:9", "raaa-4", "rzzz-2")
	seedZSet(t, mr, "concurrency:live:account:1", "raaa-5")
	seedZSet(t, mr, "concurrency:account:active_index", "raaa-6")
	seedZSet(t, mr, "concurrency:account:12x", "raaa-7")
	seedZSet(t, mr, "concurrency:account:2", "raaab-1") // different prefix that merely starts with "raaa"
	require.NoError(t, mr.Set("wait:account:1", "3"))

	fresh := newRecoveryService(t, store, "rbbb")
	stats, err := fresh.RecoverOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 5, stats.SlotsRemoved)

	members := func(key string) []string {
		m, err := mr.ZMembers(key)
		if err != nil {
			return nil
		}
		return m
	}
	require.ElementsMatch(t, []string{"rzzz-1", "rbbb-1"}, members("concurrency:account:1"))
	require.Empty(t, members("concurrency:user:7"))
	require.ElementsMatch(t, []string{"rzzz-2"}, members("concurrency:api_key:9"))
	require.Empty(t, members("concurrency:live:account:1"))
	require.Equal(t, []string{"raaa-6"}, members("concurrency:account:active_index"))
	require.Equal(t, []string{"raaa-7"}, members("concurrency:account:12x"))
	require.Equal(t, []string{"raaab-1"}, members("concurrency:account:2"))
	v, err := mr.Get("wait:account:1")
	require.NoError(t, err)
	require.Equal(t, "3", v)

	// The previous prefix is forgotten after a successful cleanup.
	prefixes, err := mr.Members(imageTaskInstancePrefixesKey(recoveryTestInstance))
	require.NoError(t, err)
	require.Equal(t, []string{"rbbb"}, prefixes)
}

func TestImageTaskSlotCleanupRejectsMalformedPrefixes(t *testing.T) {
	mr, _, store := newCallbackTestStore(t)
	seedZSet(t, mr, "concurrency:account:1", "x-1", "-1", "r-1")
	n, err := store.CleanupSlotsForPrefixes(context.Background(), []string{"", "x", "r", "R1"})
	require.NoError(t, err)
	require.Zero(t, n)
	m, err := mr.ZMembers("concurrency:account:1")
	require.NoError(t, err)
	require.Len(t, m, 3)
}

func TestImageTaskRecoveryTaskInterruptedWhileStoringIsUncertain(t *testing.T) {
	_, rdb, store := newCallbackTestStore(t)
	ctx := context.Background()
	old := newRecoveryService(t, store, "raaa")
	storing := createRunTask(t, old, store)
	require.NoError(t, store.MarkDispatched(ctx, storing, time.Now()))
	require.NoError(t, store.MarkStoring(ctx, storing, time.Now()))
	first, err := store.Get(ctx, storing)
	require.NoError(t, err)
	require.NoError(t, store.MarkStoring(ctx, storing, time.Now().Add(time.Hour)))
	again, err := store.Get(ctx, storing)
	require.NoError(t, err)
	require.Equal(t, *first.StoringAt, *again.StoringAt, "an existing stage timestamp is kept")

	fresh := newRecoveryService(t, store, "rbbb")
	stats, err := fresh.RecoverOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, stats.Uncertain)

	got, err := store.Get(ctx, storing)
	require.NoError(t, err)
	require.Equal(t, service.ImageTaskStatusFailed, got.Status)
	require.Equal(t, service.ImageTaskOutcomeUncertain, got.Execution.Outcome)
	require.Equal(t, service.ImageTaskReasonStorageInterrupted, got.Execution.Reason)
	require.True(t, got.Execution.Dispatched)
	require.Equal(t, []string{service.ImageTaskCallbackEventID(storing)}, dueEvents(t, rdb))
	require.ErrorIs(t, store.MarkStoring(ctx, storing, time.Now()), service.ErrImageTaskDispatchRejected)
}
