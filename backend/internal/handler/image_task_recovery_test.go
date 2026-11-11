package handler

// YeaGrow T2.3: dispatch persistence and shutdown drain on the async Images path.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

type recordingRecoveryStore struct {
	mu            sync.Mutex
	upstream      *execPlaneUpstream
	inflight      map[string]bool
	dispatches    []string
	callsAtMark   []int
	dispatchErr   error
	markedRecords map[string]bool
}

func newRecordingRecoveryStore(upstream *execPlaneUpstream) *recordingRecoveryStore {
	return &recordingRecoveryStore{upstream: upstream, inflight: map[string]bool{}, markedRecords: map[string]bool{}}
}

func (s *recordingRecoveryStore) snapshot() (inflight int, dispatches []string, callsAtMark []int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.inflight), append([]string(nil), s.dispatches...), append([]int(nil), s.callsAtMark...)
}

func (s *recordingRecoveryStore) AddInflight(_ context.Context, _ string, taskID string, _ time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inflight[taskID] = true
	return nil
}
func (s *recordingRecoveryStore) RemoveInflight(_ context.Context, _ string, taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.inflight, taskID)
	return nil
}
func (s *recordingRecoveryStore) ListInflight(context.Context, string) ([]string, error) {
	return nil, nil
}
func (s *recordingRecoveryStore) MarkDispatched(_ context.Context, taskID string, _ time.Time) error {
	calls := len(s.upstream.callLog())
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dispatches = append(s.dispatches, taskID)
	s.callsAtMark = append(s.callsAtMark, calls)
	if s.dispatchErr != nil {
		return s.dispatchErr
	}
	s.markedRecords[taskID] = true
	return nil
}
func (s *recordingRecoveryStore) MarkStoring(context.Context, string, time.Time) error {
	return nil
}
func (s *recordingRecoveryStore) AcquireInstanceLease(context.Context, string, string, time.Duration) (bool, error) {
	return true, nil
}
func (s *recordingRecoveryStore) RefreshInstanceLease(context.Context, string, string, time.Duration) (bool, error) {
	return true, nil
}
func (s *recordingRecoveryStore) ReleaseInstanceLease(context.Context, string, string) error {
	return nil
}
func (s *recordingRecoveryStore) RegisterProcessPrefix(context.Context, string, string) ([]string, error) {
	return nil, nil
}
func (s *recordingRecoveryStore) ForgetProcessPrefixes(context.Context, string, []string) error {
	return nil
}
func (s *recordingRecoveryStore) CleanupSlotsForPrefixes(context.Context, []string) (int, error) {
	return 0, nil
}

func newRecoveryHarness(t *testing.T, steps map[int64][]execPlaneStep, accounts []service.Account) (*execPlaneHarness, *recordingRecoveryStore) {
	t.Helper()
	h := newExecPlaneHarness(t, true, accounts, steps, time.Minute)
	store := newRecordingRecoveryStore(h.upstream)
	require.NoError(t, h.tasks.EnableExecutionPlane(store, "inst-1", "rtest", 0))
	return h, store
}

func TestExecutionPlaneRecovery_DispatchPersistedBeforeFirstUpstreamCallOnly(t *testing.T) {
	accounts := []service.Account{execPlaneAPIKeyAccount(1, 0, nil), execPlaneAPIKeyAccount(2, 1, nil)}
	steps := map[int64][]execPlaneStep{
		1: {execPlaneJSON(http.StatusTooManyRequests, `{"error":{"message":"slow down","type":"rate_limit"}}`)},
		2: {execPlaneSuccess()},
	}
	h, store := newRecoveryHarness(t, steps, accounts)
	task := h.run()

	require.Equal(t, service.ImageTaskStatusCompleted, task.Status)
	require.Equal(t, []int64{1, 2}, h.upstream.callLog())
	_, dispatches, callsAtMark := store.snapshot()
	require.Len(t, dispatches, 1, "the marker is written once per task")
	require.Equal(t, []int{0}, callsAtMark, "the marker is written before the first upstream call")
	require.Eventually(t, func() bool { n, _, _ := store.snapshot(); return n == 0 }, time.Second, 5*time.Millisecond,
		"finish removes the in-flight entry")
}

func TestExecutionPlaneRecovery_DispatchMarkerFailureSendsNothing(t *testing.T) {
	accounts := []service.Account{execPlaneAPIKeyAccount(1, 0, nil)}
	steps := map[int64][]execPlaneStep{1: {execPlaneSuccess()}}
	h, store := newRecoveryHarness(t, steps, accounts)
	store.dispatchErr = errors.New("redis unavailable")
	task := h.run()

	require.Empty(t, h.upstream.callLog(), "no request may leave without a persisted dispatch marker")
	require.Equal(t, service.ImageTaskStatusFailed, task.Status)
	require.NotNil(t, task.Execution)
	require.Equal(t, service.ImageTaskOutcomeNotSent, task.Execution.Outcome)
	require.Equal(t, service.ImageTaskReasonDispatchNotRecorded, task.Execution.Reason)
	require.False(t, task.Execution.Dispatched)
}

func TestExecutionPlaneRecovery_DrainRefusesSubmitsAndWaitsForRunningTask(t *testing.T) {
	release := make(chan struct{})
	accounts := []service.Account{execPlaneAPIKeyAccount(1, 0, nil)}
	steps := map[int64][]execPlaneStep{1: {execPlaneBlockingSuccess(release)}}
	h, store := newRecoveryHarness(t, steps, accounts)

	w := h.submit()
	require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
	var accepted struct {
		TaskID string `json:"task_id"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &accepted))
	require.Eventually(t, func() bool { return len(h.upstream.callLog()) == 1 }, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, 1, h.tasks.RunningTasks())

	drained := make(chan int, 1)
	go func() { drained <- h.tasks.Drain(5 * time.Second) }()
	require.Eventually(t, func() bool {
		releaseProbe, accepting := h.tasks.BeginRun()
		if accepting {
			releaseProbe()
		}
		return !accepting
	}, time.Second, 5*time.Millisecond)
	refused := h.submit()
	require.Equal(t, http.StatusServiceUnavailable, refused.Code, refused.Body.String())
	require.NotEmpty(t, refused.Header().Get("Retry-After"))
	n, _, _ := store.snapshot()
	require.Equal(t, 1, n, "a refused submit must not create a task")

	close(release)
	select {
	case remaining := <-drained:
		require.Zero(t, remaining)
	case <-time.After(3 * time.Second):
		t.Fatal("drain did not finish after the running task completed")
	}
	got, err := h.tasks.Get(context.Background(), service.ImageTaskOwner{UserID: 7, APIKeyID: 9}, accepted.TaskID)
	require.NoError(t, err)
	require.Equal(t, service.ImageTaskStatusCompleted, got.Status)
}

// execPlaneBlockingSuccess holds the upstream response until release is closed.
func execPlaneBlockingSuccess(release <-chan struct{}) execPlaneStep {
	ok := execPlaneSuccess()
	return func(req *http.Request) (*http.Response, error) {
		<-release
		return ok(req)
	}
}
