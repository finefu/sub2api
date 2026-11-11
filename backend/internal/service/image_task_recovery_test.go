package service

// YeaGrow T2.3: drain, dispatch persistence and configuration.

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type nopRecoveryStore struct{}

func (nopRecoveryStore) AddInflight(context.Context, string, string, time.Time) error { return nil }
func (nopRecoveryStore) RemoveInflight(context.Context, string, string) error         { return nil }
func (nopRecoveryStore) ListInflight(context.Context, string) ([]string, error)       { return nil, nil }
func (nopRecoveryStore) MarkDispatched(context.Context, string, time.Time) error      { return nil }
func (nopRecoveryStore) MarkStoring(context.Context, string, time.Time) error         { return nil }
func (nopRecoveryStore) AcquireInstanceLease(context.Context, string, string, time.Duration) (bool, error) {
	return true, nil
}
func (nopRecoveryStore) RefreshInstanceLease(context.Context, string, string, time.Duration) (bool, error) {
	return true, nil
}
func (nopRecoveryStore) ReleaseInstanceLease(context.Context, string, string) error { return nil }
func (nopRecoveryStore) RegisterProcessPrefix(context.Context, string, string) ([]string, error) {
	return nil, nil
}
func (nopRecoveryStore) ForgetProcessPrefixes(context.Context, string, []string) error { return nil }
func (nopRecoveryStore) CleanupSlotsForPrefixes(context.Context, []string) (int, error) {
	return 0, nil
}

func planeService(t *testing.T) *ImageTaskService {
	t.Helper()
	svc := NewImageTaskService(nil)
	require.NoError(t, svc.EnableExecutionPlane(nopRecoveryStore{}, "inst-1", "rtest", time.Second))
	return svc
}

func TestImageTaskBeginRunWithoutPlaneAlwaysAccepts(t *testing.T) {
	svc := NewImageTaskService(nil)
	release, ok := svc.BeginRun()
	require.True(t, ok)
	release()
	require.Zero(t, svc.Drain(time.Second))
	_, ok = svc.BeginRun()
	require.True(t, ok, "drain is a no-op when the execution plane is off")
}

func TestImageTaskDrainRefusesNewRunsAndWaitsForRunning(t *testing.T) {
	svc := planeService(t)
	release, ok := svc.BeginRun()
	require.True(t, ok)
	require.Equal(t, 1, svc.RunningTasks())

	done := make(chan int)
	go func() { done <- svc.Drain(5 * time.Second) }()
	require.Eventually(t, func() bool {
		probe, ok := svc.BeginRun()
		if ok {
			probe()
		}
		return !ok
	}, time.Second, 5*time.Millisecond, "new runs are refused while draining")

	time.Sleep(50 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("drain returned while a task was running")
	default:
	}
	release()
	release() // idempotent
	select {
	case remaining := <-done:
		require.Zero(t, remaining)
	case <-time.After(2 * time.Second):
		t.Fatal("drain did not return after the last task finished")
	}
}

func TestImageTaskDrainTimesOutAndReportsRemaining(t *testing.T) {
	svc := planeService(t)
	_, ok := svc.BeginRun()
	require.True(t, ok)
	_, ok = svc.BeginRun()
	require.True(t, ok)
	start := time.Now()
	require.Equal(t, 2, svc.Drain(80*time.Millisecond))
	require.GreaterOrEqual(t, time.Since(start), 80*time.Millisecond)
}

func TestExecutionPlaneTrackerRecordsDispatchOnce(t *testing.T) {
	tracker := NewExecutionPlaneTracker()
	require.NoError(t, tracker.RecordDispatch(context.Background()), "no recorder installed")
	var calls atomic.Int32
	tracker.SetDispatchRecorder(func(context.Context) error { calls.Add(1); return nil })
	require.NoError(t, tracker.RecordDispatch(context.Background()))
	require.NoError(t, tracker.RecordDispatch(context.Background()))
	require.EqualValues(t, 1, calls.Load())

	var nilTracker *ExecutionPlaneTracker
	require.NoError(t, nilTracker.RecordDispatch(context.Background()))
}

func TestExecutionPlaneDispatchNotRecordedIsNotSent(t *testing.T) {
	tracker := NewExecutionPlaneTracker()
	fail := errors.New("redis down")
	var calls atomic.Int32
	tracker.SetDispatchRecorder(func(context.Context) error { calls.Add(1); return fail })

	svc := &OpenAIGatewayService{}
	req, err := http.NewRequestWithContext(WithExecutionPlaneTracker(context.Background(), tracker), http.MethodPost, "http://127.0.0.1:1/v1/images/generations", nil)
	require.NoError(t, err)
	resp, err := svc.doOpenAIUpstream(req, "", &Account{ID: 1})
	require.Nil(t, resp)
	require.ErrorIs(t, err, fail)
	require.True(t, IsUpstreamPreSendError(err))
	require.EqualValues(t, 1, calls.Load())

	snap := tracker.Snapshot()
	require.Equal(t, ImageTaskOutcomeNotSent, snap.Outcome)
	require.Equal(t, ImageTaskReasonDispatchNotRecorded, snap.Reason)
	require.False(t, snap.Dispatched)
	require.True(t, tracker.LastAttemptNotSent())
	require.True(t, tracker.AllowsAccountSwitch())
}

func TestImageTaskExecutionPlaneEnvParsing(t *testing.T) {
	t.Setenv(ImageTaskInstanceIDEnv, "")
	id, err := ImageTaskInstanceIDFromEnv()
	require.NoError(t, err)
	require.Equal(t, DefaultImageTaskInstanceID, id)
	t.Setenv(ImageTaskInstanceIDEnv, "sg-1")
	id, err = ImageTaskInstanceIDFromEnv()
	require.NoError(t, err)
	require.Equal(t, "sg-1", id)
	t.Setenv(ImageTaskInstanceIDEnv, "bad id/with:chars")
	_, err = ImageTaskInstanceIDFromEnv()
	require.Error(t, err)

	t.Setenv(ImageTaskShutdownDrainTimeoutEnv, "")
	d, err := ImageTaskShutdownDrainTimeoutFromEnv()
	require.NoError(t, err)
	require.Equal(t, 120*time.Second, d)
	t.Setenv(ImageTaskShutdownDrainTimeoutEnv, "45s")
	d, err = ImageTaskShutdownDrainTimeoutFromEnv()
	require.NoError(t, err)
	require.Equal(t, 45*time.Second, d)
	t.Setenv(ImageTaskShutdownDrainTimeoutEnv, "0")
	d, err = ImageTaskShutdownDrainTimeoutFromEnv()
	require.NoError(t, err)
	require.Zero(t, d)
	t.Setenv(ImageTaskShutdownDrainTimeoutEnv, "soon")
	_, err = ImageTaskShutdownDrainTimeoutFromEnv()
	require.Error(t, err)

	t.Setenv(ImageTaskExecutionPlaneSafeModeEnv, "")
	require.False(t, ImageTaskInstanceScopedSlotCleanupEnabled())
	t.Setenv(ImageTaskExecutionPlaneSafeModeEnv, "true")
	require.True(t, ImageTaskInstanceScopedSlotCleanupEnabled())
}

func TestImageTaskEnableExecutionPlaneValidates(t *testing.T) {
	svc := NewImageTaskService(nil)
	require.Error(t, svc.EnableExecutionPlane(nil, "inst", "rabc", 0))
	require.Error(t, svc.EnableExecutionPlane(nopRecoveryStore{}, "", "rabc", 0))
	require.Error(t, svc.EnableExecutionPlane(nopRecoveryStore{}, "inst", "", 0))
	require.Error(t, svc.EnableExecutionPlane(nopRecoveryStore{}, "inst", "abc", 0))
	require.NoError(t, svc.EnableExecutionPlane(nopRecoveryStore{}, "inst", "rabc", 0))
	require.Equal(t, "inst", svc.ExecutionPlaneInstance())
}
