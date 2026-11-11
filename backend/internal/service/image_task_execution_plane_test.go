package service

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestImageTaskExecutionPlaneSafeModeFromEnv(t *testing.T) {
	t.Setenv(ImageTaskExecutionPlaneSafeModeEnv, "")
	enabled, err := ImageTaskExecutionPlaneSafeModeFromEnv()
	require.NoError(t, err)
	require.False(t, enabled, "default must be off")

	t.Setenv(ImageTaskExecutionPlaneSafeModeEnv, "true")
	enabled, err = ImageTaskExecutionPlaneSafeModeFromEnv()
	require.NoError(t, err)
	require.True(t, enabled)

	t.Setenv(ImageTaskExecutionPlaneSafeModeEnv, "yes-please")
	enabled, err = ImageTaskExecutionPlaneSafeModeFromEnv()
	require.Error(t, err)
	require.False(t, enabled)
}

func TestExecutionPlaneTrackerNilIsInert(t *testing.T) {
	var tracker *ExecutionPlaneTracker
	tracker.BeginAttempt()
	tracker.MarkUpstreamDispatching()
	tracker.ObserveUpstreamResult(&http.Response{StatusCode: http.StatusInternalServerError}, nil)
	tracker.MarkNotSent(ImageTaskReasonNoAccount)
	tracker.MarkSucceeded()
	require.True(t, tracker.AllowsAccountSwitch())
	require.True(t, tracker.AllowsSameAccountRetry())
	require.False(t, tracker.LastAttemptNotSent())
	require.Nil(t, ExecutionForFinishedTask(tracker, true, false, ""))
	require.Nil(t, ExecutionPlaneTrackerFromContext(context.Background()))
}

func TestExecutionPlaneTrackerFromContext(t *testing.T) {
	tracker := NewExecutionPlaneTracker()
	ctx := WithExecutionPlaneTracker(context.Background(), tracker)
	require.Same(t, tracker, ExecutionPlaneTrackerFromContext(ctx))
}

func dialError() error {
	return &url.Error{Op: "Post", URL: "https://upstream.test", Err: &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}}
}

func TestExecutionPlaneTrackerClassification(t *testing.T) {
	type call struct {
		status int
		err    error
	}
	cases := []struct {
		name          string
		calls         []call
		succeeded     bool
		notSent       string
		wantOutcome   string
		wantReason    string
		wantSwitch    bool
		wantNotSent   bool
		wantDispatch  bool
		wantLastState int
	}{
		{name: "no call", wantOutcome: ImageTaskOutcomeNotSent, wantReason: ImageTaskReasonNotDispatched, wantSwitch: true},
		{name: "no account", notSent: ImageTaskReasonNoAccount, wantOutcome: ImageTaskOutcomeNotSent, wantReason: ImageTaskReasonNoAccount, wantSwitch: true},
		{name: "dial failure", calls: []call{{err: dialError()}}, wantOutcome: ImageTaskOutcomeNotSent, wantReason: ImageTaskReasonConnectFailed, wantSwitch: true, wantNotSent: true},
		{name: "dns failure", calls: []call{{err: &url.Error{Op: "Post", URL: "https://x", Err: &net.DNSError{Err: "no such host", Name: "x"}}}}, wantOutcome: ImageTaskOutcomeNotSent, wantReason: ImageTaskReasonConnectFailed, wantSwitch: true, wantNotSent: true},
		{name: "proxy connect failure", calls: []call{{err: &net.OpError{Op: "proxyconnect", Net: "tcp", Err: errors.New("refused")}}}, wantOutcome: ImageTaskOutcomeNotSent, wantReason: ImageTaskReasonConnectFailed, wantSwitch: true, wantNotSent: true},
		{name: "read timeout", calls: []call{{err: &url.Error{Op: "Post", URL: "https://x", Err: context.DeadlineExceeded}}}, wantOutcome: ImageTaskOutcomeUncertain, wantReason: ImageTaskReasonTransportError, wantDispatch: true},
		{name: "connection reset", calls: []call{{err: &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")}}}, wantOutcome: ImageTaskOutcomeUncertain, wantReason: ImageTaskReasonTransportError, wantDispatch: true},
		{name: "opaque error", calls: []call{{err: errors.New("upstream request failed")}}, wantOutcome: ImageTaskOutcomeUncertain, wantReason: ImageTaskReasonTransportError, wantDispatch: true},
		{name: "500", calls: []call{{status: 500}}, wantOutcome: ImageTaskOutcomeUncertain, wantReason: ImageTaskReasonUpstream5xx, wantDispatch: true, wantLastState: 500},
		{name: "503", calls: []call{{status: 503}}, wantOutcome: ImageTaskOutcomeUncertain, wantReason: ImageTaskReasonUpstream5xx, wantDispatch: true, wantLastState: 503},
		{name: "529", calls: []call{{status: 529}}, wantOutcome: ImageTaskOutcomeRejected, wantReason: ImageTaskReasonUpstreamRejected, wantSwitch: true, wantDispatch: true, wantLastState: 529},
		{name: "429", calls: []call{{status: 429}}, wantOutcome: ImageTaskOutcomeRejected, wantReason: ImageTaskReasonUpstreamRejected, wantSwitch: true, wantDispatch: true, wantLastState: 429},
		{name: "401 then 403", calls: []call{{status: 401}, {status: 403}}, wantOutcome: ImageTaskOutcomeRejected, wantReason: ImageTaskReasonUpstreamRejected, wantSwitch: true, wantDispatch: true, wantLastState: 403},
		{name: "200 unconfirmed", calls: []call{{status: 200}}, wantOutcome: ImageTaskOutcomeUncertain, wantReason: ImageTaskReasonResponseUnconfirmed, wantDispatch: true, wantLastState: 200},
		{name: "200 succeeded", calls: []call{{status: 200}}, succeeded: true, wantOutcome: ImageTaskOutcomeSucceeded, wantDispatch: true, wantLastState: 200},
		{name: "429 then 500", calls: []call{{status: 429}, {status: 500}}, wantOutcome: ImageTaskOutcomeUncertain, wantReason: ImageTaskReasonUpstream5xx, wantDispatch: true, wantLastState: 500},
		{name: "dial then 429", calls: []call{{err: dialError()}, {status: 429}}, wantOutcome: ImageTaskOutcomeRejected, wantReason: ImageTaskReasonUpstreamRejected, wantSwitch: true, wantDispatch: true, wantLastState: 429},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tracker := NewExecutionPlaneTracker()
			if tc.notSent != "" {
				tracker.MarkNotSent(tc.notSent)
			}
			for _, c := range tc.calls {
				tracker.BeginAttempt()
				tracker.MarkUpstreamDispatching()
				var resp *http.Response
				if c.err == nil {
					resp = &http.Response{StatusCode: c.status}
				}
				tracker.ObserveUpstreamResult(resp, c.err)
			}
			if tc.succeeded {
				tracker.MarkSucceeded()
			}
			snap := tracker.Snapshot()
			require.Equal(t, tc.wantOutcome, snap.Outcome)
			require.Equal(t, tc.wantReason, snap.Reason)
			require.Equal(t, tc.wantDispatch, snap.Dispatched)
			require.Equal(t, len(tc.calls), snap.UpstreamCalls)
			require.Equal(t, tc.wantLastState, snap.LastUpstreamStatus)
			require.Equal(t, tc.wantSwitch, tracker.AllowsAccountSwitch())
			require.Equal(t, tc.wantNotSent, tracker.LastAttemptNotSent())
			require.False(t, tracker.AllowsSameAccountRetry())
		})
	}
}

func TestExecutionPlaneTrackerCallInFlightIsUncertain(t *testing.T) {
	tracker := NewExecutionPlaneTracker()
	tracker.MarkUpstreamDispatching()
	snap := tracker.Snapshot()
	require.Equal(t, ImageTaskOutcomeUncertain, snap.Outcome)
	require.Equal(t, ImageTaskReasonInFlight, snap.Reason)
	require.True(t, snap.Dispatched)
	require.False(t, tracker.AllowsAccountSwitch())
}

func TestExecutionForFinishedTask(t *testing.T) {
	succeeded := NewExecutionPlaneTracker()
	succeeded.MarkUpstreamDispatching()
	succeeded.ObserveUpstreamResult(&http.Response{StatusCode: 200}, nil)
	succeeded.MarkSucceeded()

	require.Equal(t, ImageTaskOutcomeSucceeded, ExecutionForFinishedTask(succeeded, true, false, "").Outcome)
	notDelivered := ExecutionForFinishedTask(succeeded, false, false, "")
	require.Equal(t, ImageTaskOutcomeUncertain, notDelivered.Outcome)
	require.Equal(t, ImageTaskReasonResponseNotDelivered, notDelivered.Reason)

	unconfirmed := NewExecutionPlaneTracker()
	unconfirmed.MarkUpstreamDispatching()
	unconfirmed.ObserveUpstreamResult(&http.Response{StatusCode: 200}, nil)
	delivered := ExecutionForFinishedTask(unconfirmed, true, false, "")
	require.Equal(t, ImageTaskOutcomeUncertain, delivered.Outcome)
	require.Equal(t, ImageTaskReasonResponseUnconfirmed, delivered.Reason)
	timedOut := ExecutionForFinishedTask(unconfirmed, false, true, "")
	require.Equal(t, ImageTaskOutcomeUncertain, timedOut.Outcome)
	require.Equal(t, ImageTaskReasonTimeout, timedOut.Reason)
	panicked := ExecutionForFinishedTask(unconfirmed, false, false, ImageTaskReasonPanic)
	require.Equal(t, ImageTaskReasonPanic, panicked.Reason)

	idle := NewExecutionPlaneTracker()
	notSentTimeout := ExecutionForFinishedTask(idle, false, true, "")
	require.Equal(t, ImageTaskOutcomeNotSent, notSentTimeout.Outcome)
	require.Equal(t, ImageTaskReasonTimeout, notSentTimeout.Reason)
	notSentPanic := ExecutionForFinishedTask(idle, false, false, ImageTaskReasonPanic)
	require.Equal(t, ImageTaskOutcomeNotSent, notSentPanic.Outcome, "panic before dispatch stays not_sent")
}

type executionPlaneMemoryStore struct {
	tasks map[string]*ImageTaskRecord
}

func (s *executionPlaneMemoryStore) Save(_ context.Context, task *ImageTaskRecord, _ time.Duration) error {
	copy := *task
	s.tasks[task.ID] = &copy
	return nil
}

func (s *executionPlaneMemoryStore) Get(_ context.Context, id string) (*ImageTaskRecord, error) {
	task, ok := s.tasks[id]
	if !ok {
		return nil, ErrImageTaskNotFound
	}
	copy := *task
	return &copy, nil
}

type failingImageStorage struct{}

func (failingImageStorage) Save(context.Context, string, string, []byte) (string, error) {
	return "", errors.New("bucket unavailable")
}

func TestImageTaskServiceStoresExecution(t *testing.T) {
	store := &executionPlaneMemoryStore{tasks: map[string]*ImageTaskRecord{}}
	svc := NewImageTaskServiceWithUploader(store, nil, time.Hour, time.Minute)
	owner := ImageTaskOwner{UserID: 1, APIKeyID: 2}

	task, err := svc.Create(context.Background(), owner)
	require.NoError(t, err)
	exec := &ImageTaskExecution{Outcome: ImageTaskOutcomeSucceeded, Dispatched: true, UpstreamCalls: 1, LastUpstreamStatus: 200}
	require.NoError(t, svc.CompleteWithExecution(context.Background(), task.ID, 200, json.RawMessage(`{"data":[]}`), exec))
	got, err := svc.Get(context.Background(), owner, task.ID)
	require.NoError(t, err)
	require.Equal(t, ImageTaskStatusCompleted, got.Status)
	require.Equal(t, exec, got.Execution)

	// Safe mode off: no execution field, JSON unchanged.
	plain, err := svc.Create(context.Background(), owner)
	require.NoError(t, err)
	require.NoError(t, svc.Complete(context.Background(), plain.ID, 200, json.RawMessage(`{"data":[]}`)))
	gotPlain, err := svc.Get(context.Background(), owner, plain.ID)
	require.NoError(t, err)
	require.Nil(t, gotPlain.Execution)
	raw, err := json.Marshal(gotPlain)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "execution")
}

func TestImageTaskServiceStorageFailureIsUncertain(t *testing.T) {
	store := &executionPlaneMemoryStore{tasks: map[string]*ImageTaskRecord{}}
	uploader := NewImageResultUploader(failingImageStorage{}, "generated/", 0, nil)
	svc := NewImageTaskServiceWithUploader(store, uploader, time.Hour, time.Minute)
	owner := ImageTaskOwner{UserID: 1, APIKeyID: 2}
	task, err := svc.Create(context.Background(), owner)
	require.NoError(t, err)

	exec := &ImageTaskExecution{Outcome: ImageTaskOutcomeSucceeded, Dispatched: true, UpstreamCalls: 1, LastUpstreamStatus: 200}
	result := json.RawMessage(`{"data":[{"b64_json":"iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="}]}`)
	require.NoError(t, svc.CompleteWithExecution(context.Background(), task.ID, 200, result, exec))
	got, err := svc.Get(context.Background(), owner, task.ID)
	require.NoError(t, err)
	require.Equal(t, ImageTaskStatusFailed, got.Status)
	require.Equal(t, ImageTaskOutcomeUncertain, got.Execution.Outcome)
	require.Equal(t, ImageTaskReasonStorageFailed, got.Execution.Reason)
	require.Equal(t, ImageTaskOutcomeSucceeded, exec.Outcome, "caller's record is not mutated")
}
