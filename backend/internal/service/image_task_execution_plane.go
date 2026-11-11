package service

// YeaGrow execution-plane safe mode for asynchronous image tasks.
// Origin: finefu/sub2api branch yeagrow/execution-plane (base Wei-Shaw/sub2api ee58c0013).
//
// When enabled, an async image task carries an ExecutionPlaneTracker in its
// request context. The tracker is marked explicitly around every upstream call
// (see doOpenAIUpstream) so the task can report whether the image may have been
// generated, and so the Images handler only switches accounts when the previous
// attempt provably produced nothing. The synchronous Images API and every other
// gateway path never carry a tracker and keep their existing behaviour.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
)

// ImageTaskExecutionPlaneSafeModeEnv toggles the safe mode. Default: off.
const ImageTaskExecutionPlaneSafeModeEnv = "IMAGE_TASK_EXECUTION_PLANE_SAFE_MODE"

// Execution outcomes reported on the task record.
const (
	// ImageTaskOutcomeNotSent: no request reached the upstream (no account,
	// capacity wait failed, connection could not be established).
	ImageTaskOutcomeNotSent = "not_sent"
	// ImageTaskOutcomeRejected: the upstream explicitly refused every attempt
	// before generating (4xx such as 401/402/403/405/429, or 529).
	ImageTaskOutcomeRejected = "rejected"
	// ImageTaskOutcomeUncertain: a request was sent and no confirmed result came
	// back (5xx, timeout, disconnect, unreadable body, storage failure).
	ImageTaskOutcomeUncertain = "uncertain"
	// ImageTaskOutcomeSucceeded: the gateway confirmed a successful upstream result.
	ImageTaskOutcomeSucceeded = "succeeded"
)

// Reasons attached to an outcome.
const (
	ImageTaskReasonNoAccount            = "no_account"
	ImageTaskReasonNotDispatched        = "not_dispatched"
	ImageTaskReasonConnectFailed        = "connect_failed"
	ImageTaskReasonUpstreamRejected     = "upstream_rejected"
	ImageTaskReasonUpstream5xx          = "upstream_5xx"
	ImageTaskReasonTransportError       = "transport_error"
	ImageTaskReasonInFlight             = "in_flight"
	ImageTaskReasonResponseUnconfirmed  = "response_unconfirmed"
	ImageTaskReasonResponseNotDelivered = "response_not_delivered"
	ImageTaskReasonInvalidResponse      = "invalid_response"
	ImageTaskReasonStorageFailed        = "storage_failed"
	ImageTaskReasonTimeout              = "timeout"
	ImageTaskReasonPanic                = "panic"
)

// ImageTaskExecution is the structured execution result stored on the task.
// It is only present for tasks executed in safe mode.
type ImageTaskExecution struct {
	Outcome            string `json:"outcome"`
	Reason             string `json:"reason,omitempty"`
	Dispatched         bool   `json:"dispatched"`
	UpstreamCalls      int    `json:"upstream_calls"`
	LastUpstreamStatus int    `json:"last_upstream_status,omitempty"`
}

// ImageTaskExecutionPlaneSafeModeFromEnv parses IMAGE_TASK_EXECUTION_PLANE_SAFE_MODE.
// Empty means off. An unparsable value returns an error and off.
func ImageTaskExecutionPlaneSafeModeFromEnv() (bool, error) {
	raw := strings.TrimSpace(os.Getenv(ImageTaskExecutionPlaneSafeModeEnv))
	if raw == "" {
		return false, nil
	}
	enabled, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("invalid %s=%q: %w", ImageTaskExecutionPlaneSafeModeEnv, raw, err)
	}
	return enabled, nil
}

// ExecutionPlaneTracker records explicit markers for one async image task.
// All methods are safe on a nil receiver and then report "no safe mode".
type ExecutionPlaneTracker struct {
	mu sync.Mutex

	calls      int  // upstream calls started (marked before doOpenAIUpstream)
	inFlight   int  // started but not yet observed
	dispatched bool // at least one call may have reached the upstream
	lastStatus int

	uncertain       bool
	uncertainReason string
	rejected        bool
	notSentReason   string
	succeeded       bool

	attemptCalls   int
	attemptPreSend int
}

func NewExecutionPlaneTracker() *ExecutionPlaneTracker {
	return &ExecutionPlaneTracker{}
}

type executionPlaneTrackerKey struct{}

// WithExecutionPlaneTracker attaches the tracker to ctx.
func WithExecutionPlaneTracker(ctx context.Context, tracker *ExecutionPlaneTracker) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, executionPlaneTrackerKey{}, tracker)
}

// ExecutionPlaneTrackerFromContext returns the tracker, or nil outside safe mode.
func ExecutionPlaneTrackerFromContext(ctx context.Context) *ExecutionPlaneTracker {
	if ctx == nil {
		return nil
	}
	tracker, _ := ctx.Value(executionPlaneTrackerKey{}).(*ExecutionPlaneTracker)
	return tracker
}

// BeginAttempt starts a new account attempt (one ForwardImages call).
func (t *ExecutionPlaneTracker) BeginAttempt() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.attemptCalls = 0
	t.attemptPreSend = 0
}

// MarkUpstreamDispatching is called immediately before an upstream request is
// handed to the transport.
func (t *ExecutionPlaneTracker) MarkUpstreamDispatching() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.calls++
	t.inFlight++
	t.attemptCalls++
}

// ObserveUpstreamResult is called right after the transport returns.
func (t *ExecutionPlaneTracker) ObserveUpstreamResult(resp *http.Response, err error) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.inFlight > 0 {
		t.inFlight--
	}
	if err != nil {
		if IsUpstreamPreSendError(err) {
			t.attemptPreSend++
			if t.notSentReason == "" {
				t.notSentReason = ImageTaskReasonConnectFailed
			}
			return
		}
		t.dispatched = true
		t.markUncertainLocked(ImageTaskReasonTransportError)
		return
	}
	t.dispatched = true
	if resp == nil {
		t.markUncertainLocked(ImageTaskReasonTransportError)
		return
	}
	t.lastStatus = resp.StatusCode
	switch {
	case isExecutionPlaneRejectionStatus(resp.StatusCode):
		t.rejected = true
	case resp.StatusCode >= http.StatusInternalServerError:
		t.markUncertainLocked(ImageTaskReasonUpstream5xx)
	default:
		// 2xx/3xx: generation may have happened. Stays uncertain until the
		// gateway confirms success via MarkSucceeded.
		t.markUncertainLocked(ImageTaskReasonResponseUnconfirmed)
	}
}

func (t *ExecutionPlaneTracker) markUncertainLocked(reason string) {
	t.uncertain = true
	t.uncertainReason = reason
}

// MarkNotSent records why no request was sent (e.g. no schedulable account).
func (t *ExecutionPlaneTracker) MarkNotSent(reason string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.notSentReason == "" || t.notSentReason == ImageTaskReasonConnectFailed {
		t.notSentReason = reason
	}
}

// MarkSucceeded records that the gateway completed the upstream call successfully.
func (t *ExecutionPlaneTracker) MarkSucceeded() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.succeeded = true
}

// AllowsAccountSwitch reports whether another account may be tried. Without a
// tracker (safe mode off) the answer is always true so callers keep their
// existing failover rules.
func (t *ExecutionPlaneTracker) AllowsAccountSwitch() bool {
	if t == nil {
		return true
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return !t.uncertain && !t.succeeded && t.inFlight == 0
}

// AllowsSameAccountRetry is false whenever safe mode is active.
func (t *ExecutionPlaneTracker) AllowsSameAccountRetry() bool {
	return t == nil
}

// LastAttemptNotSent reports whether every upstream call in the current
// attempt failed before the request could be sent.
func (t *ExecutionPlaneTracker) LastAttemptNotSent() bool {
	if t == nil {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.attemptCalls > 0 && t.attemptPreSend == t.attemptCalls && !t.uncertain && !t.succeeded
}

// Snapshot returns the current classification.
func (t *ExecutionPlaneTracker) Snapshot() ImageTaskExecution {
	if t == nil {
		return ImageTaskExecution{}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	out := ImageTaskExecution{
		Dispatched:         t.dispatched || t.inFlight > 0,
		UpstreamCalls:      t.calls,
		LastUpstreamStatus: t.lastStatus,
	}
	switch {
	case t.succeeded:
		out.Outcome = ImageTaskOutcomeSucceeded
	case t.uncertain:
		out.Outcome = ImageTaskOutcomeUncertain
		out.Reason = t.uncertainReason
	case t.inFlight > 0:
		out.Outcome = ImageTaskOutcomeUncertain
		out.Reason = ImageTaskReasonInFlight
	case t.rejected:
		out.Outcome = ImageTaskOutcomeRejected
		out.Reason = ImageTaskReasonUpstreamRejected
	default:
		out.Outcome = ImageTaskOutcomeNotSent
		out.Reason = t.notSentReason
		if out.Reason == "" {
			out.Reason = ImageTaskReasonNotDispatched
		}
	}
	return out
}

// isExecutionPlaneRejectionStatus: statuses where the upstream refused the
// request before generating anything.
func isExecutionPlaneRejectionStatus(status int) bool {
	if status == 529 {
		return true
	}
	return status >= http.StatusBadRequest && status < http.StatusInternalServerError
}

// IsUpstreamPreSendError reports transport errors that provably happened before
// any request byte could reach the upstream: DNS resolution and connection
// establishment (direct or via proxy). Everything else is treated as possibly
// sent.
func IsUpstreamPreSendError(err error) bool {
	if err == nil {
		return false
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return opErr.Op == "dial" || opErr.Op == "proxyconnect"
	}
	return false
}

// ExecutionForFinishedTask converts the tracker state into the value stored on
// the task. delivered tells whether the gateway produced a successful (2xx)
// response for the task. Returns nil outside safe mode.
func ExecutionForFinishedTask(tracker *ExecutionPlaneTracker, delivered, timedOut bool, override string) *ImageTaskExecution {
	if tracker == nil {
		return nil
	}
	snap := tracker.Snapshot()
	switch {
	case delivered && snap.Outcome != ImageTaskOutcomeSucceeded:
		snap.Outcome = ImageTaskOutcomeUncertain
		snap.Reason = ImageTaskReasonResponseUnconfirmed
	case !delivered && snap.Outcome == ImageTaskOutcomeSucceeded:
		snap.Outcome = ImageTaskOutcomeUncertain
		snap.Reason = ImageTaskReasonResponseNotDelivered
	}
	if override != "" && snap.Outcome == ImageTaskOutcomeUncertain {
		snap.Reason = override
	}
	if timedOut && snap.Outcome != ImageTaskOutcomeSucceeded {
		snap.Reason = ImageTaskReasonTimeout
	}
	return &snap
}

// downgradeImageTaskExecution marks a confirmed result as uncertain when it
// cannot be delivered (invalid body, object storage failure).
func downgradeImageTaskExecution(exec *ImageTaskExecution, reason string) *ImageTaskExecution {
	if exec == nil {
		return nil
	}
	out := *exec
	if out.Outcome == ImageTaskOutcomeSucceeded || out.Dispatched {
		out.Outcome = ImageTaskOutcomeUncertain
		out.Reason = reason
	}
	return &out
}

// CompleteWithExecution stores a successful result together with its execution record.
func (s *ImageTaskService) CompleteWithExecution(ctx context.Context, id string, statusCode int, result json.RawMessage, exec *ImageTaskExecution) error {
	return s.complete(ctx, id, statusCode, result, exec)
}

// FailWithExecution stores a failure together with its execution record.
func (s *ImageTaskService) FailWithExecution(ctx context.Context, id string, statusCode int, taskErr json.RawMessage, exec *ImageTaskExecution) error {
	return s.fail(ctx, id, statusCode, taskErr, exec)
}
