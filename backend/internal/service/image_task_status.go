package service

// YeaGrow execution plane: read-only status counts for the YeaGrow admin overview.
// Origin: finefu/sub2api branch yeagrow/execution-plane (base Wei-Shaw/sub2api ee58c0013).
//
// Contract: GET /v1/execution-plane/status ->
//   200 {"status":"ok","callback_queue":{"due":N,"dead":M},"inflight":K}
//   503 when Redis cannot be read (never 200 with zeros).
// Only aggregate counts are exposed: no task, event, user or key identifiers.

import (
	"context"
	"errors"
)

// ImageTaskExecutionPlaneCounts holds the aggregate counts reported by the status endpoint.
type ImageTaskExecutionPlaneCounts struct {
	// CallbackDue is the number of callbacks waiting for (re)delivery.
	CallbackDue int64
	// CallbackDead is the number of abandoned callbacks.
	CallbackDead int64
	// Inflight is the length of this instance's in-flight index (0 when the execution plane is off).
	Inflight int64
}

// ImageTaskStatusStore reads the counts. instanceID is empty when the execution plane is off.
type ImageTaskStatusStore interface {
	ExecutionPlaneCounts(ctx context.Context, instanceID string) (ImageTaskExecutionPlaneCounts, error)
}

// ErrImageTaskStatusUnavailable means the counts could not be read; callers must not report zeros.
var ErrImageTaskStatusUnavailable = errors.New("image task status unavailable")

// ExecutionPlaneStatus returns the aggregate counts, or ErrImageTaskStatusUnavailable.
func (s *ImageTaskService) ExecutionPlaneStatus(ctx context.Context) (ImageTaskExecutionPlaneCounts, error) {
	if s == nil || s.store == nil {
		return ImageTaskExecutionPlaneCounts{}, ErrImageTaskStatusUnavailable
	}
	reader, ok := s.store.(ImageTaskStatusStore)
	if !ok {
		return ImageTaskExecutionPlaneCounts{}, ErrImageTaskStatusUnavailable
	}
	counts, err := reader.ExecutionPlaneCounts(ctx, s.ExecutionPlaneInstance())
	if err != nil {
		return ImageTaskExecutionPlaneCounts{}, errors.Join(ErrImageTaskStatusUnavailable, err)
	}
	if counts.CallbackDue < 0 || counts.CallbackDead < 0 || counts.Inflight < 0 {
		return ImageTaskExecutionPlaneCounts{}, ErrImageTaskStatusUnavailable
	}
	return counts, nil
}
