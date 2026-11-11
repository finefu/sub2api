package service

// YeaGrow execution plane: status counts never fall back to zeros.

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type statusOnlyStore struct {
	ImageTaskStore
	counts   ImageTaskExecutionPlaneCounts
	err      error
	instance string
}

func (s *statusOnlyStore) ExecutionPlaneCounts(_ context.Context, instanceID string) (ImageTaskExecutionPlaneCounts, error) {
	s.instance = instanceID
	return s.counts, s.err
}

func TestImageTaskExecutionPlaneStatus(t *testing.T) {
	ctx := context.Background()
	var nilService *ImageTaskService
	_, err := nilService.ExecutionPlaneStatus(ctx)
	require.ErrorIs(t, err, ErrImageTaskStatusUnavailable)

	store := &statusOnlyStore{counts: ImageTaskExecutionPlaneCounts{CallbackDue: 1, CallbackDead: 2, Inflight: 3}}
	counts, err := NewImageTaskService(store).ExecutionPlaneStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, store.counts, counts)
	require.Empty(t, store.instance)

	store.err = errors.New("redis down")
	_, err = NewImageTaskService(store).ExecutionPlaneStatus(ctx)
	require.ErrorIs(t, err, ErrImageTaskStatusUnavailable)

	store.err = nil
	store.counts = ImageTaskExecutionPlaneCounts{CallbackDue: -1}
	_, err = NewImageTaskService(store).ExecutionPlaneStatus(ctx)
	require.ErrorIs(t, err, ErrImageTaskStatusUnavailable)
}
