package service

// YeaGrow execution plane (T2.3 / P3): restart recovery, graceful drain and
// instance-scoped concurrency slot cleanup for asynchronous image tasks.
// Origin: finefu/sub2api branch yeagrow/execution-plane (base Wei-Shaw/sub2api ee58c0013).
//
// Everything here is active only together with the execution-plane safe mode
// (IMAGE_TASK_EXECUTION_PLANE_SAFE_MODE=true); without it the upstream
// behaviour is unchanged.
//
//   - Every task is registered in the in-flight index of its instance before
//     its record is written, and removed by the T2.2 finish CAS.
//   - Before the first upstream request of a task leaves the gateway, the
//     dispatch is persisted on the task record (dispatched_at). If that write
//     fails the request is not sent.
//   - On startup the instance waits for its lease (so a duplicated instance ID
//     cannot steal a live process's tasks), then finishes every in-flight task
//     of a previous process through the finish CAS: dispatched -> uncertain,
//     never dispatched (still waiting for a slot) -> not_sent. Each produces
//     exactly one callback event.
//   - It then removes concurrency slots left by previous processes of this
//     instance only, identified by their recorded request-ID prefixes.
//   - On SIGTERM new async submits are refused and running tasks get up to
//     IMAGE_TASK_SHUTDOWN_DRAIN_TIMEOUT to finish; the rest is left to the
//     next startup's recovery.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"go.uber.org/zap"
)

const (
	// ImageTaskInstanceIDEnv names this gateway instance. Every instance that
	// shares a Redis must use a distinct value. Default: "default".
	ImageTaskInstanceIDEnv = "SUB2API_INSTANCE_ID"
	// DefaultImageTaskInstanceID is used when ImageTaskInstanceIDEnv is unset.
	DefaultImageTaskInstanceID = "default"
	// ImageTaskShutdownDrainTimeoutEnv bounds how long SIGTERM waits for running
	// async image tasks (Go duration, e.g. "120s"; "0" disables waiting).
	ImageTaskShutdownDrainTimeoutEnv = "IMAGE_TASK_SHUTDOWN_DRAIN_TIMEOUT"
	// DefaultImageTaskShutdownDrainTimeout is the default drain bound.
	DefaultImageTaskShutdownDrainTimeout = 120 * time.Second

	// ImageTaskReasonInstanceRestarted: the task was interrupted by a gateway
	// restart and finished by startup recovery.
	ImageTaskReasonInstanceRestarted = "instance_restarted"
	// ImageTaskReasonDispatchNotRecorded: the dispatch marker could not be
	// persisted, so the request was not sent.
	ImageTaskReasonDispatchNotRecorded = "dispatch_not_recorded"
	// ImageTaskReasonStorageInterrupted: a restart hit the task while its
	// result was being written to object storage (always uncertain).
	ImageTaskReasonStorageInterrupted = "storage_interrupted"

	imageTaskInstanceLeaseTTL     = 30 * time.Second
	imageTaskInstanceLeaseRefresh = 10 * time.Second
	imageTaskInstanceLeaseRetry   = 2 * time.Second
)

var (
	imageTaskInstanceIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	// processPrefixPattern matches RequestIDPrefix(): "r" + base36.
	processPrefixPattern = regexp.MustCompile(`^r[0-9a-z]{1,20}$`)

	// ErrImageTaskShuttingDown is returned to new submits while draining.
	ErrImageTaskShuttingDown = errors.New("image task service is shutting down")
	// ErrImageTaskInstanceLeaseHeld: another live process holds this instance ID.
	ErrImageTaskInstanceLeaseHeld = errors.New("image task instance lease is held by another process")
	// ErrImageTaskDispatchRejected: the task is no longer processing, do not send.
	ErrImageTaskDispatchRejected = errors.New("image task is not processing; dispatch refused")
)

// ExecutionPlaneDispatchNotRecordedError wraps a failure to persist the
// dispatch marker. The request was not sent; IsUpstreamPreSendError reports it.
type ExecutionPlaneDispatchNotRecordedError struct{ Err error }

func (e *ExecutionPlaneDispatchNotRecordedError) Error() string {
	return "execution plane: dispatch marker not recorded, request not sent: " + e.Err.Error()
}

func (e *ExecutionPlaneDispatchNotRecordedError) Unwrap() error { return e.Err }

// ImageTaskRecoveryStore is the persistence used by recovery (Redis in production).
type ImageTaskRecoveryStore interface {
	AddInflight(ctx context.Context, instanceID, taskID string, createdAt time.Time) error
	RemoveInflight(ctx context.Context, instanceID, taskID string) error
	ListInflight(ctx context.Context, instanceID string) ([]string, error)
	// MarkDispatched sets dispatched_at on a processing record. Returns
	// ErrImageTaskDispatchRejected if the record is not processing and
	// ErrImageTaskNotFound if it is gone.
	MarkDispatched(ctx context.Context, taskID string, at time.Time) error
	// MarkStoring sets storing_at with the same rules as MarkDispatched.
	MarkStoring(ctx context.Context, taskID string, at time.Time) error
	AcquireInstanceLease(ctx context.Context, instanceID, owner string, ttl time.Duration) (bool, error)
	RefreshInstanceLease(ctx context.Context, instanceID, owner string, ttl time.Duration) (bool, error)
	ReleaseInstanceLease(ctx context.Context, instanceID, owner string) error
	// RegisterProcessPrefix records prefix for the instance and returns the
	// previously recorded prefixes (excluding prefix).
	RegisterProcessPrefix(ctx context.Context, instanceID, prefix string) ([]string, error)
	ForgetProcessPrefixes(ctx context.Context, instanceID string, prefixes []string) error
	// CleanupSlotsForPrefixes removes concurrency slot members created by the
	// given process prefixes from the confirmed slot key families only.
	CleanupSlotsForPrefixes(ctx context.Context, prefixes []string) (int, error)
}

type imageTaskExecutionPlane struct {
	store        ImageTaskRecoveryStore
	instanceID   string
	process      string
	drainTimeout time.Duration

	mu       sync.Mutex
	draining bool
	running  int
	idle     chan struct{}

	leaseMu   sync.Mutex
	leaseHeld bool

	stopOnce sync.Once
	stop     chan struct{}
}

// ImageTaskRecoveryStats summarises one recovery pass.
type ImageTaskRecoveryStats struct {
	Scanned, Uncertain, NotSent, SkippedLive, AlreadyFinished, Missing int
	SlotsRemoved                                                       int
}

// ImageTaskInstanceIDFromEnv returns the configured instance ID (validated).
func ImageTaskInstanceIDFromEnv() (string, error) {
	raw := strings.TrimSpace(os.Getenv(ImageTaskInstanceIDEnv))
	if raw == "" {
		return DefaultImageTaskInstanceID, nil
	}
	if !imageTaskInstanceIDPattern.MatchString(raw) {
		return "", fmt.Errorf("invalid %s=%q", ImageTaskInstanceIDEnv, raw)
	}
	return raw, nil
}

// ImageTaskShutdownDrainTimeoutFromEnv parses the drain bound.
func ImageTaskShutdownDrainTimeoutFromEnv() (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(ImageTaskShutdownDrainTimeoutEnv))
	if raw == "" {
		return DefaultImageTaskShutdownDrainTimeout, nil
	}
	if raw == "0" {
		return 0, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < 0 {
		return DefaultImageTaskShutdownDrainTimeout, fmt.Errorf("invalid %s=%q", ImageTaskShutdownDrainTimeoutEnv, raw)
	}
	return d, nil
}

// ImageTaskInstanceScopedSlotCleanupEnabled reports whether the upstream
// "remove every foreign prefix" startup slot cleanup must be skipped because
// the execution plane cleans only its own instance's slots.
func ImageTaskInstanceScopedSlotCleanupEnabled() bool {
	enabled, err := ImageTaskExecutionPlaneSafeModeFromEnv()
	return err == nil && enabled
}

// EnableExecutionPlane turns on in-flight indexing, dispatch persistence,
// recovery and draining. process is the request-ID prefix of this process.
func (s *ImageTaskService) EnableExecutionPlane(store ImageTaskRecoveryStore, instanceID, process string, drainTimeout time.Duration) error {
	if s == nil || store == nil {
		return errors.New("image task execution plane: store is required")
	}
	if !imageTaskInstanceIDPattern.MatchString(instanceID) {
		return fmt.Errorf("image task execution plane: invalid instance id %q", instanceID)
	}
	if !processPrefixPattern.MatchString(process) {
		return fmt.Errorf("image task execution plane: invalid process prefix %q", process)
	}
	s.plane = &imageTaskExecutionPlane{
		store: store, instanceID: instanceID, process: process, drainTimeout: drainTimeout,
		stop: make(chan struct{}),
	}
	return nil
}

// ExecutionPlaneInstance returns the instance ID, or "" when disabled.
func (s *ImageTaskService) ExecutionPlaneInstance() string {
	if s == nil || s.plane == nil {
		return ""
	}
	return s.plane.instanceID
}

// BeginRun reserves a slot for one async task execution. ok=false while the
// service is draining for shutdown; release must be called exactly once.
func (s *ImageTaskService) BeginRun() (release func(), ok bool) {
	if s == nil || s.plane == nil {
		return func() {}, true
	}
	p := s.plane
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.draining {
		return nil, false
	}
	p.running++
	var once sync.Once
	return func() {
		once.Do(func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			p.running--
			if p.running == 0 && p.idle != nil {
				close(p.idle)
				p.idle = nil
			}
		})
	}, true
}

// RunningTasks reports executions started by this process and not yet finished.
func (s *ImageTaskService) RunningTasks() int {
	if s == nil || s.plane == nil {
		return 0
	}
	s.plane.mu.Lock()
	defer s.plane.mu.Unlock()
	return s.plane.running
}

// Drain refuses new submits and waits up to timeout for running tasks. It
// returns how many were still running; those stay in the in-flight index for
// the next startup's recovery. The instance lease is released afterwards.
func (s *ImageTaskService) Drain(timeout time.Duration) int {
	if s == nil || s.plane == nil {
		return 0
	}
	p := s.plane
	p.mu.Lock()
	p.draining = true
	var idle chan struct{}
	if p.running > 0 {
		if p.idle == nil {
			p.idle = make(chan struct{})
		}
		idle = p.idle
	}
	p.mu.Unlock()

	if idle != nil && timeout > 0 {
		timer := time.NewTimer(timeout)
		select {
		case <-idle:
		case <-timer.C:
		}
		timer.Stop()
	}
	remaining := s.RunningTasks()
	p.stopOnce.Do(func() { close(p.stop) })
	p.leaseMu.Lock()
	if p.leaseHeld {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err := p.store.ReleaseInstanceLease(ctx, p.instanceID, p.process); err != nil {
			logger.L().Warn("image_task.instance_lease_release_failed", zap.String("instance_id", p.instanceID), zap.Error(err))
		}
		cancel()
		p.leaseHeld = false
	}
	p.leaseMu.Unlock()
	return remaining
}

// registerInflight stamps the record with its instance and process and adds
// it to the in-flight index. Called before the record is first written so a
// crash in between leaves at most an index entry without a record.
func (s *ImageTaskService) registerInflight(ctx context.Context, record *ImageTaskRecord) error {
	if s == nil || s.plane == nil || record == nil {
		return nil
	}
	record.InstanceID = s.plane.instanceID
	record.OwnerProcess = s.plane.process
	return s.plane.store.AddInflight(ctx, s.plane.instanceID, record.ID, time.Now())
}

func (s *ImageTaskService) unregisterInflight(ctx context.Context, record *ImageTaskRecord) {
	if s == nil || s.plane == nil || record == nil || record.InstanceID == "" {
		return
	}
	if err := s.plane.store.RemoveInflight(ctx, record.InstanceID, record.ID); err != nil {
		logger.L().Warn("image_task.inflight_remove_failed", zap.String("task_id", record.ID), zap.Error(err))
	}
}

// DispatchRecorder returns the hook that persists the dispatch marker of a
// task before its first upstream request; nil when the plane is disabled.
func (s *ImageTaskService) DispatchRecorder(taskID string) func(context.Context) error {
	if s == nil || s.plane == nil {
		return nil
	}
	store := s.plane.store
	return func(ctx context.Context) error {
		writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		return store.MarkDispatched(writeCtx, taskID, time.Now())
	}
}

// markStoring persists storing_at before a result is written to object
// storage. A task that is no longer processing gets ErrImageTaskAlreadyFinished
// (its result must not be stored); other write errors are logged and ignored
// because dispatched_at already makes a restart report uncertain.
func (s *ImageTaskService) markStoring(ctx context.Context, taskID string) error {
	if s == nil || s.plane == nil {
		return nil
	}
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	err := s.plane.store.MarkStoring(writeCtx, taskID, time.Now())
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrImageTaskDispatchRejected):
		logger.L().Warn("image_task.late_result_dropped", zap.String("task_id", taskID))
		return ErrImageTaskAlreadyFinished
	default:
		logger.L().Warn("image_task.storing_marker_failed", zap.String("task_id", taskID), zap.Error(err))
		return nil
	}
}

// StartRecovery runs lease acquisition, recovery and slot cleanup in the
// background, then keeps the lease alive until Drain.
func (s *ImageTaskService) StartRecovery() {
	if s == nil || s.plane == nil {
		return
	}
	go s.recoveryLoop()
}

func (s *ImageTaskService) recoveryLoop() {
	p := s.plane
	log := logger.L().With(zap.String("instance_id", p.instanceID), zap.String("process", p.process))
	started := time.Now()
	warned := false
	for {
		stats, err := s.RecoverOnce(context.Background())
		if err == nil {
			log.Info("image_task.recovery_done",
				zap.Int("scanned", stats.Scanned), zap.Int("uncertain", stats.Uncertain), zap.Int("not_sent", stats.NotSent),
				zap.Int("skipped_live", stats.SkippedLive), zap.Int("already_finished", stats.AlreadyFinished),
				zap.Int("missing", stats.Missing), zap.Int("slots_removed", stats.SlotsRemoved),
				zap.Duration("waited", time.Since(started)))
			break
		}
		if errors.Is(err, ErrImageTaskInstanceLeaseHeld) {
			if !warned && time.Since(started) > 2*imageTaskInstanceLeaseTTL {
				log.Error("image_task.instance_lease_still_held; is another live process using the same " + ImageTaskInstanceIDEnv + "?")
				warned = true
			}
		} else {
			log.Warn("image_task.recovery_failed", zap.Error(err))
		}
		select {
		case <-p.stop:
			return
		case <-time.After(imageTaskInstanceLeaseRetry):
		}
	}
	ticker := time.NewTicker(imageTaskInstanceLeaseRefresh)
	defer ticker.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-ticker.C:
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		ok, err := p.store.RefreshInstanceLease(ctx, p.instanceID, p.process, imageTaskInstanceLeaseTTL)
		cancel()
		if err != nil {
			log.Warn("image_task.instance_lease_refresh_failed", zap.Error(err))
		} else if !ok {
			log.Error("image_task.instance_lease_lost; another process took " + ImageTaskInstanceIDEnv)
		}
	}
}

// RecoverOnce acquires the instance lease and recovers every in-flight task
// left by previous processes of this instance. Returns
// ErrImageTaskInstanceLeaseHeld while another process holds the lease.
func (s *ImageTaskService) RecoverOnce(ctx context.Context) (ImageTaskRecoveryStats, error) {
	var stats ImageTaskRecoveryStats
	if s == nil || s.plane == nil {
		return stats, nil
	}
	p := s.plane
	p.leaseMu.Lock()
	if !p.leaseHeld {
		ok, err := p.store.AcquireInstanceLease(ctx, p.instanceID, p.process, imageTaskInstanceLeaseTTL)
		if err != nil {
			p.leaseMu.Unlock()
			return stats, err
		}
		if !ok {
			p.leaseMu.Unlock()
			return stats, ErrImageTaskInstanceLeaseHeld
		}
		p.leaseHeld = true
	}
	p.leaseMu.Unlock()

	previous, err := p.store.RegisterProcessPrefix(ctx, p.instanceID, p.process)
	if err != nil {
		return stats, fmt.Errorf("register process prefix: %w", err)
	}
	ids, err := p.store.ListInflight(ctx, p.instanceID)
	if err != nil {
		return stats, fmt.Errorf("list in-flight tasks: %w", err)
	}
	for _, id := range ids {
		stats.Scanned++
		switch s.recoverTask(ctx, id) {
		case recoveredUncertain:
			stats.Uncertain++
		case recoveredNotSent:
			stats.NotSent++
		case recoverySkippedLive:
			stats.SkippedLive++
		case recoveryAlreadyFinished:
			stats.AlreadyFinished++
		case recoveryMissing:
			stats.Missing++
		case recoveryError:
			return stats, fmt.Errorf("recover task %s failed", id)
		}
	}
	valid := make([]string, 0, len(previous))
	for _, prefix := range previous {
		if processPrefixPattern.MatchString(prefix) && prefix != p.process {
			valid = append(valid, prefix)
		}
	}
	if len(valid) > 0 {
		removed, err := p.store.CleanupSlotsForPrefixes(ctx, valid)
		stats.SlotsRemoved = removed
		if err != nil {
			return stats, fmt.Errorf("cleanup slots of previous processes: %w", err)
		}
	}
	if len(previous) > 0 {
		if err := p.store.ForgetProcessPrefixes(ctx, p.instanceID, previous); err != nil {
			logger.L().Warn("image_task.process_prefix_forget_failed", zap.Error(err))
		}
	}
	return stats, nil
}

type recoveryResult int

const (
	recoveredUncertain recoveryResult = iota
	recoveredNotSent
	recoverySkippedLive
	recoveryAlreadyFinished
	recoveryMissing
	recoveryError
)

func (s *ImageTaskService) recoverTask(ctx context.Context, id string) recoveryResult {
	p := s.plane
	log := logger.L().With(zap.String("task_id", id), zap.String("instance_id", p.instanceID))
	task, err := s.store.Get(ctx, id)
	if err != nil {
		if errors.Is(err, ErrImageTaskNotFound) {
			_ = p.store.RemoveInflight(ctx, p.instanceID, id)
			return recoveryMissing
		}
		log.Warn("image_task.recovery_get_failed", zap.Error(err))
		return recoveryError
	}
	if task.Status != ImageTaskStatusProcessing {
		_ = p.store.RemoveInflight(ctx, p.instanceID, id)
		return recoveryAlreadyFinished
	}
	if task.OwnerProcess == p.process {
		return recoverySkippedLive
	}
	exec, status, taskErr, result := recoveredImageTaskExecution(task)
	err = s.finish(ctx, id, ImageTaskStatusFailed, status, nil, taskErr, exec)
	switch {
	case err == nil:
		log.Warn("image_task.recovered_after_restart", zap.String("outcome", exec.Outcome),
			zap.String("owner_process", task.OwnerProcess), zap.Bool("dispatched", exec.Dispatched))
		return result
	case errors.Is(err, ErrImageTaskAlreadyFinished):
		_ = p.store.RemoveInflight(ctx, p.instanceID, id)
		return recoveryAlreadyFinished
	case errors.Is(err, ErrImageTaskNotFound):
		_ = p.store.RemoveInflight(ctx, p.instanceID, id)
		return recoveryMissing
	default:
		log.Warn("image_task.recovery_finish_failed", zap.Error(err))
		return recoveryError
	}
}

func recoveredImageTaskExecution(task *ImageTaskRecord) (*ImageTaskExecution, int, json.RawMessage, recoveryResult) {
	if task.StoringAt != nil {
		// The upstream already answered and the result was being stored
		// (T2.4 object write or legacy upload): generated, delivery unknown.
		return &ImageTaskExecution{
				Outcome: ImageTaskOutcomeUncertain, Reason: ImageTaskReasonStorageInterrupted,
				Dispatched: true, UpstreamCalls: 1,
			}, http.StatusBadGateway,
			imageTaskErrorJSON("api_error", "image task interrupted by a gateway restart while storing the generated image; the result is unknown"),
			recoveredUncertain
	}
	if task.DispatchedAt != nil {
		calls := 1
		if task.Execution != nil && task.Execution.UpstreamCalls > calls {
			calls = task.Execution.UpstreamCalls
		}
		return &ImageTaskExecution{
				Outcome: ImageTaskOutcomeUncertain, Reason: ImageTaskReasonInstanceRestarted,
				Dispatched: true, UpstreamCalls: calls,
			}, http.StatusBadGateway,
			imageTaskErrorJSON("upstream_error", "image task interrupted by a gateway restart after the upstream request was sent; the result is unknown"),
			recoveredUncertain
	}
	return &ImageTaskExecution{
			Outcome: ImageTaskOutcomeNotSent, Reason: ImageTaskReasonInstanceRestarted,
			Dispatched: false, UpstreamCalls: 0,
		}, http.StatusServiceUnavailable,
		imageTaskErrorJSON("api_error", "image task interrupted by a gateway restart before any upstream request was sent"),
		recoveredNotSent
}

// ---- process-wide shutdown hook (cmd/server/main.go) ----

var (
	shutdownImageTasksMu sync.Mutex
	shutdownImageTasks   []*ImageTaskService
)

func registerImageTaskServiceForShutdown(s *ImageTaskService) {
	shutdownImageTasksMu.Lock()
	defer shutdownImageTasksMu.Unlock()
	shutdownImageTasks = append(shutdownImageTasks, s)
}

// DrainImageTasksForShutdown is called on SIGTERM before the HTTP server stops:
// new async submits are refused (503) while polls keep working, and running
// tasks get up to the configured drain timeout.
func DrainImageTasksForShutdown() {
	shutdownImageTasksMu.Lock()
	services := append([]*ImageTaskService(nil), shutdownImageTasks...)
	shutdownImageTasksMu.Unlock()
	for _, s := range services {
		if s == nil || s.plane == nil {
			continue
		}
		started := time.Now()
		running := s.RunningTasks()
		logger.L().Info("image_task.shutdown_drain_started", zap.Int("running", running), zap.Duration("timeout", s.plane.drainTimeout))
		remaining := s.Drain(s.plane.drainTimeout)
		logger.L().Info("image_task.shutdown_drain_finished", zap.Int("running_at_start", running),
			zap.Int("left_for_recovery", remaining), zap.Duration("took", time.Since(started)))
	}
}

// ConfigureImageTaskExecutionPlaneFromEnv enables P3 when safe mode is on and
// the store supports it. Errors leave the plane off and are logged.
func ConfigureImageTaskExecutionPlaneFromEnv(s *ImageTaskService, store ImageTaskStore) {
	if s == nil {
		return
	}
	enabled, err := ImageTaskExecutionPlaneSafeModeFromEnv()
	if err != nil || !enabled {
		return
	}
	recoveryStore, ok := store.(ImageTaskRecoveryStore)
	if !ok {
		logger.L().Error("image_task.execution_plane_recovery_unavailable; store does not support recovery")
		return
	}
	instanceID, err := ImageTaskInstanceIDFromEnv()
	if err != nil {
		logger.L().Error("image_task.execution_plane_recovery_disabled", zap.Error(err))
		return
	}
	drain, err := ImageTaskShutdownDrainTimeoutFromEnv()
	if err != nil {
		logger.L().Error("image_task.shutdown_drain_timeout_invalid; using default", zap.Error(err), zap.Duration("default", drain))
	}
	if err := s.EnableExecutionPlane(recoveryStore, instanceID, RequestIDPrefix(), drain); err != nil {
		logger.L().Error("image_task.execution_plane_recovery_disabled", zap.Error(err))
		return
	}
	registerImageTaskServiceForShutdown(s)
	logger.L().Info("image_task.execution_plane_recovery_enabled", zap.String("instance_id", instanceID),
		zap.String("process", RequestIDPrefix()), zap.Duration("drain_timeout", drain))
	s.StartRecovery()
}
