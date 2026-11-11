package service

// YeaGrow execution plane (T2.2): signed terminal-state callbacks for async image tasks.
// Origin: finefu/sub2api branch yeagrow/execution-plane (base Wei-Shaw/sub2api ee58c0013).
//
// Flow: finish() moves a task from processing to its terminal state and, in
// the same Redis Lua script, enqueues one callback event (see
// repository/image_task_callback_store.go). A background deliverer POSTs the
// event to the URL configured for the task's API key, signed with
// hex(HMAC-SHA256(secret, "<unix seconds>.<raw body>")), and retries on
// anything but 2xx at 10s/30s/2m/10m/30m/2h, then every 2h, giving up 24h
// after the event was created. The event body is fixed at enqueue time, so
// event_id and body are identical on every retry; only the timestamp and
// signature are recomputed.

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

const (
	// ImageTaskCallbacksEnv holds the callback targets inline (YAML or JSON).
	ImageTaskCallbacksEnv = "IMAGE_TASK_CALLBACKS"
	// ImageTaskCallbacksFileEnv points at a YAML/JSON file with the targets (preferred for secrets).
	ImageTaskCallbacksFileEnv = "IMAGE_TASK_CALLBACKS_FILE"
	// ImageTaskCallbacksAllowHTTPEnv permits http:// callback URLs (local testing only).
	ImageTaskCallbacksAllowHTTPEnv = "IMAGE_TASK_CALLBACKS_ALLOW_HTTP"

	imageTaskCallbackMinSecretLen = 32
	imageTaskCallbackMaxMessage   = 1024

	// ImageTaskCallbackGiveUpAfter: stop retrying this long after the event was created.
	ImageTaskCallbackGiveUpAfter = 24 * time.Hour
	// ImageTaskCallbackRetention keeps abandoned events for inspection.
	ImageTaskCallbackRetention = 7 * 24 * time.Hour

	imageTaskCallbackRequestTimeout = 15 * time.Second
	imageTaskCallbackLease          = 60 * time.Second
	imageTaskCallbackPollInterval   = time.Second
	imageTaskCallbackBatch          = 32
	imageTaskCallbackParallel       = 8
)

// imageTaskCallbackBackoff is the contract retry schedule; the last step repeats.
var imageTaskCallbackBackoff = []time.Duration{
	10 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute, 30 * time.Minute, 2 * time.Hour,
}

// ImageTaskCallbackBackoff returns the delay after the n-th failed delivery (n >= 1).
func ImageTaskCallbackBackoff(failures int) time.Duration {
	if failures < 1 {
		failures = 1
	}
	if failures > len(imageTaskCallbackBackoff) {
		failures = len(imageTaskCallbackBackoff)
	}
	return imageTaskCallbackBackoff[failures-1]
}

// imageTaskRunIDPattern mirrors SAFE_EXECUTION_PLANE_ID in YeaGrow lib/execution-plane/callback.ts.
var imageTaskRunIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

// IsValidImageTaskRunID reports whether s can be used as the contract run_id.
func IsValidImageTaskRunID(s string) bool {
	return imageTaskRunIDPattern.MatchString(s)
}

// ErrImageTaskRunIDRequired: callback-enabled API keys must submit Idempotency-Key = run_id.
var ErrImageTaskRunIDRequired = infraerrors.BadRequest("IMAGE_TASK_RUN_ID_REQUIRED",
	"this API key delivers callbacks: Idempotency-Key must be the run_id ([A-Za-z0-9][A-Za-z0-9._:-]{0,127})")

// ---------------------------------------------------------------------------
// Configuration: API key ID -> URL + secret, from env or a YAML/JSON file.
// No api_keys schema change.

type ImageTaskCallbackTarget struct {
	APIKeyID int64  `yaml:"api_key_id" json:"api_key_id"`
	URL      string `yaml:"url" json:"url"`
	Secret   string `yaml:"secret" json:"secret"`
}

type ImageTaskCallbackConfig struct {
	targets map[int64]ImageTaskCallbackTarget
}

// Target returns the callback target for an API key.
func (c *ImageTaskCallbackConfig) Target(apiKeyID int64) (ImageTaskCallbackTarget, bool) {
	if c == nil {
		return ImageTaskCallbackTarget{}, false
	}
	t, ok := c.targets[apiKeyID]
	return t, ok
}

// Len reports how many API keys have callbacks.
func (c *ImageTaskCallbackConfig) Len() int {
	if c == nil {
		return 0
	}
	return len(c.targets)
}

// ParseImageTaskCallbackConfig parses a YAML (or JSON) list of targets.
func ParseImageTaskCallbackConfig(raw []byte, allowHTTP bool) (*ImageTaskCallbackConfig, error) {
	var targets []ImageTaskCallbackTarget
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&targets); err != nil {
		if errors.Is(err, io.EOF) {
			return &ImageTaskCallbackConfig{targets: map[int64]ImageTaskCallbackTarget{}}, nil
		}
		return nil, fmt.Errorf("parse image task callbacks: %w", err)
	}
	cfg := &ImageTaskCallbackConfig{targets: make(map[int64]ImageTaskCallbackTarget, len(targets))}
	for i, t := range targets {
		t.URL = strings.TrimSpace(t.URL)
		if t.APIKeyID <= 0 {
			return nil, fmt.Errorf("image task callbacks[%d]: api_key_id must be positive", i)
		}
		if _, dup := cfg.targets[t.APIKeyID]; dup {
			return nil, fmt.Errorf("image task callbacks[%d]: duplicate api_key_id %d", i, t.APIKeyID)
		}
		u, err := url.Parse(t.URL)
		if err != nil || u.Host == "" || (u.Scheme != "https" && !(allowHTTP && u.Scheme == "http")) {
			return nil, fmt.Errorf("image task callbacks[%d]: url must be an absolute https URL", i)
		}
		if len(t.Secret) < imageTaskCallbackMinSecretLen {
			return nil, fmt.Errorf("image task callbacks[%d]: secret must be at least %d characters", i, imageTaskCallbackMinSecretLen)
		}
		cfg.targets[t.APIKeyID] = t
	}
	return cfg, nil
}

// LoadImageTaskCallbackConfigFromEnv reads IMAGE_TASK_CALLBACKS_FILE or
// IMAGE_TASK_CALLBACKS. Neither set: (nil, nil), callbacks disabled.
func LoadImageTaskCallbackConfigFromEnv() (*ImageTaskCallbackConfig, error) {
	path := strings.TrimSpace(os.Getenv(ImageTaskCallbacksFileEnv))
	inline := strings.TrimSpace(os.Getenv(ImageTaskCallbacksEnv))
	if path != "" && inline != "" {
		return nil, fmt.Errorf("set only one of %s and %s", ImageTaskCallbacksFileEnv, ImageTaskCallbacksEnv)
	}
	allowHTTP, _ := strconv.ParseBool(strings.TrimSpace(os.Getenv(ImageTaskCallbacksAllowHTTPEnv)))
	switch {
	case path != "":
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", ImageTaskCallbacksFileEnv, err)
		}
		return ParseImageTaskCallbackConfig(raw, allowHTTP)
	case inline != "":
		return ParseImageTaskCallbackConfig([]byte(inline), allowHTTP)
	default:
		return nil, nil
	}
}

// ---------------------------------------------------------------------------
// Contract body (strict: only these fields; see YeaGrow callback.ts).

type ImageTaskCallbackEvent struct {
	EventID  string                     `json:"event_id"`
	TaskID   string                     `json:"task_id"`
	RunID    string                     `json:"run_id"`
	Attempt  int                        `json:"attempt"`
	Status   string                     `json:"status"`
	Upstream *ImageTaskCallbackUpstream `json:"upstream,omitempty"`
	Assets   []ImageTaskAsset           `json:"assets"`
	Error    *ImageTaskCallbackError    `json:"error,omitempty"`
}

type ImageTaskCallbackUpstream struct {
	RequestID  *string `json:"request_id,omitempty"`
	HTTPStatus *int    `json:"http_status,omitempty"`
	DurationMS *int64  `json:"duration_ms,omitempty"`
	AccountRef *string `json:"account_ref,omitempty"`
}

// ImageTaskAsset is one stored image. Filled by the storage/cleaning patches
// (T2.4/T2.5); the key is ImageTaskAssetKey(run_id, index).
type ImageTaskAsset struct {
	R2Key   string `json:"r2_key"`
	MIME    string `json:"mime"`
	Width   int    `json:"width"`
	Height  int    `json:"height"`
	Bytes   int64  `json:"bytes"`
	SHA256  string `json:"sha256"`
	Cleaned bool   `json:"cleaned"`
}

type ImageTaskCallbackError struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

// Contract error types.
const (
	ImageTaskCallbackErrorRateLimit      = "rate_limit"
	ImageTaskCallbackErrorInvalidRequest = "invalid_request"
	ImageTaskCallbackErrorUpstream5xx    = "upstream_5xx"
	ImageTaskCallbackErrorTimeout        = "timeout"
	ImageTaskCallbackErrorNoAccount      = "no_account"
)

// ImageTaskAssetKey is the contract object key: generated/<run_id>/image-<index>, no extension.
func ImageTaskAssetKey(runID string, index int) string {
	return "generated/" + runID + "/image-" + strconv.Itoa(index)
}

// ImageTaskCallbackEventID is stable for a task: one terminal state, one event.
func ImageTaskCallbackEventID(taskID string) string {
	return "evt_" + taskID
}

// BuildImageTaskCallbackEvent turns a finished task into the contract body.
// status comes straight from the P0 execution outcome; nothing is inferred
// from HTTP status codes except picking the contract error type for a
// rejection (429 -> rate_limit).
func BuildImageTaskCallbackEvent(task *ImageTaskRecord) (*ImageTaskCallbackEvent, error) {
	if task == nil || task.Execution == nil {
		return nil, errors.New("task has no execution outcome (safe mode off)")
	}
	if !IsValidImageTaskRunID(task.RunID) {
		return nil, errors.New("task has no valid run_id")
	}
	exec := task.Execution
	switch exec.Outcome {
	case ImageTaskOutcomeSucceeded, ImageTaskOutcomeNotSent, ImageTaskOutcomeRejected, ImageTaskOutcomeUncertain:
	default:
		return nil, fmt.Errorf("unknown execution outcome %q", exec.Outcome)
	}
	event := &ImageTaskCallbackEvent{
		EventID: ImageTaskCallbackEventID(task.ID),
		TaskID:  task.ID,
		RunID:   task.RunID,
		Attempt: 1,
		Status:  exec.Outcome,
		Assets:  []ImageTaskAsset{},
	}
	if exec.LastUpstreamStatus >= 100 && exec.LastUpstreamStatus <= 599 {
		status := exec.LastUpstreamStatus
		event.Upstream = &ImageTaskCallbackUpstream{HTTPStatus: &status}
	}
	if exec.Outcome == ImageTaskOutcomeSucceeded {
		event.Assets = append(event.Assets, task.Assets...)
		return event, nil
	}
	event.Error = &ImageTaskCallbackError{
		Type:    imageTaskCallbackErrorType(exec),
		Message: imageTaskCallbackErrorMessage(exec, task.Error),
	}
	return event, nil
}

func imageTaskCallbackErrorType(exec *ImageTaskExecution) string {
	switch exec.Outcome {
	case ImageTaskOutcomeNotSent:
		if exec.Reason == ImageTaskReasonTimeout {
			return ImageTaskCallbackErrorTimeout
		}
		return ImageTaskCallbackErrorNoAccount
	case ImageTaskOutcomeRejected:
		if exec.LastUpstreamStatus == http.StatusTooManyRequests {
			return ImageTaskCallbackErrorRateLimit
		}
		return ImageTaskCallbackErrorInvalidRequest
	default: // uncertain
		if exec.Reason == ImageTaskReasonTimeout {
			return ImageTaskCallbackErrorTimeout
		}
		return ImageTaskCallbackErrorUpstream5xx
	}
}

// imageTaskCallbackErrorMessage: "<P0 reason>: <task error message>", printable ASCII, <=1024.
func imageTaskCallbackErrorMessage(exec *ImageTaskExecution, taskErr json.RawMessage) string {
	msg := exec.Reason
	var parsed struct {
		Message string `json:"message"`
	}
	if len(taskErr) > 0 && json.Unmarshal(taskErr, &parsed) == nil && strings.TrimSpace(parsed.Message) != "" {
		if msg != "" {
			msg += ": "
		}
		msg += parsed.Message
	}
	var b strings.Builder
	for _, r := range msg {
		if r < 0x20 || r > 0x7e {
			r = '?'
		}
		b.WriteRune(r)
		if b.Len() >= imageTaskCallbackMaxMessage {
			break
		}
	}
	return b.String()
}

// SignImageTaskCallback returns hex(HMAC-SHA256(secret, "<timestamp>.<body>")).
func SignImageTaskCallback(secret string, timestamp int64, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.FormatInt(timestamp, 10)))
	mac.Write([]byte("."))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// ---------------------------------------------------------------------------
// Queue contract (implemented by the Redis task store).

// ImageTaskCallbackEnvelope is a queued event.
type ImageTaskCallbackEnvelope struct {
	EventID   string
	APIKeyID  int64
	Body      []byte
	CreatedAt time.Time
	Failures  int
}

// ImageTaskFinishResult reports what the atomic finish did.
type ImageTaskFinishResult int

const (
	ImageTaskFinishApplied       ImageTaskFinishResult = iota
	ImageTaskFinishNotProcessing                       // already terminal: late finish rejected
	ImageTaskFinishMissing                             // record expired or lost
)

// ImageTaskFinishStore moves a task from processing to its terminal record and
// enqueues the callback event (if any) in one atomic step.
type ImageTaskFinishStore interface {
	FinishProcessing(ctx context.Context, next *ImageTaskRecord, ttl time.Duration, event *ImageTaskCallbackEnvelope) (ImageTaskFinishResult, error)
}

// ImageTaskCallbackQueue is the delivery side of the queue.
type ImageTaskCallbackQueue interface {
	// ClaimDueCallbacks leases up to limit due events until now+lease.
	ClaimDueCallbacks(ctx context.Context, now time.Time, lease time.Duration, limit int) ([]ImageTaskCallbackEnvelope, error)
	AckCallback(ctx context.Context, eventID string) error
	RescheduleCallback(ctx context.Context, eventID string, failures int, next time.Time, lastError string) error
	AbandonCallback(ctx context.Context, eventID string, failures int, reason string, now time.Time) error
}

// ErrImageTaskAlreadyFinished: a second finish for a task that is no longer processing.
var ErrImageTaskAlreadyFinished = infraerrors.New(http.StatusConflict, "IMAGE_TASK_ALREADY_FINISHED", "image task already reached a terminal state")

// SetCallbackConfig enables callbacks for the configured API keys.
func (s *ImageTaskService) SetCallbackConfig(cfg *ImageTaskCallbackConfig) {
	if s != nil {
		s.callbacks = cfg
	}
}

// CallbacksConfiguredFor reports whether the API key has a callback target.
func (s *ImageTaskService) CallbacksConfiguredFor(apiKeyID int64) bool {
	if s == nil {
		return false
	}
	_, ok := s.callbacks.Target(apiKeyID)
	return ok
}

// callbackEnvelopeFor builds the event for a finished record, or nil when the
// task's API key has no callback target.
func (s *ImageTaskService) callbackEnvelopeFor(task *ImageTaskRecord, now time.Time) *ImageTaskCallbackEnvelope {
	if !s.CallbacksConfiguredFor(task.APIKeyID) {
		return nil
	}
	event, err := BuildImageTaskCallbackEvent(task)
	if err != nil {
		logger.L().Error("image_task.callback_event_not_built", zap.String("task_id", task.ID), zap.Int64("api_key_id", task.APIKeyID), zap.Error(err))
		return nil
	}
	if event.Status == ImageTaskOutcomeSucceeded && len(event.Assets) == 0 {
		logger.L().Warn("image_task.callback_succeeded_without_assets", zap.String("task_id", task.ID))
	}
	body, err := json.Marshal(event)
	if err != nil {
		logger.L().Error("image_task.callback_event_encode_failed", zap.String("task_id", task.ID), zap.Error(err))
		return nil
	}
	return &ImageTaskCallbackEnvelope{EventID: event.EventID, APIKeyID: task.APIKeyID, Body: body, CreatedAt: now}
}

// ---------------------------------------------------------------------------
// Delivery worker.

type ImageTaskCallbackDeliverer struct {
	queue  ImageTaskCallbackQueue
	config *ImageTaskCallbackConfig
	client *http.Client
	now    func() time.Time

	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
}

func NewImageTaskCallbackDeliverer(queue ImageTaskCallbackQueue, config *ImageTaskCallbackConfig, client *http.Client) *ImageTaskCallbackDeliverer {
	if client == nil {
		client = &http.Client{
			Timeout: imageTaskCallbackRequestTimeout,
			// A redirect is not a delivery; never follow it with the signed body.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
	}
	return &ImageTaskCallbackDeliverer{queue: queue, config: config, client: client, now: time.Now,
		stop: make(chan struct{}), done: make(chan struct{})}
}

// Start runs the delivery loop until Stop.
func (d *ImageTaskCallbackDeliverer) Start() {
	go func() {
		defer close(d.done)
		ticker := time.NewTicker(imageTaskCallbackPollInterval)
		defer ticker.Stop()
		for {
			if _, err := d.RunOnce(context.Background()); err != nil {
				logger.L().Warn("image_task.callback_poll_failed", zap.Error(err))
			}
			select {
			case <-d.stop:
				return
			case <-ticker.C:
			}
		}
	}()
}

// Stop ends the loop and waits for the current batch.
func (d *ImageTaskCallbackDeliverer) Stop() {
	d.stopOnce.Do(func() { close(d.stop) })
	<-d.done
}

// ImageTaskCallbackRunStats summarizes one RunOnce.
type ImageTaskCallbackRunStats struct {
	Delivered, Retried, Abandoned int
}

// RunOnce claims due events and attempts each once.
func (d *ImageTaskCallbackDeliverer) RunOnce(ctx context.Context) (ImageTaskCallbackRunStats, error) {
	var stats ImageTaskCallbackRunStats
	envelopes, err := d.queue.ClaimDueCallbacks(ctx, d.now(), imageTaskCallbackLease, imageTaskCallbackBatch)
	if err != nil || len(envelopes) == 0 {
		return stats, err
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, imageTaskCallbackParallel)
	for _, env := range envelopes {
		wg.Add(1)
		sem <- struct{}{}
		go func(env ImageTaskCallbackEnvelope) {
			defer wg.Done()
			defer func() { <-sem }()
			result := d.deliver(ctx, env)
			mu.Lock()
			switch result {
			case callbackDelivered:
				stats.Delivered++
			case callbackRetried:
				stats.Retried++
			case callbackAbandoned:
				stats.Abandoned++
			}
			mu.Unlock()
		}(env)
	}
	wg.Wait()
	return stats, nil
}

type callbackResult int

const (
	callbackDelivered callbackResult = iota
	callbackRetried
	callbackAbandoned
	callbackStoreError
)

func (d *ImageTaskCallbackDeliverer) deliver(ctx context.Context, env ImageTaskCallbackEnvelope) callbackResult {
	log := logger.L().With(zap.String("event_id", env.EventID), zap.Int64("api_key_id", env.APIKeyID), zap.Int("failures", env.Failures))
	target, ok := d.config.Target(env.APIKeyID)
	if !ok {
		log.Error("image_task.callback_abandoned_no_target")
		if err := d.queue.AbandonCallback(ctx, env.EventID, env.Failures, "no_callback_target", d.now()); err != nil {
			log.Error("image_task.callback_abandon_store_failed", zap.Error(err))
			return callbackStoreError
		}
		return callbackAbandoned
	}

	deliveryErr := d.post(ctx, target, env.Body)
	now := d.now()
	if deliveryErr == nil {
		if err := d.queue.AckCallback(ctx, env.EventID); err != nil {
			// Delivered; the lease expires and it is sent once more. Receivers dedupe on event_id.
			log.Error("image_task.callback_ack_failed", zap.Error(err))
			return callbackStoreError
		}
		log.Info("image_task.callback_delivered")
		return callbackDelivered
	}

	failures := env.Failures + 1
	giveUpAt := env.CreatedAt.Add(ImageTaskCallbackGiveUpAfter)
	if !now.Before(giveUpAt) {
		log.Error("image_task.callback_abandoned_after_24h", zap.Error(deliveryErr))
		if err := d.queue.AbandonCallback(ctx, env.EventID, failures, truncateCallbackError(deliveryErr), now); err != nil {
			log.Error("image_task.callback_abandon_store_failed", zap.Error(err))
			return callbackStoreError
		}
		return callbackAbandoned
	}
	next := now.Add(ImageTaskCallbackBackoff(failures))
	if next.After(giveUpAt) {
		next = giveUpAt // one final attempt at the 24h mark
	}
	log.Warn("image_task.callback_retry_scheduled", zap.Time("next_attempt", next), zap.Error(deliveryErr))
	if err := d.queue.RescheduleCallback(ctx, env.EventID, failures, next, truncateCallbackError(deliveryErr)); err != nil {
		log.Error("image_task.callback_reschedule_failed", zap.Error(err))
		return callbackStoreError
	}
	return callbackRetried
}

func (d *ImageTaskCallbackDeliverer) post(ctx context.Context, target ImageTaskCallbackTarget, body []byte) error {
	ts := d.now().Unix()
	reqCtx, cancel := context.WithTimeout(ctx, imageTaskCallbackRequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, target.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Exec-Timestamp", strconv.FormatInt(ts, 10))
	req.Header.Set("X-Exec-Signature", SignImageTaskCallback(target.Secret, ts, body))
	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("callback receiver answered HTTP %d", resp.StatusCode)
	}
	return nil
}

func truncateCallbackError(err error) string {
	msg := err.Error()
	if len(msg) > 256 {
		msg = msg[:256]
	}
	return msg
}

// StartImageTaskCallbacksFromEnv loads the callback config and, if any target
// is configured and the store can queue, starts the deliverer. Returns the
// deliverer (nil when disabled).
func StartImageTaskCallbacksFromEnv(svc *ImageTaskService) *ImageTaskCallbackDeliverer {
	cfg, err := LoadImageTaskCallbackConfigFromEnv()
	if err != nil {
		logger.L().Error("image_task.callback_config_invalid; callbacks disabled", zap.Error(err))
		return nil
	}
	if cfg.Len() == 0 || svc == nil {
		return nil
	}
	queue, ok := svc.store.(ImageTaskCallbackQueue)
	if !ok {
		logger.L().Error("image_task.callback_store_unsupported; callbacks disabled")
		return nil
	}
	if _, ok := svc.store.(ImageTaskFinishStore); !ok {
		logger.L().Error("image_task.callback_finish_store_unsupported; callbacks disabled")
		return nil
	}
	svc.SetCallbackConfig(cfg)
	deliverer := NewImageTaskCallbackDeliverer(queue, cfg, nil)
	deliverer.Start()
	logger.L().Info("image_task.callbacks_enabled", zap.Int("targets", cfg.Len()))
	return deliverer
}

// finishAtomically applies the terminal record only if the stored task is
// still processing, enqueueing the callback in the same step. A late second
// finish (e.g. restart recovery racing a slow completion) is rejected and logged.
func (s *ImageTaskService) finishAtomically(ctx context.Context, finisher ImageTaskFinishStore, task *ImageTaskRecord, status string, now time.Time) error {
	event := s.callbackEnvelopeFor(task, now)
	result, err := finisher.FinishProcessing(ctx, task, s.ttl, event)
	if err != nil {
		return ErrImageTaskUnavailable.WithCause(err)
	}
	switch result {
	case ImageTaskFinishApplied:
		return nil
	case ImageTaskFinishMissing:
		logger.L().Warn("image_task.finish_rejected_missing", zap.String("task_id", task.ID), zap.String("attempted_status", status))
		return ErrImageTaskNotFound
	default:
		logger.L().Warn("image_task.finish_rejected_not_processing", zap.String("task_id", task.ID), zap.String("attempted_status", status))
		return ErrImageTaskAlreadyFinished
	}
}

// SetClock replaces the deliverer's clock (tests).
func (d *ImageTaskCallbackDeliverer) SetClock(now func() time.Time) {
	if d != nil && now != nil {
		d.now = now
	}
}
