package service

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Cross-implementation vectors: the signatures below were produced by WebCrypto
// HMAC (Node 24) and each body was accepted by YeaGrow's
// verifyExecutionPlaneCallback (lib/execution-plane/callback.ts @ yeagrow
// 04f129bc) with timestamp 1794400000 and this secret.
const (
	cbTestSecret    = "yeagrow-exec-plane-callback-test-secret-0001"
	cbTestTimestamp = int64(1794400000)
	cbTestTaskID    = "imgtask_0123456789abcdef0123456789abcdef"
	cbTestRunID     = "run_exec_plane_1"
)

var cbVectors = []struct {
	name, body, signature string
	task                  *ImageTaskRecord
}{
	{
		name:      "uncertain",
		body:      `{"event_id":"evt_` + cbTestTaskID + `","task_id":"` + cbTestTaskID + `","run_id":"run_exec_plane_1","attempt":1,"status":"uncertain","upstream":{"http_status":502},"assets":[],"error":{"type":"upstream_5xx","message":"upstream_5xx: Upstream service temporarily unavailable"}}`,
		signature: "1a0b1a3c025765b6385a4fceca560828c26556c7569f798e95ee6f86dbcb7177",
		task: &ImageTaskRecord{ID: cbTestTaskID, RunID: cbTestRunID, Status: ImageTaskStatusFailed,
			Error:     json.RawMessage(`{"type":"upstream_error","message":"Upstream service temporarily unavailable"}`),
			Execution: &ImageTaskExecution{Outcome: ImageTaskOutcomeUncertain, Reason: ImageTaskReasonUpstream5xx, Dispatched: true, UpstreamCalls: 1, LastUpstreamStatus: 502}},
	},
	{
		name:      "rejected",
		body:      `{"event_id":"evt_` + cbTestTaskID + `","task_id":"` + cbTestTaskID + `","run_id":"run_exec_plane_1","attempt":1,"status":"rejected","upstream":{"http_status":429},"assets":[],"error":{"type":"rate_limit","message":"upstream_rejected: Rate limit exceeded"}}`,
		signature: "a10d1273f91d29484f46933e81e0d683184b3f7232652cb74995a32fb6fa0e3e",
		task: &ImageTaskRecord{ID: cbTestTaskID, RunID: cbTestRunID, Status: ImageTaskStatusFailed,
			Error:     json.RawMessage(`{"type":"rate_limit_error","message":"Rate limit exceeded"}`),
			Execution: &ImageTaskExecution{Outcome: ImageTaskOutcomeRejected, Reason: ImageTaskReasonUpstreamRejected, Dispatched: true, UpstreamCalls: 2, LastUpstreamStatus: 429}},
	},
	{
		name:      "not_sent",
		body:      `{"event_id":"evt_` + cbTestTaskID + `","task_id":"` + cbTestTaskID + `","run_id":"run_exec_plane_1","attempt":1,"status":"not_sent","assets":[],"error":{"type":"no_account","message":"no_account: No available compatible accounts"}}`,
		signature: "a63a8bd1f0f5afdc6b351517e0519660e2673604242f62d3cac5dd4b35de31a1",
		task: &ImageTaskRecord{ID: cbTestTaskID, RunID: cbTestRunID, Status: ImageTaskStatusFailed,
			Error:     json.RawMessage(`{"type":"api_error","message":"No available compatible accounts"}`),
			Execution: &ImageTaskExecution{Outcome: ImageTaskOutcomeNotSent, Reason: ImageTaskReasonNoAccount}},
	},
}

// independentCallbackSignature is a second, separately written implementation
// (single message buffer, fmt hex) used to cross-check SignImageTaskCallback.
func independentCallbackSignature(secret, timestamp string, body []byte) string {
	msg := append([]byte(timestamp+"."), body...)
	h := hmac.New(sha256.New, []byte(secret))
	_, _ = h.Write(msg)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func TestImageTaskCallbackSignatureMatchesYeaGrowVectors(t *testing.T) {
	for _, v := range cbVectors {
		t.Run(v.name, func(t *testing.T) {
			event, err := BuildImageTaskCallbackEvent(v.task)
			require.NoError(t, err)
			body, err := json.Marshal(event)
			require.NoError(t, err)
			require.Equal(t, v.body, string(body), "Go body must be byte-identical to the body YeaGrow verified")

			require.Equal(t, v.signature, SignImageTaskCallback(cbTestSecret, cbTestTimestamp, body))
			require.Equal(t, v.signature, independentCallbackSignature(cbTestSecret, "1794400000", body))
		})
	}
}

func TestImageTaskCallbackSignatureDetectsTampering(t *testing.T) {
	body := []byte(cbVectors[0].body)
	sig := SignImageTaskCallback(cbTestSecret, cbTestTimestamp, body)
	require.NotEqual(t, sig, SignImageTaskCallback(cbTestSecret, cbTestTimestamp+1, body))
	require.NotEqual(t, sig, SignImageTaskCallback(cbTestSecret+"x", cbTestTimestamp, body))
	require.NotEqual(t, sig, SignImageTaskCallback(cbTestSecret, cbTestTimestamp, append([]byte{}, append(body, ' ')...)))
	require.Regexp(t, `^[0-9a-f]{64}$`, sig)
}

func TestBuildImageTaskCallbackEventSucceeded(t *testing.T) {
	asset := ImageTaskAsset{R2Key: ImageTaskAssetKey(cbTestRunID, 0), MIME: "image/png", Width: 1024, Height: 1024, Bytes: 1234567, SHA256: strings.Repeat("a", 64), Cleaned: true}
	event, err := BuildImageTaskCallbackEvent(&ImageTaskRecord{ID: cbTestTaskID, RunID: cbTestRunID, Status: ImageTaskStatusCompleted,
		Execution: &ImageTaskExecution{Outcome: ImageTaskOutcomeSucceeded, Dispatched: true, UpstreamCalls: 1, LastUpstreamStatus: 200},
		Assets:    []ImageTaskAsset{asset}})
	require.NoError(t, err)
	require.Equal(t, "generated/run_exec_plane_1/image-0", asset.R2Key)
	body, err := json.Marshal(event)
	require.NoError(t, err)
	require.Equal(t, `{"event_id":"evt_`+cbTestTaskID+`","task_id":"`+cbTestTaskID+`","run_id":"run_exec_plane_1","attempt":1,"status":"succeeded","upstream":{"http_status":200},"assets":[{"r2_key":"generated/run_exec_plane_1/image-0","mime":"image/png","width":1024,"height":1024,"bytes":1234567,"sha256":"`+strings.Repeat("a", 64)+`","cleaned":true}]}`, string(body))

	// Strict field set: exactly the contract keys.
	var generic map[string]any
	require.NoError(t, json.Unmarshal(body, &generic))
	for key := range generic {
		require.Contains(t, []string{"event_id", "task_id", "run_id", "attempt", "status", "upstream", "assets", "error"}, key)
	}
}

func TestBuildImageTaskCallbackEventErrorTypes(t *testing.T) {
	cases := []struct {
		exec ImageTaskExecution
		want string
	}{
		{ImageTaskExecution{Outcome: ImageTaskOutcomeNotSent, Reason: ImageTaskReasonNoAccount}, "no_account"},
		{ImageTaskExecution{Outcome: ImageTaskOutcomeNotSent, Reason: ImageTaskReasonConnectFailed}, "no_account"},
		{ImageTaskExecution{Outcome: ImageTaskOutcomeNotSent, Reason: ImageTaskReasonTimeout}, "timeout"},
		{ImageTaskExecution{Outcome: ImageTaskOutcomeRejected, Reason: ImageTaskReasonUpstreamRejected, LastUpstreamStatus: 429}, "rate_limit"},
		{ImageTaskExecution{Outcome: ImageTaskOutcomeRejected, Reason: ImageTaskReasonUpstreamRejected, LastUpstreamStatus: 400}, "invalid_request"},
		{ImageTaskExecution{Outcome: ImageTaskOutcomeUncertain, Reason: ImageTaskReasonTimeout}, "timeout"},
		{ImageTaskExecution{Outcome: ImageTaskOutcomeUncertain, Reason: ImageTaskReasonUpstream5xx}, "upstream_5xx"},
		{ImageTaskExecution{Outcome: ImageTaskOutcomeUncertain, Reason: ImageTaskReasonStorageFailed}, "upstream_5xx"},
	}
	for _, tc := range cases {
		exec := tc.exec
		event, err := BuildImageTaskCallbackEvent(&ImageTaskRecord{ID: cbTestTaskID, RunID: cbTestRunID, Execution: &exec})
		require.NoError(t, err)
		require.Equal(t, tc.want, event.Error.Type, "%+v", tc.exec)
		require.Empty(t, event.Assets)
		require.NotNil(t, event.Assets)
	}
}

func TestBuildImageTaskCallbackEventRequiresOutcomeAndRunID(t *testing.T) {
	_, err := BuildImageTaskCallbackEvent(&ImageTaskRecord{ID: cbTestTaskID, RunID: cbTestRunID})
	require.Error(t, err, "safe mode off: no status to report")
	_, err = BuildImageTaskCallbackEvent(&ImageTaskRecord{ID: cbTestTaskID, Execution: &ImageTaskExecution{Outcome: ImageTaskOutcomeNotSent}})
	require.Error(t, err)
	_, err = BuildImageTaskCallbackEvent(&ImageTaskRecord{ID: cbTestTaskID, RunID: "../x", Execution: &ImageTaskExecution{Outcome: ImageTaskOutcomeNotSent}})
	require.Error(t, err)
}

func TestImageTaskCallbackErrorMessageIsPrintableAndBounded(t *testing.T) {
	msg := imageTaskCallbackErrorMessage(&ImageTaskExecution{Reason: "upstream_5xx"},
		json.RawMessage(`{"message":"line1\nline2 中 `+strings.Repeat("x", 2000)+`"}`))
	require.LessOrEqual(t, len(msg), imageTaskCallbackMaxMessage)
	for _, r := range msg {
		require.True(t, r >= 0x20 && r <= 0x7e)
	}
	require.True(t, strings.HasPrefix(msg, "upstream_5xx: line1?line2 ? "))
}

func TestIsValidImageTaskRunID(t *testing.T) {
	for _, ok := range []string{"run_1", "a", "run-abc.1:2", strings.Repeat("a", 128)} {
		require.True(t, IsValidImageTaskRunID(ok), ok)
	}
	for _, bad := range []string{"", "_run", "../x", "run/1", "run 1", strings.Repeat("a", 129)} {
		require.False(t, IsValidImageTaskRunID(bad), bad)
	}
}

func TestImageTaskCallbackBackoffSchedule(t *testing.T) {
	want := []time.Duration{10 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute, 30 * time.Minute, 2 * time.Hour, 2 * time.Hour, 2 * time.Hour}
	for i, d := range want {
		require.Equal(t, d, ImageTaskCallbackBackoff(i+1), "failure %d", i+1)
	}
}

func TestParseImageTaskCallbackConfig(t *testing.T) {
	secret := strings.Repeat("s", 32)
	cfg, err := ParseImageTaskCallbackConfig([]byte(`
- api_key_id: 12
  url: https://yeagrow.example/api/internal/execution-plane/callback
  secret: `+secret+`
`), false)
	require.NoError(t, err)
	target, ok := cfg.Target(12)
	require.True(t, ok)
	require.Equal(t, secret, target.Secret)
	_, ok = cfg.Target(13)
	require.False(t, ok)

	jsonCfg, err := ParseImageTaskCallbackConfig([]byte(`[{"api_key_id":5,"url":"https://a.example/cb","secret":"`+secret+`"}]`), false)
	require.NoError(t, err)
	require.Equal(t, 1, jsonCfg.Len())

	empty, err := ParseImageTaskCallbackConfig([]byte(""), false)
	require.NoError(t, err)
	require.Zero(t, empty.Len())

	bad := map[string]string{
		"http":        `[{"api_key_id":1,"url":"http://a.example/cb","secret":"` + secret + `"}]`,
		"relative":    `[{"api_key_id":1,"url":"/cb","secret":"` + secret + `"}]`,
		"short":       `[{"api_key_id":1,"url":"https://a.example/cb","secret":"short"}]`,
		"zero id":     `[{"api_key_id":0,"url":"https://a.example/cb","secret":"` + secret + `"}]`,
		"duplicate":   `[{"api_key_id":1,"url":"https://a.example/cb","secret":"` + secret + `"},{"api_key_id":1,"url":"https://b.example/cb","secret":"` + secret + `"}]`,
		"extra field": `[{"api_key_id":1,"url":"https://a.example/cb","secret":"` + secret + `","sekret":"x"}]`,
	}
	for name, raw := range bad {
		_, err := ParseImageTaskCallbackConfig([]byte(raw), false)
		require.Error(t, err, name)
	}
	_, err = ParseImageTaskCallbackConfig([]byte(bad["http"]), true)
	require.NoError(t, err, "http allowed only with the explicit flag")
}

func TestLoadImageTaskCallbackConfigFromEnv(t *testing.T) {
	secret := strings.Repeat("s", 32)
	t.Setenv(ImageTaskCallbacksEnv, "")
	t.Setenv(ImageTaskCallbacksFileEnv, "")
	cfg, err := LoadImageTaskCallbackConfigFromEnv()
	require.NoError(t, err)
	require.Nil(t, cfg)

	path := filepath.Join(t.TempDir(), "callbacks.yaml")
	require.NoError(t, os.WriteFile(path, []byte("- api_key_id: 3\n  url: https://a.example/cb\n  secret: "+secret+"\n"), 0o600))
	t.Setenv(ImageTaskCallbacksFileEnv, path)
	cfg, err = LoadImageTaskCallbackConfigFromEnv()
	require.NoError(t, err)
	require.Equal(t, 1, cfg.Len())

	t.Setenv(ImageTaskCallbacksEnv, `[]`)
	_, err = LoadImageTaskCallbackConfigFromEnv()
	require.Error(t, err, "both sources set")

	t.Setenv(ImageTaskCallbacksFileEnv, "")
	t.Setenv(ImageTaskCallbacksEnv, `[{"api_key_id":4,"url":"http://127.0.0.1:9/cb","secret":"`+secret+`"}]`)
	_, err = LoadImageTaskCallbackConfigFromEnv()
	require.Error(t, err)
	t.Setenv(ImageTaskCallbacksAllowHTTPEnv, "true")
	cfg, err = LoadImageTaskCallbackConfigFromEnv()
	require.NoError(t, err)
	require.Equal(t, 1, cfg.Len())
}

// memCallbackQueue is an in-memory ImageTaskCallbackQueue for deliverer logic tests.
type memCallbackQueue struct {
	mu        sync.Mutex
	due       map[string]time.Time
	events    map[string]ImageTaskCallbackEnvelope
	dead      map[string]string
	lastError map[string]string
}

func newMemCallbackQueue() *memCallbackQueue {
	return &memCallbackQueue{due: map[string]time.Time{}, events: map[string]ImageTaskCallbackEnvelope{}, dead: map[string]string{}, lastError: map[string]string{}}
}

func (q *memCallbackQueue) add(env ImageTaskCallbackEnvelope) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.events[env.EventID] = env
	q.due[env.EventID] = env.CreatedAt
}

func (q *memCallbackQueue) ClaimDueCallbacks(_ context.Context, now time.Time, lease time.Duration, limit int) ([]ImageTaskCallbackEnvelope, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var out []ImageTaskCallbackEnvelope
	for id, at := range q.due {
		if len(out) >= limit {
			break
		}
		if !at.After(now) {
			q.due[id] = now.Add(lease)
			out = append(out, q.events[id])
		}
	}
	return out, nil
}

func (q *memCallbackQueue) AckCallback(_ context.Context, id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.due, id)
	delete(q.events, id)
	return nil
}

func (q *memCallbackQueue) RescheduleCallback(_ context.Context, id string, failures int, next time.Time, lastError string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	env := q.events[id]
	env.Failures = failures
	q.events[id] = env
	q.due[id] = next
	q.lastError[id] = lastError
	return nil
}

func (q *memCallbackQueue) AbandonCallback(_ context.Context, id string, failures int, reason string, _ time.Time) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.due, id)
	q.dead[id] = reason
	env := q.events[id]
	env.Failures = failures
	q.events[id] = env
	return nil
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func TestImageTaskCallbackDelivererRetriesOnScheduleUntilGiveUp(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Header.Get("X-Exec-Signature"))
		mu.Unlock()
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer receiver.Close()

	secret := strings.Repeat("s", 32)
	cfg, err := ParseImageTaskCallbackConfig([]byte(`[{"api_key_id":9,"url":"`+receiver.URL+`","secret":"`+secret+`"}]`), true)
	require.NoError(t, err)
	clock := &fakeClock{t: time.Unix(1_800_000_000, 0)}
	created := clock.Now()
	queue := newMemCallbackQueue()
	queue.add(ImageTaskCallbackEnvelope{EventID: "evt_x", APIKeyID: 9, Body: []byte(`{"event_id":"evt_x"}`), CreatedAt: created})
	d := NewImageTaskCallbackDeliverer(queue, cfg, nil)
	d.SetClock(clock.Now)

	var offsets []time.Duration
	for i := 0; i < 40; i++ {
		stats, err := d.RunOnce(context.Background())
		require.NoError(t, err)
		if stats.Abandoned == 1 {
			break
		}
		require.Equal(t, 1, stats.Retried)
		queue.mu.Lock()
		next := queue.due["evt_x"]
		queue.mu.Unlock()
		offsets = append(offsets, next.Sub(clock.Now()))
		clock.Advance(next.Sub(clock.Now()))
	}
	require.Equal(t, []time.Duration{10 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute, 30 * time.Minute, 2 * time.Hour}, offsets[:6])
	require.Contains(t, queue.dead, "evt_x")
	require.False(t, clock.Now().Before(created.Add(24*time.Hour)), "abandoned no earlier than 24h")
	require.Equal(t, created.Add(24*time.Hour), clock.Now(), "the last attempt is exactly at the 24h mark")
	require.Equal(t, len(offsets)+1, len(seen))
}

func TestImageTaskCallbackDelivererOnly2xxCounts(t *testing.T) {
	secret := strings.Repeat("s", 32)
	for _, code := range []int{http.StatusOK, http.StatusNoContent, http.StatusFound, http.StatusBadRequest, http.StatusUnauthorized} {
		receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if code == http.StatusFound {
				w.Header().Set("Location", "/elsewhere")
			}
			w.WriteHeader(code)
		}))
		cfg, err := ParseImageTaskCallbackConfig([]byte(`[{"api_key_id":9,"url":"`+receiver.URL+`","secret":"`+secret+`"}]`), true)
		require.NoError(t, err)
		queue := newMemCallbackQueue()
		queue.add(ImageTaskCallbackEnvelope{EventID: "evt_x", APIKeyID: 9, Body: []byte(`{}`), CreatedAt: time.Now()})
		stats, err := NewImageTaskCallbackDeliverer(queue, cfg, nil).RunOnce(context.Background())
		require.NoError(t, err)
		if code >= 200 && code < 300 {
			require.Equal(t, 1, stats.Delivered, "code %d", code)
			require.Empty(t, queue.events)
		} else {
			require.Equal(t, 1, stats.Retried, "code %d", code)
			require.Contains(t, queue.lastError["evt_x"], fmt.Sprint(code))
		}
		receiver.Close()
	}
}

func TestImageTaskCallbackDelivererUnreachableAndNoTarget(t *testing.T) {
	secret := strings.Repeat("s", 32)
	cfg, err := ParseImageTaskCallbackConfig([]byte(`[{"api_key_id":9,"url":"http://127.0.0.1:1/cb","secret":"`+secret+`"}]`), true)
	require.NoError(t, err)
	queue := newMemCallbackQueue()
	queue.add(ImageTaskCallbackEnvelope{EventID: "evt_a", APIKeyID: 9, Body: []byte(`{}`), CreatedAt: time.Now()})
	queue.add(ImageTaskCallbackEnvelope{EventID: "evt_b", APIKeyID: 77, Body: []byte(`{}`), CreatedAt: time.Now()})
	stats, err := NewImageTaskCallbackDeliverer(queue, cfg, nil).RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, stats.Retried)
	require.Equal(t, 1, stats.Abandoned)
	require.Equal(t, "no_callback_target", queue.dead["evt_b"])
}

// finishStoreFake records FinishProcessing calls (service-level wiring test).
type finishStoreFake struct {
	*idemTaskStore
	mu     sync.Mutex
	events []*ImageTaskCallbackEnvelope
}

func (s *finishStoreFake) FinishProcessing(_ context.Context, next *ImageTaskRecord, _ time.Duration, event *ImageTaskCallbackEnvelope) (ImageTaskFinishResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.idemTaskStore.mu.Lock()
	defer s.idemTaskStore.mu.Unlock()
	cur, ok := s.tasks[next.ID]
	if !ok {
		return ImageTaskFinishMissing, nil
	}
	if cur.Status != ImageTaskStatusProcessing {
		return ImageTaskFinishNotProcessing, nil
	}
	copy := *next
	s.tasks[next.ID] = &copy
	if event != nil {
		s.events = append(s.events, event)
	}
	return ImageTaskFinishApplied, nil
}

func TestImageTaskServiceFinishEnqueuesOneEventAndRejectsLateFinish(t *testing.T) {
	store := &finishStoreFake{idemTaskStore: newIdemTaskStore()}
	svc := NewImageTaskServiceWithUploader(store, nil, time.Hour, time.Minute)
	cfg, err := ParseImageTaskCallbackConfig([]byte(`[{"api_key_id":9,"url":"https://a.example/cb","secret":"`+strings.Repeat("s", 32)+`"}]`), false)
	require.NoError(t, err)
	svc.SetCallbackConfig(cfg)
	require.NoError(t, store.Save(context.Background(), &ImageTaskRecord{ID: cbTestTaskID, UserID: 7, APIKeyID: 9, RunID: cbTestRunID, Status: ImageTaskStatusProcessing}, time.Hour))

	exec := &ImageTaskExecution{Outcome: ImageTaskOutcomeUncertain, Reason: ImageTaskReasonUpstream5xx, Dispatched: true, UpstreamCalls: 1, LastUpstreamStatus: 502}
	require.NoError(t, svc.FailWithExecution(context.Background(), cbTestTaskID, 502, json.RawMessage(`{"type":"upstream_error","message":"Upstream service temporarily unavailable"}`), exec))
	require.Len(t, store.events, 1)
	require.Equal(t, cbVectors[0].body, string(store.events[0].Body))
	require.Equal(t, "evt_"+cbTestTaskID, store.events[0].EventID)

	late := svc.CompleteWithExecution(context.Background(), cbTestTaskID, 200, json.RawMessage(`{"data":[]}`), &ImageTaskExecution{Outcome: ImageTaskOutcomeSucceeded})
	require.True(t, errors.Is(late, ErrImageTaskAlreadyFinished), "late finish must be rejected, got %v", late)
	require.Len(t, store.events, 1)
	got, err := store.Get(context.Background(), cbTestTaskID)
	require.NoError(t, err)
	require.Equal(t, ImageTaskStatusFailed, got.Status)
}

func TestImageTaskServiceFinishWithoutCallbackTarget(t *testing.T) {
	store := &finishStoreFake{idemTaskStore: newIdemTaskStore()}
	svc := NewImageTaskServiceWithUploader(store, nil, time.Hour, time.Minute)
	require.NoError(t, store.Save(context.Background(), &ImageTaskRecord{ID: "imgtask_a", UserID: 7, APIKeyID: 9, RunID: cbTestRunID, Status: ImageTaskStatusProcessing}, time.Hour))
	require.NoError(t, svc.Fail(context.Background(), "imgtask_a", 502, nil))
	require.Empty(t, store.events)
	require.False(t, svc.CallbacksConfiguredFor(9))
}

func TestImageTaskServiceLegacyStoreAlsoRejectsLateFinish(t *testing.T) {
	store := newIdemTaskStore()
	svc := NewImageTaskServiceWithUploader(store, nil, time.Hour, time.Minute)
	require.NoError(t, store.Save(context.Background(), &ImageTaskRecord{ID: "imgtask_a", UserID: 7, APIKeyID: 9, Status: ImageTaskStatusProcessing}, time.Hour))
	require.NoError(t, svc.Fail(context.Background(), "imgtask_a", 502, nil))
	err := svc.Complete(context.Background(), "imgtask_a", 200, json.RawMessage(`{"data":[]}`))
	require.True(t, errors.Is(err, ErrImageTaskAlreadyFinished))
}
