package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// End-to-end tests for the YeaGrow execution-plane safe mode: the async image
// task runs through the real OpenAIGatewayHandler.Images loop and
// OpenAIGatewayService.ForwardImages against a scripted upstream transport.

const execPlanePNG1x1 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="

type execPlaneAccountRepo struct {
	service.AccountRepository
	accounts []service.Account
}

func (r execPlaneAccountRepo) GetByID(_ context.Context, id int64) (*service.Account, error) {
	for i := range r.accounts {
		if r.accounts[i].ID == id {
			account := r.accounts[i]
			return &account, nil
		}
	}
	return nil, service.ErrNoAvailableAccounts
}

func (r execPlaneAccountRepo) ListSchedulableByGroupIDAndPlatform(_ context.Context, _ int64, platform string) ([]service.Account, error) {
	return r.forPlatform(platform), nil
}

func (r execPlaneAccountRepo) ListSchedulableByPlatform(_ context.Context, platform string) ([]service.Account, error) {
	return r.forPlatform(platform), nil
}

func (r execPlaneAccountRepo) ListSchedulableUngroupedByPlatform(_ context.Context, platform string) ([]service.Account, error) {
	return r.forPlatform(platform), nil
}

func (r execPlaneAccountRepo) ListModelAvailabilityCandidates(_ context.Context, _ *int64, platforms []string, _ bool) ([]service.Account, error) {
	var out []service.Account
	for _, p := range platforms {
		out = append(out, r.forPlatform(p)...)
	}
	return out, nil
}

func (r execPlaneAccountRepo) forPlatform(platform string) []service.Account {
	out := make([]service.Account, 0, len(r.accounts))
	for _, account := range r.accounts {
		if account.Platform == platform {
			out = append(out, account)
		}
	}
	return out
}

type execPlaneStep func(req *http.Request) (*http.Response, error)

// execPlaneUpstream answers each account with its scripted step (the last step
// repeats) and records the order of calls.
type execPlaneUpstream struct {
	service.HTTPUpstream
	mu    sync.Mutex
	steps map[int64][]execPlaneStep
	calls []int64
}

func (u *execPlaneUpstream) Do(req *http.Request, _ string, accountID int64, _ int) (*http.Response, error) {
	u.mu.Lock()
	u.calls = append(u.calls, accountID)
	steps := u.steps[accountID]
	n := 0
	for _, id := range u.calls {
		if id == accountID {
			n++
		}
	}
	u.mu.Unlock()
	if len(steps) == 0 {
		return nil, errors.New("no scripted response")
	}
	if n > len(steps) {
		n = len(steps)
	}
	return steps[n-1](req)
}

func (u *execPlaneUpstream) callLog() []int64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]int64(nil), u.calls...)
}

func execPlaneJSON(status int, body string) execPlaneStep {
	return func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: status,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(body)),
		}, nil
	}
}

func execPlaneSuccess() execPlaneStep {
	return execPlaneJSON(http.StatusOK, `{"id":"up-task-1","created":1,"data":[{"b64_json":"`+execPlanePNG1x1+`"}]}`)
}

func execPlaneErr(err error) execPlaneStep {
	return func(*http.Request) (*http.Response, error) { return nil, err }
}

type execPlaneBrokenBody struct{}

func (execPlaneBrokenBody) Read([]byte) (int, error) {
	return 0, errors.New("connection reset by peer")
}
func (execPlaneBrokenBody) Close() error { return nil }

func execPlaneDialError() error {
	return &url.Error{Op: "Post", URL: "https://upstream.test/v1/images/generations", Err: &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}}
}

func execPlaneAPIKeyAccount(id int64, priority int, extra map[string]any) service.Account {
	creds := map[string]any{"api_key": "sk-test", "base_url": "https://upstream.test"}
	for k, v := range extra {
		creds[k] = v
	}
	return service.Account{
		ID:          id,
		Name:        "image-apikey",
		Platform:    service.PlatformOpenAI,
		Type:        service.AccountTypeAPIKey,
		Status:      service.StatusActive,
		Schedulable: true,
		Priority:    priority,
		Credentials: creds,
	}
}

type execPlaneHarness struct {
	t        *testing.T
	router   *gin.Engine
	tasks    *service.ImageTaskService
	upstream *execPlaneUpstream
	platform string
}

func newExecPlaneHarness(t *testing.T, safeMode bool, accounts []service.Account, steps map[int64][]execPlaneStep, executionTimeout time.Duration) *execPlaneHarness {
	t.Helper()
	gin.SetMode(gin.TestMode)
	upstream := &execPlaneUpstream{steps: steps}
	cfg := &config.Config{RunMode: config.RunModeSimple}
	cfg.Security.URLAllowlist.Enabled = false
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	gatewayService := service.NewOpenAIGatewayService(
		execPlaneAccountRepo{accounts: accounts},
		nil, nil, nil, nil, nil, nil,
		cfg,
		nil, nil, nil, nil, nil,
		upstream,
		nil, nil, nil, nil, nil, nil, nil, nil,
	)
	billingService := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
	t.Cleanup(billingService.Stop)
	gateway := NewOpenAIGatewayHandler(
		gatewayService,
		service.NewConcurrencyService(nil),
		billingService,
		service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg),
		nil, nil, nil, nil,
		cfg,
	)
	gateway.maxAccountSwitches = 10

	store := &asyncImageMemoryStore{tasks: make(map[string]*service.ImageTaskRecord)}
	tasks := service.NewImageTaskServiceWithUploader(store, nil, time.Hour, executionTimeout)
	async := NewAsyncImageHandler(tasks, gateway)
	async.safeMode = safeMode

	h := &execPlaneHarness{t: t, tasks: tasks, upstream: upstream, platform: service.PlatformOpenAI}
	router := gin.New()
	router.Use(func(c *gin.Context) {
		groupID := int64(42)
		c.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{
			ID:      9,
			UserID:  7,
			GroupID: &groupID,
			Group:   &service.Group{ID: groupID, Platform: h.platform, AllowImageGeneration: true},
			User:    &service.User{ID: 7},
		})
		c.Set(string(middleware2.ContextKeyUser), middleware2.AuthSubject{UserID: 7})
		c.Next()
	})
	router.POST("/v1/images/generations/async", async.Submit)
	h.router = router
	return h
}

func (h *execPlaneHarness) submit() *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations/async",
		bytes.NewBufferString(`{"model":"gpt-image-1","prompt":"cat","n":1,"size":"1024x1024","response_format":"b64_json"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.router.ServeHTTP(w, req)
	return w
}

func (h *execPlaneHarness) run() *service.ImageTask {
	h.t.Helper()
	w := h.submit()
	require.Equal(h.t, http.StatusAccepted, w.Code, w.Body.String())
	var accepted struct {
		TaskID string `json:"task_id"`
	}
	require.NoError(h.t, json.Unmarshal(w.Body.Bytes(), &accepted))
	var task *service.ImageTask
	require.Eventually(h.t, func() bool {
		got, err := h.tasks.Get(context.Background(), service.ImageTaskOwner{UserID: 7, APIKeyID: 9}, accepted.TaskID)
		if err != nil || got.Status == service.ImageTaskStatusProcessing {
			return false
		}
		task = got
		return true
	}, 5*time.Second, 10*time.Millisecond)
	return task
}

func TestExecutionPlaneSafeMode_5xxDoesNotSwitchAccount(t *testing.T) {
	accounts := []service.Account{execPlaneAPIKeyAccount(1, 0, nil), execPlaneAPIKeyAccount(2, 1, nil)}
	steps := map[int64][]execPlaneStep{
		1: {execPlaneJSON(http.StatusInternalServerError, `{"error":{"message":"boom","type":"server_error"}}`)},
		2: {execPlaneSuccess()},
	}
	h := newExecPlaneHarness(t, true, accounts, steps, time.Minute)
	task := h.run()

	require.Equal(t, []int64{1}, h.upstream.callLog(), "5xx must not be replayed on another account")
	require.Equal(t, service.ImageTaskStatusFailed, task.Status)
	require.NotNil(t, task.Execution)
	require.Equal(t, service.ImageTaskOutcomeUncertain, task.Execution.Outcome)
	require.Equal(t, service.ImageTaskReasonUpstream5xx, task.Execution.Reason)
	require.True(t, task.Execution.Dispatched)
	require.Equal(t, 1, task.Execution.UpstreamCalls)
	require.Equal(t, http.StatusInternalServerError, task.Execution.LastUpstreamStatus)
}

func TestExecutionPlaneSafeMode_PoolModeDoesNotRetrySameAccount(t *testing.T) {
	accounts := []service.Account{execPlaneAPIKeyAccount(1, 0, map[string]any{"pool_mode": true})}
	steps := map[int64][]execPlaneStep{
		1: {execPlaneJSON(http.StatusServiceUnavailable, `{"error":{"message":"busy","type":"server_error"}}`), execPlaneSuccess()},
	}
	h := newExecPlaneHarness(t, true, accounts, steps, time.Minute)
	task := h.run()

	require.Equal(t, []int64{1}, h.upstream.callLog(), "pool mode same-account retry must be off in safe mode")
	require.Equal(t, service.ImageTaskOutcomeUncertain, task.Execution.Outcome)
}

func TestExecutionPlaneSafeMode_429SwitchesAccount(t *testing.T) {
	accounts := []service.Account{execPlaneAPIKeyAccount(1, 0, nil), execPlaneAPIKeyAccount(2, 1, nil)}
	steps := map[int64][]execPlaneStep{
		1: {execPlaneJSON(http.StatusTooManyRequests, `{"error":{"message":"slow down","type":"rate_limit_error"}}`)},
		2: {execPlaneSuccess()},
	}
	h := newExecPlaneHarness(t, true, accounts, steps, time.Minute)
	task := h.run()

	require.Equal(t, []int64{1, 2}, h.upstream.callLog())
	require.Equal(t, service.ImageTaskStatusCompleted, task.Status)
	require.Equal(t, service.ImageTaskOutcomeSucceeded, task.Execution.Outcome)
	require.Empty(t, task.Execution.Reason)
	require.Equal(t, 2, task.Execution.UpstreamCalls)
	require.Equal(t, http.StatusOK, task.Execution.LastUpstreamStatus)
}

func TestExecutionPlaneSafeMode_429OnEveryAccountIsRejected(t *testing.T) {
	accounts := []service.Account{execPlaneAPIKeyAccount(1, 0, nil), execPlaneAPIKeyAccount(2, 1, nil)}
	rateLimited := execPlaneJSON(http.StatusTooManyRequests, `{"error":{"message":"slow down","type":"rate_limit_error"}}`)
	steps := map[int64][]execPlaneStep{1: {rateLimited}, 2: {rateLimited}}
	h := newExecPlaneHarness(t, true, accounts, steps, time.Minute)
	task := h.run()

	require.Equal(t, []int64{1, 2}, h.upstream.callLog())
	require.Equal(t, service.ImageTaskStatusFailed, task.Status)
	require.Equal(t, service.ImageTaskOutcomeRejected, task.Execution.Outcome)
	require.Equal(t, service.ImageTaskReasonUpstreamRejected, task.Execution.Reason)
}

func TestExecutionPlaneSafeMode_ConnectFailureSwitchesAccount(t *testing.T) {
	accounts := []service.Account{execPlaneAPIKeyAccount(1, 0, nil), execPlaneAPIKeyAccount(2, 1, nil)}
	steps := map[int64][]execPlaneStep{
		1: {execPlaneErr(execPlaneDialError())},
		2: {execPlaneSuccess()},
	}
	h := newExecPlaneHarness(t, true, accounts, steps, time.Minute)
	task := h.run()

	require.Equal(t, []int64{1, 2}, h.upstream.callLog())
	require.Equal(t, service.ImageTaskOutcomeSucceeded, task.Execution.Outcome)
}

func TestExecutionPlaneSafeMode_BodyReadFailureIsUncertain(t *testing.T) {
	accounts := []service.Account{execPlaneAPIKeyAccount(1, 0, nil), execPlaneAPIKeyAccount(2, 1, nil)}
	steps := map[int64][]execPlaneStep{
		1: {func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: execPlaneBrokenBody{}}, nil
		}},
		2: {execPlaneSuccess()},
	}
	h := newExecPlaneHarness(t, true, accounts, steps, time.Minute)
	task := h.run()

	require.Equal(t, []int64{1}, h.upstream.callLog(), "a 2xx whose body could not be read must not be replayed")
	require.Equal(t, service.ImageTaskStatusFailed, task.Status)
	require.Equal(t, service.ImageTaskOutcomeUncertain, task.Execution.Outcome)
	require.Equal(t, service.ImageTaskReasonResponseUnconfirmed, task.Execution.Reason)
}

func TestExecutionPlaneSafeMode_TimeoutIsUncertain(t *testing.T) {
	accounts := []service.Account{execPlaneAPIKeyAccount(1, 0, nil), execPlaneAPIKeyAccount(2, 1, nil)}
	steps := map[int64][]execPlaneStep{
		1: {func(*http.Request) (*http.Response, error) {
			// The task's execution deadline (50ms) passes while the request is in flight.
			time.Sleep(200 * time.Millisecond)
			return nil, &url.Error{Op: "Post", URL: "https://upstream.test/v1/images/generations", Err: context.DeadlineExceeded}
		}},
		2: {execPlaneSuccess()},
	}
	h := newExecPlaneHarness(t, true, accounts, steps, 50*time.Millisecond)
	task := h.run()

	require.Equal(t, []int64{1}, h.upstream.callLog())
	require.Equal(t, service.ImageTaskStatusFailed, task.Status)
	require.Equal(t, service.ImageTaskOutcomeUncertain, task.Execution.Outcome)
	require.Equal(t, service.ImageTaskReasonTimeout, task.Execution.Reason)
	require.True(t, task.Execution.Dispatched)
}

func TestExecutionPlaneSafeMode_NoAccountIsNotSent(t *testing.T) {
	h := newExecPlaneHarness(t, true, nil, nil, time.Minute)
	task := h.run()

	require.Empty(t, h.upstream.callLog())
	require.Equal(t, http.StatusServiceUnavailable, task.HTTPStatus)
	require.Contains(t, string(task.Error), "No available compatible accounts")
	require.Equal(t, service.ImageTaskStatusFailed, task.Status)
	require.Equal(t, service.ImageTaskOutcomeNotSent, task.Execution.Outcome)
	require.Equal(t, service.ImageTaskReasonNoAccount, task.Execution.Reason)
	require.False(t, task.Execution.Dispatched)
	require.Zero(t, task.Execution.UpstreamCalls)
}

func TestExecutionPlaneSafeMode_RejectsGrokPlatform(t *testing.T) {
	h := newExecPlaneHarness(t, true, nil, nil, time.Minute)
	h.platform = service.PlatformGrok
	w := h.submit()
	require.Equal(t, http.StatusNotFound, w.Code)
	require.Contains(t, w.Body.String(), "safe mode")
}

// Safe mode off: identical to upstream behaviour (5xx fails over, connection
// errors end the task, no execution field).
func TestExecutionPlaneSafeModeOff_KeepsExistingBehaviour(t *testing.T) {
	t.Run("5xx fails over", func(t *testing.T) {
		accounts := []service.Account{execPlaneAPIKeyAccount(1, 0, nil), execPlaneAPIKeyAccount(2, 1, nil)}
		steps := map[int64][]execPlaneStep{
			1: {execPlaneJSON(http.StatusInternalServerError, `{"error":{"message":"boom","type":"server_error"}}`)},
			2: {execPlaneSuccess()},
		}
		h := newExecPlaneHarness(t, false, accounts, steps, time.Minute)
		task := h.run()
		require.Equal(t, []int64{1, 2}, h.upstream.callLog())
		require.Equal(t, service.ImageTaskStatusCompleted, task.Status)
		require.Nil(t, task.Execution)
		raw, err := json.Marshal(task)
		require.NoError(t, err)
		require.NotContains(t, string(raw), "execution")
	})
	t.Run("connect failure does not switch", func(t *testing.T) {
		accounts := []service.Account{execPlaneAPIKeyAccount(1, 0, nil), execPlaneAPIKeyAccount(2, 1, nil)}
		steps := map[int64][]execPlaneStep{
			1: {execPlaneErr(execPlaneDialError())},
			2: {execPlaneSuccess()},
		}
		h := newExecPlaneHarness(t, false, accounts, steps, time.Minute)
		task := h.run()
		require.Equal(t, []int64{1}, h.upstream.callLog())
		require.Equal(t, service.ImageTaskStatusFailed, task.Status)
		require.Nil(t, task.Execution)
	})
	t.Run("no account", func(t *testing.T) {
		h := newExecPlaneHarness(t, false, nil, nil, time.Minute)
		task := h.run()
		require.Equal(t, service.ImageTaskStatusFailed, task.Status)
		require.Nil(t, task.Execution)
	})
	t.Run("grok still accepted", func(t *testing.T) {
		h := newExecPlaneHarness(t, false, nil, nil, time.Minute)
		h.platform = service.PlatformGrok
		w := h.submit()
		require.NotEqual(t, http.StatusNotFound, w.Code, w.Body.String())
	})
}

func TestAsyncImageSafeModeFromEnv(t *testing.T) {
	t.Setenv(service.ImageTaskExecutionPlaneSafeModeEnv, "1")
	require.True(t, NewAsyncImageHandler(nil, nil).safeMode)
	t.Setenv(service.ImageTaskExecutionPlaneSafeModeEnv, "")
	require.False(t, NewAsyncImageHandler(nil, nil).safeMode)
	t.Setenv(service.ImageTaskExecutionPlaneSafeModeEnv, "bogus")
	require.False(t, NewAsyncImageHandler(nil, nil).safeMode)
}
