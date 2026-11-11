package service

// YeaGrow T2.6: X-Client-Request-Id passthrough, upstream ID body fallback,
// Responses probe switch.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestSanitizeCorrelationID(t *testing.T) {
	require.Equal(t, "disp_01HX:abc-1.2", SanitizeCorrelationID("  disp_01HX:abc-1.2 "))
	require.Empty(t, SanitizeCorrelationID(""))
	require.Empty(t, SanitizeCorrelationID("has space"))
	require.Empty(t, SanitizeCorrelationID("new\nline"))
	require.Empty(t, SanitizeCorrelationID("ünicode"))
	long := make([]byte, 129)
	for i := range long {
		long[i] = 'a'
	}
	require.Empty(t, SanitizeCorrelationID(string(long)))
	require.Equal(t, string(long[:128]), SanitizeCorrelationID(string(long[:128])))
}

func TestUpstreamRequestIDFromJSONBody(t *testing.T) {
	require.Equal(t, "fake-AigcImageTask-3", UpstreamRequestIDFromJSONBody([]byte(`{"id":"fake-AigcImageTask-3","data":[]}`)))
	require.Empty(t, UpstreamRequestIDFromJSONBody([]byte(`{"id":12345}`)))
	require.Empty(t, UpstreamRequestIDFromJSONBody([]byte(`{"data":[]}`)))
	require.Empty(t, UpstreamRequestIDFromJSONBody([]byte(`{"id":"bad id"}`)))
	require.Empty(t, UpstreamRequestIDFromJSONBody([]byte(`not json`)))
	require.Empty(t, UpstreamRequestIDFromJSONBody(nil))
}

func TestUsageUpstreamRequestIDFallsBackToBodyID(t *testing.T) {
	withHeader := &Account{Extra: map[string]any{AccountExtraUpstreamRequestIDHeader: "x-request-id"}}
	h := http.Header{}
	h.Set("x-request-id", "req_header_1")

	got := usageUpstreamRequestIDPtrWithBodyFallback(withHeader, h, false, "body_1")
	require.NotNil(t, got)
	require.Equal(t, "req_header_1", *got, "the configured header wins")

	got = usageUpstreamRequestIDPtrWithBodyFallback(withHeader, http.Header{}, false, "body_1")
	require.NotNil(t, got)
	require.Equal(t, "body_1", *got, "missing header falls back to the body id")

	got = usageUpstreamRequestIDPtrWithBodyFallback(&Account{}, h, false, "body_1")
	require.NotNil(t, got)
	require.Equal(t, "body_1", *got, "no configured header also falls back")

	require.Nil(t, usageUpstreamRequestIDPtrWithBodyFallback(&Account{}, nil, false, ""))
	require.Nil(t, usageUpstreamRequestIDPtrWithBodyFallback(&Account{}, nil, false, "bad id"))
	require.Nil(t, usageUpstreamRequestIDPtrWithBodyFallback(&Account{}, nil, true, "body_1"), "WS turns keep nil")
}

func TestAccountResponsesProbeEnabledDefaultsOff(t *testing.T) {
	t.Setenv(AccountResponsesProbeEnabledEnv, "")
	require.False(t, AccountResponsesProbeEnabled())
	t.Setenv(AccountResponsesProbeEnabledEnv, "nope")
	require.False(t, AccountResponsesProbeEnabled())
	t.Setenv(AccountResponsesProbeEnabledEnv, "false")
	require.False(t, AccountResponsesProbeEnabled())
	t.Setenv(AccountResponsesProbeEnabledEnv, "true")
	require.True(t, AccountResponsesProbeEnabled())
}

func t26ImagesRequest(t *testing.T, clientHeaders map[string]string, account *Account) *http.Request {
	t.Helper()
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/generations", nil)
	for k, v := range clientHeaders {
		c.Request.Header.Set(k, v)
	}
	cfg := &config.Config{}
	cfg.Security.URLAllowlist.Enabled = false
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	s := &OpenAIGatewayService{cfg: cfg}
	req, err := s.buildOpenAIImagesRequest(context.Background(), c, account, []byte(`{}`), "application/json", "sk-test", openAIImagesGenerationsEndpoint)
	require.NoError(t, err)
	return req
}

func t26APIKeyAccount(overrides map[string]any) *Account {
	creds := map[string]any{"api_key": "sk-test", "base_url": "http://fakeup:8000"}
	if overrides != nil {
		creds["header_override_enabled"] = true
		creds["header_overrides"] = overrides
	}
	return &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: creds}
}

// t26HeaderValues reads a header regardless of key casing (overrides use wire casing).
func t26HeaderValues(h http.Header, name string) []string {
	var out []string
	for k, v := range h {
		if strings.EqualFold(k, name) {
			out = append(out, v...)
		}
	}
	return out
}

func TestBuildOpenAIImagesRequestForwardsClientRequestID(t *testing.T) {
	req := t26ImagesRequest(t, map[string]string{"X-Client-Request-Id": "disp_01HXYZ"}, t26APIKeyAccount(nil))
	require.Equal(t, "disp_01HXYZ", req.Header.Get("X-Client-Request-Id"))
	require.Len(t, req.Header.Values("X-Client-Request-Id"), 1)
}

func TestBuildOpenAIImagesRequestClientRequestIDSurvivesHeaderOverrides(t *testing.T) {
	// The override layer refuses x-client-request-id outright (blocklist), so an
	// account override can never replace the caller's id; the explicit set after
	// ApplyHeaderOverrides is a second guard.
	require.True(t, isHeaderOverrideBlockedName("x-client-request-id"))

	account := t26APIKeyAccount(map[string]any{"x-client-request-id": "from-override", "X-Tenant": "t-1"})
	require.Equal(t, map[string]string{"x-tenant": "t-1"}, account.GetHeaderOverrides(), "the blocked override is dropped")

	req := t26ImagesRequest(t, map[string]string{"X-Client-Request-Id": "disp_01HXYZ"}, account)
	require.Equal(t, []string{"disp_01HXYZ"}, t26HeaderValues(req.Header, "X-Client-Request-Id"))
	require.Equal(t, []string{"t-1"}, t26HeaderValues(req.Header, "X-Tenant"), "other overrides still apply")

	req = t26ImagesRequest(t, nil, account)
	require.Empty(t, t26HeaderValues(req.Header, "X-Client-Request-Id"))
}

func TestSetClientRequestIDHeaderReplacesAnyCasing(t *testing.T) {
	h := http.Header{}
	h["x-client-request-id"] = []string{"stale"}
	h["X-CLIENT-REQUEST-ID"] = []string{"stale2"}
	setClientRequestIDHeader(h, "disp_1")
	require.Equal(t, []string{"disp_1"}, t26HeaderValues(h, "X-Client-Request-Id"))
	setClientRequestIDHeader(h, "")
	require.Equal(t, []string{"disp_1"}, t26HeaderValues(h, "X-Client-Request-Id"), "empty id leaves the header alone")
}

func TestBuildOpenAIImagesRequestDropsUnsafeOrMissingClientRequestID(t *testing.T) {
	req := t26ImagesRequest(t, map[string]string{"X-Client-Request-Id": "bad id with spaces"}, t26APIKeyAccount(nil))
	require.Empty(t, req.Header.Get("X-Client-Request-Id"))
	req = t26ImagesRequest(t, nil, t26APIKeyAccount(nil))
	require.Empty(t, req.Header.Get("X-Client-Request-Id"))
}

func TestClientRequestIDIsNotOnTheGlobalPassthroughAllowlist(t *testing.T) {
	require.False(t, openaiPassthroughAllowedHeaders["x-client-request-id"])
}

func TestCallbackUpstreamRequestIDPrefersHeaderThenBody(t *testing.T) {
	base := func(exec *ImageTaskExecution, result string) *ImageTaskRecord {
		return &ImageTaskRecord{ID: "imgtask_1", RunID: "run_1", Status: ImageTaskStatusCompleted,
			Execution: exec, Result: json.RawMessage(result)}
	}
	ok := &ImageTaskExecution{Outcome: ImageTaskOutcomeSucceeded, Dispatched: true, UpstreamCalls: 1, LastUpstreamStatus: 200}

	event, err := BuildImageTaskCallbackEvent(base(ok, `{"id":"fake-AigcImageTask-9","data":[{"r2_key":"k"}]}`))
	require.NoError(t, err)
	require.NotNil(t, event.Upstream)
	require.NotNil(t, event.Upstream.RequestID)
	require.Equal(t, "fake-AigcImageTask-9", *event.Upstream.RequestID, "body id when no header id")

	withHeader := *ok
	withHeader.UpstreamRequestID = "req_hdr_7"
	event, err = BuildImageTaskCallbackEvent(base(&withHeader, `{"id":"fake-AigcImageTask-9"}`))
	require.NoError(t, err)
	require.Equal(t, "req_hdr_7", *event.Upstream.RequestID, "header id wins")

	uncertain := &ImageTaskExecution{Outcome: ImageTaskOutcomeUncertain, Reason: ImageTaskReasonUpstream5xx, Dispatched: true, UpstreamCalls: 1, LastUpstreamStatus: 500}
	rec := base(uncertain, ``)
	rec.Status = ImageTaskStatusFailed
	event, err = BuildImageTaskCallbackEvent(rec)
	require.NoError(t, err)
	require.NotNil(t, event.Upstream)
	require.Nil(t, event.Upstream.RequestID, "no id on a failed call without a header id")
}

func TestExecutionPlaneTrackerKeepsLatestUpstreamRequestID(t *testing.T) {
	tracker := NewExecutionPlaneTracker()
	tracker.ObserveUpstreamRequestID("")
	require.Empty(t, tracker.Snapshot().UpstreamRequestID)
	tracker.ObserveUpstreamRequestID("req_1")
	tracker.ObserveUpstreamRequestID("req_2")
	require.Equal(t, "req_2", tracker.Snapshot().UpstreamRequestID)
}
