package handler

// YeaGrow T2.6: the caller's X-Client-Request-Id reaches the upstream through
// the async Images path; the gateway's own correlation id is not forwarded.

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestAsyncImagesForwardsClientRequestIDToUpstream(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	capture := func(req *http.Request) (*http.Response, error) {
		mu.Lock()
		seen = append(seen, req.Header.Get("X-Client-Request-Id"))
		mu.Unlock()
		return execPlaneSuccess()(req)
	}
	accounts := []service.Account{execPlaneAPIKeyAccount(1, 0, nil)}
	h := newExecPlaneHarness(t, true, accounts, map[int64][]execPlaneStep{1: {capture}}, time.Minute)

	submit := func(clientID string) {
		req := httptest.NewRequest(http.MethodPost, "/v1/images/generations/async",
			bytes.NewBufferString(`{"model":"gpt-image-1","prompt":"cat","n":1,"size":"1024x1024","response_format":"b64_json"}`))
		req.Header.Set("Content-Type", "application/json")
		if clientID != "" {
			req.Header.Set("X-Client-Request-Id", clientID)
		}
		w := httptest.NewRecorder()
		h.router.ServeHTTP(w, req)
		require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
	}
	submit("disp_01HXYZ")
	submit("")
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == 2
	}, 5*time.Second, 10*time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	require.ElementsMatch(t, []string{"disp_01HXYZ", ""}, seen)
}
