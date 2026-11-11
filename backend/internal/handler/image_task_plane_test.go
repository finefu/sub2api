package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

const planeMeta = `{"route_version":"route-v3","provider_id":"kemeitv","created_at":1794400000123}`

type planeHarness struct {
	router *gin.Engine
	store  idemAsyncStore
	tasks  *service.ImageTaskService
}

func newPlaneHarness(t *testing.T) *planeHarness {
	t.Helper()
	gin.SetMode(gin.TestMode)
	h := &planeHarness{store: idemAsyncStore{&asyncImageMemoryStore{tasks: make(map[string]*service.ImageTaskRecord)}}}
	h.tasks = service.NewImageTaskServiceWithUploader(h.store, nil, time.Hour, time.Minute)
	h.tasks.SetIdempotencyRepository(newUserMemoryIdempotencyRepoStub())
	async := &AsyncImageHandler{tasks: h.tasks}
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	async.execute = func(_ string, c *gin.Context) {
		<-release // keep tasks processing during the test
		c.JSON(http.StatusOK, gin.H{"data": []gin.H{}})
	}
	router := gin.New()
	router.Use(func(c *gin.Context) {
		groupID := int64(3)
		c.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{ID: 9, UserID: 7, GroupID: &groupID,
			Group: &service.Group{ID: groupID, Platform: service.PlatformOpenAI, AllowImageGeneration: true}})
		c.Next()
	})
	router.POST("/v1/images/generations/async", async.Submit)
	router.GET("/v1/images/tasks/:task_id", async.Get)
	h.router = router
	return h
}

func (h *planeHarness) submit(key, prefix, meta, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations/async", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	if prefix != "" {
		req.Header.Set(service.ImageTaskAssetPrefixHeader, prefix)
	}
	if meta != "" {
		req.Header.Set(service.ImageTaskAssetMetadataHeader, meta)
	}
	w := httptest.NewRecorder()
	h.router.ServeHTTP(w, req)
	return w
}

func TestExecutionPlaneSubmitValidation(t *testing.T) {
	h := newPlaneHarness(t)
	body := `{"model":"gpt-image-2-low","prompt":"cat","n":1}`
	bad := []struct{ name, key, prefix, meta, body string }{
		{"prefix other run", "run_1", "generated/run_2/", planeMeta, body},
		{"prefix traversal", "run_1", "generated/../run_1/", planeMeta, body},
		{"prefix nested", "run_1", "generated/run_1/sub/", planeMeta, body},
		{"prefix absolute", "run_1", "/generated/run_1/", planeMeta, body},
		{"prefix double slash", "run_1", "generated//run_1/", planeMeta, body},
		{"metadata missing", "run_1", "generated/run_1/", "", body},
		{"prefix missing", "run_1", "", planeMeta, body},
		{"metadata extra key", "run_1", "generated/run_1/", `{"route_version":"r","provider_id":"p","created_at":1,"x":1}`, body},
		{"n=2", "run_1", "generated/run_1/", planeMeta, `{"model":"gpt-image-2-low","prompt":"cat","n":2}`},
		{"no idempotency key", "", "generated/run_1/", planeMeta, body},
	}
	for _, tc := range bad {
		w := h.submit(tc.key, tc.prefix, tc.meta, tc.body)
		require.Equal(t, http.StatusBadRequest, w.Code, "%s: %s", tc.name, w.Body.String())
	}
	require.Empty(t, h.store.tasks, "rejected submits create no task")

	ok := h.submit("run_1", "generated/run_1/", planeMeta, body)
	require.Equal(t, http.StatusAccepted, ok.Code, ok.Body.String())
	rec, err := h.store.Get(context.Background(), taskIDOf(t, ok))
	require.NoError(t, err)
	require.Equal(t, &service.ImageTaskPlaneSpec{AssetPrefix: "generated/run_1/", RouteVersion: "route-v3", ProviderID: "kemeitv", CreatedAt: 1794400000123}, rec.Plane)
	require.Equal(t, "run_1", rec.RunID)

	// Same key + body, same headers: replay.
	require.Equal(t, http.StatusOK, h.submit("run_1", "generated/run_1/", planeMeta, body).Code)
	// Same key + body, different asset metadata: fingerprint conflict.
	conflict := h.submit("run_1", "generated/run_1/", `{"route_version":"route-v4","provider_id":"kemeitv","created_at":1794400000123}`, body)
	require.Equal(t, http.StatusConflict, conflict.Code, conflict.Body.String())
}

func TestExecutionPlaneGetUsesCallbackBody(t *testing.T) {
	h := newPlaneHarness(t)
	ok := h.submit("run_1", "generated/run_1/", planeMeta, `{"model":"gpt-image-2-low","prompt":"cat"}`)
	require.Equal(t, http.StatusAccepted, ok.Code, ok.Body.String())
	taskID := taskIDOf(t, ok)

	get := func() *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		h.router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/images/tasks/"+taskID, nil))
		return w
	}
	w := get()
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `{"task_id":"`+taskID+`","run_id":"run_1","status":"processing"}`, w.Body.String())

	// Finish the task directly (as the executor would) and compare with the callback serializer.
	exec := &service.ImageTaskExecution{Outcome: service.ImageTaskOutcomeNotSent, Reason: service.ImageTaskReasonNoAccount}
	require.NoError(t, h.tasks.FailWithExecution(context.Background(), taskID, 503, json.RawMessage(`{"message":"No available compatible accounts"}`), exec))
	rec, err := h.store.Get(context.Background(), taskID)
	require.NoError(t, err)
	event, err := service.BuildImageTaskCallbackEvent(rec)
	require.NoError(t, err)
	want, _ := json.Marshal(event)

	w = get()
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, string(want), strings.TrimSpace(w.Body.String()))
	require.NotContains(t, w.Body.String(), "://")
}
