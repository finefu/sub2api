package handler

// YeaGrow execution plane: GET /v1/execution-plane/status handler.

import (
	"context"
	"encoding/json"
	"errors"
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

// statusCountingStore adds the status counts to the in-memory task store.
type statusCountingStore struct {
	*asyncImageMemoryStore
	counts service.ImageTaskExecutionPlaneCounts
	err    error
	calls  int
}

func (s *statusCountingStore) ExecutionPlaneCounts(_ context.Context, _ string) (service.ImageTaskExecutionPlaneCounts, error) {
	s.calls++
	return s.counts, s.err
}

func newStatusRouter(t *testing.T, store service.ImageTaskStore, apiKeyID int64) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	tasks := service.NewImageTaskServiceWithUploader(store, nil, time.Hour, time.Minute)
	cfg, err := service.ParseImageTaskCallbackConfig([]byte(`[{"api_key_id":9,"url":"https://a.example/cb","secret":"`+strings.Repeat("s", 32)+`"}]`), false)
	require.NoError(t, err)
	tasks.SetCallbackConfig(cfg)
	async := &AsyncImageHandler{tasks: tasks}
	router := gin.New()
	router.Use(func(c *gin.Context) {
		if apiKeyID > 0 {
			c.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{ID: apiKeyID, UserID: 7})
		}
		c.Next()
	})
	router.GET("/v1/execution-plane/status", async.ExecutionPlaneStatus)
	return router
}

func getStatus(router *gin.Engine) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/execution-plane/status", nil))
	return w
}

func newStatusStore(counts service.ImageTaskExecutionPlaneCounts, err error) *statusCountingStore {
	memory := &asyncImageMemoryStore{tasks: map[string]*service.ImageTaskRecord{
		"task_secret_1": {ID: "task_secret_1", UserID: 7, APIKeyID: 9, RunID: "run_secret_1", Status: service.ImageTaskStatusProcessing},
	}}
	return &statusCountingStore{asyncImageMemoryStore: memory, counts: counts, err: err}
}

func TestExecutionPlaneStatusReturnsOnlyAggregateCountsToTheCallbackKey(t *testing.T) {
	store := newStatusStore(service.ImageTaskExecutionPlaneCounts{CallbackDue: 4, CallbackDead: 2, Inflight: 9}, nil)
	w := getStatus(newStatusRouter(t, store, 9))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Equal(t, "no-store", w.Header().Get("Cache-Control"))
	require.JSONEq(t, `{"status":"ok","callback_queue":{"due":4,"dead":2},"inflight":9}`, w.Body.String())
	var body map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Len(t, body, 3)
	require.NotContains(t, w.Body.String(), "task_secret_1")
	require.NotContains(t, w.Body.String(), "run_secret_1")
}

func TestExecutionPlaneStatusZeroCountsAreStillOK(t *testing.T) {
	w := getStatus(newStatusRouter(t, newStatusStore(service.ImageTaskExecutionPlaneCounts{}, nil), 9))
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `{"status":"ok","callback_queue":{"due":0,"dead":0},"inflight":0}`, w.Body.String())
}

func TestExecutionPlaneStatusIsNotFoundForOtherKeys(t *testing.T) {
	for _, keyID := range []int64{0, 10} {
		store := newStatusStore(service.ImageTaskExecutionPlaneCounts{CallbackDue: 4}, nil)
		w := getStatus(newStatusRouter(t, store, keyID))
		require.Equal(t, http.StatusNotFound, w.Code, "key %d", keyID)
		require.NotContains(t, w.Body.String(), "callback_queue")
		require.Zero(t, store.calls, "key %d must not read Redis", keyID)
	}
}

func TestExecutionPlaneStatusRedisErrorIs503NotZeros(t *testing.T) {
	w := getStatus(newStatusRouter(t, newStatusStore(service.ImageTaskExecutionPlaneCounts{}, errors.New("redis: connection refused")), 9))
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.NotContains(t, w.Body.String(), `"status":"ok"`)
	require.NotContains(t, w.Body.String(), "connection refused")
}

func TestExecutionPlaneStatusStoreWithoutCountsIs503(t *testing.T) {
	memory := &asyncImageMemoryStore{tasks: map[string]*service.ImageTaskRecord{}}
	w := getStatus(newStatusRouter(t, memory, 9))
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
}
