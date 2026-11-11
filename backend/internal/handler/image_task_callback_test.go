package handler

import (
	"context"
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

func TestAsyncImageCallbackKeyRequiresRunIDIdempotencyKey(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := idemAsyncStore{&asyncImageMemoryStore{tasks: make(map[string]*service.ImageTaskRecord)}}
	tasks := service.NewImageTaskServiceWithUploader(store, nil, time.Hour, time.Minute)
	tasks.SetIdempotencyRepository(newUserMemoryIdempotencyRepoStub())
	cfg, err := service.ParseImageTaskCallbackConfig([]byte(`[{"api_key_id":9,"url":"https://a.example/cb","secret":"`+strings.Repeat("s", 32)+`"}]`), false)
	require.NoError(t, err)
	tasks.SetCallbackConfig(cfg)
	async := &AsyncImageHandler{tasks: tasks}
	async.execute = func(_ string, c *gin.Context) { c.JSON(http.StatusOK, gin.H{"data": []gin.H{}}) }

	router := gin.New()
	router.Use(func(c *gin.Context) {
		groupID := int64(3)
		c.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{ID: 9, UserID: 7, GroupID: &groupID,
			Group: &service.Group{ID: groupID, Platform: service.PlatformOpenAI, AllowImageGeneration: true}})
		c.Next()
	})
	router.POST("/v1/images/generations/async", async.Submit)
	submit := func(key string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/images/generations/async", strings.NewReader(`{"model":"gpt-image-2-low","prompt":"cat"}`))
		req.Header.Set("Content-Type", "application/json")
		if key != "" {
			req.Header.Set("Idempotency-Key", key)
			// T2.4: callback-enabled keys are execution-plane keys and send the asset headers.
			req.Header.Set(service.ImageTaskAssetPrefixHeader, "generated/"+key+"/")
			req.Header.Set(service.ImageTaskAssetMetadataHeader, `{"route_version":"route-v3","provider_id":"kemeitv","created_at":1794400000123}`)
		}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		return w
	}

	for _, key := range []string{"", "_starts-with-underscore", "has/slash"} {
		w := submit(key)
		require.Equal(t, http.StatusBadRequest, w.Code, "key %q: %s", key, w.Body.String())
		require.Contains(t, w.Body.String(), "IMAGE_TASK_RUN_ID_REQUIRED")
	}
	require.Empty(t, store.tasks)

	ok := submit("run_exec_plane_1")
	require.Equal(t, http.StatusAccepted, ok.Code, ok.Body.String())
	stored, err := store.Get(context.Background(), taskIDOf(t, ok))
	require.NoError(t, err)
	require.Equal(t, "run_exec_plane_1", stored.RunID)
}
