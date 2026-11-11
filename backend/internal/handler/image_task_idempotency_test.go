package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// idemAsyncStore adds an atomic SaveIfAbsent to the async test store.
type idemAsyncStore struct {
	*asyncImageMemoryStore
}

func (s idemAsyncStore) SaveIfAbsent(_ context.Context, task *service.ImageTaskRecord, _ time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tasks[task.ID]; ok {
		return false, nil
	}
	copy := *task
	s.tasks[task.ID] = &copy
	return true, nil
}

func (s idemAsyncStore) count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.tasks)
}

type idemAsyncHarness struct {
	router   *gin.Engine
	store    idemAsyncStore
	executed atomic.Int32
}

func newIdemAsyncHarness(t *testing.T) *idemAsyncHarness {
	t.Helper()
	gin.SetMode(gin.TestMode)
	h := &idemAsyncHarness{store: idemAsyncStore{&asyncImageMemoryStore{tasks: make(map[string]*service.ImageTaskRecord)}}}
	tasks := service.NewImageTaskServiceWithUploader(h.store, nil, time.Hour, time.Minute)
	tasks.SetIdempotencyRepository(newUserMemoryIdempotencyRepoStub())
	async := &AsyncImageHandler{tasks: tasks}
	async.execute = func(_ string, c *gin.Context) {
		h.executed.Add(1)
		c.JSON(http.StatusOK, gin.H{"created": 1, "data": []gin.H{{"url": "https://example.test/image.png"}}})
	}
	router := gin.New()
	router.Use(func(c *gin.Context) {
		keyID := int64(9)
		if c.GetHeader("X-Test-Key") == "other" {
			keyID = 10
		}
		groupID := int64(3)
		c.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{
			ID:      keyID,
			UserID:  7,
			GroupID: &groupID,
			Group:   &service.Group{ID: groupID, Platform: service.PlatformOpenAI, AllowImageGeneration: true},
		})
		c.Next()
	})
	router.POST("/v1/images/generations/async", async.Submit)
	h.router = router
	return h
}

func (h *idemAsyncHarness) submit(key, body, apiKey string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations/async", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	if apiKey != "" {
		req.Header.Set("X-Test-Key", apiKey)
	}
	w := httptest.NewRecorder()
	h.router.ServeHTTP(w, req)
	return w
}

func taskIDOf(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	var payload struct {
		TaskID string `json:"task_id"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &payload), w.Body.String())
	require.NotEmpty(t, payload.TaskID)
	return payload.TaskID
}

const idemBody = `{"model":"gpt-image-2-low","prompt":"cat","n":1}`

func TestAsyncImageIdempotency_ReplayReturnsSameTaskWith200(t *testing.T) {
	h := newIdemAsyncHarness(t)

	first := h.submit("run-1", idemBody, "")
	require.Equal(t, http.StatusAccepted, first.Code, first.Body.String())
	second := h.submit("run-1", idemBody, "")
	require.Equal(t, http.StatusOK, second.Code, second.Body.String())
	require.Equal(t, taskIDOf(t, first), taskIDOf(t, second))
	require.Equal(t, "/v1/images/tasks/"+taskIDOf(t, first), second.Header().Get("Location"))
	require.Contains(t, second.Body.String(), `"replayed":true`)

	require.Eventually(t, func() bool { return h.executed.Load() == 1 }, time.Second, 5*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, int32(1), h.executed.Load(), "a replay must not execute again")
	require.Equal(t, 1, h.store.count())
}

func TestAsyncImageIdempotency_DifferentBodyIs409WithoutTask(t *testing.T) {
	h := newIdemAsyncHarness(t)
	require.Equal(t, http.StatusAccepted, h.submit("run-1", idemBody, "").Code)

	conflict := h.submit("run-1", `{"model":"gpt-image-2-low","prompt":"dog","n":1}`, "")
	require.Equal(t, http.StatusConflict, conflict.Code, conflict.Body.String())
	require.Contains(t, conflict.Body.String(), "IDEMPOTENCY_KEY_CONFLICT")
	require.Equal(t, 1, h.store.count())
	require.Eventually(t, func() bool { return h.executed.Load() == 1 }, time.Second, 5*time.Millisecond)
}

func TestAsyncImageIdempotency_TenConcurrentSubmitsCreateOneTask(t *testing.T) {
	h := newIdemAsyncHarness(t)
	const n = 10
	codes := make([]int, n)
	ids := make([]string, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			w := h.submit("run-concurrent", idemBody, "")
			codes[i] = w.Code
			if w.Code == http.StatusAccepted || w.Code == http.StatusOK {
				ids[i] = taskIDOf(t, w)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	accepted, replayed := 0, 0
	for i := 0; i < n; i++ {
		switch codes[i] {
		case http.StatusAccepted:
			accepted++
		case http.StatusOK:
			replayed++
		default:
			t.Fatalf("unexpected status %d", codes[i])
		}
		require.Equal(t, ids[0], ids[i])
	}
	require.Equal(t, 1, accepted)
	require.Equal(t, n-1, replayed)
	require.Equal(t, 1, h.store.count())
	require.Eventually(t, func() bool { return h.executed.Load() == 1 }, time.Second, 5*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, int32(1), h.executed.Load())
}

func TestAsyncImageIdempotency_SameKeyOtherAPIKeyIsIndependent(t *testing.T) {
	h := newIdemAsyncHarness(t)
	a := h.submit("run-1", idemBody, "")
	b := h.submit("run-1", `{"model":"gpt-image-2-low","prompt":"dog","n":1}`, "other")
	require.Equal(t, http.StatusAccepted, a.Code)
	require.Equal(t, http.StatusAccepted, b.Code, b.Body.String())
	require.NotEqual(t, taskIDOf(t, a), taskIDOf(t, b))
	require.Eventually(t, func() bool { return h.executed.Load() == 2 }, time.Second, 5*time.Millisecond)
}

func TestAsyncImageIdempotency_InvalidKeyIs400AndNoKeyKeepsOldBehaviour(t *testing.T) {
	h := newIdemAsyncHarness(t)
	bad := h.submit("bad key", idemBody, "")
	require.Equal(t, http.StatusBadRequest, bad.Code)
	require.Zero(t, h.store.count())

	one := h.submit("", idemBody, "")
	two := h.submit("", idemBody, "")
	require.Equal(t, http.StatusAccepted, one.Code)
	require.Equal(t, http.StatusAccepted, two.Code)
	require.NotEqual(t, taskIDOf(t, one), taskIDOf(t, two), "without Idempotency-Key every submit is a new task")
	require.Eventually(t, func() bool { return h.executed.Load() == 2 }, time.Second, 5*time.Millisecond)
}
