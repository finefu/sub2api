package repository

// YeaGrow execution plane: higher callback concurrency must not post the same
// event twice at the same time. The lease set by the claim script is what
// guarantees it; these tests run several deliverers against the real script.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"
)

// enqueueCallbacks writes n due events exactly as FinishProcessing lays them out.
func enqueueCallbacks(t *testing.T, mr *miniredis.Miniredis, prefix string, n int) []string {
	t.Helper()
	createdMS := time.Now().Add(-time.Second).UnixMilli()
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("evt_%s_%04d", prefix, i)
		ids = append(ids, id)
		mr.HSet(imageTaskCallbackEventKey(id), "body", `{"event_id":"`+id+`"}`, "api_key_id", "9",
			"created_ms", strconv.FormatInt(createdMS, 10), "failures", "0")
		_, err := mr.ZAdd(imageTaskCallbackDueKey, float64(createdMS), id)
		require.NoError(t, err)
	}
	return ids
}

// deliveryCounter records how often each event_id is posted and how many
// posts of the same event overlap in time.
type deliveryCounter struct {
	mu       sync.Mutex
	posts    map[string]int
	inflight map[string]int
	overlaps int
	delay    time.Duration
	total    atomic.Int64
}

func newDeliveryCounter(delay time.Duration) *deliveryCounter {
	return &deliveryCounter{posts: map[string]int{}, inflight: map[string]int{}, delay: delay}
}

func (c *deliveryCounter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	var body struct {
		EventID string `json:"event_id"`
	}
	_ = json.Unmarshal(raw, &body)
	c.mu.Lock()
	c.posts[body.EventID]++
	c.inflight[body.EventID]++
	if c.inflight[body.EventID] > 1 {
		c.overlaps++
	}
	c.mu.Unlock()
	time.Sleep(c.delay)
	c.mu.Lock()
	c.inflight[body.EventID]--
	c.mu.Unlock()
	c.total.Add(1)
	w.WriteHeader(http.StatusOK)
}

func TestImageTaskCallbackConcurrentDeliverersPostEachEventOnce(t *testing.T) {
	mr, _, store := newCallbackTestStore(t)
	ids := enqueueCallbacks(t, mr, "many", 400)
	counter := newDeliveryCounter(5 * time.Millisecond)
	server := httptest.NewServer(counter)
	defer server.Close()

	// Four workers (e.g. several gateway instances) at the new defaults share one queue.
	var deliverers []*service.ImageTaskCallbackDeliverer
	for i := 0; i < 4; i++ {
		d := service.NewImageTaskCallbackDeliverer(store, callbackConfigFor(t, server.URL), nil).
			WithTuning(service.DefaultImageTaskCallbackTuning())
		require.Equal(t, service.ImageTaskCallbackTuning{Batch: 64, Concurrency: 32, PollInterval: 200 * time.Millisecond}, d.Tuning())
		d.Start()
		deliverers = append(deliverers, d)
	}
	require.Eventually(t, func() bool { return !mr.Exists(imageTaskCallbackDueKey) }, 20*time.Second, 20*time.Millisecond)
	for _, d := range deliverers {
		d.Stop()
	}

	counter.mu.Lock()
	defer counter.mu.Unlock()
	require.Zero(t, counter.overlaps, "no event is posted twice at the same time")
	require.Len(t, counter.posts, len(ids))
	for _, id := range ids {
		require.Equal(t, 1, counter.posts[id], "event %s", id)
		require.False(t, mr.Exists(imageTaskCallbackEventKey(id)), "acked event %s is removed", id)
	}
}

func TestImageTaskCallbackLeaseBlocksReclaimWhileDeliveryIsInFlight(t *testing.T) {
	mr, _, store := newCallbackTestStore(t)
	enqueueCallbacks(t, mr, "lease", 50)
	release := make(chan struct{})
	var posts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		posts.Add(1)
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	tuning := service.ImageTaskCallbackTuning{Batch: 64, Concurrency: 32, PollInterval: 200 * time.Millisecond}

	first := service.NewImageTaskCallbackDeliverer(store, callbackConfigFor(t, server.URL), nil).WithTuning(tuning)
	done := make(chan service.ImageTaskCallbackRunStats, 1)
	go func() { stats, _ := first.RunOnce(context.Background()); done <- stats }()
	// 32 posts in flight, 18 more claimed and waiting for a slot.
	require.Eventually(t, func() bool { return posts.Load() == 32 }, 5*time.Second, 5*time.Millisecond)

	// A second worker sees every event leased: nothing to claim, nothing posted twice.
	second := service.NewImageTaskCallbackDeliverer(store, callbackConfigFor(t, server.URL), nil).WithTuning(tuning)
	for i := 0; i < 5; i++ {
		stats, err := second.RunOnce(context.Background())
		require.NoError(t, err)
		require.Equal(t, service.ImageTaskCallbackRunStats{}, stats)
	}
	close(release)
	stats := <-done
	require.Equal(t, service.ImageTaskCallbackRunStats{Claimed: 50, Delivered: 50}, stats)
	require.Equal(t, int64(50), posts.Load())
	require.False(t, mr.Exists(imageTaskCallbackDueKey))
}
