package service

// YeaGrow execution plane: callback delivery throughput settings.

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func envLookup(values map[string]string) func(string) string {
	return func(name string) string { return values[name] }
}

func TestParseImageTaskCallbackTuningDefaultsAndValidValues(t *testing.T) {
	tuning, warnings := ParseImageTaskCallbackTuning(envLookup(nil))
	require.Empty(t, warnings)
	require.Equal(t, ImageTaskCallbackTuning{Batch: 64, Concurrency: 32, PollInterval: 200 * time.Millisecond}, tuning)

	tuning, warnings = ParseImageTaskCallbackTuning(envLookup(map[string]string{
		ImageTaskCallbackBatchEnv: " 128 ", ImageTaskCallbackConcurrencyEnv: "64", ImageTaskCallbackPollIntervalEnv: "500ms",
	}))
	require.Empty(t, warnings)
	require.Equal(t, ImageTaskCallbackTuning{Batch: 128, Concurrency: 64, PollInterval: 500 * time.Millisecond}, tuning)

	// Bounds are inclusive.
	tuning, warnings = ParseImageTaskCallbackTuning(envLookup(map[string]string{
		ImageTaskCallbackBatchEnv: "256", ImageTaskCallbackConcurrencyEnv: "128", ImageTaskCallbackPollIntervalEnv: "10s",
	}))
	require.Empty(t, warnings)
	require.Equal(t, ImageTaskCallbackTuning{Batch: 256, Concurrency: 128, PollInterval: 10 * time.Second}, tuning)
	tuning, warnings = ParseImageTaskCallbackTuning(envLookup(map[string]string{
		ImageTaskCallbackBatchEnv: "1", ImageTaskCallbackConcurrencyEnv: "1", ImageTaskCallbackPollIntervalEnv: "50ms",
	}))
	require.Empty(t, warnings)
	require.Equal(t, ImageTaskCallbackTuning{Batch: 1, Concurrency: 1, PollInterval: 50 * time.Millisecond}, tuning)
}

func TestParseImageTaskCallbackTuningFallsBackPerValue(t *testing.T) {
	for _, tc := range []struct {
		name, value string
	}{
		{ImageTaskCallbackBatchEnv, "0"}, {ImageTaskCallbackBatchEnv, "257"}, {ImageTaskCallbackBatchEnv, "64x"}, {ImageTaskCallbackBatchEnv, "-1"},
		{ImageTaskCallbackConcurrencyEnv, "0"}, {ImageTaskCallbackConcurrencyEnv, "129"}, {ImageTaskCallbackConcurrencyEnv, "1.5"},
		{ImageTaskCallbackPollIntervalEnv, "200"}, {ImageTaskCallbackPollIntervalEnv, "49ms"}, {ImageTaskCallbackPollIntervalEnv, "11s"}, {ImageTaskCallbackPollIntervalEnv, "-1s"},
	} {
		tuning, warnings := ParseImageTaskCallbackTuning(envLookup(map[string]string{tc.name: tc.value}))
		require.Len(t, warnings, 1, "%s=%s", tc.name, tc.value)
		require.Contains(t, warnings[0], tc.name)
		require.Equal(t, DefaultImageTaskCallbackTuning(), tuning, "%s=%s", tc.name, tc.value)
	}
	// One bad value does not discard the others.
	tuning, warnings := ParseImageTaskCallbackTuning(envLookup(map[string]string{
		ImageTaskCallbackBatchEnv: "oops", ImageTaskCallbackConcurrencyEnv: "48", ImageTaskCallbackPollIntervalEnv: "1s",
	}))
	require.Len(t, warnings, 1)
	require.Equal(t, ImageTaskCallbackTuning{Batch: 64, Concurrency: 48, PollInterval: time.Second}, tuning)
}

func TestParseImageTaskCallbackTuningKeepsClaimedEventsWithinTheLease(t *testing.T) {
	// 256 / 64 = 4 waves × 15 s request timeout would outlast the 60 s lease.
	tuning, warnings := ParseImageTaskCallbackTuning(envLookup(map[string]string{
		ImageTaskCallbackBatchEnv: "256", ImageTaskCallbackConcurrencyEnv: "64", ImageTaskCallbackPollIntervalEnv: "1s",
	}))
	require.Len(t, warnings, 1)
	require.Contains(t, warnings[0], "waves")
	require.Equal(t, ImageTaskCallbackTuning{Batch: 64, Concurrency: 32, PollInterval: time.Second}, tuning)
	// Three waves are allowed and stay below the lease.
	tuning, warnings = ParseImageTaskCallbackTuning(envLookup(map[string]string{
		ImageTaskCallbackBatchEnv: "96", ImageTaskCallbackConcurrencyEnv: "32",
	}))
	require.Empty(t, warnings)
	require.Equal(t, 96, tuning.Batch)
	require.Less(t, time.Duration(tuning.waves())*imageTaskCallbackRequestTimeout, imageTaskCallbackLease)
	// The defaults themselves satisfy the bound.
	def := DefaultImageTaskCallbackTuning()
	require.LessOrEqual(t, def.waves(), imageTaskCallbackMaxWaves)
	require.Less(t, time.Duration(imageTaskCallbackMaxWaves)*imageTaskCallbackRequestTimeout, imageTaskCallbackLease)
}

func TestImageTaskCallbackDelivererWithTuningRejectsOutOfRange(t *testing.T) {
	d := NewImageTaskCallbackDeliverer(newMemCallbackQueue(), nil, nil)
	require.Equal(t, DefaultImageTaskCallbackTuning(), d.Tuning())
	ok := ImageTaskCallbackTuning{Batch: 8, Concurrency: 4, PollInterval: 100 * time.Millisecond}
	require.Equal(t, ok, d.WithTuning(ok).Tuning())
	for _, bad := range []ImageTaskCallbackTuning{
		{Batch: 0, Concurrency: 4, PollInterval: time.Second},
		{Batch: 8, Concurrency: 0, PollInterval: time.Second},
		{Batch: 8, Concurrency: 4, PollInterval: time.Millisecond},
		{Batch: 200, Concurrency: 8, PollInterval: time.Second},
	} {
		require.Equal(t, DefaultImageTaskCallbackTuning(), d.WithTuning(bad).Tuning(), "%+v", bad)
	}
}

func callbackTargetConfig(t *testing.T, url string) *ImageTaskCallbackConfig {
	t.Helper()
	cfg, err := ParseImageTaskCallbackConfig([]byte(`[{"api_key_id":9,"url":"`+url+`","secret":"`+strings.Repeat("s", 32)+`"}]`), true)
	require.NoError(t, err)
	return cfg
}

// countingQueue wraps the in-memory queue and counts claim rounds.
type countingQueue struct {
	*memCallbackQueue
	claims atomic.Int64
}

func (q *countingQueue) ClaimDueCallbacks(ctx context.Context, now time.Time, lease time.Duration, limit int) ([]ImageTaskCallbackEnvelope, error) {
	q.claims.Add(1)
	return q.memCallbackQueue.ClaimDueCallbacks(ctx, now, lease, limit)
}

func (q *countingQueue) pending() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.due)
}

func TestImageTaskCallbackDelivererDrainsBacklogWithoutWaitingForThePollInterval(t *testing.T) {
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer receiver.Close()
	queue := &countingQueue{memCallbackQueue: newMemCallbackQueue()}
	created := time.Now().Add(-time.Second)
	for i := 0; i < 200; i++ {
		id := fmt.Sprintf("evt_%03d", i)
		queue.add(ImageTaskCallbackEnvelope{EventID: id, APIKeyID: 9, Body: []byte(`{"event_id":"` + id + `"}`), CreatedAt: created})
	}
	// With the old behaviour (one claim per poll) four batches would take ≥ 30 s at a 10 s interval.
	d := NewImageTaskCallbackDeliverer(queue, callbackTargetConfig(t, receiver.URL), nil).
		WithTuning(ImageTaskCallbackTuning{Batch: 64, Concurrency: 32, PollInterval: 10 * time.Second})
	d.Start()
	defer d.Stop()
	require.Eventually(t, func() bool { return queue.pending() == 0 }, 5*time.Second, 10*time.Millisecond)
	// 4 claiming rounds plus the empty one that starts the idle wait.
	require.Eventually(t, func() bool { return queue.claims.Load() == 5 }, 2*time.Second, 5*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, int64(5), queue.claims.Load(), "an empty round waits for the poll interval")
}

func TestImageTaskCallbackDelivererStopsWhileIdleOrDraining(t *testing.T) {
	queue := &countingQueue{memCallbackQueue: newMemCallbackQueue()}
	d := NewImageTaskCallbackDeliverer(queue, callbackTargetConfig(t, "http://127.0.0.1:1/cb"), nil).
		WithTuning(ImageTaskCallbackTuning{Batch: 1, Concurrency: 1, PollInterval: 10 * time.Second})
	d.Start()
	stopped := make(chan struct{})
	go func() { d.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not interrupt the idle wait")
	}
}

func TestImageTaskCallbackDelivererHonoursTheConcurrencyLimit(t *testing.T) {
	var inflight, peak atomic.Int64
	release := make(chan struct{})
	var once sync.Once
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := inflight.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		<-release
		inflight.Add(-1)
		w.WriteHeader(http.StatusOK)
	}))
	defer receiver.Close()
	defer once.Do(func() { close(release) })
	queue := newMemCallbackQueue()
	for i := 0; i < 40; i++ {
		id := fmt.Sprintf("evt_c%02d", i)
		queue.add(ImageTaskCallbackEnvelope{EventID: id, APIKeyID: 9, Body: []byte(`{}`), CreatedAt: time.Now().Add(-time.Second)})
	}
	d := NewImageTaskCallbackDeliverer(queue, callbackTargetConfig(t, receiver.URL), nil).
		WithTuning(ImageTaskCallbackTuning{Batch: 20, Concurrency: 8, PollInterval: time.Second})
	done := make(chan ImageTaskCallbackRunStats, 1)
	go func() { stats, _ := d.RunOnce(context.Background()); done <- stats }()
	require.Eventually(t, func() bool { return inflight.Load() == 8 }, 5*time.Second, 5*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, int64(8), peak.Load(), "never more than the configured concurrency")
	once.Do(func() { close(release) })
	stats := <-done
	require.Equal(t, ImageTaskCallbackRunStats{Claimed: 20, Delivered: 20}, stats)
}
