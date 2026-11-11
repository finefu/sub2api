package repository

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

const cbRepoSecret = "yeagrow-exec-plane-callback-test-secret-0001"

func newCallbackTestStore(t *testing.T) (*miniredis.Miniredis, *redis.Client, *imageTaskStore) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return mr, rdb, NewImageTaskStore(rdb).(*imageTaskStore)
}

func processingTask(id string) *service.ImageTaskRecord {
	return &service.ImageTaskRecord{ID: id, UserID: 7, APIKeyID: 9, RunID: "run_" + id, Status: service.ImageTaskStatusProcessing}
}

func TestImageTaskFinishProcessingIsCASWithSingleEvent(t *testing.T) {
	mr, _, store := newCallbackTestStore(t)
	ctx := context.Background()
	require.NoError(t, store.Save(ctx, processingTask("t1"), time.Hour))

	done := *processingTask("t1")
	done.Status = service.ImageTaskStatusFailed
	event := &service.ImageTaskCallbackEnvelope{EventID: "evt_t1", APIKeyID: 9, Body: []byte(`{"event_id":"evt_t1"}`), CreatedAt: time.UnixMilli(1_800_000_000_000)}
	res, err := store.FinishProcessing(ctx, &done, time.Hour, event)
	require.NoError(t, err)
	require.Equal(t, service.ImageTaskFinishApplied, res)

	got, err := store.Get(ctx, "t1")
	require.NoError(t, err)
	require.Equal(t, service.ImageTaskStatusFailed, got.Status)
	members, err := mr.ZMembers(imageTaskCallbackDueKey)
	require.NoError(t, err)
	require.Equal(t, []string{"evt_t1"}, members)
	require.Equal(t, `{"event_id":"evt_t1"}`, mr.HGet(imageTaskCallbackEventKey("evt_t1"), "body"))
	require.Equal(t, "9", mr.HGet(imageTaskCallbackEventKey("evt_t1"), "api_key_id"))
	require.Greater(t, mr.TTL(imageTaskCallbackEventKey("evt_t1")), 24*time.Hour)

	// Late second finish: rejected, record and queue unchanged.
	late := *processingTask("t1")
	late.Status = service.ImageTaskStatusCompleted
	res, err = store.FinishProcessing(ctx, &late, time.Hour, &service.ImageTaskCallbackEnvelope{EventID: "evt_t1", APIKeyID: 9, Body: []byte(`{"late":true}`), CreatedAt: time.Now()})
	require.NoError(t, err)
	require.Equal(t, service.ImageTaskFinishNotProcessing, res)
	got, _ = store.Get(ctx, "t1")
	require.Equal(t, service.ImageTaskStatusFailed, got.Status)
	require.Equal(t, `{"event_id":"evt_t1"}`, mr.HGet(imageTaskCallbackEventKey("evt_t1"), "body"))

	res, err = store.FinishProcessing(ctx, processingTask("missing"), time.Hour, nil)
	require.NoError(t, err)
	require.Equal(t, service.ImageTaskFinishMissing, res)
	require.False(t, mr.Exists(imageTaskKey("missing")))
}

func TestImageTaskFinishProcessingWithoutEvent(t *testing.T) {
	mr, _, store := newCallbackTestStore(t)
	ctx := context.Background()
	require.NoError(t, store.Save(ctx, processingTask("t2"), time.Hour))
	done := *processingTask("t2")
	done.Status = service.ImageTaskStatusCompleted
	res, err := store.FinishProcessing(ctx, &done, time.Hour, nil)
	require.NoError(t, err)
	require.Equal(t, service.ImageTaskFinishApplied, res)
	require.False(t, mr.Exists(imageTaskCallbackDueKey))
	require.Greater(t, mr.TTL(imageTaskKey("t2")), 59*time.Minute)
}

func TestImageTaskFinishProcessingConcurrentFinishersProduceOneTerminalAndOneEvent(t *testing.T) {
	mr, _, store := newCallbackTestStore(t)
	ctx := context.Background()
	require.NoError(t, store.Save(ctx, processingTask("t3"), time.Hour))

	var applied atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			next := *processingTask("t3")
			next.Status = service.ImageTaskStatusFailed
			next.HTTPStatus = 500 + i
			res, err := store.FinishProcessing(ctx, &next, time.Hour, &service.ImageTaskCallbackEnvelope{
				EventID: "evt_t3", APIKeyID: 9, Body: []byte(`{"finisher":` + strconv.Itoa(i) + `}`), CreatedAt: time.Now()})
			require.NoError(t, err)
			if res == service.ImageTaskFinishApplied {
				applied.Add(1)
			}
		}(i)
	}
	wg.Wait()
	require.Equal(t, int32(1), applied.Load())
	members, _ := mr.ZMembers(imageTaskCallbackDueKey)
	require.Len(t, members, 1)
	got, _ := store.Get(ctx, "t3")
	require.Equal(t, `{"finisher":`+strconv.Itoa(got.HTTPStatus-500)+`}`, mr.HGet(imageTaskCallbackEventKey("evt_t3"), "body"),
		"the queued event belongs to the finisher whose terminal record won")
}

// verifyLikeYeaGrow re-implements the receiver check independently.
func verifyLikeYeaGrow(r *http.Request, body []byte, secret string, now time.Time) error {
	ts := r.Header.Get("X-Exec-Timestamp")
	sec, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return err
	}
	if d := now.Unix() - sec; d > 300 || d < -300 {
		return errors.New("stale")
	}
	got, err := hex.DecodeString(r.Header.Get("X-Exec-Signature"))
	if err != nil {
		return err
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(ts + "." + string(body)))
	if !hmac.Equal(got, mac.Sum(nil)) {
		return errors.New("bad signature")
	}
	return nil
}

type recordingReceiver struct {
	mu       sync.Mutex
	statuses []int // response per call; last repeats
	bodies   []string
	sigErrs  []error
	clock    func() time.Time
}

func (rr *recordingReceiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	rr.mu.Lock()
	n := len(rr.bodies)
	rr.bodies = append(rr.bodies, string(body))
	rr.sigErrs = append(rr.sigErrs, verifyLikeYeaGrow(r, body, cbRepoSecret, rr.clock()))
	status := rr.statuses[min(n, len(rr.statuses)-1)]
	rr.mu.Unlock()
	w.WriteHeader(status)
}

func callbackConfigFor(t *testing.T, url string) *service.ImageTaskCallbackConfig {
	t.Helper()
	cfg, err := service.ParseImageTaskCallbackConfig([]byte(`[{"api_key_id":9,"url":"`+url+`","secret":"`+cbRepoSecret+`"}]`), true)
	require.NoError(t, err)
	return cfg
}

func TestImageTaskCallbackEndToEndRetryThenDeliver(t *testing.T) {
	mr, _, store := newCallbackTestStore(t)
	ctx := context.Background()
	clock := time.Unix(1_800_000_000, 0)
	now := func() time.Time { return clock }
	receiver := &recordingReceiver{statuses: []int{500, 200}, clock: now}
	server := httptest.NewServer(receiver)
	defer server.Close()
	cfg := callbackConfigFor(t, server.URL)

	// Finish a real task through the service so the body is built by production code.
	svc := service.NewImageTaskServiceWithUploader(store, nil, time.Hour, time.Minute)
	svc.SetCallbackConfig(cfg)
	require.NoError(t, store.Save(ctx, processingTask("t4"), time.Hour))
	exec := &service.ImageTaskExecution{Outcome: service.ImageTaskOutcomeRejected, Reason: service.ImageTaskReasonUpstreamRejected, Dispatched: true, UpstreamCalls: 1, LastUpstreamStatus: 429}
	require.NoError(t, svc.FailWithExecution(ctx, "t4", 429, json.RawMessage(`{"message":"slow down"}`), exec))
	require.True(t, errors.Is(svc.Fail(ctx, "t4", 500, nil), service.ErrImageTaskAlreadyFinished))

	// The event was enqueued at real time; run the deliverer far enough ahead.
	clock = time.Now().Add(time.Second)
	d := service.NewImageTaskCallbackDeliverer(store, cfg, nil)
	d.SetClock(now)

	stats, err := d.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, stats.Retried)
	require.Equal(t, "1", mr.HGet(imageTaskCallbackEventKey("evt_t4"), "failures"))
	require.Contains(t, mr.HGet(imageTaskCallbackEventKey("evt_t4"), "last_error"), "500")
	score, err := mr.ZScore(imageTaskCallbackDueKey, "evt_t4")
	require.NoError(t, err)
	require.Equal(t, float64(clock.Add(10*time.Second).UnixMilli()), score)

	stats, err = d.RunOnce(ctx)
	require.NoError(t, err)
	require.Zero(t, stats.Delivered+stats.Retried, "not due before 10s")

	clock = clock.Add(10 * time.Second)
	stats, err = d.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, stats.Delivered)
	require.False(t, mr.Exists(imageTaskCallbackEventKey("evt_t4")))
	require.False(t, mr.Exists(imageTaskCallbackDueKey))

	require.Len(t, receiver.bodies, 2)
	require.Equal(t, receiver.bodies[0], receiver.bodies[1], "body and event_id are identical on retry")
	require.Equal(t, `{"event_id":"evt_t4","task_id":"t4","run_id":"run_t4","attempt":1,"status":"rejected","upstream":{"http_status":429},"assets":[],"error":{"type":"rate_limit","message":"upstream_rejected: slow down"}}`, receiver.bodies[0])
	for _, sigErr := range receiver.sigErrs {
		require.NoError(t, sigErr)
	}
}

func TestImageTaskCallbackSurvivesRestart(t *testing.T) {
	mr, _, store := newCallbackTestStore(t)
	ctx := context.Background()
	require.NoError(t, store.Save(ctx, processingTask("t5"), time.Hour))
	done := *processingTask("t5")
	done.Status = service.ImageTaskStatusFailed
	created := time.Unix(1_800_000_000, 0)
	_, err := store.FinishProcessing(ctx, &done, time.Hour, &service.ImageTaskCallbackEnvelope{EventID: "evt_t5", APIKeyID: 9, Body: []byte(`{"event_id":"evt_t5"}`), CreatedAt: created})
	require.NoError(t, err)

	// Process A claims the event and dies before delivering.
	claimed, err := store.ClaimDueCallbacks(ctx, created, time.Minute, 10)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	again, err := store.ClaimDueCallbacks(ctx, created.Add(30*time.Second), time.Minute, 10)
	require.NoError(t, err)
	require.Empty(t, again, "leased events are not handed out twice")

	// Process B: a fresh client on the same Redis, after the lease.
	rdbB := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdbB.Close() }()
	storeB := NewImageTaskStore(rdbB).(*imageTaskStore)
	clock := created.Add(61 * time.Second)
	receiver := &recordingReceiver{statuses: []int{204}, clock: func() time.Time { return clock }}
	server := httptest.NewServer(receiver)
	defer server.Close()
	d := service.NewImageTaskCallbackDeliverer(storeB, callbackConfigFor(t, server.URL), nil)
	d.SetClock(func() time.Time { return clock })
	stats, err := d.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, stats.Delivered)
	require.Equal(t, []string{`{"event_id":"evt_t5"}`}, receiver.bodies)
	require.NoError(t, receiver.sigErrs[0])
}

func TestImageTaskCallbackAbandonAfter24h(t *testing.T) {
	mr, _, store := newCallbackTestStore(t)
	ctx := context.Background()
	require.NoError(t, store.Save(ctx, processingTask("t6"), time.Hour))
	done := *processingTask("t6")
	done.Status = service.ImageTaskStatusFailed
	created := time.Unix(1_800_000_000, 0)
	_, err := store.FinishProcessing(ctx, &done, time.Hour, &service.ImageTaskCallbackEnvelope{EventID: "evt_t6", APIKeyID: 9, Body: []byte(`{}`), CreatedAt: created})
	require.NoError(t, err)

	clock := created.Add(24 * time.Hour)
	receiver := &recordingReceiver{statuses: []int{503}, clock: func() time.Time { return clock }}
	server := httptest.NewServer(receiver)
	defer server.Close()
	d := service.NewImageTaskCallbackDeliverer(store, callbackConfigFor(t, server.URL), nil)
	d.SetClock(func() time.Time { return clock })
	stats, err := d.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, stats.Abandoned)
	require.False(t, mr.Exists(imageTaskCallbackDueKey))
	dead, err := mr.ZMembers(imageTaskCallbackDeadKey)
	require.NoError(t, err)
	require.Equal(t, []string{"evt_t6"}, dead)
	require.True(t, strings.Contains(mr.HGet(imageTaskCallbackEventKey("evt_t6"), "last_error"), "503"))
	require.LessOrEqual(t, mr.TTL(imageTaskCallbackEventKey("evt_t6")), service.ImageTaskCallbackRetention)
}

func TestImageTaskClaimDropsDanglingSchedule(t *testing.T) {
	mr, rdb, store := newCallbackTestStore(t)
	ctx := context.Background()
	require.NoError(t, rdb.ZAdd(ctx, imageTaskCallbackDueKey, redis.Z{Score: 1, Member: "evt_gone"}).Err())
	claimed, err := store.ClaimDueCallbacks(ctx, time.UnixMilli(10), time.Minute, 10)
	require.NoError(t, err)
	require.Empty(t, claimed)
	require.False(t, mr.Exists(imageTaskCallbackDueKey))
}
