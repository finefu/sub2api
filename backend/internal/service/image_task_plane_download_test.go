package service

// Execution-plane result-URL download retry (YeaGrow downloadResultWithRetry parity).
// Uses the guarded production download stack from image_task_plane_url_test.go.

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// useFastPlaneDownloadPolicy shortens the timings for tests; restored afterwards.
// The tests that call it are not parallel.
func useFastPlaneDownloadPolicy(t *testing.T) {
	t.Helper()
	saved := imageTaskPlaneDownload
	imageTaskPlaneDownload = imageTaskPlaneDownloadPolicy{
		MaxAttempts: 3, BaseDelay: 50 * time.Millisecond, RetryWindow: 2 * time.Second,
		Idle: 300 * time.Millisecond, Total: 5 * time.Second, URLValidity: 15 * time.Minute,
	}
	t.Cleanup(func() { imageTaskPlaneDownload = saved })
}

func setPlaneDownloadPolicy(t *testing.T, p imageTaskPlaneDownloadPolicy) {
	t.Helper()
	useFastPlaneDownloadPolicy(t)
	imageTaskPlaneDownload = p
}

// guardedClientResolving is guardedClient with a resolver called on every dial,
// so a test can change the DNS answer between attempts.
func (s *urlTestServer) guardedClientResolving(resolve func(host string) (string, bool), timeout time.Duration) *http.Client {
	base := newImageDownloadBaseTransport(nil)
	clientTransport, ok := s.srv.Client().Transport.(*http.Transport)
	if !ok {
		panic("httptest TLS client transport is not *http.Transport")
	}
	tlsCfg := clientTransport.TLSClientConfig.Clone()
	tlsCfg.ServerName = "example.com"
	base.TLSClientConfig = tlsCfg
	base.DisableKeepAlives = true // every attempt dials (and is checked) again
	target := s.srv.Listener.Addr().String()
	var dialer net.Dialer
	base.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		resolved, ok := resolve(host)
		if !ok {
			return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
		}
		if err := imageDownloadDialControl(network, net.JoinHostPort(resolved, port), nil); err != nil {
			return nil, err
		}
		return dialer.DialContext(ctx, network, target)
	}
	return &http.Client{Timeout: timeout, Transport: &imageDownloadTransport{direct: base}}
}

const retryHost = "img-cdn.vncps.com"
const retryURL = "https://" + retryHost + "/aigc/x.png"

func publicDNS() map[string]string { return map[string]string{retryHost: "203.0.113.10"} }

// sequence answers the n-th request (1-based) with steps[n-1]; the last step repeats.
func sequence(steps ...http.HandlerFunc) http.HandlerFunc {
	var n atomic.Int64
	return func(w http.ResponseWriter, r *http.Request) {
		i := int(n.Add(1)) - 1
		if i >= len(steps) {
			i = len(steps) - 1
		}
		steps[i](w, r)
	}
}

func answerStatus(code int, headers ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		for i := 0; i+1 < len(headers); i += 2 {
			w.Header().Set(headers[i], headers[i+1])
		}
		w.WriteHeader(code)
	}
}

// resetMidBody sends half of a declared body and drops the connection.
func resetMidBody(body []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			panic("response writer does not support hijacking")
		}
		conn, buf, err := hijacker.Hijack()
		if err != nil {
			panic(err)
		}
		_, _ = fmt.Fprintf(buf, "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Length: %d\r\n\r\n", len(body))
		_, _ = buf.Write(body[:len(body)/2])
		_ = buf.Flush()
		_ = conn.Close()
	}
}

// stallAfterHeaders sends headers and part of the body, then makes no progress.
func stallAfterHeaders(body []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		_, _ = w.Write(body[:len(body)/2])
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		select {
		case <-r.Context().Done():
		case <-time.After(10 * time.Second):
		}
	}
}

func taskRecord(t *testing.T, store *finishStoreFake) *ImageTaskRecord {
	t.Helper()
	rec, err := store.Get(context.Background(), cbTestTaskID)
	require.NoError(t, err)
	return rec
}

func requireSucceededAfter(t *testing.T, store *finishStoreFake, attempts int) {
	t.Helper()
	rec := taskRecord(t, store)
	require.Equal(t, ImageTaskStatusCompleted, rec.Status)
	require.Equal(t, ImageTaskOutcomeSucceeded, rec.Execution.Outcome)
	require.Equal(t, attempts, rec.Execution.DownloadAttempts)
	require.Len(t, store.events, 1)
	var event ImageTaskCallbackEvent
	require.NoError(t, json.Unmarshal(store.events[0].Body, &event))
	require.Equal(t, ImageTaskOutcomeSucceeded, event.Status)
	require.Len(t, event.Assets, 1)
}

func callbackMessage(t *testing.T, store *finishStoreFake) string {
	t.Helper()
	require.Len(t, store.events, 1)
	var event ImageTaskCallbackEvent
	require.NoError(t, json.Unmarshal(store.events[0].Body, &event))
	require.NotNil(t, event.Error)
	return event.Error.Message
}

func TestPlaneDownloadPolicyMatchesYeaGrow(t *testing.T) {
	p := imageTaskPlaneDownload
	require.Equal(t, 3, p.MaxAttempts, "MAX_EXTRA_ATTEMPTS = 2")
	require.Equal(t, time.Second, p.BaseDelay, "1_000 * 2 ** attempt")
	require.Equal(t, 30*time.Second, p.RetryWindow, "RETRY_WINDOW_MS")
	require.Equal(t, 15*time.Second, p.Idle, "DOWNLOAD_IDLE_MS")
	require.Equal(t, 90*time.Second, p.Total, "min(90_000, retryUntil - now)")
	require.Equal(t, 15*time.Minute, p.URLValidity, "retryUntil = acceptedAt + 15 * 60_000")
	for _, code := range []int{429, 500, 502, 503, 504, 524} {
		require.True(t, imageTaskPlaneRetryableStatus[code], code)
	}
	require.Len(t, imageTaskPlaneRetryableStatus, 6)
}

func TestPlaneRetryDelay(t *testing.T) {
	now := time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC)
	require.Equal(t, time.Second, planeRetryDelay("", 1, time.Second, now))
	require.Equal(t, 2*time.Second, planeRetryDelay("", 2, time.Second, now))
	require.Equal(t, 5*time.Second, planeRetryDelay("5", 1, time.Second, now), "Retry-After seconds when longer")
	require.Equal(t, 2*time.Second, planeRetryDelay("0", 2, time.Second, now), "never shorter than the backoff")
	require.Equal(t, 7*time.Second, planeRetryDelay(now.Add(7*time.Second).Format(http.TimeFormat), 1, time.Second, now))
	require.Equal(t, time.Second, planeRetryDelay("soon", 1, time.Second, now), "unparsable falls back")
	require.GreaterOrEqual(t, planeRetryDelay("999999999", 1, time.Second, now), time.Hour, "absurd values stop the retry")
}

func TestPlaneDeliveryDeadlineIsTheTightestBound(t *testing.T) {
	start := time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC)
	created := start.Add(-time.Minute).Unix()
	require.True(t, start.Add(90*time.Second).Equal(imageTaskPlaneDeliveryDeadline(start, start, created, 30*time.Minute)), "90 s")
	require.True(t, start.Add(10*time.Second).Equal(imageTaskPlaneDeliveryDeadline(start, start.Add(-(15*time.Minute-10*time.Second)), created, 30*time.Minute)), "URL validity")
	require.True(t, start.Add(20*time.Second).Equal(imageTaskPlaneDeliveryDeadline(start, start, created, time.Minute+20*time.Second)), "task execution timeout")
}

func TestPlaneDownloadRetriesTransientFailuresThenSucceeds(t *testing.T) {
	useFastPlaneDownloadPolicy(t)
	image := testPNG(t, 16, 16)
	cases := map[string]struct {
		first  http.HandlerFunc
		client time.Duration
	}{
		"500 then 200":                {first: answerStatus(http.StatusInternalServerError)},
		"503 then 200":                {first: answerStatus(http.StatusServiceUnavailable)},
		"429 then 200":                {first: answerStatus(http.StatusTooManyRequests)},
		"524 then 200":                {first: answerStatus(524)},
		"connection reset mid-body":   {first: resetMidBody(image)},
		"idle mid-body (watchdog)":    {first: stallAfterHeaders(image), client: 10 * time.Second},
		"client timeout on first try": {first: stallAfterHeaders(image), client: 200 * time.Millisecond},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			server := newURLTestServer(t, sequence(tc.first, pngHandler(image)))
			timeout := tc.client
			if timeout == 0 {
				timeout = 10 * time.Second
			}
			svc, store, _ := newPlaneURLService(t, server.guardedClientResolving(func(h string) (string, bool) { ip, ok := publicDNS()[h]; return ip, ok }, timeout))
			completeURL(t, svc, retryURL)
			require.EqualValues(t, 2, server.hits.Load())
			requireSucceededAfter(t, store, 2)
		})
	}
}

func TestPlaneDownloadAllAttemptsFailIsUncertainWithAttemptCount(t *testing.T) {
	useFastPlaneDownloadPolicy(t)
	server := newURLTestServer(t, answerStatus(http.StatusBadGateway))
	svc, store, storage := newPlaneURLService(t, server.guardedClient(publicDNS(), 5*time.Second))
	completeURL(t, svc, retryURL)
	require.EqualValues(t, 3, server.hits.Load())
	requireStorageFailed(t, store, storage)
	require.Equal(t, 3, taskRecord(t, store).Execution.DownloadAttempts)
	require.Equal(t, "storage_failed: failed to store generated image to object storage (result download attempts: 3)", callbackMessage(t, store))
}

func TestPlaneDownloadNonRetryableFailuresRequestOnce(t *testing.T) {
	useFastPlaneDownloadPolicy(t)
	image := testPNG(t, 8, 8)
	gif := []byte("GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xff\xff\xff!\xf9\x04\x01\x00\x00\x00\x00,\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02D\x01\x00;")
	cases := map[string]http.HandlerFunc{
		"404":                   answerStatus(http.StatusNotFound),
		"403 expired signature": answerStatus(http.StatusForbidden),
		"400":                   answerStatus(http.StatusBadRequest),
		"410":                   answerStatus(http.StatusGone),
		"501":                   answerStatus(http.StatusNotImplemented),
		"oversized": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "image/png")
			_, _ = w.Write(image)
			_, _ = w.Write(make([]byte, ImageTaskAssetMaxBytes))
		},
		"not an image": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte("<html>busy</html>"))
		},
		"cleaning rejects (gif)": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "image/gif")
			_, _ = w.Write(gif)
		},
		"redirect to private host": func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "https://internal.example/x.png", http.StatusFound)
		},
	}
	for name, handler := range cases {
		t.Run(name, func(t *testing.T) {
			server := newURLTestServer(t, handler)
			dns := publicDNS()
			dns["internal.example"] = "10.0.0.9"
			svc, store, storage := newPlaneURLService(t, server.guardedClient(dns, 5*time.Second))
			completeURL(t, svc, retryURL)
			require.EqualValues(t, 1, server.hits.Load())
			requireStorageFailed(t, store, storage)
			require.Equal(t, 1, taskRecord(t, store).Execution.DownloadAttempts)
			require.True(t, strings.HasSuffix(callbackMessage(t, store), "(result download attempts: 1)"))
		})
	}
}

func TestPlaneDownloadEveryRetryPassesTheSSRFGuard(t *testing.T) {
	useFastPlaneDownloadPolicy(t)
	// DNS rebinding between attempts: the first answer is public (and fails
	// with 503), the retry resolves to a private address and is refused before
	// connecting. The refusal is final.
	var lookups atomic.Int64
	resolve := func(host string) (string, bool) {
		if host != retryHost {
			return "", false
		}
		if lookups.Add(1) == 1 {
			return "203.0.113.10", true
		}
		return "10.0.0.5", true
	}
	server := newURLTestServer(t, answerStatus(http.StatusServiceUnavailable))
	svc, store, storage := newPlaneURLService(t, server.guardedClientResolving(resolve, 5*time.Second))
	completeURL(t, svc, retryURL)
	require.EqualValues(t, 1, server.hits.Load(), "the private retry never connects")
	require.EqualValues(t, 2, lookups.Load())
	requireStorageFailed(t, store, storage)
	require.Equal(t, 2, taskRecord(t, store).Execution.DownloadAttempts)
}

func TestPlaneDownloadRetryAfterIsHonouredAndBoundedByTheWindow(t *testing.T) {
	image := testPNG(t, 8, 8)
	t.Run("Retry-After within the window", func(t *testing.T) {
		useFastPlaneDownloadPolicy(t)
		var first, second time.Time
		server := newURLTestServer(t, sequence(
			func(w http.ResponseWriter, r *http.Request) {
				first = time.Now()
				answerStatus(http.StatusServiceUnavailable, "Retry-After", "1")(w, r)
			},
			func(w http.ResponseWriter, r *http.Request) { second = time.Now(); pngHandler(image)(w, r) },
		))
		svc, store, _ := newPlaneURLService(t, server.guardedClient(publicDNS(), 5*time.Second))
		completeURL(t, svc, retryURL)
		require.GreaterOrEqual(t, second.Sub(first), 900*time.Millisecond)
		requireSucceededAfter(t, store, 2)
	})
	t.Run("Retry-After beyond the window stops", func(t *testing.T) {
		useFastPlaneDownloadPolicy(t) // window 2 s
		server := newURLTestServer(t, answerStatus(http.StatusServiceUnavailable, "Retry-After", "10"))
		svc, store, storage := newPlaneURLService(t, server.guardedClient(publicDNS(), 5*time.Second))
		started := time.Now()
		completeURL(t, svc, retryURL)
		require.Less(t, time.Since(started), time.Second, "no 10 s wait")
		require.EqualValues(t, 1, server.hits.Load())
		requireStorageFailed(t, store, storage)
	})
}

func TestPlaneDownloadStopsAtTheRetryWindowAndTheDeadline(t *testing.T) {
	t.Run("retry window", func(t *testing.T) {
		// 1st fails at ~0, retry after 200 ms, 2nd fails, next retry would start at
		// ~600 ms >= window 500 ms: stop after 2 attempts.
		setPlaneDownloadPolicy(t, imageTaskPlaneDownloadPolicy{MaxAttempts: 3, BaseDelay: 200 * time.Millisecond,
			RetryWindow: 500 * time.Millisecond, Idle: time.Second, Total: 10 * time.Second, URLValidity: 15 * time.Minute})
		server := newURLTestServer(t, answerStatus(http.StatusServiceUnavailable))
		svc, store, storage := newPlaneURLService(t, server.guardedClient(publicDNS(), 5*time.Second))
		completeURL(t, svc, retryURL)
		require.EqualValues(t, 2, server.hits.Load())
		requireStorageFailed(t, store, storage)
		require.Equal(t, 2, taskRecord(t, store).Execution.DownloadAttempts)
	})
	t.Run("total deadline cuts a running attempt", func(t *testing.T) {
		// Total 700 ms; every attempt stalls (idle 2 s would not fire): the
		// delivery deadline aborts the first attempt and nothing is retried.
		setPlaneDownloadPolicy(t, imageTaskPlaneDownloadPolicy{MaxAttempts: 3, BaseDelay: 50 * time.Millisecond,
			RetryWindow: 10 * time.Second, Idle: 2 * time.Second, Total: 700 * time.Millisecond, URLValidity: 15 * time.Minute})
		image := testPNG(t, 8, 8)
		server := newURLTestServer(t, stallAfterHeaders(image))
		svc, store, storage := newPlaneURLService(t, server.guardedClient(publicDNS(), 30*time.Second))
		started := time.Now()
		completeURL(t, svc, retryURL)
		require.Less(t, time.Since(started), 1500*time.Millisecond)
		require.EqualValues(t, 1, server.hits.Load())
		requireStorageFailed(t, store, storage)
	})
	t.Run("execution timeout already spent", func(t *testing.T) {
		useFastPlaneDownloadPolicy(t)
		server := newURLTestServer(t, pngHandler(testPNG(t, 8, 8)))
		svc, store, storage := newPlaneURLService(t, server.guardedClient(publicDNS(), 5*time.Second))
		rec := taskRecord(t, store)
		rec.CreatedAt = time.Now().Add(-2 * svc.ExecutionTimeout()).Unix()
		require.NoError(t, store.Save(context.Background(), rec, time.Hour))
		completeURL(t, svc, retryURL)
		require.Zero(t, server.hits.Load(), "no download beyond the task execution timeout")
		requireStorageFailed(t, store, storage)
		require.Equal(t, "storage_failed: failed to store generated image to object storage", callbackMessage(t, store))
	})
}

func TestPlaneDownloadTaskStaysProcessingWhileRetrying(t *testing.T) {
	useFastPlaneDownloadPolicy(t)
	image := testPNG(t, 8, 8)
	release := make(chan struct{})
	inSecond := make(chan struct{})
	server := newURLTestServer(t, sequence(
		answerStatus(http.StatusServiceUnavailable),
		func(w http.ResponseWriter, r *http.Request) { close(inSecond); <-release; pngHandler(image)(w, r) },
	))
	svc, store, _ := newPlaneURLService(t, server.guardedClient(publicDNS(), 10*time.Second))
	done := make(chan struct{})
	go func() { defer close(done); completeURL(t, svc, retryURL) }()
	select {
	case <-inSecond:
	case <-time.After(5 * time.Second):
		t.Fatal("second attempt did not start")
	}
	rec := taskRecord(t, store)
	require.Equal(t, ImageTaskStatusProcessing, rec.Status, "processing while the download is retried")
	require.Empty(t, store.events)
	close(release)
	<-done
	requireSucceededAfter(t, store, 2)
}

func TestPlaneDownloadInlineResultsAreNotCounted(t *testing.T) {
	svc, store := newPlaneService(t, newMemObjectStorage())
	require.NoError(t, svc.CompleteWithExecution(context.Background(), cbTestTaskID, 200, b64Result(testPNG(t, 4, 4)), planeSucceeded))
	rec := taskRecord(t, store)
	require.Equal(t, ImageTaskStatusCompleted, rec.Status)
	require.Zero(t, rec.Execution.DownloadAttempts)
}
