package service

// YeaGrow execution plane: the URL result path (Kemei answers
// response_format:"url"), download → clean → object write → callback.
//
// The production download stack is used unchanged: imageDownloadTransport
// (hostname guard, redirect hops) and imageDownloadDialControl (resolved-IP
// guard). Only DNS and the final socket are replaced, in this file: a fake
// resolver maps the URL host to a chosen IP, the production dial check runs on
// that IP, and an allowed connection is then made to an httptest TLS server.
// No production code knows about this injection point; it is the existing
// NewImageResultUploader httpClient parameter.

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Observed Kemei result hosts (YeaGrow lib/providers/kemeitv-field-assistant-image-capabilities.ts,
// result host policy v3). Public IPs below are TEST-NET-3 documentation addresses.
var kemeiResultHosts = []string{
	"img-cdn.vncps.com",
	"sysmain.tos-cn-beijing.volces.com",
	"uskaichi.tos-accelerate.bytepluses.com.cn",
}

type urlTestServer struct {
	srv  *httptest.Server
	hits atomic.Int64
}

func newURLTestServer(t *testing.T, handler http.HandlerFunc) *urlTestServer {
	t.Helper()
	s := &urlTestServer{}
	s.srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.hits.Add(1)
		handler(w, r)
	}))
	t.Cleanup(s.srv.Close)
	return s
}

// guardedClient is the production download client with test DNS: dns maps a
// hostname to the address the resolver "returns"; the production dial check
// runs on it, and only allowed connections reach the test server.
func (s *urlTestServer) guardedClient(dns map[string]string, timeout time.Duration) *http.Client {
	base := newImageDownloadBaseTransport(nil)
	clientTransport, ok := s.srv.Client().Transport.(*http.Transport)
	if !ok {
		panic("httptest TLS client transport is not *http.Transport")
	}
	tlsCfg := clientTransport.TLSClientConfig.Clone()
	tlsCfg.ServerName = "example.com" // httptest certificate name
	base.TLSClientConfig = tlsCfg
	target := s.srv.Listener.Addr().String()
	var dialer net.Dialer
	base.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		resolved, ok := dns[host]
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

func newPlaneURLService(t *testing.T, client *http.Client) (*ImageTaskService, *finishStoreFake, *memObjectStorage) {
	t.Helper()
	storage := newMemObjectStorage()
	store := &finishStoreFake{idemTaskStore: newIdemTaskStore()}
	svc := NewImageTaskServiceWithUploader(store, NewImageResultUploader(storage, "images/", 0, client), time.Hour, time.Minute)
	cfg, err := ParseImageTaskCallbackConfig([]byte(`[{"api_key_id":9,"url":"https://a.example/cb","secret":"`+strings.Repeat("s", 32)+`"}]`), false)
	require.NoError(t, err)
	svc.SetCallbackConfig(cfg)
	require.NoError(t, store.Save(context.Background(), &ImageTaskRecord{ID: cbTestTaskID, UserID: 7, APIKeyID: 9, RunID: planeRunID, Plane: planeSpec(), Status: ImageTaskStatusProcessing}, time.Hour))
	return svc, store, storage
}

// urlResult is a Kemei-shaped response: body id, one url item, no b64_json.
func urlResult(rawURL string) json.RawMessage {
	return json.RawMessage(`{"id":"AigcImageTask-1","created":1,"data":[{"url":"` + rawURL + `"}],"size":"1024x1024"}`)
}

func completeURL(t *testing.T, svc *ImageTaskService, rawURL string) {
	t.Helper()
	require.NoError(t, svc.CompleteWithExecution(context.Background(), cbTestTaskID, 200, urlResult(rawURL), planeSucceeded))
}

// requireStorageFailed: the upstream already generated (and billed) the image,
// so any failure to fetch or store it is uncertain/storage_failed, never not_sent.
func requireStorageFailed(t *testing.T, store *finishStoreFake, storage *memObjectStorage) {
	t.Helper()
	rec, err := store.Get(context.Background(), cbTestTaskID)
	require.NoError(t, err)
	require.Equal(t, ImageTaskStatusFailed, rec.Status)
	require.Equal(t, ImageTaskOutcomeUncertain, rec.Execution.Outcome)
	require.Equal(t, ImageTaskReasonStorageFailed, rec.Execution.Reason)
	require.True(t, rec.Execution.Dispatched)
	require.Empty(t, rec.Assets)
	require.Len(t, store.events, 1, "exactly one callback event")
	var event ImageTaskCallbackEvent
	require.NoError(t, json.Unmarshal(store.events[0].Body, &event))
	require.Equal(t, ImageTaskOutcomeUncertain, event.Status)
	require.Empty(t, event.Assets)
	require.NotNil(t, event.Error)
	require.Equal(t, ImageTaskCallbackErrorUpstream5xx, event.Error.Type)
	require.True(t, strings.HasPrefix(event.Error.Message, ImageTaskReasonStorageFailed+":"), event.Error.Message)
	storage.mu.Lock()
	defer storage.mu.Unlock()
	require.Zero(t, storage.putCalls, "nothing is written to object storage")
}

func pngHandler(body []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(body)
	}
}

func TestExecutionPlaneURLResultSucceedsForObservedKemeiHosts(t *testing.T) {
	for _, host := range kemeiResultHosts {
		t.Run(host, func(t *testing.T) {
			image := testPNG(t, 64, 64)
			var gotPath, gotMethod string
			server := newURLTestServer(t, func(w http.ResponseWriter, r *http.Request) {
				gotPath, gotMethod = r.URL.RequestURI(), r.Method
				pngHandler(image)(w, r)
			})
			client := server.guardedClient(map[string]string{host: "203.0.113.10"}, 10*time.Second)
			svc, store, storage := newPlaneURLService(t, client)

			// Signed CDN-style URL: the query string is passed through untouched.
			completeURL(t, svc, "https://"+host+"/aigc/AigcImageTask-1.png?X-Tos-Expires=3600&X-Tos-Signature=abc")
			require.Equal(t, http.MethodGet, gotMethod)
			require.Equal(t, "/aigc/AigcImageTask-1.png?X-Tos-Expires=3600&X-Tos-Signature=abc", gotPath)

			rec, err := store.Get(context.Background(), cbTestTaskID)
			require.NoError(t, err)
			require.Equal(t, ImageTaskStatusCompleted, rec.Status)
			require.Equal(t, ImageTaskOutcomeSucceeded, rec.Execution.Outcome)
			require.Len(t, rec.Assets, 1)
			require.Equal(t, "generated/"+planeRunID+"/image-0", rec.Assets[0].R2Key)
			require.Equal(t, 64, rec.Assets[0].Width)
			require.NotContains(t, string(rec.Result), host, "the provider URL is not kept in the result")
			stored := storage.objects["generated/"+planeRunID+"/image-0"]
			require.Equal(t, "image/png", stored.ContentType)
			require.Len(t, store.events, 1)
			var event ImageTaskCallbackEvent
			require.NoError(t, json.Unmarshal(store.events[0].Body, &event))
			require.Equal(t, ImageTaskOutcomeSucceeded, event.Status)
			require.Len(t, event.Assets, 1)
			require.NotNil(t, event.Upstream)
			require.NotNil(t, event.Upstream.RequestID)
			require.Equal(t, "AigcImageTask-1", *event.Upstream.RequestID)
		})
	}
}

func TestExecutionPlaneURLDownloadFailuresAreUncertainStorageFailed(t *testing.T) {
	image := testPNG(t, 8, 8)
	const host = "img-cdn.vncps.com"
	publicDNS := map[string]string{host: "203.0.113.10"}
	cases := map[string]struct {
		handler http.HandlerFunc
		timeout time.Duration
	}{
		"404": {handler: func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) }},
		"403 expired signature": {handler: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte("<Error><Code>AccessDenied</Code><Message>Request has expired</Message></Error>"))
		}},
		"500": {handler: func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusInternalServerError) }},
		"oversized (10 MiB + 1)": {handler: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "image/png")
			_, _ = w.Write(image)
			_, _ = w.Write(make([]byte, ImageTaskAssetMaxBytes+1-len(image)))
		}},
		"headers never arrive": {timeout: 300 * time.Millisecond, handler: func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-r.Context().Done():
			case <-time.After(5 * time.Second):
			}
		}},
		"body stalls": {timeout: 300 * time.Millisecond, handler: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "image/png")
			w.Header().Set("Content-Length", fmt.Sprint(len(image)))
			_, _ = w.Write(image[:len(image)/2])
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			select {
			case <-r.Context().Done():
			case <-time.After(5 * time.Second):
			}
		}},
		"html page": {handler: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write([]byte("<!doctype html><title>CDN error</title>"))
		}},
		"html labelled image/png": {handler: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "image/png")
			_, _ = w.Write([]byte("<!doctype html><title>login</title>"))
		}},
		"truncated png": {handler: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "image/png")
			_, _ = w.Write(image[:len(image)-12])
		}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			server := newURLTestServer(t, tc.handler)
			timeout := tc.timeout
			if timeout == 0 {
				timeout = 10 * time.Second
			}
			svc, store, storage := newPlaneURLService(t, server.guardedClient(publicDNS, timeout))
			started := time.Now()
			completeURL(t, svc, "https://"+host+"/aigc/x.png")
			require.Less(t, time.Since(started), 4*time.Second, "bounded by the client timeout")
			require.EqualValues(t, 1, server.hits.Load(), "downloaded once, never resubmitted upstream")
			requireStorageFailed(t, store, storage)
		})
	}
}

func TestExecutionPlaneURLPrivateDestinationsAreRefusedAndUncertain(t *testing.T) {
	image := testPNG(t, 8, 8)
	cases := map[string]struct {
		url string
		dns map[string]string
	}{
		"host resolves to RFC1918 (DNS rebinding)": {url: "https://img-cdn.vncps.com/x.png", dns: map[string]string{"img-cdn.vncps.com": "10.0.0.5"}},
		"host resolves to loopback":                {url: "https://img-cdn.vncps.com/x.png", dns: map[string]string{"img-cdn.vncps.com": "127.0.0.1"}},
		"host resolves to cloud metadata":          {url: "https://img-cdn.vncps.com/x.png", dns: map[string]string{"img-cdn.vncps.com": "169.254.169.254"}},
		"host resolves to CGNAT (tailnet)":         {url: "https://img-cdn.vncps.com/x.png", dns: map[string]string{"img-cdn.vncps.com": "100.70.235.60"}},
		"literal private IP":                       {url: "https://10.1.2.3/x.png", dns: map[string]string{"10.1.2.3": "10.1.2.3"}},
		"localhost name":                           {url: "https://localhost/x.png", dns: map[string]string{"localhost": "203.0.113.10"}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			server := newURLTestServer(t, pngHandler(image))
			svc, store, storage := newPlaneURLService(t, server.guardedClient(tc.dns, 5*time.Second))
			completeURL(t, svc, tc.url)
			require.Zero(t, server.hits.Load(), "no connection to a private destination")
			requireStorageFailed(t, store, storage)
		})
	}
}

func TestExecutionPlaneURLRedirectHopsAreGuarded(t *testing.T) {
	image := testPNG(t, 8, 8)
	t.Run("public to public is followed", func(t *testing.T) {
		server := newURLTestServer(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Host == "img-cdn.vncps.com" {
				http.Redirect(w, r, "https://sysmain.tos-cn-beijing.volces.com/x.png", http.StatusFound)
				return
			}
			pngHandler(image)(w, r)
		})
		svc, store, _ := newPlaneURLService(t, server.guardedClient(map[string]string{
			"img-cdn.vncps.com": "203.0.113.10", "sysmain.tos-cn-beijing.volces.com": "203.0.113.11"}, 5*time.Second))
		completeURL(t, svc, "https://img-cdn.vncps.com/x.png")
		rec, _ := store.Get(context.Background(), cbTestTaskID)
		require.Equal(t, ImageTaskStatusCompleted, rec.Status)
		require.EqualValues(t, 2, server.hits.Load())
	})
	t.Run("public to private is refused", func(t *testing.T) {
		server := newURLTestServer(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Host == "img-cdn.vncps.com" {
				http.Redirect(w, r, "https://internal.example/x.png", http.StatusFound)
				return
			}
			pngHandler(image)(w, r)
		})
		svc, store, storage := newPlaneURLService(t, server.guardedClient(map[string]string{
			"img-cdn.vncps.com": "203.0.113.10", "internal.example": "192.168.1.20"}, 5*time.Second))
		completeURL(t, svc, "https://img-cdn.vncps.com/x.png")
		require.EqualValues(t, 1, server.hits.Load(), "the private hop is never connected")
		requireStorageFailed(t, store, storage)
	})
}

func TestExecutionPlaneURLDownloadCapIsTenMiB(t *testing.T) {
	// The plane path caps the download at YeaGrow's 10 MiB object limit even if
	// the uploader is configured larger (default 32 MiB): exactly 10 MiB of
	// junk after a valid header fails on size, 10 MiB + 1 on the download cap.
	require.Equal(t, 10*1024*1024, imageTaskPlaneMaxDownloadBytes)
	uploader := NewImageResultUploader(newMemObjectStorage(), "images/", 0, nil)
	require.EqualValues(t, defaultImageMaxDownloadBytes, uploader.maxDownloadBytes)
	require.Greater(t, uploader.maxDownloadBytes, int64(imageTaskPlaneMaxDownloadBytes))
}

func TestProductionImageDownloadClientIsGuardedAndBounded(t *testing.T) {
	client := newImageDownloadHTTPClient(nil)
	require.Equal(t, 60*time.Second, client.Timeout, "whole download, headers and body, is bounded")
	transport, ok := client.Transport.(*imageDownloadTransport)
	require.True(t, ok)
	require.NotNil(t, transport.direct)
	// The resolved-IP guard accepts public addresses and refuses private ones.
	for _, ok := range []string{"203.0.113.10:443", "8.8.8.8:443", "[2001:4860:4860::8888]:443"} {
		require.NoError(t, imageDownloadDialControl("tcp", ok, nil), ok)
	}
	for _, blocked := range []string{"10.0.0.1:443", "127.0.0.1:443", "169.254.169.254:80", "100.64.0.1:443", "[::1]:443", "[fd00::1]:443"} {
		require.Error(t, imageDownloadDialControl("tcp", blocked, nil), blocked)
	}
	for _, host := range kemeiResultHosts {
		require.False(t, isBlockedImageDownloadHost(host), host)
	}
}
