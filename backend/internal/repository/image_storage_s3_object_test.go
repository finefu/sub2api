package repository

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

// fakeS3 is a minimal path-style S3 endpoint (PUT/GET object) that records
// what the SDK sends and echoes metadata and the SHA-256 checksum on GET.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string]fakeS3Object
	lastPut http.Header
}

type fakeS3Object struct {
	body     []byte
	header   http.Header
	checksum string
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch r.Method {
	case http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		sum := sha256.Sum256(body)
		claimed := r.Header.Get("X-Amz-Checksum-Sha256")
		if claimed != "" && claimed != base64.StdEncoding.EncodeToString(sum[:]) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `<Error><Code>BadDigest</Code></Error>`)
			return
		}
		hdr := http.Header{}
		for k, v := range r.Header {
			if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-") || strings.EqualFold(k, "Content-Type") {
				hdr[k] = v
			}
		}
		f.objects[r.URL.Path] = fakeS3Object{body: body, header: hdr, checksum: claimed}
		f.lastPut = r.Header.Clone()
		w.Header().Set("ETag", `"etag"`)
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		obj, ok := f.objects[r.URL.Path]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `<Error><Code>NoSuchKey</Code></Error>`)
			return
		}
		for k, v := range obj.header {
			w.Header()[k] = v
		}
		if strings.EqualFold(r.Header.Get("X-Amz-Checksum-Mode"), "ENABLED") && obj.checksum != "" {
			w.Header().Set("X-Amz-Checksum-Sha256", obj.checksum)
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(obj.body)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(obj.body)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func TestS3ImageStoragePutAndGetImageObject(t *testing.T) {
	fake := &fakeS3{objects: map[string]fakeS3Object{}}
	server := httptest.NewServer(fake)
	defer server.Close()
	storage, err := NewS3ImageStorage(context.Background(), &config.ImageStorageConfig{
		Enabled: true, Endpoint: server.URL, Region: "auto", Bucket: "yeagrow-generated",
		AccessKeyID: "ak", SecretAccessKey: "sk", ForcePathStyle: true,
	})
	require.NoError(t, err)

	data := []byte("\x89PNG\r\n\x1a\nnot-really-a-png")
	digest := sha256.Sum256(data)
	metadata := map[string]string{"run_id": "run_1", "route_version": "v3", "provider_id": "kemeitv", "mime_type": "image/png", "sha256": "abc", "created_at": "1794400000123"}
	require.NoError(t, storage.PutImageObject(context.Background(), "generated/run_1/image-0", "image/png", data, metadata, digest))

	require.Equal(t, base64.StdEncoding.EncodeToString(digest[:]), fake.lastPut.Get("X-Amz-Checksum-Sha256"), "SHA-256 checksum header is sent")
	require.NotContains(t, strings.ToLower(fake.lastPut.Get("Content-Encoding")), "aws-chunked", "plain body, no trailer encoding")
	require.Equal(t, "image/png", fake.lastPut.Get("Content-Type"))
	require.Contains(t, fake.objects, "/yeagrow-generated/generated/run_1/image-0")
	require.Equal(t, data, fake.objects["/yeagrow-generated/generated/run_1/image-0"].body)

	stored, err := storage.GetImageObject(context.Background(), "generated/run_1/image-0")
	require.NoError(t, err)
	require.Equal(t, data, stored.Data)
	require.Equal(t, "image/png", stored.ContentType)
	require.Equal(t, metadata, stored.Metadata)
	require.Equal(t, digest[:], stored.ChecksumSHA256)

	// A wrong checksum is refused by the store.
	var wrong [32]byte
	require.Error(t, storage.PutImageObject(context.Background(), "generated/run_1/image-0", "image/png", data, metadata, wrong))

	_, err = storage.GetImageObject(context.Background(), "generated/missing/image-0")
	require.Error(t, err)
}
