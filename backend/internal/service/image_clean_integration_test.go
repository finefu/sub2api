package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// pngWithChunk inserts a chunk right after IHDR.
func pngWithChunk(t *testing.T, base []byte, typ string, data []byte) []byte {
	t.Helper()
	ihdrEnd := 8 + 12 + 13
	out := append([]byte(nil), base[:ihdrEnd]...)
	out = append(out, makePNGChunk(typ, data)...)
	return append(out, base[ihdrEnd:]...)
}

func TestExecutionPlaneStoresYeaGrowCleanedBytes(t *testing.T) {
	clean := testPNG(t, 3, 2)
	dirty := pngWithChunk(t, clean, "tEXt", []byte("Author\x00Jane Doe"))
	storage := newMemObjectStorage()
	svc, store := newPlaneService(t, storage)

	require.NoError(t, svc.CompleteWithExecution(context.Background(), cbTestTaskID, 200, b64Result(dirty), planeSucceeded))

	stored := storage.objects["generated/"+planeRunID+"/image-0"]
	require.Equal(t, clean, stored.Data, "private text chunk removed, everything else byte-identical")
	digest := sha256.Sum256(clean)
	require.Equal(t, hex.EncodeToString(digest[:]), stored.Metadata["sha256"])
	rec, _ := store.Get(context.Background(), cbTestTaskID)
	require.Equal(t, ImageTaskStatusCompleted, rec.Status)
	require.True(t, rec.Assets[0].Cleaned)
	require.Equal(t, int64(len(clean)), rec.Assets[0].Bytes)

	// What was stored passes YeaGrow's readback rule: re-cleaning changes nothing.
	again, err := CleanGeneratedImage(stored.Data, "image/png")
	require.NoError(t, err)
	require.False(t, again.Changed)
}

func TestExecutionPlaneCleanerRejectionIsUncertainStorageFailed(t *testing.T) {
	cases := map[string][]byte{
		"unknown text keyword": pngWithChunk(t, testPNG(t, 3, 2), "tEXt", []byte("Comment\x00hello")),
		"animated png":         pngWithChunk(t, testPNG(t, 3, 2), "acTL", make([]byte, 8)),
		"truncated png":        testPNG(t, 3, 2)[:40],
	}
	for name, img := range cases {
		t.Run(name, func(t *testing.T) {
			storage := newMemObjectStorage()
			svc, store := newPlaneService(t, storage)
			require.NoError(t, svc.CompleteWithExecution(context.Background(), cbTestTaskID, 200, b64Result(img), planeSucceeded))
			rec, _ := store.Get(context.Background(), cbTestTaskID)
			require.Equal(t, ImageTaskStatusFailed, rec.Status)
			require.Equal(t, ImageTaskOutcomeUncertain, rec.Execution.Outcome)
			require.Equal(t, ImageTaskReasonStorageFailed, rec.Execution.Reason)
			require.Zero(t, storage.putCalls, "nothing is written when YeaGrow's rules reject the image")
		})
	}
}

func TestExecutionPlaneDownloadCappedAt10MiB(t *testing.T) {
	big := append(testPNG(t, 2, 2), make([]byte, ImageTaskAssetMaxBytes)...)
	var served atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		n, _ := w.Write(big)
		served.Add(int64(n))
	}))
	defer server.Close()

	storage := newMemObjectStorage()
	store := &finishStoreFake{idemTaskStore: newIdemTaskStore()}
	// The uploader itself allows 32 MiB (upstream default); execution-plane tasks must still stop at 10 MiB.
	svc := NewImageTaskServiceWithUploader(store, NewImageResultUploader(storage, "images/", 32*1024*1024, server.Client()), time.Hour, time.Minute)
	require.NoError(t, store.Save(context.Background(), &ImageTaskRecord{ID: cbTestTaskID, UserID: 7, APIKeyID: 9, RunID: planeRunID, Plane: planeSpec(), Status: ImageTaskStatusProcessing}, time.Hour))

	result := json.RawMessage(`{"data":[{"url":"` + server.URL + `/big.png"}]}`)
	require.NoError(t, svc.CompleteWithExecution(context.Background(), cbTestTaskID, 200, result, planeSucceeded))
	rec, _ := store.Get(context.Background(), cbTestTaskID)
	require.Equal(t, ImageTaskOutcomeUncertain, rec.Execution.Outcome)
	require.Equal(t, ImageTaskReasonStorageFailed, rec.Execution.Reason)
	require.Zero(t, storage.putCalls)

	// Non-execution-plane tasks keep the uploader's own limit (32 MiB here).
	plain := &ImageTaskRecord{ID: "imgtask_plain", UserID: 7, APIKeyID: 9, Status: ImageTaskStatusProcessing}
	require.NoError(t, store.Save(context.Background(), plain, time.Hour))
	require.NoError(t, svc.Complete(context.Background(), "imgtask_plain", 200, result))
	got, _ := store.Get(context.Background(), "imgtask_plain")
	require.Equal(t, ImageTaskStatusCompleted, got.Status)
	require.True(t, strings.Contains(string(got.Result), "https://cdn.example/images/imgtask_plain-0.png"))
	require.Equal(t, 1, storage.saveCalls)
}

func TestCleanGeneratedImageRejectsOtherTypes(t *testing.T) {
	_, err := CleanGeneratedImage([]byte("GIF89a"), "image/gif")
	var cleanErr *ImageCleanError
	require.ErrorAs(t, err, &cleanErr)
	require.Equal(t, "invalid_declared_format", cleanErr.Code)
	_, err = yeagrowImageAssetCleaner(context.Background(), []byte("GIF89a"), "image/gif")
	require.Error(t, err)
	_, err = CleanGeneratedImage(bytes.Repeat([]byte{0}, 4), "image/png")
	require.ErrorAs(t, err, &cleanErr)
	require.Equal(t, "image_format_mismatch", cleanErr.Code)
}

func TestIsSafeGeneratedImageDimensions(t *testing.T) {
	// Expected values computed with YeaGrow's isSafeGeneratedImageDimensions @ 215bd837.
	cases := []struct {
		w, h int64
		ok   bool
	}{
		{1, 1, true}, {0, 1, false}, {4096, 4096, true}, {4097, 1, false}, {4096, 4097, true},
		{3892, 3892, true}, {4300, 4300, true}, {4301, 4300, false}, {3891, 4096, true},
		{5504, 3072, true}, {6336, 2688, true}, {2304, 4096, true}, {4096, 1, true}, {4097, 4097, true}, {5780, 3072, false},
	}
	for _, c := range cases {
		require.Equal(t, c.ok, isSafeGeneratedImageDimensions(c.w, c.h), "%dx%d", c.w, c.h)
	}
}
