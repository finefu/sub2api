package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"image"
	"image/color"
	"image/png"
	"mime/multipart"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const planeRunID = "run_exec_plane_1"

func planeSpec() *ImageTaskPlaneSpec {
	return &ImageTaskPlaneSpec{AssetPrefix: "generated/" + planeRunID + "/", RouteVersion: "route-v3", ProviderID: "kemeitv", CreatedAt: 1794400000123}
}

func testPNG(t *testing.T, w, h int) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	img.Set(0, 0, color.RGBA{R: 255, A: 255})
	var buf bytes.Buffer
	require.NoError(t, png.Encode(&buf, img))
	return buf.Bytes()
}

// memObjectStorage implements ImageStorage and ImageObjectStorage in memory.
type memObjectStorage struct {
	mu        sync.Mutex
	objects   map[string]StoredImageObject
	putErr    error
	tamper    func(*StoredImageObject)
	putCalls  int
	saveCalls int
}

func newMemObjectStorage() *memObjectStorage {
	return &memObjectStorage{objects: map[string]StoredImageObject{}}
}

func (m *memObjectStorage) Save(_ context.Context, key, _ string, _ []byte) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.saveCalls++
	return "https://cdn.example/" + key, nil
}

func (m *memObjectStorage) PutImageObject(_ context.Context, key, contentType string, data []byte, metadata map[string]string, digest [32]byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.putCalls++
	if m.putErr != nil {
		return m.putErr
	}
	meta := map[string]string{}
	for k, v := range metadata {
		meta[k] = v
	}
	m.objects[key] = StoredImageObject{ContentType: contentType, Metadata: meta, ChecksumSHA256: append([]byte(nil), digest[:]...), Data: append([]byte(nil), data...)}
	return nil
}

func (m *memObjectStorage) GetImageObject(_ context.Context, key string) (*StoredImageObject, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, ok := m.objects[key]
	if !ok {
		return nil, errors.New("no such key")
	}
	meta := map[string]string{}
	for k, v := range obj.Metadata {
		meta[k] = v
	}
	obj.Metadata = meta
	if m.tamper != nil {
		m.tamper(&obj)
	}
	return &obj, nil
}

func newPlaneService(t *testing.T, storage ImageStorage) (*ImageTaskService, *finishStoreFake) {
	t.Helper()
	store := &finishStoreFake{idemTaskStore: newIdemTaskStore()}
	svc := NewImageTaskServiceWithUploader(store, NewImageResultUploader(storage, "images/", 0, nil), time.Hour, time.Minute)
	cfg, err := ParseImageTaskCallbackConfig([]byte(`[{"api_key_id":9,"url":"https://a.example/cb","secret":"`+strings.Repeat("s", 32)+`"}]`), false)
	require.NoError(t, err)
	svc.SetCallbackConfig(cfg)
	require.NoError(t, store.Save(context.Background(), &ImageTaskRecord{ID: cbTestTaskID, UserID: 7, APIKeyID: 9, RunID: planeRunID, Plane: planeSpec(), Status: ImageTaskStatusProcessing}, time.Hour))
	return svc, store
}

func b64Result(data []byte) json.RawMessage {
	return json.RawMessage(`{"id":"up-1","created":1,"data":[{"b64_json":"` + base64.StdEncoding.EncodeToString(data) + `","revised_prompt":"cat"}]}`)
}

var planeSucceeded = &ImageTaskExecution{Outcome: ImageTaskOutcomeSucceeded, Dispatched: true, UpstreamCalls: 1, LastUpstreamStatus: 200}

func TestExecutionPlaneCompleteWritesKeyMetadataChecksumAndCallback(t *testing.T) {
	storage := newMemObjectStorage()
	svc, store := newPlaneService(t, storage)
	pngBytes := testPNG(t, 3, 2)
	digest := sha256.Sum256(pngBytes)
	digestHex := hex.EncodeToString(digest[:])

	require.NoError(t, svc.CompleteWithExecution(context.Background(), cbTestTaskID, 200, b64Result(pngBytes), planeSucceeded))

	obj, ok := storage.objects["generated/"+planeRunID+"/image-0"]
	require.True(t, ok, "object key is exactly generated/<run_id>/image-0")
	require.Len(t, storage.objects, 1)
	require.Zero(t, storage.saveCalls, "execution-plane tasks never take the URL path")
	require.Equal(t, "image/png", obj.ContentType)
	require.Equal(t, pngBytes, obj.Data)
	require.Equal(t, digest[:], obj.ChecksumSHA256)
	require.Equal(t, map[string]string{
		"run_id": planeRunID, "route_version": "route-v3", "provider_id": "kemeitv",
		"mime_type": "image/png", "sha256": digestHex, "created_at": "1794400000123",
	}, obj.Metadata)

	rec, err := store.Get(context.Background(), cbTestTaskID)
	require.NoError(t, err)
	require.Equal(t, ImageTaskStatusCompleted, rec.Status)
	require.Equal(t, []ImageTaskAsset{{R2Key: "generated/" + planeRunID + "/image-0", MIME: "image/png", Width: 3, Height: 2, Bytes: int64(len(pngBytes)), SHA256: digestHex, Cleaned: false}}, rec.Assets)
	require.NotContains(t, string(rec.Result), "b64_json")
	require.NotContains(t, string(rec.Result), "://")
	require.Contains(t, string(rec.Result), `"r2_key":"generated/run_exec_plane_1/image-0"`)
	require.Contains(t, string(rec.Result), `"revised_prompt":"cat"`)

	require.Len(t, store.events, 1)
	body := string(store.events[0].Body)
	require.Contains(t, body, `"status":"succeeded"`)
	require.Contains(t, body, `"assets":[{"r2_key":"generated/run_exec_plane_1/image-0","mime":"image/png","width":3,"height":2,"bytes":`)
	require.Contains(t, body, `"cleaned":false`, "T2.5 not implemented yet")
	require.NotContains(t, body, "://", "no URL in the callback")
}

func TestExecutionPlaneCleanerHookIsTheT25CallSite(t *testing.T) {
	original := imageTaskAssetCleaner
	t.Cleanup(func() { imageTaskAssetCleaner = original })
	cleanedBytes := testPNG(t, 4, 4)
	var gotMime string
	imageTaskAssetCleaner = func(_ context.Context, data []byte, mimeType string) ([]byte, bool, error) {
		gotMime = mimeType
		return cleanedBytes, true, nil
	}
	storage := newMemObjectStorage()
	svc, store := newPlaneService(t, storage)
	require.NoError(t, svc.CompleteWithExecution(context.Background(), cbTestTaskID, 200, b64Result(testPNG(t, 3, 2)), planeSucceeded))

	require.Equal(t, "image/png", gotMime)
	digest := sha256.Sum256(cleanedBytes)
	rec, _ := store.Get(context.Background(), cbTestTaskID)
	require.Equal(t, hex.EncodeToString(digest[:]), rec.Assets[0].SHA256, "hash, size and dims describe the cleaned bytes")
	require.Equal(t, 4, rec.Assets[0].Width)
	require.True(t, rec.Assets[0].Cleaned)
	require.Equal(t, cleanedBytes, storage.objects["generated/"+planeRunID+"/image-0"].Data)
}

type urlOnlyStorage struct{}

func (urlOnlyStorage) Save(context.Context, string, string, []byte) (string, error) {
	return "https://cdn.example/x", nil
}

func TestExecutionPlaneStorageFailuresAreUncertain(t *testing.T) {
	gif := []byte("GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xff\xff\xff!\xf9\x04\x01\x00\x00\x00\x00,\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02D\x01\x00;")
	oversized := append(append([]byte{}, testPNG(t, 1, 1)...), make([]byte, ImageTaskAssetMaxBytes)...)
	cases := map[string]struct {
		storage ImageStorage
		result  func(t *testing.T) json.RawMessage
	}{
		"put fails":         {storage: &memObjectStorage{objects: map[string]StoredImageObject{}, putErr: errors.New("r2 down")}},
		"readback metadata": {storage: &memObjectStorage{objects: map[string]StoredImageObject{}, tamper: func(o *StoredImageObject) { o.Metadata["extra"] = "x" }}},
		"readback checksum": {storage: &memObjectStorage{objects: map[string]StoredImageObject{}, tamper: func(o *StoredImageObject) { o.ChecksumSHA256 = nil }}},
		"readback bytes":    {storage: &memObjectStorage{objects: map[string]StoredImageObject{}, tamper: func(o *StoredImageObject) { o.Data = o.Data[:len(o.Data)-1] }}},
		"readback type":     {storage: &memObjectStorage{objects: map[string]StoredImageObject{}, tamper: func(o *StoredImageObject) { o.ContentType = "image/jpeg" }}},
		"no metadata api":   {storage: urlOnlyStorage{}},
		"two images": {storage: newMemObjectStorage(), result: func(t *testing.T) json.RawMessage {
			b := base64.StdEncoding.EncodeToString(testPNG(t, 1, 1))
			return json.RawMessage(`{"data":[{"b64_json":"` + b + `"},{"b64_json":"` + b + `"}]}`)
		}},
		"gif":       {storage: newMemObjectStorage(), result: func(*testing.T) json.RawMessage { return b64Result(gif) }},
		"oversized": {storage: newMemObjectStorage(), result: func(*testing.T) json.RawMessage { return b64Result(oversized) }},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			svc, store := newPlaneService(t, tc.storage)
			result := b64Result(testPNG(t, 2, 2))
			if tc.result != nil {
				result = tc.result(t)
			}
			require.NoError(t, svc.CompleteWithExecution(context.Background(), cbTestTaskID, 200, result, planeSucceeded))
			rec, err := store.Get(context.Background(), cbTestTaskID)
			require.NoError(t, err)
			require.Equal(t, ImageTaskStatusFailed, rec.Status)
			require.Equal(t, ImageTaskOutcomeUncertain, rec.Execution.Outcome)
			require.Equal(t, ImageTaskReasonStorageFailed, rec.Execution.Reason)
			require.Empty(t, rec.Assets)
			require.Len(t, store.events, 1)
			require.Contains(t, string(store.events[0].Body), `"status":"uncertain"`)
			require.Contains(t, string(store.events[0].Body), `storage_failed`)
		})
	}
}

func TestExecutionPlaneStorageFailureWithoutSafeModeStillRecordsUncertain(t *testing.T) {
	storage := &memObjectStorage{objects: map[string]StoredImageObject{}, putErr: errors.New("r2 down")}
	svc, store := newPlaneService(t, storage)
	require.NoError(t, svc.Complete(context.Background(), cbTestTaskID, 200, b64Result(testPNG(t, 2, 2))))
	rec, _ := store.Get(context.Background(), cbTestTaskID)
	require.Equal(t, ImageTaskOutcomeUncertain, rec.Execution.Outcome)
	require.Equal(t, ImageTaskReasonStorageFailed, rec.Execution.Reason)
}

func TestNonPlaneTaskKeepsUpstreamKeyRule(t *testing.T) {
	storage := newMemObjectStorage()
	store := newIdemTaskStore()
	svc := NewImageTaskServiceWithUploader(store, NewImageResultUploader(storage, "images/", 0, nil), time.Hour, time.Minute)
	require.NoError(t, store.Save(context.Background(), &ImageTaskRecord{ID: "imgtask_plain", UserID: 7, APIKeyID: 9, Status: ImageTaskStatusProcessing}, time.Hour))
	require.NoError(t, svc.Complete(context.Background(), "imgtask_plain", 200, b64Result(testPNG(t, 2, 2))))
	rec, _ := store.Get(context.Background(), "imgtask_plain")
	require.Equal(t, ImageTaskStatusCompleted, rec.Status)
	require.Contains(t, string(rec.Result), "https://cdn.example/images/imgtask_plain-0.png")
	require.Equal(t, 1, storage.saveCalls)
	require.Zero(t, storage.putCalls)
}

func TestParseImageTaskPlaneSpec(t *testing.T) {
	meta := `{"route_version":"route-v3","provider_id":"kemeitv","created_at":1794400000123}`
	spec, err := ParseImageTaskPlaneSpec(planeRunID, "generated/"+planeRunID+"/", meta)
	require.NoError(t, err)
	require.Equal(t, planeSpec(), spec)
	require.Equal(t, "generated/run_exec_plane_1/image-0", spec.AssetKey())

	for _, prefix := range []string{
		"", "generated/" + planeRunID, "/generated/" + planeRunID + "/", "generated//" + planeRunID + "/",
		"generated/" + planeRunID + "//", "generated/../" + planeRunID + "/", "generated/" + planeRunID + "/../",
		"generated/" + planeRunID + "/sub/", "generated/other_run/", "uploads/" + planeRunID + "/",
		"generated/7/" + planeRunID + "/", " generated/" + planeRunID + "/",
	} {
		_, err := ParseImageTaskPlaneSpec(planeRunID, prefix, meta)
		require.Error(t, err, "prefix %q", prefix)
	}
	for _, m := range []string{
		"", "{}", "[]", "null",
		`{"route_version":"route-v3","provider_id":"kemeitv"}`,
		`{"route_version":"route-v3","provider_id":"kemeitv","created_at":1,"extra":1}`,
		`{"route_version":"route-v3","provider_id":"kemeitv","created_at":-1}`,
		`{"route_version":"route-v3","provider_id":"kemeitv","created_at":1.5}`,
		`{"route_version":"route-v3","provider_id":"kemeitv","created_at":"1"}`,
		`{"route_version":"route-v3","provider_id":"kemeitv","created_at":9007199254740992}`,
		`{"route_version":"../x","provider_id":"kemeitv","created_at":1}`,
		`{"route_version":"route-v3","provider_id":"","created_at":1}`,
		`{"route_version":"route-v3","provider_id":"kemeitv","created_at":1} {}`,
	} {
		_, err := ParseImageTaskPlaneSpec(planeRunID, "generated/"+planeRunID+"/", m)
		require.Error(t, err, "metadata %q", m)
	}
	_, err = ParseImageTaskPlaneSpec("", "generated//", meta)
	require.Error(t, err)
}

func TestValidateExecutionPlaneN(t *testing.T) {
	require.NoError(t, ValidateExecutionPlaneN("application/json", []byte(`{"prompt":"cat"}`)))
	require.NoError(t, ValidateExecutionPlaneN("application/json", []byte(`{"prompt":"cat","n":1}`)))
	for _, body := range []string{`{"n":2}`, `{"n":0}`, `{"n":1.5}`, `{"n":"1"}`, `not json`} {
		require.Error(t, ValidateExecutionPlaneN("application/json", []byte(body)), body)
	}
	multi := func(n string) (string, []byte) {
		var buf bytes.Buffer
		w := multipart.NewWriter(&buf)
		_ = w.WriteField("prompt", "cat")
		if n != "" {
			_ = w.WriteField("n", n)
		}
		_ = w.Close()
		return w.FormDataContentType(), buf.Bytes()
	}
	ct, body := multi("")
	require.NoError(t, ValidateExecutionPlaneN(ct, body))
	ct, body = multi("1")
	require.NoError(t, ValidateExecutionPlaneN(ct, body))
	ct, body = multi("2")
	require.Error(t, ValidateExecutionPlaneN(ct, body))
}

func TestExecutionPlaneFingerprintIncludesAssetHeaders(t *testing.T) {
	body := []byte(`{"n":1}`)
	base := ImageTaskIdempotencyFingerprint("/v1/images/generations/async", "application/json", body)
	require.Equal(t, base, ImageTaskIdempotencyFingerprint("/v1/images/generations/async", "application/json", body, nil), "non-plane fingerprint unchanged")
	withPlane := ImageTaskIdempotencyFingerprint("/v1/images/generations/async", "application/json", body, planeSpec())
	require.NotEqual(t, base, withPlane)
	for _, mutate := range []func(*ImageTaskPlaneSpec){
		func(p *ImageTaskPlaneSpec) { p.RouteVersion = "route-v4" },
		func(p *ImageTaskPlaneSpec) { p.ProviderID = "cloudpass" },
		func(p *ImageTaskPlaneSpec) { p.CreatedAt++ },
		func(p *ImageTaskPlaneSpec) { p.AssetPrefix = "generated/other/" },
	} {
		p := planeSpec()
		mutate(p)
		require.NotEqual(t, withPlane, ImageTaskIdempotencyFingerprint("/v1/images/generations/async", "application/json", body, p))
	}
}

func TestExecutionPlaneQueryBodySharesCallbackSerializer(t *testing.T) {
	storage := newMemObjectStorage()
	svc, store := newPlaneService(t, storage)
	owner := ImageTaskOwner{UserID: 7, APIKeyID: 9}

	view, isPlane, err := svc.GetExecutionPlaneView(context.Background(), owner, cbTestTaskID)
	require.NoError(t, err)
	require.True(t, isPlane)
	raw, _ := json.Marshal(view)
	require.Equal(t, `{"task_id":"`+cbTestTaskID+`","run_id":"run_exec_plane_1","status":"processing"}`, string(raw))

	require.NoError(t, svc.CompleteWithExecution(context.Background(), cbTestTaskID, 200, b64Result(testPNG(t, 3, 2)), planeSucceeded))
	view, _, err = svc.GetExecutionPlaneView(context.Background(), owner, cbTestTaskID)
	require.NoError(t, err)
	raw, _ = json.Marshal(view)
	require.Equal(t, string(store.events[0].Body), string(raw), "query body is byte-identical to the callback body")

	_, _, err = svc.GetExecutionPlaneView(context.Background(), ImageTaskOwner{UserID: 7, APIKeyID: 10}, cbTestTaskID)
	require.ErrorIs(t, err, ErrImageTaskNotFound)

	require.NoError(t, store.Save(context.Background(), &ImageTaskRecord{ID: "imgtask_plain", UserID: 7, APIKeyID: 9, Status: ImageTaskStatusProcessing}, time.Hour))
	_, isPlane, err = svc.GetExecutionPlaneView(context.Background(), owner, "imgtask_plain")
	require.NoError(t, err)
	require.False(t, isPlane)
}

func TestImageTaskTTLAtLeastSevenDays(t *testing.T) {
	svc := NewImageTaskService(newIdemTaskStore())
	require.GreaterOrEqual(t, svc.ttl, 7*24*time.Hour)
	require.Greater(t, svc.idempotencyTTL(), svc.ttl)
	require.Greater(t, svc.ttl, ImageTaskCallbackGiveUpAfter)
}

func TestCreateIdempotentWithPlaneStoresSpec(t *testing.T) {
	svc, store, _ := newIdemService(t)
	sub, err := svc.CreateIdempotentWithPlane(context.Background(), idemOwner, planeRunID, "fp", planeSpec())
	require.NoError(t, err)
	require.True(t, sub.Created)
	rec, err := store.Get(context.Background(), sub.Task.ID)
	require.NoError(t, err)
	require.Equal(t, planeSpec(), rec.Plane)
	require.Equal(t, planeRunID, rec.RunID)

	_, err = svc.CreateIdempotentWithPlane(context.Background(), idemOwner, "", "fp", planeSpec())
	require.ErrorIs(t, err, ErrImageTaskRunIDRequired)
}
