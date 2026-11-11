package service

// YeaGrow execution plane (T2.4): per-task R2 object generated/<run_id>/image-0.
// Origin: finefu/sub2api branch yeagrow/execution-plane (base Wei-Shaw/sub2api ee58c0013).
//
// An execution-plane task is a submit that carries X-YeaGrow-Asset-Prefix /
// X-YeaGrow-Asset-Metadata, or any submit by an API key with a callback target.
// Its single result image is written to <prefix>image-0 with the exact
// customMetadata and SHA-256 checksum YeaGrow's recoverStoredGeneratedAsset
// (lib/generated-image-assets.ts) requires, read back and verified. Callbacks
// and the task query carry only the object key, never a URL.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"go.uber.org/zap"
)

const (
	// ImageTaskAssetPrefixHeader must be exactly "generated/<run_id>/".
	ImageTaskAssetPrefixHeader = "X-YeaGrow-Asset-Prefix"
	// ImageTaskAssetMetadataHeader carries the reservation fields only YeaGrow knows:
	// {"route_version":"<id>","provider_id":"<id>","created_at":<unix ms>} (exactly these keys).
	ImageTaskAssetMetadataHeader = "X-YeaGrow-Asset-Metadata"

	// ImageTaskAssetMaxBytes mirrors YeaGrow's 10 MiB stored-object limit.
	ImageTaskAssetMaxBytes  = 10 * 1024 * 1024
	imageTaskMaxSafeInteger = 1<<53 - 1
)

func planeInvalid(msg string) error {
	return infraerrors.BadRequest("IMAGE_TASK_EXECUTION_PLANE_INVALID", msg)
}

// ImageTaskPlaneSpec is stored on the task record for execution-plane tasks.
type ImageTaskPlaneSpec struct {
	AssetPrefix  string `json:"asset_prefix"`
	RouteVersion string `json:"route_version"`
	ProviderID   string `json:"provider_id"`
	CreatedAt    int64  `json:"created_at"`
}

// AssetKey is the single object key: <prefix>image-0.
func (p *ImageTaskPlaneSpec) AssetKey() string {
	return p.AssetPrefix + "image-0"
}

// fingerprintPart is folded into the Idempotency-Key fingerprint.
func (p *ImageTaskPlaneSpec) fingerprintPart() string {
	return "plane\n" + p.AssetPrefix + "\n" + p.RouteVersion + "\n" + p.ProviderID + "\n" + strconv.FormatInt(p.CreatedAt, 10)
}

// IsExecutionPlaneSubmit reports whether a submit must follow the execution-plane rules.
func IsExecutionPlaneSubmit(prefixHeader, metadataHeader string, callbacksConfigured bool) bool {
	return callbacksConfigured || strings.TrimSpace(prefixHeader) != "" || strings.TrimSpace(metadataHeader) != ""
}

// ParseImageTaskPlaneSpec validates the two headers against the run_id.
// The prefix must be exactly "generated/<run_id>/", which rules out "..",
// absolute paths, "//" and nested directories.
func ParseImageTaskPlaneSpec(runID, prefixHeader, metadataHeader string) (*ImageTaskPlaneSpec, error) {
	if !IsValidImageTaskRunID(runID) {
		return nil, ErrImageTaskRunIDRequired
	}
	if prefixHeader != "generated/"+runID+"/" {
		return nil, planeInvalid(ImageTaskAssetPrefixHeader + " must be exactly generated/<Idempotency-Key>/")
	}
	dec := json.NewDecoder(strings.NewReader(metadataHeader))
	dec.DisallowUnknownFields()
	var meta struct {
		RouteVersion *string         `json:"route_version"`
		ProviderID   *string         `json:"provider_id"`
		CreatedAt    json.RawMessage `json:"created_at"`
	}
	if err := dec.Decode(&meta); err != nil || dec.More() {
		return nil, planeInvalid(ImageTaskAssetMetadataHeader + " must be a JSON object with route_version, provider_id, created_at")
	}
	if meta.RouteVersion == nil || meta.ProviderID == nil || len(meta.CreatedAt) == 0 ||
		!IsValidImageTaskRunID(*meta.RouteVersion) || !IsValidImageTaskRunID(*meta.ProviderID) {
		return nil, planeInvalid(ImageTaskAssetMetadataHeader + ": route_version and provider_id must be safe ids")
	}
	// Raw token: a JSON integer only (no string, fraction or exponent).
	createdAt, err := strconv.ParseInt(string(meta.CreatedAt), 10, 64)
	if err != nil || createdAt < 0 || createdAt > imageTaskMaxSafeInteger {
		return nil, planeInvalid(ImageTaskAssetMetadataHeader + ": created_at must be a non-negative integer (unix ms)")
	}
	return &ImageTaskPlaneSpec{AssetPrefix: prefixHeader, RouteVersion: *meta.RouteVersion, ProviderID: *meta.ProviderID, CreatedAt: createdAt}, nil
}

// ImageTaskRequestedN returns the image count requested by a JSON or multipart
// images body; an absent n means 1.
func ImageTaskRequestedN(contentType string, body []byte) (int, error) {
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err == nil && strings.EqualFold(mediaType, "multipart/form-data") {
		reader := multipart.NewReader(bytes.NewReader(body), params["boundary"])
		for {
			part, err := reader.NextPart()
			if errors.Is(err, io.EOF) {
				return 1, nil
			}
			if err != nil {
				return 0, err
			}
			if part.FormName() != "n" {
				continue
			}
			raw, err := io.ReadAll(io.LimitReader(part, 32))
			if err != nil {
				return 0, err
			}
			return strconv.Atoi(strings.TrimSpace(string(raw)))
		}
	}
	var envelope struct {
		N json.RawMessage `json:"n"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return 0, err
	}
	if len(envelope.N) == 0 {
		return 1, nil
	}
	// Raw token: a JSON integer only (no string, fraction or exponent).
	return strconv.Atoi(string(envelope.N))
}

// ValidateExecutionPlaneN rejects execution-plane submits that do not ask for exactly one image.
func ValidateExecutionPlaneN(contentType string, body []byte) error {
	n, err := ImageTaskRequestedN(contentType, body)
	if err != nil || n != 1 {
		return planeInvalid("execution-plane image tasks require n = 1")
	}
	return nil
}

// ---------------------------------------------------------------------------
// Object storage with metadata, checksum and readback.

// ImageObjectStorage is implemented by storages that can write an object with
// custom metadata and a SHA-256 checksum and read it back (S3/R2).
type ImageObjectStorage interface {
	PutImageObject(ctx context.Context, key, contentType string, data []byte, metadata map[string]string, sha256Digest [32]byte) error
	GetImageObject(ctx context.Context, key string) (*StoredImageObject, error)
}

// StoredImageObject is what a readback returns.
type StoredImageObject struct {
	ContentType string
	Metadata    map[string]string
	// ChecksumSHA256 is the stored SHA-256 checksum (raw 32 bytes), nil if the store returned none.
	ChecksumSHA256 []byte
	Data           []byte
}

// ImageTaskAssetCleaner receives the downloaded bytes and their detected MIME
// type and returns the bytes to store with YeaGrow's dimensions/digest. The
// default (T2.5) is the byte-exact port of YeaGrow's cleaners; any rejection
// fails the task as uncertain/storage_failed.
type ImageTaskAssetCleaner func(ctx context.Context, data []byte, mimeType string) (*ImageCleanResult, error)

var imageTaskAssetCleaner ImageTaskAssetCleaner = yeagrowImageAssetCleaner

// imageTaskPlaneMaxDownloadBytes caps execution-plane result downloads (T2.5):
// YeaGrow rejects stored images above 10 MiB, so nothing larger is fetched.
const imageTaskPlaneMaxDownloadBytes = ImageTaskAssetMaxBytes

// ImageTaskAssetMetadata is the exact customMetadata YeaGrow requires (six keys).
func ImageTaskAssetMetadata(runID string, spec *ImageTaskPlaneSpec, mimeType, sha256Hex string) map[string]string {
	return map[string]string{
		"run_id":        runID,
		"route_version": spec.RouteVersion,
		"provider_id":   spec.ProviderID,
		"mime_type":     mimeType,
		"sha256":        sha256Hex,
		"created_at":    strconv.FormatInt(spec.CreatedAt, 10),
	}
}

// storeExecutionPlaneAsset writes the task's single image and verifies the
// readback. It returns the asset descriptor and a compact result that keeps
// the upstream envelope but replaces image data with the object key.
func storeExecutionPlaneAsset(ctx context.Context, uploader *ImageResultUploader, task *ImageTaskRecord, result json.RawMessage) (*ImageTaskAsset, json.RawMessage, error) {
	if uploader == nil || uploader.storage == nil {
		return nil, nil, errors.New("object storage is not configured")
	}
	objects, ok := uploader.storage.(ImageObjectStorage)
	if !ok {
		return nil, nil, errors.New("object storage does not support metadata writes")
	}
	spec := task.Plane
	var top map[string]json.RawMessage
	if err := json.Unmarshal(result, &top); err != nil {
		return nil, nil, fmt.Errorf("parse image response: %w", err)
	}
	var items []map[string]json.RawMessage
	if err := json.Unmarshal(top["data"], &items); err != nil {
		return nil, nil, fmt.Errorf("parse image response data: %w", err)
	}
	if len(items) != 1 {
		return nil, nil, fmt.Errorf("execution-plane task expects exactly 1 image, upstream returned %d", len(items))
	}
	planeUploader := *uploader
	if planeUploader.maxDownloadBytes <= 0 || planeUploader.maxDownloadBytes > imageTaskPlaneMaxDownloadBytes {
		planeUploader.maxDownloadBytes = imageTaskPlaneMaxDownloadBytes
	}
	data, _, err := planeUploader.fetchImageBytes(ctx, items[0])
	if err != nil {
		return nil, nil, err
	}
	if len(data) > imageTaskPlaneMaxDownloadBytes {
		return nil, nil, fmt.Errorf("image size %d exceeds %d bytes", len(data), imageTaskPlaneMaxDownloadBytes)
	}
	cleanedImage, err := imageTaskAssetCleaner(ctx, data, detectedImageContentType(data))
	if err != nil {
		return nil, nil, fmt.Errorf("clean image: %w", err)
	}
	data = cleanedImage.Bytes
	mimeType := cleanedImage.MIMEType
	if mimeType != "image/png" && mimeType != "image/jpeg" {
		return nil, nil, fmt.Errorf("unsupported image type %q", mimeType)
	}
	if len(data) < 1 || len(data) > ImageTaskAssetMaxBytes || cleanedImage.Width < 1 || cleanedImage.Height < 1 {
		return nil, nil, fmt.Errorf("cleaned image size %d / %dx%d out of range", len(data), cleanedImage.Width, cleanedImage.Height)
	}
	digest := sha256.Sum256(data)
	digestHex := hex.EncodeToString(digest[:])
	key := spec.AssetKey()
	metadata := ImageTaskAssetMetadata(task.RunID, spec, mimeType, digestHex)

	if err := objects.PutImageObject(ctx, key, mimeType, data, metadata, digest); err != nil {
		return nil, nil, fmt.Errorf("put object: %w", err)
	}
	if err := verifyStoredImageObject(ctx, objects, key, mimeType, metadata, digest); err != nil {
		return nil, nil, err
	}

	asset := &ImageTaskAsset{R2Key: key, MIME: mimeType, Width: cleanedImage.Width, Height: cleanedImage.Height, Bytes: int64(len(data)), SHA256: digestHex, Cleaned: true}
	keyRaw, _ := json.Marshal(key)
	compactItem := map[string]json.RawMessage{"r2_key": keyRaw}
	if revised, ok := items[0]["revised_prompt"]; ok {
		compactItem["revised_prompt"] = revised
	}
	itemsRaw, _ := json.Marshal([]map[string]json.RawMessage{compactItem})
	top["data"] = itemsRaw
	compact, err := json.Marshal(top)
	if err != nil {
		return nil, nil, err
	}
	return asset, compact, nil
}

// verifyStoredImageObject reads the object back and checks bytes, content
// type, the exact six metadata keys and the stored SHA-256 checksum.
func verifyStoredImageObject(ctx context.Context, objects ImageObjectStorage, key, mimeType string, metadata map[string]string, digest [32]byte) error {
	stored, err := objects.GetImageObject(ctx, key)
	if err != nil {
		return fmt.Errorf("read back object: %w", err)
	}
	if sha256.Sum256(stored.Data) != digest {
		return errors.New("read back object: content differs")
	}
	if stored.ContentType != mimeType {
		return fmt.Errorf("read back object: content type %q", stored.ContentType)
	}
	if len(stored.Metadata) != len(metadata) {
		return fmt.Errorf("read back object: %d metadata keys, want %d", len(stored.Metadata), len(metadata))
	}
	for k, v := range metadata {
		if stored.Metadata[k] != v {
			return fmt.Errorf("read back object: metadata %s mismatch", k)
		}
	}
	if !bytes.Equal(stored.ChecksumSHA256, digest[:]) {
		return errors.New("read back object: stored sha256 checksum missing or different")
	}
	return nil
}

// completeExecutionPlane stores the image of an execution-plane task and
// finishes it. Any storage failure leaves the task failed with
// outcome uncertain / storage_failed (the image may exist upstream).
func logImageTaskStorageFailure(taskID string, err error) {
	logger.L().Error("image_task.execution_plane_store_failed", zap.String("task_id", taskID), zap.Error(err))
}

func (s *ImageTaskService) completeExecutionPlane(ctx context.Context, task *ImageTaskRecord, statusCode int, result json.RawMessage, exec *ImageTaskExecution) error {
	uploader, _ := s.current()
	asset, compact, err := storeExecutionPlaneAsset(ctx, uploader, task, result)
	if err != nil {
		logImageTaskStorageFailure(task.ID, err)
		return s.fail(ctx, task.ID, http.StatusBadGateway,
			imageTaskErrorJSON("api_error", "failed to store generated image to object storage"),
			planeStorageFailure(exec))
	}
	return s.finishWithAssets(ctx, task.ID, ImageTaskStatusCompleted, statusCode, compact, nil, exec, []ImageTaskAsset{*asset})
}

// planeStorageFailure marks the outcome uncertain/storage_failed. Without a
// safe-mode record (exec == nil) it still records the outcome, because an
// execution-plane task reached completion only after an upstream success.
func planeStorageFailure(exec *ImageTaskExecution) *ImageTaskExecution {
	if exec == nil {
		return &ImageTaskExecution{Outcome: ImageTaskOutcomeUncertain, Reason: ImageTaskReasonStorageFailed, Dispatched: true}
	}
	out := *exec
	out.Outcome = ImageTaskOutcomeUncertain
	out.Reason = ImageTaskReasonStorageFailed
	return &out
}

// ---------------------------------------------------------------------------
// Query body shared with the callback.

// ImageTaskQueryProcessing is returned while an execution-plane task runs.
type ImageTaskQueryProcessing struct {
	TaskID string `json:"task_id"`
	RunID  string `json:"run_id"`
	Status string `json:"status"`
}

// ExecutionPlaneQueryBody returns the GET body for an execution-plane task:
// the callback event (same serializer) once terminal, or a processing stub.
func ExecutionPlaneQueryBody(task *ImageTaskRecord) (any, error) {
	if task.Status == ImageTaskStatusProcessing {
		return &ImageTaskQueryProcessing{TaskID: task.ID, RunID: task.RunID, Status: ImageTaskStatusProcessing}, nil
	}
	return BuildImageTaskCallbackEvent(task)
}

// GetExecutionPlaneView returns (body, true) for an execution-plane task owned
// by owner, or (nil, false) for other tasks. Ownership errors match Get.
func (s *ImageTaskService) GetExecutionPlaneView(ctx context.Context, owner ImageTaskOwner, id string) (any, bool, error) {
	if s == nil || s.store == nil {
		return nil, false, ErrImageTaskUnavailable
	}
	task, err := s.store.Get(ctx, strings.TrimSpace(id))
	if err != nil {
		if errors.Is(err, ErrImageTaskNotFound) {
			return nil, false, ErrImageTaskNotFound
		}
		return nil, false, ErrImageTaskUnavailable.WithCause(err)
	}
	if task.UserID != owner.UserID || task.APIKeyID != owner.APIKeyID {
		return nil, false, ErrImageTaskNotFound
	}
	if task.Plane == nil {
		return nil, false, nil
	}
	body, err := ExecutionPlaneQueryBody(task)
	if err != nil {
		return nil, true, infraerrors.InternalServer("IMAGE_TASK_EXECUTION_PLANE_VIEW", "execution-plane task has no reportable outcome").WithCause(err)
	}
	return body, true, nil
}
