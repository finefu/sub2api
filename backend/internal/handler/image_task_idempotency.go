package handler

// YeaGrow execution plane (T2.1): Idempotency-Key handling for async image submits.
// Origin: finefu/sub2api branch yeagrow/execution-plane (base Wei-Shaw/sub2api ee58c0013).

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

// createImageTask creates the task, honouring Idempotency-Key when present.
// created=false means the key was already bound to a task with the same
// request fingerprint; the caller must reply 200 and must not execute.
func (h *AsyncImageHandler) createImageTask(c *gin.Context, apiKey *service.APIKey, body []byte) (*service.ImageTask, bool, error) {
	owner := service.ImageTaskOwner{UserID: apiKey.UserID, APIKeyID: apiKey.ID}
	rawKey := c.GetHeader("Idempotency-Key")
	if h.tasks.CallbacksConfiguredFor(apiKey.ID) && !service.IsValidImageTaskRunID(strings.TrimSpace(rawKey)) {
		// YeaGrow T2.2: callbacks carry run_id, which is the Idempotency-Key.
		return nil, false, service.ErrImageTaskRunIDRequired
	}
	// YeaGrow T2.4: execution-plane submits carry the asset headers; validate
	// them, require n = 1 and fold them into the idempotency fingerprint.
	var plane *service.ImageTaskPlaneSpec
	prefixHeader := c.GetHeader(service.ImageTaskAssetPrefixHeader)
	metadataHeader := c.GetHeader(service.ImageTaskAssetMetadataHeader)
	if service.IsExecutionPlaneSubmit(prefixHeader, metadataHeader, h.tasks.CallbacksConfiguredFor(apiKey.ID)) {
		spec, err := service.ParseImageTaskPlaneSpec(strings.TrimSpace(rawKey), prefixHeader, metadataHeader)
		if err != nil {
			return nil, false, err
		}
		if err := service.ValidateExecutionPlaneN(c.GetHeader("Content-Type"), body); err != nil {
			return nil, false, err
		}
		plane = spec
	}
	if strings.TrimSpace(rawKey) == "" {
		task, err := h.tasks.Create(c.Request.Context(), owner)
		return task, err == nil, err
	}
	fingerprint := service.ImageTaskIdempotencyFingerprint(c.Request.URL.Path, c.GetHeader("Content-Type"), body, plane)
	submission, err := h.tasks.CreateIdempotentWithPlane(c.Request.Context(), owner, rawKey, fingerprint, plane)
	if err != nil {
		if retryAfter := service.RetryAfterSecondsFromError(err); retryAfter > 0 {
			c.Header("Retry-After", strconv.Itoa(retryAfter))
		}
		return nil, false, err
	}
	return submission.Task, submission.Created, nil
}

// respondReplayedImageTask answers a duplicate submit with the bound task (HTTP 200).
func respondReplayedImageTask(c *gin.Context, task *service.ImageTask) {
	pollURL := imageTaskPollURL(c.Request.URL.Path, task.ID)
	c.Header("Cache-Control", "no-store")
	c.Header("Location", pollURL)
	if task.Status == service.ImageTaskStatusProcessing {
		c.Header("Retry-After", "3")
	}
	payload := gin.H{
		"id":       task.ID,
		"task_id":  task.TaskID,
		"object":   task.Object,
		"status":   task.Status,
		"poll_url": pollURL,
		"replayed": true,
	}
	if task.CreatedAt > 0 {
		payload["created_at"] = task.CreatedAt
	}
	if task.ExpiresAt > 0 {
		payload["expires_at"] = task.ExpiresAt
	}
	c.JSON(http.StatusOK, payload)
}
