package handler

// YeaGrow execution plane: GET /v1/execution-plane/status (read-only aggregate counts).
// Origin: finefu/sub2api branch yeagrow/execution-plane (base Wei-Shaw/sub2api ee58c0013).

import (
	"net/http"

	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/gin-gonic/gin"
)

type executionPlaneCallbackQueue struct {
	Due  int64 `json:"due"`
	Dead int64 `json:"dead"`
}

type executionPlaneStatusResponse struct {
	Status        string                      `json:"status"`
	CallbackQueue executionPlaneCallbackQueue `json:"callback_queue"`
	Inflight      int64                       `json:"inflight"`
}

// ExecutionPlaneStatus answers only API keys that have a callback target
// configured (the YeaGrow service key); every other key gets the same 404 as
// an unknown route. Redis errors are 503, never 200 with zero counts.
func (h *AsyncImageHandler) ExecutionPlaneStatus(c *gin.Context) {
	apiKey, ok := middleware2.GetAPIKeyFromContext(c)
	if h == nil || h.tasks == nil || !ok || apiKey == nil || apiKey.ID <= 0 || !h.tasks.CallbacksConfiguredFor(apiKey.ID) {
		imageTaskJSONError(c, http.StatusNotFound, "not_found_error", "not found")
		return
	}
	c.Header("Cache-Control", "no-store")
	counts, err := h.tasks.ExecutionPlaneStatus(c.Request.Context())
	if err != nil {
		imageTaskJSONError(c, http.StatusServiceUnavailable, "api_error", "execution plane status unavailable")
		return
	}
	c.JSON(http.StatusOK, executionPlaneStatusResponse{
		Status:        "ok",
		CallbackQueue: executionPlaneCallbackQueue{Due: counts.CallbackDue, Dead: counts.CallbackDead},
		Inflight:      counts.Inflight,
	})
}
