package handler

// YeaGrow execution-plane safe mode wiring for async image tasks.
// Origin: finefu/sub2api branch yeagrow/execution-plane (base Wei-Shaw/sub2api ee58c0013).

import (
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// asyncImageSafeModeFromEnv reads IMAGE_TASK_EXECUTION_PLANE_SAFE_MODE once at
// construction. An invalid value keeps the mode off and is logged as an error.
func asyncImageSafeModeFromEnv() bool {
	enabled, err := service.ImageTaskExecutionPlaneSafeModeFromEnv()
	if err != nil {
		logger.L().Error("image_task.execution_plane_safe_mode_invalid; safe mode stays off", zap.Error(err))
		return false
	}
	if enabled {
		logger.L().Info("image_task.execution_plane_safe_mode_enabled")
	}
	return enabled
}

// safeModeSupportsPlatform: only the OpenAI Images path carries dispatch
// markers. Grok uses its own failover loop, so it is refused in safe mode
// rather than executed without the no-replay guarantee.
func (h *AsyncImageHandler) safeModeSupportsPlatform(platform string) bool {
	if h == nil || !h.safeMode {
		return true
	}
	return platform == service.PlatformOpenAI
}

// attachExecutionPlaneTracker puts a fresh tracker on the detached task
// request so the Images handler and doOpenAIUpstream can mark it.
func (h *AsyncImageHandler) attachExecutionPlaneTracker(taskCtx *gin.Context) {
	if h == nil || !h.safeMode || taskCtx == nil || taskCtx.Request == nil {
		return
	}
	ctx := service.WithExecutionPlaneTracker(taskCtx.Request.Context(), service.NewExecutionPlaneTracker())
	taskCtx.Request = taskCtx.Request.WithContext(ctx)
}
