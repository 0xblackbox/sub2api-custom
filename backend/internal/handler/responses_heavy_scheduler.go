package handler

import (
	"context"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

func (h *OpenAIGatewayHandler) acquireResponsesHeavyWebSearchSlot(
	c *gin.Context,
	body []byte,
	userID int64,
	apiKeyID int64,
	reqLog *zap.Logger,
) (func(), bool) {
	if c == nil {
		return nil, true
	}
	if acquired, _ := c.Get(responsesHeavySlotAlreadyAcquiredKey); acquired == true {
		return nil, true
	}
	if !backgroundResponseIsHeavyWebSearch(body) {
		return nil, true
	}

	release, queueWait, err := defaultBackgroundResponseHeavyQueue.acquire(c.Request.Context(), userID)
	if err != nil {
		fields := []zap.Field{
			zap.Int64("user_id", userID),
			zap.Int64("api_key_id", apiKeyID),
			zap.Int64("queue_wait_ms", queueWait.Milliseconds()),
			zap.Int("queue_waiting", defaultBackgroundResponseHeavyQueue.waitingCount()),
			zap.Error(err),
		}
		if reqLog != nil {
			reqLog.Warn("openai.responses_heavy_queue_acquire_failed", fields...)
		}
		switch {
		case errors.Is(err, errBackgroundHeavyQueueFull):
			c.Header("Retry-After", "3")
			markOpenAIRateLimitLayer(c, "proxy_heavy_queue", http.StatusTooManyRequests, "")
			h.handleStreamingAwareError(c, http.StatusTooManyRequests, "proxy_heavy_queue_full", "heavy web search queue is full, please retry later", false)
		case errors.Is(err, context.Canceled):
			h.handleStreamingAwareError(c, statusClientClosedRequest, "downstream_client_cancelled", "client disconnected before heavy web search could start", false)
		default:
			c.Header("Retry-After", "3")
			markOpenAIRateLimitLayer(c, "proxy_heavy_queue", http.StatusTooManyRequests, "")
			h.handleStreamingAwareError(c, http.StatusTooManyRequests, "proxy_user_concurrency_timeout", "timeout waiting for heavy web search queue", false)
		}
		return nil, false
	}
	c.Set(responsesSkipUserSlotKey, true)
	c.Set(responsesHeavySlotAlreadyAcquiredKey, true)
	recordResponsesLifecycle(c, body, "heavy_queue_acquired",
		zap.Int64("user_id", userID),
		zap.Int64("api_key_id", apiKeyID),
		zap.Int64("queue_wait_ms", queueWait.Milliseconds()),
		zap.Int("queue_waiting", defaultBackgroundResponseHeavyQueue.waitingCount()),
	)
	stopOrphanMonitor := startResponsesOrphanMonitor(c, "sync_heavy_web_search")
	return wrapReleaseOnDone(c.Request.Context(), func() {
		stopOrphanMonitor()
		release()
	}), true
}
