package handler

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

var responsesOrphanActiveTasks atomic.Int64

func startResponsesOrphanMonitor(c *gin.Context, kind string) func() {
	if c == nil || c.Request == nil {
		return func() {}
	}
	var done atomic.Bool
	var counted atomic.Bool
	log := requestLogger(c, "handler.openai_gateway.responses_orphan")
	stop := context.AfterFunc(c.Request.Context(), func() {
		time.AfterFunc(5*time.Second, func() {
			if done.Load() {
				return
			}
			counted.Store(true)
			current := responsesOrphanActiveTasks.Add(1)
			log.Warn("orphan_active_tasks",
				zap.String("kind", kind),
				zap.Int64("value", current),
			)
		})
	})
	return func() {
		done.Store(true)
		if stop != nil {
			_ = stop()
		}
		if counted.Load() {
			current := responsesOrphanActiveTasks.Add(-1)
			log.Info("orphan_active_tasks",
				zap.String("kind", kind),
				zap.Int64("value", current),
			)
		}
	}
}

func responsesOrphanActiveTasksForTest() int64 {
	return responsesOrphanActiveTasks.Load()
}
