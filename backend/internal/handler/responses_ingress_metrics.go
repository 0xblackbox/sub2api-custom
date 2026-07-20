package handler

import (
	"strings"
	"sync"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ip"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
)

const (
	responsesIngressRecordedKey           = "subtoproxy_responses_ingress_recorded"
	responsesInternalBackgroundExecuteKey = "subtoproxy_responses_internal_background_execute"
)

type responsesIngressMetricKey struct {
	Kind            string
	UserID          int64
	APIKeyID        int64
	Model           string
	SourceIP        string
	ClientRequestID string
	ProxyRequestID  string
}

var responsesIngressMetrics = struct {
	mu     sync.Mutex
	total  map[string]uint64
	byDims map[responsesIngressMetricKey]uint64
}{
	total:  make(map[string]uint64),
	byDims: make(map[responsesIngressMetricKey]uint64),
}

func recordResponsesIngressOnce(c *gin.Context, body []byte, kind string) {
	if c == nil {
		return
	}
	if internal, _ := c.Get(responsesInternalBackgroundExecuteKey); internal == true {
		recordResponsesLifecycle(c, body, "internal_background_execute")
		return
	}
	if recorded, _ := c.Get(responsesIngressRecordedKey); recorded == true {
		return
	}
	c.Set(responsesIngressRecordedKey, true)
	recordResponsesIngress(c, body, strings.TrimSpace(kind))
}

func recordResponsesIngress(c *gin.Context, body []byte, kind string) {
	if kind == "" {
		kind = "responses_http"
	}
	dims := responsesIngressDimensions(c, body, kind)
	responsesIngressMetrics.mu.Lock()
	responsesIngressMetrics.total[kind]++
	responsesIngressMetrics.byDims[dims]++
	total := responsesIngressMetrics.total[kind]
	dimTotal := responsesIngressMetrics.byDims[dims]
	responsesIngressMetrics.mu.Unlock()

	log := requestLogger(c, "handler.openai_gateway.responses_ingress")
	log.Info("ingress_request_total",
		zap.String("kind", kind),
		zap.Uint64("total", total),
		zap.Uint64("dimension_total", dimTotal),
		zap.Int64("user_id", dims.UserID),
		zap.Int64("api_key_id", dims.APIKeyID),
		zap.String("model", dims.Model),
		zap.String("source_ip", dims.SourceIP),
		zap.String("client_request_id", dims.ClientRequestID),
		zap.String("proxy_request_id", dims.ProxyRequestID),
		zap.Bool("has_web_search", service.OpenAIResponsesNativeContract(body).HasHostedWebSearch),
		zap.Bool("heavy_web_search", backgroundResponseIsHeavyWebSearch(body)),
	)
}

func recordResponsesLifecycle(c *gin.Context, body []byte, kind string, fields ...zap.Field) {
	kind = strings.TrimSpace(kind)
	if kind == "" {
		kind = "lifecycle"
	}
	dims := responsesIngressDimensions(c, body, kind)
	logFields := []zap.Field{
		zap.String("kind", kind),
		zap.Int64("user_id", dims.UserID),
		zap.Int64("api_key_id", dims.APIKeyID),
		zap.String("model", dims.Model),
		zap.String("source_ip", dims.SourceIP),
		zap.String("client_request_id", dims.ClientRequestID),
		zap.String("proxy_request_id", dims.ProxyRequestID),
	}
	logFields = append(logFields, fields...)
	requestLogger(c, "handler.openai_gateway.responses_lifecycle").Info("responses_lifecycle_event", logFields...)
}

func responsesIngressDimensions(c *gin.Context, body []byte, kind string) responsesIngressMetricKey {
	var userID, apiKeyID int64
	if apiKey, ok := middleware2.GetAPIKeyFromContext(c); ok && apiKey != nil {
		userID = apiKey.UserID
		apiKeyID = apiKey.ID
	}
	if subject, ok := middleware2.GetAuthSubjectFromContext(c); ok && subject.UserID > 0 {
		userID = subject.UserID
	}
	clientRequestID, _ := c.Request.Context().Value(ctxkey.ClientRequestID).(string)
	proxyRequestID, _ := c.Request.Context().Value(ctxkey.RequestID).(string)
	model := strings.TrimSpace(gjson.GetBytes(body, "model").String())
	return responsesIngressMetricKey{
		Kind:            kind,
		UserID:          userID,
		APIKeyID:        apiKeyID,
		Model:           model,
		SourceIP:        ip.GetClientIP(c),
		ClientRequestID: strings.TrimSpace(clientRequestID),
		ProxyRequestID:  strings.TrimSpace(proxyRequestID),
	}
}

func resetResponsesIngressMetricsForTest() {
	responsesIngressMetrics.mu.Lock()
	defer responsesIngressMetrics.mu.Unlock()
	responsesIngressMetrics.total = make(map[string]uint64)
	responsesIngressMetrics.byDims = make(map[responsesIngressMetricKey]uint64)
}

func responsesIngressKindCountForTest(kind string) uint64 {
	responsesIngressMetrics.mu.Lock()
	defer responsesIngressMetrics.mu.Unlock()
	return responsesIngressMetrics.total[strings.TrimSpace(kind)]
}
