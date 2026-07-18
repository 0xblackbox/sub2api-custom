package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"go.uber.org/zap"
)

// BackgroundResponseHandler detaches background=true requests from the client
// connection, executes the existing Responses gateway in streaming mode, drains
// the SSE stream server-side, and stores the final Response object in Redis for
// later polling.
type BackgroundResponseHandler struct {
	tasks   *service.BackgroundResponseTaskService
	openAI  *OpenAIGatewayHandler
	execute func(c *gin.Context)
}

func NewBackgroundResponseHandler(tasks *service.BackgroundResponseTaskService, openAI *OpenAIGatewayHandler) *BackgroundResponseHandler {
	h := &BackgroundResponseHandler{tasks: tasks, openAI: openAI}
	h.execute = h.executeWithGateway
	return h
}

// TrySubmit returns true when it handled the request. For a normal Responses
// request it restores the body and returns false so the existing handler sees
// the request unchanged.
func (h *BackgroundResponseHandler) TrySubmit(c *gin.Context) bool {
	if h == nil || c == nil || c.Request == nil || c.Request.Body == nil || !isBareOpenAIResponsesPath(c) {
		return false
	}
	var cfg *config.Config
	if h.openAI != nil {
		cfg = h.openAI.cfg
	}
	body, err := readLenientJSONRequestBodyWithPrealloc(c.Request, cfg)
	if err != nil {
		if maxErr, ok := extractMaxBytesError(err); ok {
			backgroundResponseJSONError(c, http.StatusRequestEntityTooLarge, "invalid_request_error", buildBodyTooLargeMessage(maxErr.Limit))
			return true
		}
		backgroundResponseJSONError(c, http.StatusBadRequest, "invalid_request_error", "Failed to read request body")
		return true
	}
	restoreBackgroundResponseRequestBody(c.Request, body)
	background := gjson.GetBytes(body, "background")
	if !background.Exists() || background.Type != gjson.True {
		return false
	}
	h.submit(c, body)
	return true
}

func (h *BackgroundResponseHandler) submit(c *gin.Context, body []byte) {
	if h == nil || h.tasks == nil || !h.tasks.Enabled() || h.execute == nil {
		backgroundResponseError(c, service.ErrBackgroundResponseUnavailable)
		return
	}
	if !gjson.ValidBytes(body) {
		backgroundResponseJSONError(c, http.StatusBadRequest, "invalid_request_error", "Failed to parse request body")
		return
	}
	model := strings.TrimSpace(gjson.GetBytes(body, "model").String())
	if model == "" {
		backgroundResponseJSONError(c, http.StatusBadRequest, "invalid_request_error", "model is required")
		return
	}
	if store := gjson.GetBytes(body, "store"); store.Exists() && store.Type == gjson.False {
		backgroundResponseJSONError(c, http.StatusBadRequest, "invalid_request_error", "background responses require store=true")
		return
	}
	apiKey, ok := middleware2.GetAPIKeyFromContext(c)
	if !ok || apiKey == nil || apiKey.UserID <= 0 || apiKey.ID <= 0 {
		backgroundResponseJSONError(c, http.StatusUnauthorized, "authentication_error", "Invalid API key")
		return
	}

	// The gateway itself owns persistence. Do not ask subscription/OAuth
	// upstreams to implement OpenAI's background lifecycle as well.
	executionBody, err := normalizeBackgroundResponseExecutionBody(body)
	if err != nil {
		backgroundResponseJSONError(c, http.StatusBadRequest, "invalid_request_error", "Failed to normalize background request")
		return
	}
	task, err := h.tasks.Create(c.Request.Context(), service.BackgroundResponseOwner{UserID: apiKey.UserID, APIKeyID: apiKey.ID}, model)
	if err != nil {
		backgroundResponseError(c, err)
		return
	}
	taskCtx, recorder, cancel := newBackgroundResponseContext(c, executionBody, h.tasks.ExecutionTimeout())

	c.Header("Cache-Control", "no-store")
	c.Header("Location", backgroundResponsePollURL(c.Request.URL.Path, task.ID))
	c.Header("Retry-After", "3")
	c.JSON(http.StatusOK, backgroundResponsePendingPayload(task))

	go h.run(task.ID, taskCtx, recorder, cancel)
}

func (h *BackgroundResponseHandler) Get(c *gin.Context) {
	if h == nil || h.tasks == nil || !h.tasks.Enabled() {
		backgroundResponseError(c, service.ErrBackgroundResponseUnavailable)
		return
	}
	apiKey, ok := middleware2.GetAPIKeyFromContext(c)
	if !ok || apiKey == nil || apiKey.UserID <= 0 || apiKey.ID <= 0 {
		backgroundResponseJSONError(c, http.StatusUnauthorized, "authentication_error", "Invalid API key")
		return
	}
	task, err := h.tasks.Get(c.Request.Context(), service.BackgroundResponseOwner{UserID: apiKey.UserID, APIKeyID: apiKey.ID}, c.Param("response_id"))
	if err != nil {
		backgroundResponseError(c, err)
		return
	}
	c.Header("Cache-Control", "no-store")
	if task.Status == service.BackgroundResponseStatusQueued || task.Status == service.BackgroundResponseStatusInProgress {
		c.Header("Retry-After", "3")
		c.JSON(http.StatusOK, backgroundResponsePendingPayload(task))
		return
	}
	if task.Status == service.BackgroundResponseStatusCompleted && len(task.Result) > 0 && json.Valid(task.Result) {
		result := task.Result
		result, _ = sjson.SetBytes(result, "id", task.ID)
		result, _ = sjson.SetBytes(result, "background", true)
		c.Data(http.StatusOK, "application/json; charset=utf-8", result)
		return
	}
	c.JSON(http.StatusOK, backgroundResponseFailedPayload(task))
}

func (h *BackgroundResponseHandler) executeWithGateway(c *gin.Context) {
	if h == nil || h.openAI == nil {
		backgroundResponseJSONError(c, http.StatusServiceUnavailable, "api_error", "Responses gateway is unavailable")
		return
	}
	h.openAI.Responses(c)
}

func (h *BackgroundResponseHandler) run(taskID string, taskCtx *gin.Context, recorder *httptest.ResponseRecorder, cancel context.CancelFunc) {
	defer cancel()
	defer func() {
		if recovered := recover(); recovered != nil {
			logger.L().Error("background_response.execution_panicked", zap.String("response_id", taskID), zap.Any("panic", recovered))
			h.failTask(taskID, http.StatusInternalServerError, backgroundResponseErrorPayload("api_error", "background response panicked"))
		}
	}()
	if err := h.tasks.MarkInProgress(context.Background(), taskID); err != nil {
		logger.L().Error("background_response.mark_processing_failed", zap.String("response_id", taskID), zap.Error(err))
	}
	h.execute(taskCtx)
	body := bytes.TrimSpace(recorder.Body.Bytes())
	if err := taskCtx.Request.Context().Err(); err != nil && len(body) == 0 {
		h.failTask(taskID, http.StatusGatewayTimeout, backgroundResponseErrorPayload("timeout_error", "background response timed out"))
		return
	}
	statusCode := recorder.Code
	if statusCode == 0 {
		statusCode = http.StatusOK
	}
	if statusCode >= http.StatusOK && statusCode < http.StatusMultipleChoices {
		if len(body) == 0 || !json.Valid(body) {
			if finalResponse, ok := backgroundResponseFinalResponseFromSSE(body, taskID); ok {
				if err := h.tasks.Complete(context.Background(), taskID, statusCode, finalResponse); err != nil {
					logger.L().Error("background_response.complete_store_failed", zap.String("response_id", taskID), zap.Error(err))
				}
				return
			}
			if taskErr, ok := backgroundResponseErrorFromSSE(body); ok {
				h.failTask(taskID, http.StatusBadGateway, taskErr)
				return
			}
			h.failTask(taskID, http.StatusBadGateway, backgroundResponseErrorPayload("api_error", "upstream returned an invalid response"))
			return
		}
		if err := h.tasks.Complete(context.Background(), taskID, statusCode, json.RawMessage(body)); err != nil {
			logger.L().Error("background_response.complete_store_failed", zap.String("response_id", taskID), zap.Error(err))
		}
		return
	}
	h.failTask(taskID, statusCode, extractBackgroundResponseError(body))
}

func (h *BackgroundResponseHandler) failTask(taskID string, statusCode int, taskErr json.RawMessage) {
	if err := h.tasks.Fail(context.Background(), taskID, statusCode, taskErr); err != nil {
		logger.L().Error("background_response.failure_store_failed", zap.String("response_id", taskID), zap.Error(err))
	}
}

func normalizeBackgroundResponseExecutionBody(body []byte) ([]byte, error) {
	var err error
	body, err = sjson.DeleteBytes(body, "background")
	if err != nil {
		return nil, err
	}
	// OAuth / ChatGPT internal upstreams often cut long non-streaming requests
	// around the proxy/LB timeout boundary. Execute local background jobs via
	// streaming drain instead, then persist the terminal Response object so the
	// public polling contract still matches OpenAI's background API.
	body, err = sjson.SetBytes(body, "stream", true)
	if err != nil {
		return nil, err
	}
	// Local Redis is the persistence boundary. This also keeps ChatGPT OAuth
	// upstreams compatible when they only accept store=false.
	body, err = sjson.SetBytes(body, "store", false)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func newBackgroundResponseContext(c *gin.Context, body []byte, timeoutDuration time.Duration) (*gin.Context, *httptest.ResponseRecorder, context.CancelFunc) {
	base := context.WithoutCancel(c.Request.Context())
	executionCtx, cancel := context.WithTimeout(base, timeoutDuration)
	request := c.Request.Clone(executionCtx)
	restoreBackgroundResponseRequestBody(request, body)

	taskCtx := c.Copy()
	recorder := httptest.NewRecorder()
	recorderCtx, _ := gin.CreateTestContext(recorder)
	taskCtx.Writer = recorderCtx.Writer
	taskCtx.Request = request
	return taskCtx, recorder, cancel
}

func restoreBackgroundResponseRequestBody(request *http.Request, body []byte) {
	request.Body = io.NopCloser(bytes.NewReader(body))
	request.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(body)), nil
	}
	request.ContentLength = int64(len(body))
}

func backgroundResponsePollURL(submitPath, responseID string) string {
	path := strings.TrimRight(submitPath, "/")
	return path + "/" + responseID
}

func backgroundResponsePendingPayload(task *service.BackgroundResponseTaskRecord) gin.H {
	status := task.Status
	if status == service.BackgroundResponseStatusQueued {
		status = service.BackgroundResponseStatusQueued
	}
	return gin.H{
		"id":                 task.ID,
		"object":             "response",
		"created_at":         task.CreatedAt,
		"status":             status,
		"background":         true,
		"model":              task.Model,
		"output":             []any{},
		"error":              nil,
		"incomplete_details": nil,
		"usage":              nil,
	}
}

func backgroundResponseFailedPayload(task *service.BackgroundResponseTaskRecord) gin.H {
	var responseErr any
	if len(task.Error) > 0 && json.Valid(task.Error) {
		_ = json.Unmarshal(task.Error, &responseErr)
	}
	if responseErr == nil {
		responseErr = gin.H{"type": "api_error", "message": "background response failed"}
	}
	return gin.H{
		"id":                 task.ID,
		"object":             "response",
		"created_at":         task.CreatedAt,
		"status":             service.BackgroundResponseStatusFailed,
		"background":         true,
		"model":              task.Model,
		"output":             []any{},
		"error":              responseErr,
		"incomplete_details": nil,
		"usage":              nil,
	}
}

func extractBackgroundResponseError(body []byte) json.RawMessage {
	if json.Valid(body) {
		var envelope struct {
			Error json.RawMessage `json:"error"`
		}
		if json.Unmarshal(body, &envelope) == nil && len(envelope.Error) > 0 && json.Valid(envelope.Error) {
			return envelope.Error
		}
		return json.RawMessage(body)
	}
	return backgroundResponseErrorPayload("api_error", "background response failed")
}

func backgroundResponseErrorPayload(errorType, message string) json.RawMessage {
	data, _ := json.Marshal(gin.H{"type": errorType, "message": message})
	return data
}

func backgroundResponseFinalResponseFromSSE(body []byte, fallbackID string) (json.RawMessage, bool) {
	if len(body) == 0 {
		return nil, false
	}
	var finalResponse []byte
	var outputItems []json.RawMessage
	seenItems := make(map[string]struct{})
	var textBuilder strings.Builder
	finalText := ""

	backgroundResponseForEachSSEDataPayload(body, func(data []byte) {
		if len(data) == 0 || !gjson.ValidBytes(data) {
			return
		}
		eventType := strings.TrimSpace(gjson.GetBytes(data, "type").String())
		switch eventType {
		case "response.output_text.delta":
			textBuilder.WriteString(gjson.GetBytes(data, "delta").String())
		case "response.output_text.done":
			if text := gjson.GetBytes(data, "text").String(); text != "" {
				finalText = text
			}
		case "response.output_item.done":
			item := gjson.GetBytes(data, "item")
			if !item.Exists() || !item.IsObject() || item.Raw == "" {
				return
			}
			key := strings.TrimSpace(item.Get("id").String())
			if key == "" {
				key = item.Raw
			}
			if _, ok := seenItems[key]; ok {
				return
			}
			seenItems[key] = struct{}{}
			outputItems = append(outputItems, json.RawMessage(item.Raw))
		case "response.completed", "response.done":
			if finalResponse != nil {
				return
			}
			response := gjson.GetBytes(data, "response")
			if response.Exists() && response.IsObject() && response.Raw != "" {
				finalResponse = []byte(response.Raw)
				return
			}
			// Some compatible upstreams emit a terminal event without embedding
			// response. Synthesize the minimal Response object from accumulated
			// deltas rather than losing an otherwise complete streamed result.
			if textBuilder.Len() > 0 || finalText != "" || len(outputItems) > 0 {
				model := strings.TrimSpace(gjson.GetBytes(data, "response.model").String())
				finalResponse = backgroundResponseSyntheticCompletedResponse(fallbackID, model, finalTextOrBuilder(finalText, &textBuilder), outputItems)
			}
		}
	})
	if finalResponse == nil || !json.Valid(finalResponse) {
		return nil, false
	}
	finalResponse = backgroundResponseNormalizeCompletedResponse(finalResponse, fallbackID, finalTextOrBuilder(finalText, &textBuilder), outputItems)
	return json.RawMessage(finalResponse), true
}

func backgroundResponseNormalizeCompletedResponse(response []byte, fallbackID, outputText string, outputItems []json.RawMessage) []byte {
	if !gjson.ValidBytes(response) {
		return response
	}
	updated := response
	if strings.TrimSpace(gjson.GetBytes(updated, "id").String()) == "" && strings.TrimSpace(fallbackID) != "" {
		if next, err := sjson.SetBytes(updated, "id", fallbackID); err == nil {
			updated = next
		}
	}
	if strings.TrimSpace(gjson.GetBytes(updated, "object").String()) == "" {
		if next, err := sjson.SetBytes(updated, "object", "response"); err == nil {
			updated = next
		}
	}
	if strings.TrimSpace(gjson.GetBytes(updated, "status").String()) == "" {
		if next, err := sjson.SetBytes(updated, "status", service.BackgroundResponseStatusCompleted); err == nil {
			updated = next
		}
	}
	output := gjson.GetBytes(updated, "output")
	if output.Exists() && output.IsArray() && len(output.Array()) > 0 {
		return updated
	}
	if len(outputItems) > 0 {
		if encoded, err := json.Marshal(outputItems); err == nil {
			if next, setErr := sjson.SetRawBytes(updated, "output", encoded); setErr == nil {
				updated = next
			}
		}
		return updated
	}
	if strings.TrimSpace(outputText) == "" {
		return updated
	}
	if encoded := backgroundResponseOutputTextItems(outputText); len(encoded) > 0 {
		if next, err := sjson.SetRawBytes(updated, "output", encoded); err == nil {
			updated = next
		}
	}
	return updated
}

func backgroundResponseSyntheticCompletedResponse(id, model, outputText string, outputItems []json.RawMessage) []byte {
	resp := gin.H{
		"id":         strings.TrimSpace(id),
		"object":     "response",
		"status":     service.BackgroundResponseStatusCompleted,
		"output":     []any{},
		"error":      nil,
		"model":      strings.TrimSpace(model),
		"background": true,
	}
	body, _ := json.Marshal(resp)
	return backgroundResponseNormalizeCompletedResponse(body, id, outputText, outputItems)
}

func backgroundResponseOutputTextItems(text string) []byte {
	type contentItem struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	type outputItem struct {
		Type    string        `json:"type"`
		Role    string        `json:"role"`
		Status  string        `json:"status,omitempty"`
		Content []contentItem `json:"content"`
	}
	encoded, _ := json.Marshal([]outputItem{{
		Type:   "message",
		Role:   "assistant",
		Status: service.BackgroundResponseStatusCompleted,
		Content: []contentItem{{
			Type: "output_text",
			Text: text,
		}},
	}})
	return encoded
}

func finalTextOrBuilder(finalText string, builder *strings.Builder) string {
	if finalText != "" {
		return finalText
	}
	if builder == nil {
		return ""
	}
	return builder.String()
}

func backgroundResponseErrorFromSSE(body []byte) (json.RawMessage, bool) {
	if len(body) == 0 {
		return nil, false
	}
	var taskErr json.RawMessage
	backgroundResponseForEachSSEDataPayload(body, func(data []byte) {
		if taskErr != nil || len(data) == 0 || !gjson.ValidBytes(data) {
			return
		}
		eventType := strings.TrimSpace(gjson.GetBytes(data, "type").String())
		switch eventType {
		case "response.failed":
			for _, path := range []string{"response.error", "error"} {
				value := gjson.GetBytes(data, path)
				if value.Exists() && value.IsObject() && value.Raw != "" {
					taskErr = json.RawMessage(value.Raw)
					return
				}
			}
			taskErr = backgroundResponseErrorPayload("upstream_error", backgroundResponseSSEErrorMessage(data, "background response failed"))
		case "response.incomplete":
			taskErr = backgroundResponseErrorPayload("incomplete_error", backgroundResponseSSEErrorMessage(data, "background response incomplete"))
		case "response.cancelled", "response.canceled":
			taskErr = backgroundResponseErrorPayload("cancelled_error", backgroundResponseSSEErrorMessage(data, "background response cancelled"))
		}
	})
	return taskErr, taskErr != nil
}

func backgroundResponseSSEErrorMessage(data []byte, fallback string) string {
	for _, path := range []string{"response.error.message", "error.message", "message", "response.incomplete_details.reason"} {
		if msg := strings.TrimSpace(gjson.GetBytes(data, path).String()); msg != "" {
			return msg
		}
	}
	return fallback
}

func backgroundResponseForEachSSEDataPayload(body []byte, fn func([]byte)) {
	if fn == nil || len(body) == 0 {
		return
	}
	var lines []string
	flush := func() {
		if len(lines) == 0 {
			return
		}
		backgroundResponseEmitSSEDataPayloads(lines, fn)
		lines = lines[:0]
	}
	for _, rawLine := range strings.Split(string(body), "\n") {
		line := strings.TrimRight(rawLine, "\r")
		if data, ok := backgroundResponseSSEDataLine(line); ok {
			lines = append(lines, data)
			continue
		}
		if strings.TrimSpace(line) == "" {
			flush()
		}
	}
	flush()
}

func backgroundResponseEmitSSEDataPayloads(lines []string, fn func([]byte)) {
	if len(lines) == 0 || fn == nil {
		return
	}
	if len(lines) == 1 {
		backgroundResponseEmitSSEDataPayload(lines[0], fn)
		return
	}
	joined := strings.Join(lines, "\n")
	if gjson.Valid(joined) {
		backgroundResponseEmitSSEDataPayload(joined, fn)
		return
	}
	for _, line := range lines {
		backgroundResponseEmitSSEDataPayload(line, fn)
	}
}

func backgroundResponseEmitSSEDataPayload(data string, fn func([]byte)) {
	data = strings.TrimSpace(data)
	if data == "" || data == "[DONE]" {
		return
	}
	fn([]byte(data))
}

func backgroundResponseSSEDataLine(line string) (string, bool) {
	if !strings.HasPrefix(line, "data:") {
		return "", false
	}
	return strings.TrimSpace(strings.TrimPrefix(line, "data:")), true
}

func backgroundResponseError(c *gin.Context, err error) {
	status := infraerrors.Code(err)
	code := infraerrors.Reason(err)
	message := infraerrors.Message(err)
	if status <= 0 {
		status = http.StatusInternalServerError
	}
	if strings.TrimSpace(code) == "" {
		code = "api_error"
	}
	backgroundResponseJSONError(c, status, code, message)
}

func backgroundResponseJSONError(c *gin.Context, status int, code, message string) {
	c.Header("Cache-Control", "no-store")
	c.JSON(status, gin.H{"error": gin.H{"type": code, "code": code, "message": message}})
}
