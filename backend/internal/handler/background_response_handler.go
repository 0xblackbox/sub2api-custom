package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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
	tasks      *service.BackgroundResponseTaskService
	openAI     *OpenAIGatewayHandler
	execute    func(c *gin.Context)
	heavyQueue *backgroundResponseHeavyQueue
	running    sync.Map // response_id -> context.CancelFunc
}

func NewBackgroundResponseHandler(tasks *service.BackgroundResponseTaskService, openAI *OpenAIGatewayHandler) *BackgroundResponseHandler {
	h := &BackgroundResponseHandler{tasks: tasks, openAI: openAI, heavyQueue: defaultBackgroundResponseHeavyQueue}
	h.execute = h.executeWithGateway
	h.failActiveBackgroundTasksOnStartup()
	return h
}

const (
	backgroundResponseUserSlotWaitTimeoutKey = "subtoproxy_background_response_user_slot_wait_timeout"
	responsesSkipUserSlotKey                 = "subtoproxy_responses_skip_user_slot"
	responsesHeavySlotAlreadyAcquiredKey     = "subtoproxy_responses_heavy_slot_already_acquired"
)

var defaultBackgroundResponseHeavyQueue = newBackgroundResponseHeavyQueue()

var errBackgroundHeavyQueueFull = errors.New("heavy web search queue full")

type backgroundResponseHeavyQueue struct {
	global          chan struct{}
	perUserCapacity int
	maxWaiting      int

	mu      sync.Mutex
	users   map[int64]chan struct{}
	waiting int
}

func newBackgroundResponseHeavyQueue() *backgroundResponseHeavyQueue {
	return newBackgroundResponseHeavyQueueWithOptions(1, 1, 64)
}

func newBackgroundResponseHeavyQueueWithOptions(globalConcurrency, perUserConcurrency, maxWaiting int) *backgroundResponseHeavyQueue {
	if globalConcurrency <= 0 {
		globalConcurrency = 1
	}
	if perUserConcurrency <= 0 {
		perUserConcurrency = 1
	}
	if maxWaiting < 0 {
		maxWaiting = 0
	}
	return &backgroundResponseHeavyQueue{
		global:          make(chan struct{}, globalConcurrency),
		perUserCapacity: perUserConcurrency,
		maxWaiting:      maxWaiting,
		users:           make(map[int64]chan struct{}),
	}
}

func (q *backgroundResponseHeavyQueue) acquire(ctx context.Context, userID int64) (func(), time.Duration, error) {
	if q == nil {
		return func() {}, 0, nil
	}
	start := time.Now()
	userCh := q.userChannel(userID)

	if release, ok := q.tryAcquire(userCh); ok {
		return release, time.Since(start), nil
	}
	if !q.reserveWaitingSlot() {
		return nil, time.Since(start), errBackgroundHeavyQueueFull
	}
	defer q.releaseWaitingSlot()

	select {
	case userCh <- struct{}{}:
	case <-ctx.Done():
		return nil, time.Since(start), ctx.Err()
	}
	select {
	case q.global <- struct{}{}:
		return q.releaseFunc(userCh), time.Since(start), nil
	case <-ctx.Done():
		<-userCh
		return nil, time.Since(start), ctx.Err()
	}
}

func (q *backgroundResponseHeavyQueue) tryAcquire(userCh chan struct{}) (func(), bool) {
	select {
	case userCh <- struct{}{}:
	default:
		return nil, false
	}
	select {
	case q.global <- struct{}{}:
		return q.releaseFunc(userCh), true
	default:
		<-userCh
		return nil, false
	}
}

func (q *backgroundResponseHeavyQueue) releaseFunc(userCh chan struct{}) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			<-q.global
			<-userCh
		})
	}
}

func (q *backgroundResponseHeavyQueue) canAcceptWaitingSlot() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.waiting < q.maxWaiting
}

func (q *backgroundResponseHeavyQueue) reserveWaitingSlot() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.waiting >= q.maxWaiting {
		return false
	}
	q.waiting++
	return true
}

func (q *backgroundResponseHeavyQueue) releaseWaitingSlot() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.waiting > 0 {
		q.waiting--
	}
}

func (q *backgroundResponseHeavyQueue) userChannel(userID int64) chan struct{} {
	q.mu.Lock()
	defer q.mu.Unlock()
	ch := q.users[userID]
	if ch == nil {
		ch = make(chan struct{}, q.perUserCapacity)
		q.users[userID] = ch
	}
	return ch
}

func (q *backgroundResponseHeavyQueue) waitingCount() int {
	if q == nil {
		return 0
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.waiting
}

func (h *BackgroundResponseHandler) acquireHeavyWebSearchQueue(ctx context.Context, userID int64) (func(), time.Duration, error) {
	q := defaultBackgroundResponseHeavyQueue
	if h != nil && h.heavyQueue != nil {
		q = h.heavyQueue
	}
	return q.acquire(ctx, userID)
}

func (h *BackgroundResponseHandler) failActiveBackgroundTasksOnStartup() {
	if h == nil || h.tasks == nil || !h.tasks.Enabled() {
		return
	}
	go func() {
		failed, err := h.tasks.FailActiveOnStartup(context.Background(), 1000)
		fields := []zap.Field{zap.Int("failed_active_tasks", failed)}
		if err != nil {
			fields = append(fields, zap.Error(err))
			logger.L().Warn("background_response.startup_recovery_failed", fields...)
			return
		}
		if failed > 0 {
			logger.L().Warn("background_response.startup_recovered_active_tasks", fields...)
		}
	}()
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
	if background.Exists() && background.Type == gjson.True {
		recordResponsesIngressOnce(c, body, "responses_post")
		h.submit(c, body, backgroundResponseSubmitOptions{heavyWebSearch: backgroundResponseIsHeavyWebSearch(body)})
		return true
	}
	return false
}

type backgroundResponseSubmitOptions struct {
	autoBackground bool
	heavyWebSearch bool
}

func (h *BackgroundResponseHandler) submit(c *gin.Context, body []byte, opts backgroundResponseSubmitOptions) {
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
	if !opts.autoBackground {
		if store := gjson.GetBytes(body, "store"); store.Exists() && store.Type == gjson.False {
			backgroundResponseJSONError(c, http.StatusBadRequest, "invalid_request_error", "background responses require store=true")
			return
		}
	}
	apiKey, ok := middleware2.GetAPIKeyFromContext(c)
	if !ok || apiKey == nil || apiKey.UserID <= 0 || apiKey.ID <= 0 {
		backgroundResponseJSONError(c, http.StatusUnauthorized, "authentication_error", "Invalid API key")
		return
	}
	if opts.heavyWebSearch {
		q := defaultBackgroundResponseHeavyQueue
		if h.heavyQueue != nil {
			q = h.heavyQueue
		}
		if q != nil && !q.canAcceptWaitingSlot() {
			c.Header("Retry-After", "3")
			backgroundResponseJSONError(c, http.StatusTooManyRequests, "proxy_heavy_queue_full", "heavy web search queue is full, please retry later")
			return
		}
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
	taskCtx.Set(responsesInternalBackgroundExecuteKey, true)

	c.Header("Cache-Control", "no-store")
	c.Header("Location", backgroundResponsePollURL(c.Request.URL.Path, task.ID))
	c.Header("Retry-After", "3")
	if opts.autoBackground {
		c.Header("X-SubtoProxy-Auto-Background", "true")
	}
	if opts.heavyWebSearch {
		c.Header("X-SubtoProxy-Background-Queue", "heavy_web_search")
	}
	c.JSON(http.StatusOK, backgroundResponsePendingPayload(task))

	owner := service.BackgroundResponseOwner{UserID: apiKey.UserID, APIKeyID: apiKey.ID}
	go h.run(task.ID, taskCtx, recorder, cancel, owner, opts)
}

func (h *BackgroundResponseHandler) Get(c *gin.Context) {
	if h == nil || h.tasks == nil || !h.tasks.Enabled() {
		backgroundResponseError(c, service.ErrBackgroundResponseUnavailable)
		return
	}
	recordResponsesIngressOnce(c, nil, "background_poll")
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
	if task.Status == service.BackgroundResponseStatusCancelled {
		c.JSON(http.StatusOK, backgroundResponseCancelledPayload(task))
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

func (h *BackgroundResponseHandler) Cancel(c *gin.Context) {
	if h == nil || h.tasks == nil || !h.tasks.Enabled() {
		backgroundResponseError(c, service.ErrBackgroundResponseUnavailable)
		return
	}
	recordResponsesIngressOnce(c, nil, "background_cancel")
	apiKey, ok := middleware2.GetAPIKeyFromContext(c)
	if !ok || apiKey == nil || apiKey.UserID <= 0 || apiKey.ID <= 0 {
		backgroundResponseJSONError(c, http.StatusUnauthorized, "authentication_error", "Invalid API key")
		return
	}
	responseID := strings.TrimSpace(c.Param("response_id"))
	if responseID == "" {
		responseID, _ = backgroundResponseCancelIDFromPath(c)
	}
	if cancelAny, ok := h.running.Load(responseID); ok {
		if cancel, ok := cancelAny.(context.CancelFunc); ok && cancel != nil {
			cancel()
		}
	}
	task, err := h.tasks.Cancel(c.Request.Context(), service.BackgroundResponseOwner{UserID: apiKey.UserID, APIKeyID: apiKey.ID}, responseID)
	if err != nil {
		backgroundResponseError(c, err)
		return
	}
	c.Header("Cache-Control", "no-store")
	if task.Status == service.BackgroundResponseStatusCompleted && len(task.Result) > 0 && json.Valid(task.Result) {
		result := task.Result
		result, _ = sjson.SetBytes(result, "id", task.ID)
		result, _ = sjson.SetBytes(result, "background", true)
		c.Data(http.StatusOK, "application/json; charset=utf-8", result)
		return
	}
	if task.Status == service.BackgroundResponseStatusFailed {
		c.JSON(http.StatusOK, backgroundResponseFailedPayload(task))
		return
	}
	c.JSON(http.StatusOK, backgroundResponseCancelledPayload(task))
}

func (h *BackgroundResponseHandler) TryCancel(c *gin.Context) bool {
	if h == nil || c == nil || c.Request == nil || c.Request.Method != http.MethodPost {
		return false
	}
	responseID, ok := backgroundResponseCancelIDFromPath(c)
	if !ok || responseID == "" {
		return false
	}
	c.Params = append(c.Params, gin.Param{Key: "response_id", Value: responseID})
	h.Cancel(c)
	return true
}

func backgroundResponseCancelIDFromPath(c *gin.Context) (string, bool) {
	if c == nil || c.Request == nil || c.Request.URL == nil {
		return "", false
	}
	path := strings.Trim(strings.TrimSpace(c.Request.URL.Path), "/")
	parts := strings.Split(path, "/")
	for i := 0; i+2 < len(parts); i++ {
		if parts[i] != "responses" || parts[i+2] != "cancel" {
			continue
		}
		id := strings.TrimSpace(parts[i+1])
		if id == "" || !strings.HasPrefix(id, "resp") {
			return "", false
		}
		return id, true
	}
	return "", false
}

func (h *BackgroundResponseHandler) executeWithGateway(c *gin.Context) {
	if h == nil || h.openAI == nil {
		backgroundResponseJSONError(c, http.StatusServiceUnavailable, "api_error", "Responses gateway is unavailable")
		return
	}
	h.openAI.Responses(c)
}

func (h *BackgroundResponseHandler) run(taskID string, taskCtx *gin.Context, recorder *httptest.ResponseRecorder, cancel context.CancelFunc, owner service.BackgroundResponseOwner, opts backgroundResponseSubmitOptions) {
	h.running.Store(taskID, cancel)
	defer h.running.Delete(taskID)
	defer cancel()
	defer func() {
		if recovered := recover(); recovered != nil {
			logger.L().Error("background_response.execution_panicked", zap.String("response_id", taskID), zap.Any("panic", recovered))
			h.failTask(taskID, http.StatusInternalServerError, backgroundResponseErrorPayload("api_error", "background response panicked"))
		}
	}()
	if opts.heavyWebSearch {
		release, queueWait, err := h.acquireHeavyWebSearchQueue(taskCtx.Request.Context(), owner.UserID)
		if err != nil {
			logger.L().Warn("background_response.heavy_queue_acquire_failed",
				zap.String("response_id", taskID),
				zap.Int64("user_id", owner.UserID),
				zap.Int64("queue_wait_ms", queueWait.Milliseconds()),
				zap.Error(err),
			)
			if errors.Is(err, context.Canceled) {
				h.cancelTask(taskID)
				return
			}
			if errors.Is(err, errBackgroundHeavyQueueFull) {
				h.failTask(taskID, http.StatusTooManyRequests, backgroundResponseErrorPayload("proxy_heavy_queue_full", "heavy web search queue is full"))
				return
			}
			h.failTask(taskID, http.StatusServiceUnavailable, backgroundResponseErrorPayload("proxy_heavy_queue_timeout", "heavy web search queue timed out before execution"))
			return
		}
		defer release()
		taskCtx.Set(responsesSkipUserSlotKey, true)
		taskCtx.Set(responsesHeavySlotAlreadyAcquiredKey, true)
		logger.L().Info("background_response.heavy_queue_acquired",
			zap.String("response_id", taskID),
			zap.Int64("user_id", owner.UserID),
			zap.Int64("queue_wait_ms", queueWait.Milliseconds()),
		)
	}
	if h.isTaskCancelled(taskID, owner) {
		return
	}
	if err := h.tasks.MarkInProgress(context.Background(), taskID); err != nil {
		logger.L().Error("background_response.mark_processing_failed", zap.String("response_id", taskID), zap.Error(err))
	}
	if h.isTaskCancelled(taskID, owner) {
		return
	}
	h.execute(taskCtx)
	body := bytes.TrimSpace(recorder.Body.Bytes())
	if err := taskCtx.Request.Context().Err(); err != nil && len(body) == 0 {
		if errors.Is(err, context.Canceled) {
			h.cancelTask(taskID)
			return
		}
		h.failTask(taskID, http.StatusGatewayTimeout, backgroundResponseErrorPayload("upstream_timeout", "background response timed out"))
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
	h.failTask(taskID, statusCode, classifyBackgroundResponseFailure(statusCode, body, extractBackgroundResponseError(body)))
}

func (h *BackgroundResponseHandler) failTask(taskID string, statusCode int, taskErr json.RawMessage) {
	if err := h.tasks.Fail(context.Background(), taskID, statusCode, taskErr); err != nil {
		logger.L().Error("background_response.failure_store_failed", zap.String("response_id", taskID), zap.Error(err))
	}
}

func (h *BackgroundResponseHandler) cancelTask(taskID string) {
	if h == nil || h.tasks == nil {
		return
	}
	if err := h.tasks.CancelByID(context.Background(), taskID); err == nil {
		return
	}
	// Cancel requires owner for API calls. For internal cancellation we only have
	// the id in some late failure paths; keep terminal cancellation best-effort by
	// failing with an explicit type if ownership lookup is unavailable.
	if failErr := h.tasks.Fail(context.Background(), taskID, statusClientClosedRequest, backgroundResponseErrorPayload("downstream_client_cancelled", "background response cancelled")); failErr != nil {
		logger.L().Error("background_response.cancel_store_failed", zap.String("response_id", taskID), zap.Error(failErr))
	}
}

func (h *BackgroundResponseHandler) isTaskCancelled(taskID string, owner service.BackgroundResponseOwner) bool {
	if h == nil || h.tasks == nil || !h.tasks.Enabled() {
		return false
	}
	task, err := h.tasks.Get(context.Background(), owner, taskID)
	return err == nil && task.Status == service.BackgroundResponseStatusCancelled
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

func backgroundResponseIsHeavyWebSearch(body []byte) bool {
	if !gjson.ValidBytes(body) {
		return false
	}
	contract := service.OpenAIResponsesNativeContract(body)
	if !contract.HasHostedWebSearch {
		return false
	}
	if !backgroundResponseHasHighReasoning(body) {
		return false
	}
	if !backgroundResponseHasHighSearchContext(body) {
		return false
	}
	if maxOutputTokens := gjson.GetBytes(body, "max_output_tokens").Int(); maxOutputTokens > 0 && maxOutputTokens < 4000 {
		return false
	}
	return backgroundResponseRequiresToolUse(body, contract.ToolChoice)
}

func backgroundResponseHasHighReasoning(body []byte) bool {
	switch strings.ToLower(strings.TrimSpace(gjson.GetBytes(body, "reasoning.effort").String())) {
	case "high", "xhigh", "max", "ultra":
		return true
	default:
		return false
	}
}

func backgroundResponseHasHighSearchContext(body []byte) bool {
	tools := gjson.GetBytes(body, "tools")
	if !tools.IsArray() {
		return false
	}
	for _, tool := range tools.Array() {
		toolType := strings.ToLower(strings.TrimSpace(tool.Get("type").String()))
		if toolType == "" {
			toolType = strings.ToLower(strings.TrimSpace(tool.Get("name").String()))
		}
		if !strings.HasPrefix(toolType, "web_search") {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(tool.Get("search_context_size").String())) {
		case "high", "medium":
			return true
		case "":
			return true
		}
	}
	return false
}

func backgroundResponseRequiresToolUse(body []byte, summaryToolChoice string) bool {
	toolChoice := gjson.GetBytes(body, "tool_choice")
	if !toolChoice.Exists() {
		return false
	}
	if toolChoice.Type == gjson.String {
		switch strings.ToLower(strings.TrimSpace(toolChoice.String())) {
		case "required", "web_search", "web_search_preview":
			return true
		default:
			return false
		}
	}
	summaryToolChoice = strings.ToLower(strings.TrimSpace(summaryToolChoice))
	return summaryToolChoice == "required" || strings.Contains(summaryToolChoice, "web_search")
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

func backgroundResponseCancelledPayload(task *service.BackgroundResponseTaskRecord) gin.H {
	return gin.H{
		"id":                 task.ID,
		"object":             "response",
		"created_at":         task.CreatedAt,
		"status":             service.BackgroundResponseStatusCancelled,
		"background":         true,
		"model":              task.Model,
		"output":             []any{},
		"error":              nil,
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

func classifyBackgroundResponseFailure(statusCode int, body []byte, taskErr json.RawMessage) json.RawMessage {
	bodyText := strings.ToLower(string(body))
	errText := strings.ToLower(string(taskErr))
	switch {
	case statusCode == http.StatusTooManyRequests:
		return backgroundResponseErrorPayload("upstream_rate_limited", backgroundResponseErrorMessage(taskErr, "upstream rate limited"))
	case strings.Contains(bodyText, "unexpected eof") || strings.Contains(errText, "unexpected eof"):
		return backgroundResponseErrorPayload("upstream_unexpected_eof", backgroundResponseErrorMessage(taskErr, "upstream connection closed unexpectedly"))
	case statusCode == http.StatusGatewayTimeout || strings.Contains(errText, "timeout"):
		return backgroundResponseErrorPayload("upstream_timeout", backgroundResponseErrorMessage(taskErr, "upstream request timed out"))
	default:
		return taskErr
	}
}

func backgroundResponseErrorMessage(taskErr json.RawMessage, fallback string) string {
	if len(taskErr) > 0 && json.Valid(taskErr) {
		if msg := strings.TrimSpace(gjson.GetBytes(taskErr, "message").String()); msg != "" {
			return msg
		}
		if msg := strings.TrimSpace(gjson.GetBytes(taskErr, "error.message").String()); msg != "" {
			return msg
		}
	}
	return fallback
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
			backgroundResponseAppendOutputItem(item, &outputItems, seenItems)
		case "response.output_item.added":
			item := gjson.GetBytes(data, "item")
			if backgroundResponseOutputItemCanUseAddedFallback(item.Get("type").String()) {
				backgroundResponseAppendOutputItem(item, &outputItems, seenItems)
			}
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

func backgroundResponseAppendOutputItem(item gjson.Result, outputItems *[]json.RawMessage, seenItems map[string]struct{}) {
	if !item.Exists() || !item.IsObject() || item.Raw == "" || outputItems == nil {
		return
	}
	key := backgroundResponseOutputItemKey(item)
	if _, ok := seenItems[key]; ok {
		backgroundResponseReplaceOutputItemIfRicher(item, key, outputItems)
		return
	}
	seenItems[key] = struct{}{}
	*outputItems = append(*outputItems, json.RawMessage(item.Raw))
}

func backgroundResponseReplaceOutputItemIfRicher(candidate gjson.Result, key string, outputItems *[]json.RawMessage) {
	if outputItems == nil || strings.TrimSpace(key) == "" || !candidate.Exists() || candidate.Raw == "" {
		return
	}
	for idx, raw := range *outputItems {
		if len(raw) == 0 || !json.Valid(raw) {
			continue
		}
		current := gjson.ParseBytes(raw)
		if backgroundResponseOutputItemKey(current) != key {
			continue
		}
		if backgroundResponseOutputItemIsRicher(candidate, current) {
			(*outputItems)[idx] = json.RawMessage(candidate.Raw)
		}
		return
	}
}

func backgroundResponseOutputItemIsRicher(candidate, current gjson.Result) bool {
	if !candidate.Exists() || !current.Exists() {
		return false
	}
	candidateStatus := strings.TrimSpace(candidate.Get("status").String())
	currentStatus := strings.TrimSpace(current.Get("status").String())
	if candidateStatus == "completed" && currentStatus != "completed" {
		return true
	}
	if candidate.Get("action.sources").Exists() && !current.Get("action.sources").Exists() {
		return true
	}
	if candidate.Get("action").Exists() && !current.Get("action").Exists() {
		return true
	}
	return len(candidate.Raw) > len(current.Raw) && currentStatus != "completed"
}

func backgroundResponseOutputItemKey(item gjson.Result) string {
	key := strings.TrimSpace(item.Get("id").String())
	if key == "" {
		key = item.Raw
	}
	return key
}

func backgroundResponseOutputItemCanUseAddedFallback(itemType string) bool {
	switch strings.TrimSpace(itemType) {
	case "web_search_call",
		"file_search_call",
		"image_generation_call",
		"code_interpreter_call",
		"computer_call",
		"mcp_call",
		"mcp_approval_request",
		"mcp_list_tools",
		"local_shell_call",
		"custom_tool_call",
		"compaction",
		"compaction_summary":
		return true
	default:
		return false
	}
}

func backgroundResponseMergeOutputItems(response []byte, collected []json.RawMessage) []byte {
	if len(collected) == 0 || !gjson.ValidBytes(response) {
		return response
	}
	merged := make([]json.RawMessage, 0, len(collected)+len(gjson.GetBytes(response, "output").Array()))
	seen := make(map[string]struct{}, len(collected))
	for _, raw := range collected {
		if len(raw) == 0 || !json.Valid(raw) {
			continue
		}
		item := gjson.ParseBytes(raw)
		key := backgroundResponseOutputItemKey(item)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		merged = append(merged, raw)
	}
	for _, item := range gjson.GetBytes(response, "output").Array() {
		if !item.IsObject() || item.Raw == "" {
			continue
		}
		key := backgroundResponseOutputItemKey(item)
		if _, ok := seen[key]; ok {
			backgroundResponseReplaceOutputItemIfRicher(item, key, &merged)
			continue
		}
		seen[key] = struct{}{}
		merged = append(merged, json.RawMessage(item.Raw))
	}
	if len(merged) == 0 {
		return response
	}
	encoded, err := json.Marshal(merged)
	if err != nil {
		return response
	}
	updated, err := sjson.SetRawBytes(response, "output", encoded)
	if err != nil {
		return response
	}
	return updated
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
		if len(outputItems) > 0 {
			updated = backgroundResponseMergeOutputItems(updated, outputItems)
		}
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
