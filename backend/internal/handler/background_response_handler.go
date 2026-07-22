package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
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
// connection, creates a native upstream Responses background task, and stores a
// proxy-id -> upstream-id mapping so workers poll with short GET requests rather
// than holding a 15 minute SSE/HTTP connection open.
type BackgroundResponseHandler struct {
	tasks          *service.BackgroundResponseTaskService
	openAI         *OpenAIGatewayHandler
	execute        func(c *gin.Context)
	fetchUpstream  func(ctx context.Context, task *service.BackgroundResponseTaskRecord) (*backgroundUpstreamPollResult, error)
	cancelUpstream func(ctx context.Context, task *service.BackgroundResponseTaskRecord) (*backgroundUpstreamPollResult, error)
	pollInterval   time.Duration
	heavyQueue     *backgroundResponseHeavyQueue
	running        sync.Map // response_id -> context.CancelFunc
}

type backgroundUpstreamPollResult struct {
	statusCode        int
	body              []byte
	upstreamRequestID string
	retryAfter        string
}

func NewBackgroundResponseHandler(tasks *service.BackgroundResponseTaskService, openAI *OpenAIGatewayHandler) *BackgroundResponseHandler {
	h := &BackgroundResponseHandler{tasks: tasks, openAI: openAI, heavyQueue: defaultBackgroundResponseHeavyQueue, pollInterval: 3 * time.Second}
	h.execute = h.executeWithGateway
	h.fetchUpstream = h.fetchNativeUpstreamBackgroundResponse
	h.cancelUpstream = h.cancelNativeUpstreamBackgroundResponse
	h.recoverActiveBackgroundTasksOnStartup()
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

func (h *BackgroundResponseHandler) recoverActiveBackgroundTasksOnStartup() {
	if h == nil || h.tasks == nil || !h.tasks.Enabled() {
		return
	}
	go func() {
		active, err := h.tasks.ListActive(context.Background(), 1000)
		fields := []zap.Field{zap.Int("active_tasks", len(active))}
		if err != nil {
			fields = append(fields, zap.Error(err))
			logger.L().Warn("background_response.startup_recovery_failed", fields...)
			return
		}
		resumed := 0
		failed := 0
		for _, task := range active {
			if task == nil {
				continue
			}
			if strings.TrimSpace(task.UpstreamResponseID) != "" && task.UpstreamAccountID > 0 {
				resumed++
				go h.resumePolling(task)
				continue
			}
			if err := h.tasks.Fail(context.Background(), task.ID, http.StatusServiceUnavailable, backgroundResponseAuditedErrorPayload(task, "background_task_failed", "background task did not survive proxy restart before upstream response id was stored", "")); err != nil {
				logger.L().Warn("background_response.startup_fail_unmapped_task_failed", zap.String("response_id", task.ID), zap.Error(err))
				continue
			}
			failed++
		}
		if resumed > 0 || failed > 0 {
			logger.L().Warn("background_response.startup_recovered_active_tasks",
				zap.Int("resumed_active_tasks", resumed),
				zap.Int("failed_unmapped_tasks", failed),
			)
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
			backgroundResponseJSONError(c, http.StatusTooManyRequests, "proxy_background_queue_full", "background heavy web search queue is full, please retry later")
			return
		}
	}

	// Preserve the official Responses background contract upstream. The previous
	// implementation removed background=true and drained a local SSE stream for
	// the whole model turn; production gateways cut those connections around the
	// 704-904s mark. Native background creation returns an upstream response id,
	// then workers poll via short GET requests.
	executionBody, err := normalizeBackgroundResponseNativeCreateBody(body)
	if err != nil {
		backgroundResponseJSONError(c, http.StatusBadRequest, "invalid_request_error", "Failed to normalize background request")
		return
	}
	streamingFallbackBody, err := normalizeBackgroundResponseStreamingFallbackBody(body, true)
	if err != nil {
		backgroundResponseJSONError(c, http.StatusBadRequest, "invalid_request_error", "Failed to normalize background request")
		return
	}
	owner := service.BackgroundResponseOwner{UserID: apiKey.UserID, APIKeyID: apiKey.ID}
	meta, upstreamIdempotencyKey, err := backgroundResponseTaskMetadata(c, owner, body)
	if err != nil {
		backgroundResponseError(c, err)
		return
	}
	task, created, err := h.tasks.CreateWithMetadata(c.Request.Context(), owner, model, meta)
	if err != nil {
		backgroundResponseError(c, err)
		return
	}
	taskCtx, recorder, cancel := newBackgroundResponseContext(c, executionBody, h.tasks.ExecutionTimeout())
	ensureBackgroundUpstreamIdempotencyKey(taskCtx.Request, upstreamIdempotencyKey, task.ID)
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
	backgroundResponseWriteStoredTask(c, task)

	if created && !backgroundResponseTaskTerminalForHandler(task.Status) {
		go h.run(task.ID, taskCtx, recorder, cancel, owner, opts, streamingFallbackBody)
		return
	}
	cancel()
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
	if current, getErr := h.tasks.Get(c.Request.Context(), service.BackgroundResponseOwner{UserID: apiKey.UserID, APIKeyID: apiKey.ID}, responseID); getErr == nil && current != nil && !backgroundResponseTaskTerminalForHandler(current.Status) {
		if h.cancelUpstream != nil && current.UpstreamAccountID > 0 && strings.TrimSpace(current.UpstreamResponseID) != "" {
			cancelCtx, cancelFn := context.WithTimeout(context.Background(), 10*time.Second)
			if result, err := h.cancelUpstream(cancelCtx, current); err != nil {
				logger.L().Warn("background_response.upstream_cancel_failed", zap.String("response_id", responseID), zap.String("upstream_response_id", current.UpstreamResponseID), zap.Error(err))
			} else if result != nil && result.upstreamRequestID != "" {
				_ = h.tasks.AttachUpstream(context.Background(), responseID, current.UpstreamAccountID, current.UpstreamResponseID, result.upstreamRequestID)
			}
			cancelFn()
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

func (h *BackgroundResponseHandler) run(taskID string, taskCtx *gin.Context, recorder *httptest.ResponseRecorder, cancel context.CancelFunc, owner service.BackgroundResponseOwner, opts backgroundResponseSubmitOptions, streamingFallbackBody []byte) {
	h.running.Store(taskID, cancel)
	defer h.running.Delete(taskID)
	defer cancel()
	defer func() {
		if recovered := recover(); recovered != nil {
			logger.L().Error("background_response.execution_panicked", zap.String("response_id", taskID), zap.Any("panic", recovered))
			h.failTaskWithAudit(taskID, http.StatusInternalServerError, "api_error", "background response panicked", "")
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
				h.failTaskWithAudit(taskID, http.StatusTooManyRequests, "proxy_background_queue_full", "background heavy web search queue is full", "")
				return
			}
			h.failTaskWithAudit(taskID, http.StatusTooManyRequests, "proxy_user_concurrency_timeout", "timeout waiting for heavy web search queue", "")
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

	createStatus, createBody, upstreamRequestID, createRetryAfter, accountID := h.executeNativeBackgroundCreate(taskCtx, recorder)
	if accountID > 0 || upstreamRequestID != "" {
		_ = h.tasks.AttachUpstream(context.Background(), taskID, accountID, "", upstreamRequestID)
	}
	if err := taskCtx.Request.Context().Err(); err != nil && len(createBody) == 0 {
		if errors.Is(err, context.Canceled) {
			h.cancelTask(taskID)
			return
		}
		h.failTaskWithAudit(taskID, http.StatusGatewayTimeout, "upstream_timeout", "background response create timed out", upstreamRequestID)
		return
	}

	if h.handleNativeBackgroundCreateResult(taskID, owner, createStatus, createBody, accountID, upstreamRequestID) {
		return
	}
	if backgroundResponseNativeBackgroundUnsupportedFromContext(taskCtx, createStatus, createBody) {
		logger.L().Warn("background_response.native_background_unsupported_fallback",
			zap.String("response_id", taskID),
			zap.Int("client_status", createStatus),
			zap.Int("upstream_status", backgroundResponseUpstreamStatusFromContext(taskCtx)),
			zap.String("upstream_message", backgroundResponseUpstreamMessageFromContext(taskCtx)),
		)
		if h.executeStreamingBackgroundFallback(taskID, owner, taskCtx, streamingFallbackBody, true) {
			return
		}
	}
	if createStatus >= http.StatusOK && createStatus < http.StatusMultipleChoices {
		if finalResponse, ok := backgroundResponseFinalResponseFromSSE(createBody, taskID); ok {
			if err := h.tasks.Complete(context.Background(), taskID, createStatus, finalResponse); err != nil {
				logger.L().Error("background_response.complete_store_failed", zap.String("response_id", taskID), zap.Error(err))
			}
			return
		}
		if taskErr, ok := backgroundResponseErrorFromSSE(createBody); ok {
			h.failTask(taskID, http.StatusBadGateway, taskErr)
			return
		}
	}

	// EOF/reset after response.created is recoverable: the upstream task already
	// exists, so do not convert the dropped transport into a terminal failure.
	if upstreamID := backgroundResponseUpstreamIDFromSSE(createBody); upstreamID != "" {
		if err := h.tasks.AttachUpstream(context.Background(), taskID, accountID, upstreamID, upstreamRequestID); err != nil {
			logger.L().Error("background_response.attach_upstream_from_sse_failed", zap.String("response_id", taskID), zap.String("upstream_response_id", upstreamID), zap.Error(err))
		}
		h.pollNativeBackground(taskID, owner)
		return
	}

	// If a transient transport EOF happened before any upstream id was observed,
	// retry exactly once using the same Idempotency-Key. This is the only safe
	// duplicate-create recovery path.
	if backgroundResponseCreateCanRetryWithoutResponseID(createStatus, createBody) {
		retryCtx, retryRecorder := cloneBackgroundResponseExecutionContext(taskCtx)
		retryStatus, retryBody, retryRequestID, retryRetryAfter, retryAccountID := h.executeNativeBackgroundCreate(retryCtx, retryRecorder)
		if h.handleNativeBackgroundCreateResult(taskID, owner, retryStatus, retryBody, retryAccountID, retryRequestID) {
			return
		}
		createStatus, createBody, upstreamRequestID, createRetryAfter, accountID = retryStatus, retryBody, retryRequestID, retryRetryAfter, retryAccountID
	}

	h.failTask(taskID, createStatus, backgroundResponseAttachRetryAfter(classifyBackgroundResponseFailureFromContext(taskCtx, createStatus, createBody, extractBackgroundResponseError(createBody)), createRetryAfter))
}

func (h *BackgroundResponseHandler) failTask(taskID string, statusCode int, taskErr json.RawMessage) {
	if h != nil && h.tasks != nil {
		if task, err := h.tasks.GetByID(context.Background(), taskID); err == nil {
			taskErr = backgroundResponseAttachAuditFields(task, taskErr, "")
		}
	}
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

func (h *BackgroundResponseHandler) executeNativeBackgroundCreate(c *gin.Context, recorder *httptest.ResponseRecorder) (int, []byte, string, string, int64) {
	if recorder == nil {
		recorder = httptest.NewRecorder()
	}
	h.execute(c)
	body := bytes.TrimSpace(recorder.Body.Bytes())
	statusCode := recorder.Code
	if statusCode == 0 {
		statusCode = http.StatusOK
	}
	upstreamRequestID := strings.TrimSpace(recorder.Header().Get("x-request-id"))
	accountID, _ := c.Request.Context().Value(ctxkey.AccountID).(int64)
	return statusCode, body, upstreamRequestID, strings.TrimSpace(recorder.Header().Get("Retry-After")), accountID
}

func (h *BackgroundResponseHandler) handleNativeBackgroundCreateResult(taskID string, owner service.BackgroundResponseOwner, statusCode int, body []byte, accountID int64, upstreamRequestID string) bool {
	if statusCode < http.StatusOK || statusCode >= http.StatusMultipleChoices || len(body) == 0 || !json.Valid(body) {
		return false
	}
	upstreamID := strings.TrimSpace(gjson.GetBytes(body, "id").String())
	if upstreamID == "" {
		upstreamID = strings.TrimSpace(gjson.GetBytes(body, "response.id").String())
	}
	if err := h.tasks.AttachUpstream(context.Background(), taskID, accountID, upstreamID, upstreamRequestID); err != nil {
		logger.L().Error("background_response.attach_upstream_failed", zap.String("response_id", taskID), zap.String("upstream_response_id", upstreamID), zap.Error(err))
	}
	switch strings.ToLower(strings.TrimSpace(gjson.GetBytes(body, "status").String())) {
	case service.BackgroundResponseStatusCompleted:
		h.completeTaskWithNativeResult(taskID, statusCode, body)
		return true
	case service.BackgroundResponseStatusFailed:
		h.failTaskWithAudit(taskID, statusCode, "upstream_background_failed", backgroundResponseErrorMessage(extractBackgroundResponseError(body), "upstream background response failed"), upstreamRequestID)
		return true
	case service.BackgroundResponseStatusCancelled, "canceled":
		h.cancelTask(taskID)
		return true
	case service.BackgroundResponseStatusQueued, service.BackgroundResponseStatusInProgress:
		if upstreamID == "" {
			return false
		}
		h.pollNativeBackground(taskID, owner)
		return true
	default:
		if upstreamID != "" {
			h.pollNativeBackground(taskID, owner)
			return true
		}
		// Some compat upstreams can still return a completed-looking Response with
		// no explicit status. Treat an output-bearing JSON response as terminal.
		if gjson.GetBytes(body, "output").Exists() {
			h.completeTaskWithNativeResult(taskID, statusCode, body)
			return true
		}
		return false
	}
}

func (h *BackgroundResponseHandler) executeStreamingBackgroundFallback(taskID string, owner service.BackgroundResponseOwner, baseCtx *gin.Context, body []byte, allowStoreFalseRetry bool) bool {
	if len(body) == 0 || !json.Valid(body) {
		return false
	}
	fallbackCtx, fallbackRecorder := cloneBackgroundResponseExecutionContextWithBody(baseCtx, body)
	statusCode, responseBody, upstreamRequestID, _, accountID := h.executeNativeBackgroundCreate(fallbackCtx, fallbackRecorder)
	if accountID > 0 || upstreamRequestID != "" {
		_ = h.tasks.AttachUpstream(context.Background(), taskID, accountID, "", upstreamRequestID)
	}
	if statusCode >= http.StatusOK && statusCode < http.StatusMultipleChoices {
		if finalResponse, ok := backgroundResponseFinalResponseFromSSE(responseBody, taskID); ok {
			if err := h.tasks.Complete(context.Background(), taskID, statusCode, finalResponse); err != nil {
				logger.L().Error("background_response.complete_store_failed", zap.String("response_id", taskID), zap.Error(err))
			}
			return true
		}
		if taskErr, ok := backgroundResponseErrorFromSSE(responseBody); ok {
			h.failTask(taskID, http.StatusBadGateway, taskErr)
			return true
		}
	}
	if upstreamID := backgroundResponseUpstreamIDFromSSE(responseBody); upstreamID != "" {
		if err := h.tasks.AttachUpstream(context.Background(), taskID, accountID, upstreamID, upstreamRequestID); err != nil {
			logger.L().Error("background_response.attach_upstream_from_stream_fallback_failed", zap.String("response_id", taskID), zap.String("upstream_response_id", upstreamID), zap.Error(err))
		}
		h.pollNativeBackground(taskID, owner)
		return true
	}
	if allowStoreFalseRetry && backgroundResponseStreamingStoreUnsupportedFromContext(fallbackCtx, statusCode, responseBody) {
		if original, err := normalizeBackgroundResponseStreamingFallbackBody(body, false); err == nil {
			logger.L().Warn("background_response.streaming_fallback_store_true_unsupported", zap.String("response_id", taskID))
			return h.executeStreamingBackgroundFallback(taskID, owner, baseCtx, original, false)
		}
	}
	h.failTask(taskID, statusCode, classifyBackgroundResponseFailureFromContext(fallbackCtx, statusCode, responseBody, extractBackgroundResponseError(responseBody)))
	return true
}

func (h *BackgroundResponseHandler) pollNativeBackground(taskID string, owner service.BackgroundResponseOwner) {
	if h == nil || h.tasks == nil {
		return
	}
	timeout := h.tasks.ExecutionTimeout()
	if timeout < 30*time.Minute {
		timeout = 30 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		if h.isTaskCancelled(taskID, owner) {
			return
		}
		task, err := h.tasks.Get(context.Background(), owner, taskID)
		if err != nil {
			logger.L().Warn("background_response.poll_load_task_failed", zap.String("response_id", taskID), zap.Error(err))
			return
		}
		if backgroundResponseTaskTerminalForHandler(task.Status) {
			return
		}
		result, err := h.fetchNativeBackground(ctx, task)
		if err != nil {
			if ctx.Err() != nil {
				h.failTaskWithAudit(taskID, http.StatusGatewayTimeout, "proxy_task_deadline_exceeded", "background task deadline exceeded", task.UpstreamRequestID)
				return
			}
			classification := classifyBackgroundTransportError(err)
			logger.L().Warn("background_response.poll_upstream_error",
				zap.String("response_id", taskID),
				zap.String("upstream_response_id", task.UpstreamResponseID),
				zap.String("error_type", classification),
				zap.Error(err),
			)
			if !backgroundResponseWaitForNextPoll(ctx, h.pollInterval, "") {
				h.failTaskWithAudit(taskID, http.StatusGatewayTimeout, "proxy_task_deadline_exceeded", "background task deadline exceeded", task.UpstreamRequestID)
				return
			}
			continue
		}
		if result == nil {
			if !backgroundResponseWaitForNextPoll(ctx, h.pollInterval, "") {
				h.failTaskWithAudit(taskID, http.StatusGatewayTimeout, "proxy_task_deadline_exceeded", "background task deadline exceeded", task.UpstreamRequestID)
				return
			}
			continue
		}
		if result.upstreamRequestID != "" {
			_ = h.tasks.AttachUpstream(context.Background(), taskID, task.UpstreamAccountID, task.UpstreamResponseID, result.upstreamRequestID)
		}
		if result.statusCode >= http.StatusOK && result.statusCode < http.StatusMultipleChoices && json.Valid(result.body) {
			switch strings.ToLower(strings.TrimSpace(gjson.GetBytes(result.body, "status").String())) {
			case service.BackgroundResponseStatusCompleted:
				h.completeTaskWithNativeResult(taskID, result.statusCode, result.body)
				return
			case service.BackgroundResponseStatusFailed:
				h.failTaskWithAudit(taskID, result.statusCode, "upstream_background_failed", backgroundResponseErrorMessage(extractBackgroundResponseError(result.body), "upstream background response failed"), result.upstreamRequestID)
				return
			case service.BackgroundResponseStatusCancelled, "canceled":
				h.cancelTask(taskID)
				return
			case service.BackgroundResponseStatusQueued, service.BackgroundResponseStatusInProgress, "":
				if !backgroundResponseWaitForNextPoll(ctx, h.pollInterval, result.retryAfter) {
					h.failTaskWithAudit(taskID, http.StatusGatewayTimeout, "proxy_task_deadline_exceeded", "background task deadline exceeded", result.upstreamRequestID)
					return
				}
				continue
			default:
				h.failTaskWithAudit(taskID, http.StatusBadGateway, "upstream_background_failed", "upstream returned an unknown background status", result.upstreamRequestID)
				return
			}
		}
		if result.statusCode == http.StatusTooManyRequests || backgroundResponseIsTransientPollFailure(result.statusCode, result.body) {
			logger.L().Warn("background_response.poll_upstream_transient_status",
				zap.String("response_id", taskID),
				zap.String("upstream_response_id", task.UpstreamResponseID),
				zap.Int("upstream_status", result.statusCode),
				zap.String("retry_after", result.retryAfter),
				zap.String("error_type", strings.TrimSpace(gjson.GetBytes(classifyBackgroundResponseFailure(result.statusCode, result.body, extractBackgroundResponseError(result.body)), "type").String())),
			)
			if !backgroundResponseWaitForNextPoll(ctx, h.pollInterval, result.retryAfter) {
				h.failTaskWithAudit(taskID, http.StatusGatewayTimeout, "proxy_task_deadline_exceeded", "background task deadline exceeded", result.upstreamRequestID)
				return
			}
			continue
		}
		h.failTask(taskID, result.statusCode, classifyBackgroundResponseFailure(result.statusCode, result.body, extractBackgroundResponseError(result.body)))
		return
	}
}

func (h *BackgroundResponseHandler) fetchNativeBackground(ctx context.Context, task *service.BackgroundResponseTaskRecord) (*backgroundUpstreamPollResult, error) {
	if h != nil && h.fetchUpstream != nil {
		return h.fetchUpstream(ctx, task)
	}
	return h.fetchNativeUpstreamBackgroundResponse(ctx, task)
}

func (h *BackgroundResponseHandler) fetchNativeUpstreamBackgroundResponse(ctx context.Context, task *service.BackgroundResponseTaskRecord) (*backgroundUpstreamPollResult, error) {
	if h == nil || h.openAI == nil || h.openAI.gatewayService == nil || task == nil {
		return nil, errors.New("background upstream poller unavailable")
	}
	result, err := h.openAI.gatewayService.FetchOpenAIBackgroundResponse(ctx, task.UpstreamAccountID, task.UpstreamResponseID)
	if err != nil {
		return nil, err
	}
	return &backgroundUpstreamPollResult{statusCode: result.StatusCode, body: result.Body, upstreamRequestID: result.UpstreamRequestID, retryAfter: result.RetryAfter}, nil
}

func (h *BackgroundResponseHandler) cancelNativeUpstreamBackgroundResponse(ctx context.Context, task *service.BackgroundResponseTaskRecord) (*backgroundUpstreamPollResult, error) {
	if h == nil || h.openAI == nil || h.openAI.gatewayService == nil || task == nil || task.UpstreamAccountID <= 0 || strings.TrimSpace(task.UpstreamResponseID) == "" {
		return nil, nil
	}
	result, err := h.openAI.gatewayService.CancelOpenAIBackgroundResponse(ctx, task.UpstreamAccountID, task.UpstreamResponseID)
	if err != nil {
		return nil, err
	}
	return &backgroundUpstreamPollResult{statusCode: result.StatusCode, body: result.Body, upstreamRequestID: result.UpstreamRequestID, retryAfter: result.RetryAfter}, nil
}

func (h *BackgroundResponseHandler) resumePolling(task *service.BackgroundResponseTaskRecord) {
	if h == nil || task == nil {
		return
	}
	owner := service.BackgroundResponseOwner{UserID: task.UserID, APIKeyID: task.APIKeyID}
	_, cancel := context.WithCancel(context.Background())
	h.running.Store(task.ID, cancel)
	defer h.running.Delete(task.ID)
	defer cancel()
	h.pollNativeBackground(task.ID, owner)
}

func (h *BackgroundResponseHandler) completeTaskWithNativeResult(taskID string, statusCode int, body []byte) {
	task, _ := h.tasks.GetByID(context.Background(), taskID)
	result := backgroundResponseNormalizeNativeResult(body, taskID, "")
	if task != nil {
		result = backgroundResponseNormalizeNativeResult(body, taskID, task.Model)
	}
	if err := h.tasks.Complete(context.Background(), taskID, statusCode, json.RawMessage(result)); err != nil {
		logger.L().Error("background_response.complete_store_failed", zap.String("response_id", taskID), zap.Error(err))
	}
}

func (h *BackgroundResponseHandler) failTaskWithAudit(taskID string, statusCode int, errorType, message, upstreamRequestID string) {
	task, _ := h.tasks.GetByID(context.Background(), taskID)
	h.failTask(taskID, statusCode, backgroundResponseAuditedErrorPayload(task, errorType, message, upstreamRequestID))
}

func cloneBackgroundResponseExecutionContext(c *gin.Context) (*gin.Context, *httptest.ResponseRecorder) {
	recorder := httptest.NewRecorder()
	recorderCtx, _ := gin.CreateTestContext(recorder)
	clone := c.Copy()
	clone.Writer = recorderCtx.Writer
	request := c.Request.Clone(c.Request.Context())
	if c.Request.GetBody != nil {
		if body, err := c.Request.GetBody(); err == nil {
			request.Body = body
		}
	}
	clone.Request = request
	return clone, recorder
}

func cloneBackgroundResponseExecutionContextWithBody(c *gin.Context, body []byte) (*gin.Context, *httptest.ResponseRecorder) {
	clone, recorder := cloneBackgroundResponseExecutionContext(c)
	restoreBackgroundResponseRequestBody(clone.Request, body)
	return clone, recorder
}

func normalizeBackgroundResponseNativeCreateBody(body []byte) ([]byte, error) {
	var err error
	body, err = sjson.SetBytes(body, "background", true)
	if err != nil {
		return nil, err
	}
	body, err = sjson.SetBytes(body, "store", true)
	if err != nil {
		return nil, err
	}
	body, err = sjson.SetBytes(body, "stream", false)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func normalizeBackgroundResponseStreamingFallbackBody(body []byte, store bool) ([]byte, error) {
	var err error
	body, err = sjson.DeleteBytes(body, "background")
	if err != nil {
		return nil, err
	}
	body, err = sjson.SetBytes(body, "stream", true)
	if err != nil {
		return nil, err
	}
	body, err = sjson.SetBytes(body, "store", store)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func backgroundResponseNativeBackgroundUnsupported(statusCode int, body []byte) bool {
	if statusCode != http.StatusBadRequest {
		return false
	}
	return backgroundResponseTextHasUnsupportedParameter(string(body), "background")
}

func backgroundResponseNativeBackgroundUnsupportedFromContext(c *gin.Context, statusCode int, body []byte) bool {
	if backgroundResponseNativeBackgroundUnsupported(statusCode, body) {
		return true
	}
	upstreamStatus := backgroundResponseUpstreamStatusFromContext(c)
	if upstreamStatus != http.StatusBadRequest {
		return false
	}
	return backgroundResponseTextHasUnsupportedParameter(backgroundResponseUpstreamErrorTextFromContext(c, body), "background")
}

func backgroundResponseStreamingStoreUnsupported(statusCode int, body []byte) bool {
	if statusCode != http.StatusBadRequest {
		return false
	}
	return backgroundResponseTextHasUnsupportedParameter(string(body), "store")
}

func backgroundResponseStreamingStoreUnsupportedFromContext(c *gin.Context, statusCode int, body []byte) bool {
	if backgroundResponseStreamingStoreUnsupported(statusCode, body) {
		return true
	}
	upstreamStatus := backgroundResponseUpstreamStatusFromContext(c)
	if upstreamStatus != http.StatusBadRequest {
		return false
	}
	return backgroundResponseTextHasUnsupportedParameter(backgroundResponseUpstreamErrorTextFromContext(c, body), "store")
}

func backgroundResponseTextHasUnsupportedParameter(text, param string) bool {
	text = strings.ToLower(text)
	param = strings.ToLower(strings.TrimSpace(param))
	if text == "" || param == "" || !strings.Contains(text, param) {
		return false
	}
	return strings.Contains(text, "unsupported parameter") ||
		strings.Contains(text, "unknown parameter") ||
		strings.Contains(text, "unrecognized parameter") ||
		strings.Contains(text, "unsupported request parameter")
}

func backgroundResponseUpstreamStatusFromContext(c *gin.Context) int {
	if c == nil {
		return 0
	}
	raw, ok := c.Get(service.OpsUpstreamStatusCodeKey)
	if !ok {
		return 0
	}
	switch v := raw.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	case string:
		n, _ := strconv.Atoi(strings.TrimSpace(v))
		return n
	default:
		return 0
	}
}

func backgroundResponseUpstreamMessageFromContext(c *gin.Context) string {
	if c == nil {
		return ""
	}
	raw, ok := c.Get(service.OpsUpstreamErrorMessageKey)
	if !ok {
		return ""
	}
	msg, _ := raw.(string)
	return strings.TrimSpace(msg)
}

func backgroundResponseUpstreamDetailFromContext(c *gin.Context) string {
	if c == nil {
		return ""
	}
	raw, ok := c.Get(service.OpsUpstreamErrorDetailKey)
	if !ok {
		return ""
	}
	detail, _ := raw.(string)
	return strings.TrimSpace(detail)
}

func backgroundResponseUpstreamErrorTextFromContext(c *gin.Context, body []byte) string {
	parts := []string{string(body), backgroundResponseUpstreamMessageFromContext(c), backgroundResponseUpstreamDetailFromContext(c)}
	return strings.Join(parts, "\n")
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

func backgroundResponseTaskMetadata(c *gin.Context, owner service.BackgroundResponseOwner, body []byte) (service.BackgroundResponseTaskMetadata, string, error) {
	var meta service.BackgroundResponseTaskMetadata
	fingerprint, err := service.BuildIdempotencyFingerprint("POST", "/v1/responses", fmt.Sprintf("%d:%d", owner.UserID, owner.APIKeyID), json.RawMessage(body))
	if err == nil {
		meta.RequestFingerprint = fingerprint
	}
	rawKey := ""
	if c != nil && c.Request != nil {
		rawKey = c.GetHeader("Idempotency-Key")
		if rawKey == "" {
			rawKey = c.GetHeader("X-Idempotency-Key")
		}
	}
	key, keyErr := service.NormalizeIdempotencyKey(rawKey)
	if keyErr != nil {
		return meta, "", keyErr
	}
	if key != "" {
		meta.IdempotencyKeyHash = service.HashIdempotencyKey(key)
	}
	return meta, key, err
}

func ensureBackgroundUpstreamIdempotencyKey(request *http.Request, inboundKey, taskID string) {
	if request == nil {
		return
	}
	key := strings.TrimSpace(inboundKey)
	if key == "" {
		key = "subtoproxy-bg-" + strings.TrimSpace(taskID)
	}
	if key == "" {
		return
	}
	request.Header.Set("Idempotency-Key", key)
}

func backgroundResponseWriteStoredTask(c *gin.Context, task *service.BackgroundResponseTaskRecord) {
	if c == nil || task == nil {
		return
	}
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
	if task.Status == service.BackgroundResponseStatusCancelled {
		c.JSON(http.StatusOK, backgroundResponseCancelledPayload(task))
		return
	}
	c.JSON(http.StatusOK, backgroundResponsePendingPayload(task))
}

func backgroundResponseTaskTerminalForHandler(status string) bool {
	switch status {
	case service.BackgroundResponseStatusCompleted, service.BackgroundResponseStatusFailed, service.BackgroundResponseStatusCancelled:
		return true
	default:
		return false
	}
}

func backgroundResponseNormalizeNativeResult(body []byte, proxyID, model string) []byte {
	if !json.Valid(body) {
		return body
	}
	updated := body
	if strings.TrimSpace(proxyID) != "" {
		if next, err := sjson.SetBytes(updated, "id", strings.TrimSpace(proxyID)); err == nil {
			updated = next
		}
	}
	if strings.TrimSpace(model) != "" {
		if next, err := sjson.SetBytes(updated, "model", strings.TrimSpace(model)); err == nil {
			updated = next
		}
	}
	if next, err := sjson.SetBytes(updated, "background", true); err == nil {
		updated = next
	}
	return updated
}

func backgroundResponseWaitForNextPoll(ctx context.Context, fallback time.Duration, retryAfter string) bool {
	wait := backgroundResponseRetryAfterDuration(retryAfter)
	if wait <= 0 {
		wait = fallback
	}
	if wait <= 0 {
		wait = 3 * time.Second
	}
	if wait > 30*time.Second {
		wait = 30 * time.Second
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func backgroundResponseRetryAfterDuration(value string) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil {
		return time.Until(when)
	}
	return 0
}

func backgroundResponseCreateCanRetryWithoutResponseID(statusCode int, body []byte) bool {
	if statusCode == http.StatusTooManyRequests {
		return false
	}
	return backgroundResponseIsTransientPollFailure(statusCode, body)
}

func backgroundResponseIsTransientPollFailure(statusCode int, body []byte) bool {
	bodyText := strings.ToLower(string(body))
	return statusCode == http.StatusBadGateway ||
		statusCode == http.StatusServiceUnavailable ||
		statusCode == http.StatusGatewayTimeout ||
		strings.Contains(bodyText, "unexpected eof") ||
		strings.Contains(bodyText, "connection reset") ||
		strings.Contains(bodyText, "timeout")
}

func classifyBackgroundTransportError(err error) string {
	if err == nil {
		return ""
	}
	text := strings.ToLower(err.Error())
	switch {
	case strings.Contains(text, "unexpected eof"):
		return "upstream_unexpected_eof"
	case strings.Contains(text, "connection reset") || strings.Contains(text, "reset by peer"):
		return "upstream_connection_reset"
	case strings.Contains(text, "timeout") || errors.Is(err, context.DeadlineExceeded):
		return "upstream_timeout"
	default:
		return "upstream_error"
	}
}

func backgroundResponseUpstreamIDFromSSE(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	upstreamID := ""
	backgroundResponseForEachSSEDataPayload(body, func(data []byte) {
		if upstreamID != "" || len(data) == 0 || !gjson.ValidBytes(data) {
			return
		}
		switch strings.TrimSpace(gjson.GetBytes(data, "type").String()) {
		case "response.created", "response.queued", "response.in_progress", "response.completed", "response.done":
			upstreamID = strings.TrimSpace(gjson.GetBytes(data, "response.id").String())
		}
		if upstreamID == "" {
			upstreamID = strings.TrimSpace(gjson.GetBytes(data, "id").String())
		}
	})
	return upstreamID
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
	case strings.Contains(bodyText, "connection reset") || strings.Contains(errText, "connection reset") || strings.Contains(bodyText, "reset by peer") || strings.Contains(errText, "reset by peer"):
		return backgroundResponseErrorPayload("upstream_connection_reset", backgroundResponseErrorMessage(taskErr, "upstream connection reset"))
	case strings.Contains(bodyText, "unexpected eof") || strings.Contains(errText, "unexpected eof"):
		return backgroundResponseErrorPayload("upstream_unexpected_eof", backgroundResponseErrorMessage(taskErr, "upstream connection closed unexpectedly"))
	case strings.Contains(bodyText, "deadline exceeded") || strings.Contains(errText, "deadline exceeded"):
		return backgroundResponseErrorPayload("proxy_task_deadline_exceeded", backgroundResponseErrorMessage(taskErr, "background task deadline exceeded"))
	case statusCode == http.StatusGatewayTimeout || strings.Contains(errText, "timeout"):
		return backgroundResponseErrorPayload("upstream_timeout", backgroundResponseErrorMessage(taskErr, "upstream request timed out"))
	default:
		return taskErr
	}
}

func classifyBackgroundResponseFailureFromContext(c *gin.Context, statusCode int, body []byte, taskErr json.RawMessage) json.RawMessage {
	upstreamStatus := backgroundResponseUpstreamStatusFromContext(c)
	upstreamText := backgroundResponseUpstreamErrorTextFromContext(c, body)
	upstreamMessage := backgroundResponseUpstreamMessageFromContext(c)
	if upstreamMessage == "" {
		upstreamMessage = backgroundResponseErrorMessage(taskErr, "upstream request failed")
	}
	syntheticErr := taskErr
	if upstreamMessage != "" {
		syntheticErr = backgroundResponseErrorPayload("upstream_error", upstreamMessage)
	}
	switch {
	case upstreamStatus == http.StatusTooManyRequests:
		return backgroundResponseErrorPayload("upstream_rate_limited", upstreamMessage)
	case strings.Contains(strings.ToLower(upstreamText), "connection reset") || strings.Contains(strings.ToLower(upstreamText), "reset by peer"):
		return backgroundResponseErrorPayload("upstream_connection_reset", upstreamMessage)
	case strings.Contains(strings.ToLower(upstreamText), "unexpected eof"):
		return backgroundResponseErrorPayload("upstream_unexpected_eof", upstreamMessage)
	case upstreamStatus == http.StatusGatewayTimeout || strings.Contains(strings.ToLower(upstreamText), "timeout"):
		return backgroundResponseErrorPayload("upstream_timeout", upstreamMessage)
	case upstreamStatus > 0 && statusCode >= http.StatusBadGateway:
		return backgroundResponseErrorPayload("upstream_background_failed", upstreamMessage)
	default:
		return classifyBackgroundResponseFailure(statusCode, body, syntheticErr)
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
	data, _ := json.Marshal(gin.H{"type": errorType, "code": errorType, "message": message})
	return data
}

func backgroundResponseAuditedErrorPayload(task *service.BackgroundResponseTaskRecord, errorType, message, upstreamRequestID string) json.RawMessage {
	payload := backgroundResponseErrorPayload(errorType, message)
	return backgroundResponseAttachAuditFields(task, payload, upstreamRequestID)
}

func backgroundResponseAttachAuditFields(task *service.BackgroundResponseTaskRecord, payload json.RawMessage, upstreamRequestID string) json.RawMessage {
	if !json.Valid(payload) {
		return payload
	}
	now := time.Now().UTC().Unix()
	updated := payload
	updated, _ = sjson.SetBytes(updated, "failed_at", now)
	if task != nil {
		if task.CreatedAt > 0 && now >= task.CreatedAt {
			updated, _ = sjson.SetBytes(updated, "elapsed_seconds", now-task.CreatedAt)
		}
		if task.UpstreamResponseID != "" {
			updated, _ = sjson.SetBytes(updated, "upstream_response_id", task.UpstreamResponseID)
		}
		if upstreamRequestID == "" {
			upstreamRequestID = task.UpstreamRequestID
		}
	}
	if strings.TrimSpace(upstreamRequestID) != "" {
		updated, _ = sjson.SetBytes(updated, "upstream_request_id", strings.TrimSpace(upstreamRequestID))
	}
	return updated
}

func backgroundResponseAttachRetryAfter(payload json.RawMessage, retryAfter string) json.RawMessage {
	retryAfter = strings.TrimSpace(retryAfter)
	if retryAfter == "" || !json.Valid(payload) {
		return payload
	}
	updated, err := sjson.SetBytes(payload, "retry_after", retryAfter)
	if err != nil {
		return payload
	}
	return updated
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
