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
	"regexp"
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
	"github.com/Wei-Shaw/sub2api/internal/util/logredact"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
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
	notFoundGrace  time.Duration
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
	h := &BackgroundResponseHandler{tasks: tasks, openAI: openAI, heavyQueue: defaultBackgroundResponseHeavyQueue, pollInterval: 3 * time.Second, notFoundGrace: 2 * time.Minute}
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
	// A native background create is a non-idempotent operation unless every
	// attempt stays inside the exact same upstream identity scope. OpenAI
	// idempotency keys are not portable across accounts/projects, so the generic
	// Responses failover loop must never replay this POST on another account.
	responsesDisableCrossAccountFailoverKey = "subtoproxy_responses_disable_cross_account_failover"

	backgroundResponseWorkerLeaseTTL           = 30 * time.Second
	backgroundResponseWorkerLeaseRenewInterval = 10 * time.Second
	backgroundResponseWorkerLeaseIOTimeout     = 5 * time.Second
	backgroundResponseWorkerLeaseWaitMax       = time.Second
	backgroundResponseNativeCreateTimeout      = 60 * time.Second
	backgroundResponseRecoveryInterval         = 15 * time.Second
	backgroundResponseRecoveryBatchSize        = 10000
	backgroundResponseUnmappedCreateGrace      = 2 * time.Minute
	backgroundResponseUsageRecoveryBatchSize   = 100
	backgroundResponseUsageRecoveryConcurrency = 4
	backgroundResponseUsageSettlementTimeout   = 30 * time.Second
	backgroundResponseUsageLeaseTTL            = 90 * time.Second
)

var defaultBackgroundResponseHeavyQueue = newBackgroundResponseHeavyQueue()

var errBackgroundHeavyQueueFull = errors.New("heavy web search queue full")

var backgroundResponseBearerCredentialPattern = regexp.MustCompile(`(?i)\bBearer\s+[A-Za-z0-9._~+/=-]+`)

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
		h.reconcileActiveBackgroundTasks()
		ticker := time.NewTicker(backgroundResponseRecoveryInterval)
		defer ticker.Stop()
		for range ticker.C {
			h.reconcileActiveBackgroundTasks()
		}
	}()
	// Usage settlement may spend up to 30 seconds per task (bounded to four
	// workers). Keep it on an independent loop so a billing backlog never delays
	// active-task takeover, deadline enforcement, or orphan recovery.
	go func() {
		h.reconcileBackgroundResponseUsage()
		ticker := time.NewTicker(backgroundResponseRecoveryInterval)
		defer ticker.Stop()
		for range ticker.C {
			h.reconcileBackgroundResponseUsage()
		}
	}()
}

func (h *BackgroundResponseHandler) reconcileActiveBackgroundTasks() {
	active, err := h.tasks.ListActive(context.Background(), backgroundResponseRecoveryBatchSize)
	if err != nil {
		logger.L().Warn("background_response.recovery_scan_failed", zap.Error(err))
		return
	}
	resumed := 0
	failed := 0
	for _, task := range active {
		if task == nil {
			continue
		}
		if _, alreadyRunning := h.running.Load(task.ID); alreadyRunning {
			continue
		}
		mode := strings.TrimSpace(task.UpstreamExecutionMode)
		upstreamID := strings.TrimSpace(task.UpstreamResponseID)
		if upstreamID != "" &&
			task.UpstreamAccountID > 0 &&
			mode == service.BackgroundResponseExecutionModeNative &&
			task.UpstreamPollable &&
			task.UpstreamBinding != nil {
			resumed++
			go h.resumePolling(task)
			continue
		}
		if upstreamID == "" && !h.tasks.ExecutionDeadlineExceeded(task, time.Now().UTC()) {
			createdAt := time.Unix(task.CreatedAt, 0)
			if task.CreatedAt > 0 && time.Since(createdAt) < backgroundResponseUnmappedCreateGrace {
				// Another instance may still be completing the bounded native
				// create handshake. Do not race it by failing a fresh record.
				continue
			}
		}
		errorType := "proxy_mapping_missing"
		message := "native background response mapping is incomplete after proxy restart"
		if h.tasks.ExecutionDeadlineExceeded(task, time.Now().UTC()) {
			errorType = "proxy_task_expired"
			message = "background task exceeded its persisted execution deadline"
		} else if upstreamID != "" && (mode != service.BackgroundResponseExecutionModeNative || !task.UpstreamPollable) {
			errorType = "legacy_non_pollable"
			message = "legacy streaming fallback response id is not pollable and will not be queried as a native background response"
		}
		if err := h.tasks.Fail(context.Background(), task.ID, http.StatusServiceUnavailable, backgroundResponseAuditedErrorPayload(task, errorType, message, "")); err != nil {
			logger.L().Warn("background_response.recovery_fail_unmapped_task_failed", zap.String("response_id", task.ID), zap.Error(err))
			continue
		}
		failed++
	}
	if resumed > 0 || failed > 0 {
		logger.L().Warn("background_response.reconciled_active_tasks",
			zap.Int("active_tasks", len(active)),
			zap.Int("resumed_active_tasks", resumed),
			zap.Int("failed_unmapped_tasks", failed),
		)
	}
}

func (h *BackgroundResponseHandler) reconcileBackgroundResponseUsage() {
	if !h.canSettleBackgroundResponseUsage() {
		return
	}
	pending, err := h.tasks.ListUsagePending(context.Background(), backgroundResponseUsageRecoveryBatchSize)
	if err != nil {
		logger.L().Warn("background_response.usage_recovery_scan_failed", zap.Error(err))
		return
	}
	if len(pending) == 0 {
		return
	}

	sem := make(chan struct{}, backgroundResponseUsageRecoveryConcurrency)
	var wg sync.WaitGroup
	for _, task := range pending {
		if task == nil {
			continue
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(candidate *service.BackgroundResponseTaskRecord) {
			defer wg.Done()
			defer func() { <-sem }()
			h.settleBackgroundResponseUsage(candidate, nil)
		}(task)
	}
	wg.Wait()
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

type backgroundResponseCreateOutcome struct {
	accepted   bool
	statusCode int
	errorType  string
	message    string
	retryAfter string
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
	if created {
		if err := h.tasks.SetExecutionMode(c.Request.Context(), task.ID, service.BackgroundResponseExecutionModeNative, true); err != nil {
			h.failTaskWithAudit(task.ID, http.StatusInternalServerError, "proxy_mapping_missing", "failed to persist native background execution mode", "")
			backgroundResponseError(c, err)
			return
		}
	}
	if !created || backgroundResponseTaskTerminalForHandler(task.Status) {
		if backgroundResponseTaskIsCreateRejection(task) {
			backgroundResponseWriteCreateRejection(c, task, backgroundResponseCreateOutcome{statusCode: task.HTTPStatus})
			return
		}
		h.writeAcceptedBackgroundTask(c, task, opts)
		return
	}

	createTimeout := h.tasks.ExecutionTimeout()
	if createTimeout <= 0 || createTimeout > backgroundResponseNativeCreateTimeout {
		createTimeout = backgroundResponseNativeCreateTimeout
	}
	taskCtx, recorder, cancel := newBackgroundResponseContext(c, executionBody, createTimeout)
	ensureBackgroundUpstreamIdempotencyKey(taskCtx.Request, upstreamIdempotencyKey, task.ID)
	taskCtx.Set(responsesInternalBackgroundExecuteKey, true)
	outcomeCh := make(chan backgroundResponseCreateOutcome, 1)
	go h.run(task.ID, taskCtx, recorder, cancel, owner, opts, outcomeCh)

	var outcome backgroundResponseCreateOutcome
	select {
	case outcome = <-outcomeCh:
	case <-c.Request.Context().Done():
		return
	}
	current, getErr := h.tasks.Get(context.Background(), owner, task.ID)
	if getErr == nil && current != nil {
		task = current
	}
	if !outcome.accepted {
		backgroundResponseWriteCreateRejection(c, task, outcome)
		return
	}
	h.writeAcceptedBackgroundTask(c, task, opts)
}

func (h *BackgroundResponseHandler) writeAcceptedBackgroundTask(c *gin.Context, task *service.BackgroundResponseTaskRecord, opts backgroundResponseSubmitOptions) {
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

func (h *BackgroundResponseHandler) run(taskID string, taskCtx *gin.Context, recorder *httptest.ResponseRecorder, cancel context.CancelFunc, owner service.BackgroundResponseOwner, opts backgroundResponseSubmitOptions, outcomeCh chan<- backgroundResponseCreateOutcome) {
	h.running.Store(taskID, cancel)
	defer h.running.Delete(taskID)
	defer cancel()
	outcomeSent := false
	sendOutcome := func(outcome backgroundResponseCreateOutcome) {
		if outcomeSent {
			return
		}
		outcomeSent = true
		if outcome.statusCode == 0 {
			outcome.statusCode = http.StatusOK
		}
		if outcomeCh != nil {
			outcomeCh <- outcome
		}
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			logger.L().Error("background_response.execution_panicked", zap.String("response_id", taskID), zap.Any("panic", recovered))
			h.failTaskWithAudit(taskID, http.StatusInternalServerError, "api_error", "background response panicked", "")
			sendOutcome(backgroundResponseCreateOutcome{statusCode: http.StatusInternalServerError, errorType: "api_error", message: "background response panicked"})
		} else if !outcomeSent {
			h.failTaskWithAudit(taskID, http.StatusInternalServerError, "api_error", "background response create ended without a result", "")
			sendOutcome(backgroundResponseCreateOutcome{statusCode: http.StatusInternalServerError, errorType: "api_error", message: "background response create ended without a result"})
		}
	}()
	var heavyRelease func()
	releaseHeavy := func() {
		if heavyRelease != nil {
			heavyRelease()
			heavyRelease = nil
		}
	}
	defer releaseHeavy()
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
				sendOutcome(backgroundResponseCreateOutcome{statusCode: statusClientClosedRequest, errorType: "downstream_client_cancelled", message: "background response create was cancelled"})
				return
			}
			if errors.Is(err, errBackgroundHeavyQueueFull) {
				h.failTaskWithAudit(taskID, http.StatusTooManyRequests, "proxy_background_queue_full", "background heavy web search queue is full", "")
				sendOutcome(backgroundResponseCreateOutcome{statusCode: http.StatusTooManyRequests, errorType: "proxy_background_queue_full", message: "background heavy web search queue is full", retryAfter: "3"})
				return
			}
			h.failTaskWithAudit(taskID, http.StatusTooManyRequests, "proxy_user_concurrency_timeout", "timeout waiting for heavy web search queue", "")
			sendOutcome(backgroundResponseCreateOutcome{statusCode: http.StatusTooManyRequests, errorType: "proxy_user_concurrency_timeout", message: "timeout waiting for heavy web search queue", retryAfter: "3"})
			return
		}
		heavyRelease = release
		taskCtx.Set(responsesSkipUserSlotKey, true)
		taskCtx.Set(responsesHeavySlotAlreadyAcquiredKey, true)
		logger.L().Info("background_response.heavy_queue_acquired",
			zap.String("response_id", taskID),
			zap.Int64("user_id", owner.UserID),
			zap.Int64("queue_wait_ms", queueWait.Milliseconds()),
		)
	}
	if h.isTaskCancelled(taskID, owner) {
		sendOutcome(backgroundResponseCreateOutcome{statusCode: statusClientClosedRequest, errorType: "downstream_client_cancelled", message: "background response create was cancelled"})
		return
	}
	if err := h.tasks.MarkInProgress(context.Background(), taskID); err != nil {
		logger.L().Error("background_response.mark_processing_failed", zap.String("response_id", taskID), zap.Error(err))
		h.failTaskWithAudit(taskID, http.StatusInternalServerError, "proxy_mapping_missing", "failed to persist background task state", "")
		sendOutcome(backgroundResponseCreateOutcome{statusCode: http.StatusInternalServerError, errorType: "proxy_mapping_missing", message: "failed to persist background task state"})
		return
	}
	if h.isTaskCancelled(taskID, owner) {
		sendOutcome(backgroundResponseCreateOutcome{statusCode: statusClientClosedRequest, errorType: "downstream_client_cancelled", message: "background response create was cancelled"})
		return
	}

	taskCtx.Set(responsesDisableCrossAccountFailoverKey, true)
	createStatus, createBody, upstreamRequestID, createRetryAfter, accountID := h.executeNativeBackgroundCreate(taskCtx, recorder)
	if accountID > 0 || upstreamRequestID != "" {
		_ = h.tasks.AttachUpstream(context.Background(), taskID, accountID, "", upstreamRequestID)
	}
	if err := taskCtx.Request.Context().Err(); err != nil && len(createBody) == 0 {
		if errors.Is(err, context.Canceled) {
			h.cancelTask(taskID)
			sendOutcome(backgroundResponseCreateOutcome{statusCode: statusClientClosedRequest, errorType: "downstream_client_cancelled", message: "background response create was cancelled"})
			return
		}
		h.failTaskWithAudit(taskID, http.StatusGatewayTimeout, "upstream_timeout", "background response create timed out", upstreamRequestID)
		sendOutcome(backgroundResponseCreateOutcome{statusCode: http.StatusGatewayTimeout, errorType: "upstream_timeout", message: "background response create timed out"})
		return
	}

	handled, shouldPoll, accepted := h.handleNativeBackgroundCreateResult(taskCtx.Request.Context(), taskID, createStatus, createBody, accountID, upstreamRequestID)
	if handled {
		if !accepted {
			sendOutcome(backgroundResponseCreateOutcome{statusCode: http.StatusInternalServerError, errorType: "proxy_mapping_missing", message: "failed to persist the immutable upstream background identity mapping"})
			return
		}
		// The original POST waits only for the native upstream create contract.
		// Once a stable upstream id is persisted, release the create queue and
		// return the stable proxy id while short GET polling continues detached.
		sendOutcome(backgroundResponseCreateOutcome{accepted: true, statusCode: http.StatusOK})
		releaseHeavy()
		cancel()
		if shouldPoll {
			h.pollNativeBackground(taskID, owner)
		}
		return
	}
	if backgroundResponseNativeBackgroundUnsupportedFromContext(taskCtx, createStatus, createBody) {
		_ = h.tasks.SetExecutionMode(context.Background(), taskID, "native_background_unsupported", false)
		logger.L().Warn("background_response.native_background_unsupported",
			zap.String("response_id", taskID),
			zap.Int("client_status", createStatus),
			zap.Int("upstream_status", backgroundResponseUpstreamStatusFromContext(taskCtx)),
			zap.String("upstream_message", backgroundResponseRedactErrorText(backgroundResponseUpstreamMessageFromContext(taskCtx))),
		)
		h.failTaskWithAudit(taskID, http.StatusBadRequest, "upstream_background_unsupported", "selected upstream does not support native Responses background execution", upstreamRequestID)
		sendOutcome(backgroundResponseCreateOutcome{statusCode: http.StatusBadRequest, errorType: "upstream_background_unsupported", message: "selected upstream does not support native Responses background execution"})
		return
	}

	// A response.created event received from a request that explicitly asked for
	// native background execution is not sufficient evidence that the returned
	// id supports GET /v1/responses/{id}. In particular, compatibility gateways
	// may silently downgrade the request to an ordinary SSE turn. Never treat
	// such a streaming id as a pollable background task.
	if upstreamID := backgroundResponseUpstreamIDFromSSE(createBody); upstreamID != "" {
		if err := h.tasks.AttachUpstream(context.Background(), taskID, accountID, upstreamID, upstreamRequestID); err != nil {
			logger.L().Error("background_response.attach_upstream_from_sse_failed", zap.String("response_id", taskID), zap.String("upstream_response_id", upstreamID), zap.Error(err))
		}
		_ = h.tasks.SetExecutionMode(context.Background(), taskID, "native_background_protocol_mismatch", false)
		h.failTaskWithAudit(taskID, http.StatusBadGateway, "upstream_background_unsupported", "upstream returned a streaming response instead of a pollable native background response", upstreamRequestID)
		sendOutcome(backgroundResponseCreateOutcome{statusCode: http.StatusBadGateway, errorType: "upstream_background_unsupported", message: "upstream returned a streaming response instead of a pollable native background response"})
		return
	}

	classified := backgroundResponseAttachRetryAfter(classifyBackgroundResponseFailureFromContext(taskCtx, createStatus, createBody, extractBackgroundResponseError(createBody)), createRetryAfter)
	h.failTask(taskID, createStatus, classified)
	statusCode := createStatus
	if statusCode < http.StatusBadRequest || statusCode > 599 {
		statusCode = http.StatusBadGateway
	}
	sendOutcome(backgroundResponseCreateOutcome{
		statusCode: statusCode,
		errorType:  strings.TrimSpace(gjson.GetBytes(classified, "type").String()),
		message:    backgroundResponseErrorMessage(classified, "upstream background response create failed"),
		retryAfter: createRetryAfter,
	})
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

func (h *BackgroundResponseHandler) handleNativeBackgroundCreateResult(ctx context.Context, taskID string, statusCode int, body []byte, accountID int64, upstreamRequestID string) (handled bool, shouldPoll bool, accepted bool) {
	if statusCode < http.StatusOK || statusCode >= http.StatusMultipleChoices || len(body) == 0 || !json.Valid(body) {
		return false, false, false
	}
	upstreamID := strings.TrimSpace(gjson.GetBytes(body, "id").String())
	if upstreamID == "" {
		upstreamID = strings.TrimSpace(gjson.GetBytes(body, "response.id").String())
	}
	if err := h.attachNativeBackgroundUpstream(ctx, taskID, accountID, upstreamID, upstreamRequestID); err != nil {
		logger.L().Error("background_response.attach_upstream_failed", zap.String("response_id", taskID), zap.String("upstream_response_id", upstreamID), zap.Error(err))
		h.failTaskWithAudit(taskID, http.StatusInternalServerError, "proxy_mapping_missing", "failed to persist the immutable upstream background identity mapping", upstreamRequestID)
		return true, false, false
	}
	switch strings.ToLower(strings.TrimSpace(gjson.GetBytes(body, "status").String())) {
	case service.BackgroundResponseStatusCompleted:
		h.completeTaskWithNativeResult(taskID, statusCode, body)
		return true, false, true
	case service.BackgroundResponseStatusFailed:
		h.failTaskWithAudit(taskID, statusCode, "upstream_task_failed", backgroundResponseErrorMessage(extractBackgroundResponseError(body), "upstream background response failed"), upstreamRequestID)
		return true, false, true
	case service.BackgroundResponseStatusCancelled, "canceled":
		h.failTaskWithAudit(taskID, statusCode, "upstream_task_cancelled", "upstream background response was cancelled", upstreamRequestID)
		return true, false, true
	case service.BackgroundResponseStatusQueued, service.BackgroundResponseStatusInProgress:
		if upstreamID == "" {
			return false, false, false
		}
		return true, true, true
	default:
		if upstreamID != "" {
			return true, true, true
		}
		// Some compat upstreams can still return a completed-looking Response with
		// no explicit status. Treat an output-bearing JSON response as terminal.
		if gjson.GetBytes(body, "output").Exists() {
			h.completeTaskWithNativeResult(taskID, statusCode, body)
			return true, false, true
		}
		return false, false, false
	}
}

func (h *BackgroundResponseHandler) attachNativeBackgroundUpstream(ctx context.Context, taskID string, accountID int64, upstreamResponseID, upstreamRequestID string) error {
	if h == nil || h.tasks == nil {
		return service.ErrBackgroundResponseUnavailable
	}
	if accountID <= 0 || h.openAI == nil || h.openAI.gatewayService == nil {
		// Unit-test and embedded handlers may provide a custom fetchUpstream
		// implementation without a gateway. Production handlers always persist the
		// immutable binding before polling.
		return h.tasks.AttachUpstream(ctx, taskID, accountID, upstreamResponseID, upstreamRequestID)
	}
	binding, ok := service.OpenAIBackgroundUpstreamBindingFromContext(ctx)
	if !ok || binding == nil {
		return fmt.Errorf("%w: exact creation-time upstream binding was not captured", service.ErrOpenAIBackgroundUpstreamBindingInvalid)
	}
	return h.tasks.AttachNativeUpstreamMapping(ctx, taskID, accountID, upstreamResponseID, upstreamRequestID, *binding)
}

func (h *BackgroundResponseHandler) pollNativeBackground(taskID string, owner service.BackgroundResponseOwner) {
	if h == nil || h.tasks == nil {
		return
	}
	initialTask, err := h.tasks.Get(context.Background(), owner, taskID)
	if err != nil || initialTask == nil || backgroundResponseTaskTerminalForHandler(initialTask.Status) {
		return
	}
	deadlineAt := h.tasks.ExecutionDeadline(initialTask)
	if deadlineAt.IsZero() {
		deadlineAt = time.Now().UTC()
	}
	deadlineCtx, deadlineCancel := context.WithDeadline(context.Background(), deadlineAt)
	defer deadlineCancel()
	// A peer may die immediately before the persisted task deadline while its
	// short Redis lease remains visible. Keep the takeover waiter alive for one
	// lease TTL beyond the task deadline so it can acquire the expired lease and
	// persist the terminal deadline classification instead of leaving an orphan.
	leaseWaitCtx, leaseWaitCancel := context.WithDeadline(context.Background(), deadlineAt.Add(backgroundResponseWorkerLeaseTTL))
	defer leaseWaitCancel()

	leaseHolder := "background-worker-" + uuid.NewString()
	lease, leaseAcquired := h.waitForBackgroundResponseLease(leaseWaitCtx, taskID, owner, leaseHolder)
	if !leaseAcquired {
		return
	}
	defer func() {
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), backgroundResponseWorkerLeaseIOTimeout)
		defer releaseCancel()
		if err := h.tasks.ReleaseFencedLease(releaseCtx, taskID, lease); err != nil {
			logger.L().Warn("background_response.worker_lease_release_failed",
				zap.String("response_id", taskID),
				zap.Error(err),
			)
		}
	}()

	pollCtx, pollCancel := context.WithCancel(deadlineCtx)
	defer pollCancel()
	h.running.Store(taskID, context.CancelFunc(pollCancel))
	defer h.running.Delete(taskID)

	leaseLost := make(chan struct{})
	stopLeaseRenewal := make(chan struct{})
	defer close(stopLeaseRenewal)
	go h.renewBackgroundResponseLease(pollCtx, pollCancel, taskID, lease, leaseLost, stopLeaseRenewal)

	var firstNotFoundAt time.Time
	for {
		if backgroundResponseLeaseLost(leaseLost) {
			return
		}
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
		if deadlineCtx.Err() != nil || h.tasks.ExecutionDeadlineExceeded(task, time.Now().UTC()) {
			h.failTaskWithLeaseAudit(taskID, lease, http.StatusGatewayTimeout, "proxy_task_deadline_exceeded", "background task deadline exceeded", task.UpstreamRequestID)
			return
		}
		if strings.TrimSpace(task.UpstreamExecutionMode) != service.BackgroundResponseExecutionModeNative ||
			!task.UpstreamPollable ||
			task.UpstreamAccountID <= 0 ||
			strings.TrimSpace(task.UpstreamResponseID) == "" ||
			(h.openAI != nil && h.openAI.gatewayService != nil && task.UpstreamBinding == nil) {
			h.failTaskWithLeaseAudit(taskID, lease, http.StatusInternalServerError, "proxy_mapping_missing", "native background response mapping is incomplete or is not marked pollable", task.UpstreamRequestID)
			return
		}
		result, err := h.fetchNativeBackground(pollCtx, task)
		if err != nil {
			if backgroundResponseLeaseLost(leaseLost) {
				return
			}
			if h.isTaskCancelled(taskID, owner) {
				return
			}
			if deadlineCtx.Err() != nil {
				h.failTaskWithLeaseAudit(taskID, lease, http.StatusGatewayTimeout, "proxy_task_deadline_exceeded", "background task deadline exceeded", task.UpstreamRequestID)
				return
			}
			if errors.Is(err, service.ErrOpenAIBackgroundUpstreamIdentityMismatch) {
				h.failTaskWithLeaseAudit(taskID, lease, http.StatusForbidden, "upstream_auth_scope_mismatch", "upstream account identity or routing scope changed after background creation", task.UpstreamRequestID)
				return
			}
			if errors.Is(err, service.ErrOpenAIBackgroundUpstreamBindingInvalid) {
				h.failTaskWithLeaseAudit(taskID, lease, http.StatusInternalServerError, "proxy_mapping_missing", "stored upstream background identity mapping is missing or invalid", task.UpstreamRequestID)
				return
			}
			classification := classifyBackgroundTransportError(err)
			logger.L().Warn("background_response.poll_upstream_error",
				zap.String("response_id", taskID),
				zap.String("upstream_response_id", task.UpstreamResponseID),
				zap.String("error_type", classification),
				zap.String("error", backgroundResponseRedactErrorText(err.Error())),
			)
			if !backgroundResponseWaitForNextPoll(pollCtx, h.pollInterval, "") {
				if backgroundResponseLeaseLost(leaseLost) {
					return
				}
				if h.isTaskCancelled(taskID, owner) {
					return
				}
				h.failTaskWithLeaseAudit(taskID, lease, http.StatusGatewayTimeout, "proxy_task_deadline_exceeded", "background task deadline exceeded", task.UpstreamRequestID)
				return
			}
			continue
		}
		if result == nil {
			if !backgroundResponseWaitForNextPoll(pollCtx, h.pollInterval, "") {
				if backgroundResponseLeaseLost(leaseLost) {
					return
				}
				if h.isTaskCancelled(taskID, owner) {
					return
				}
				h.failTaskWithLeaseAudit(taskID, lease, http.StatusGatewayTimeout, "proxy_task_deadline_exceeded", "background task deadline exceeded", task.UpstreamRequestID)
				return
			}
			continue
		}
		if result.statusCode != http.StatusNotFound {
			firstNotFoundAt = time.Time{}
		}
		if result.upstreamRequestID != "" {
			_ = h.tasks.AttachUpstream(context.Background(), taskID, task.UpstreamAccountID, task.UpstreamResponseID, result.upstreamRequestID)
		}
		if result.statusCode >= http.StatusOK && result.statusCode < http.StatusMultipleChoices && json.Valid(result.body) {
			switch strings.ToLower(strings.TrimSpace(gjson.GetBytes(result.body, "status").String())) {
			case service.BackgroundResponseStatusCompleted:
				h.completeTaskWithNativeResultAndLease(taskID, lease, result.statusCode, result.body)
				return
			case service.BackgroundResponseStatusFailed:
				h.failTaskWithLeaseAudit(taskID, lease, result.statusCode, "upstream_task_failed", backgroundResponseErrorMessage(extractBackgroundResponseError(result.body), "upstream background response failed"), result.upstreamRequestID)
				return
			case service.BackgroundResponseStatusCancelled, "canceled":
				h.cancelUpstreamTaskWithLeaseAudit(taskID, lease, result.statusCode, "upstream background response was cancelled", result.upstreamRequestID)
				return
			case service.BackgroundResponseStatusQueued, service.BackgroundResponseStatusInProgress, "":
				if !backgroundResponseWaitForNextPoll(pollCtx, h.pollInterval, result.retryAfter) {
					if backgroundResponseLeaseLost(leaseLost) {
						return
					}
					if h.isTaskCancelled(taskID, owner) {
						return
					}
					h.failTaskWithLeaseAudit(taskID, lease, http.StatusGatewayTimeout, "proxy_task_deadline_exceeded", "background task deadline exceeded", result.upstreamRequestID)
					return
				}
				continue
			default:
				h.failTaskWithLeaseAudit(taskID, lease, http.StatusBadGateway, "upstream_task_failed", "upstream returned an unknown background status", result.upstreamRequestID)
				return
			}
		}
		if result.statusCode == http.StatusUnauthorized || result.statusCode == http.StatusForbidden {
			h.failTaskWithLeaseAudit(taskID, lease, result.statusCode, "upstream_auth_scope_mismatch", backgroundResponseFailureDetail(result.body, "upstream credentials no longer match the identity that created this background response"), result.upstreamRequestID)
			return
		}
		if result.statusCode == http.StatusNotFound {
			now := time.Now().UTC()
			if firstNotFoundAt.IsZero() {
				firstNotFoundAt = now
			}
			notFoundGrace := h.notFoundGrace
			if notFoundGrace <= 0 {
				notFoundGrace = 2 * time.Minute
			}
			if now.Sub(firstNotFoundAt) < notFoundGrace {
				logger.L().Warn("background_response.poll_upstream_not_found_retrying",
					zap.String("response_id", taskID),
					zap.String("upstream_response_id", task.UpstreamResponseID),
					zap.Int64("not_found_elapsed_ms", now.Sub(firstNotFoundAt).Milliseconds()),
					zap.Int64("not_found_grace_ms", notFoundGrace.Milliseconds()),
					zap.String("provider_request_id", result.upstreamRequestID),
				)
				if !backgroundResponseWaitForNextPoll(pollCtx, h.pollInterval, result.retryAfter) {
					if backgroundResponseLeaseLost(leaseLost) {
						return
					}
					if h.isTaskCancelled(taskID, owner) {
						return
					}
					h.failTaskWithLeaseAudit(taskID, lease, http.StatusGatewayTimeout, "proxy_task_deadline_exceeded", "background task deadline exceeded", result.upstreamRequestID)
					return
				}
				continue
			}
			h.failTaskWithLeaseAudit(taskID, lease, http.StatusNotFound, "upstream_response_not_found", backgroundResponseFailureDetail(result.body, "upstream background response was not found after bounded polling"), result.upstreamRequestID)
			return
		}
		if result.statusCode == http.StatusTooManyRequests || backgroundResponseIsTransientPollFailure(result.statusCode, result.body) {
			logger.L().Warn("background_response.poll_upstream_transient_status",
				zap.String("response_id", taskID),
				zap.String("upstream_response_id", task.UpstreamResponseID),
				zap.Int("upstream_status", result.statusCode),
				zap.String("retry_after", result.retryAfter),
				zap.String("error_type", strings.TrimSpace(gjson.GetBytes(classifyBackgroundResponseFailure(result.statusCode, result.body, extractBackgroundResponseError(result.body)), "type").String())),
			)
			if !backgroundResponseWaitForNextPoll(pollCtx, h.pollInterval, result.retryAfter) {
				if backgroundResponseLeaseLost(leaseLost) {
					return
				}
				if h.isTaskCancelled(taskID, owner) {
					return
				}
				h.failTaskWithLeaseAudit(taskID, lease, http.StatusGatewayTimeout, "proxy_task_deadline_exceeded", "background task deadline exceeded", result.upstreamRequestID)
				return
			}
			continue
		}
		h.failTaskWithLease(taskID, lease, result.statusCode, classifyBackgroundResponseFailure(result.statusCode, result.body, extractBackgroundResponseError(result.body)))
		return
	}
}

func (h *BackgroundResponseHandler) waitForBackgroundResponseLease(ctx context.Context, taskID string, owner service.BackgroundResponseOwner, holder string) (service.BackgroundResponseTaskLease, bool) {
	wait := h.pollInterval
	if wait <= 0 || wait > backgroundResponseWorkerLeaseWaitMax {
		wait = backgroundResponseWorkerLeaseWaitMax
	}
	loggedPeer := false
	for {
		task, err := h.tasks.Get(context.Background(), owner, taskID)
		if err != nil || task == nil || backgroundResponseTaskTerminalForHandler(task.Status) {
			return service.BackgroundResponseTaskLease{}, false
		}
		leaseCtx, leaseCancel := context.WithTimeout(context.Background(), backgroundResponseWorkerLeaseIOTimeout)
		lease, acquired, leaseErr := h.tasks.AcquireFencedLease(leaseCtx, taskID, holder, backgroundResponseWorkerLeaseTTL)
		leaseCancel()
		if leaseErr != nil {
			logger.L().Warn("background_response.worker_lease_acquire_failed",
				zap.String("response_id", taskID),
				zap.Error(leaseErr),
			)
		} else if acquired {
			return lease, true
		} else if !loggedPeer {
			loggedPeer = true
			logger.L().Info("background_response.worker_lease_held_by_peer", zap.String("response_id", taskID))
		}
		if ctx.Err() != nil {
			return service.BackgroundResponseTaskLease{}, false
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return service.BackgroundResponseTaskLease{}, false
		case <-timer.C:
		}
	}
}

func (h *BackgroundResponseHandler) renewBackgroundResponseLease(
	ctx context.Context,
	cancel context.CancelFunc,
	taskID string,
	lease service.BackgroundResponseTaskLease,
	leaseLost chan struct{},
	stop <-chan struct{},
) {
	ticker := time.NewTicker(backgroundResponseWorkerLeaseRenewInterval)
	defer ticker.Stop()
	var once sync.Once
	markLost := func(err error) {
		once.Do(func() {
			fields := []zap.Field{zap.String("response_id", taskID)}
			if err != nil {
				fields = append(fields, zap.Error(err))
			}
			logger.L().Warn("background_response.worker_lease_lost", fields...)
			close(leaseLost)
			cancel()
		})
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-ticker.C:
			renewCtx, renewCancel := context.WithTimeout(context.Background(), backgroundResponseWorkerLeaseIOTimeout)
			renewed, err := h.tasks.RenewFencedLease(renewCtx, taskID, lease, backgroundResponseWorkerLeaseTTL)
			renewCancel()
			if err != nil || !renewed {
				markLost(err)
				return
			}
		}
	}
}

func backgroundResponseLeaseLost(lost <-chan struct{}) bool {
	if lost == nil {
		return false
	}
	select {
	case <-lost:
		return true
	default:
		return false
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
	if task.UpstreamBinding == nil {
		return nil, fmt.Errorf("%w: missing immutable upstream binding", service.ErrOpenAIBackgroundUpstreamBindingInvalid)
	}
	result, err := h.openAI.gatewayService.FetchOpenAIBackgroundResponseBound(ctx, *task.UpstreamBinding, task.UpstreamResponseID)
	if err != nil {
		return nil, err
	}
	return &backgroundUpstreamPollResult{statusCode: result.StatusCode, body: result.Body, upstreamRequestID: result.UpstreamRequestID, retryAfter: result.RetryAfter}, nil
}

func (h *BackgroundResponseHandler) cancelNativeUpstreamBackgroundResponse(ctx context.Context, task *service.BackgroundResponseTaskRecord) (*backgroundUpstreamPollResult, error) {
	if h == nil || h.openAI == nil || h.openAI.gatewayService == nil || task == nil || task.UpstreamBinding == nil || strings.TrimSpace(task.UpstreamResponseID) == "" {
		return nil, nil
	}
	result, err := h.openAI.gatewayService.CancelOpenAIBackgroundResponseBound(ctx, *task.UpstreamBinding, task.UpstreamResponseID)
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
		return
	}
	if h.canSettleBackgroundResponseUsage() {
		go h.settleBackgroundResponseUsageByID(taskID)
	}
}

func (h *BackgroundResponseHandler) completeTaskWithNativeResultAndLease(taskID string, lease service.BackgroundResponseTaskLease, statusCode int, body []byte) {
	task, _ := h.tasks.GetByID(context.Background(), taskID)
	result := backgroundResponseNormalizeNativeResult(body, taskID, "")
	if task != nil {
		result = backgroundResponseNormalizeNativeResult(body, taskID, task.Model)
	}
	if err := h.tasks.CompleteWithLease(context.Background(), taskID, lease, statusCode, json.RawMessage(result)); err != nil {
		logger.L().Warn("background_response.complete_fenced_store_failed",
			zap.String("response_id", taskID),
			zap.Int64("lease_fence", lease.Fence),
			zap.Error(err),
		)
		return
	}
	if h.canSettleBackgroundResponseUsage() {
		completed, err := h.tasks.GetByID(context.Background(), taskID)
		if err != nil {
			logger.L().Warn("background_response.usage_completed_task_load_failed",
				zap.String("response_id", taskID),
				zap.Error(err),
			)
			return
		}
		h.settleBackgroundResponseUsage(completed, &lease)
	}
}

func (h *BackgroundResponseHandler) canSettleBackgroundResponseUsage() bool {
	return h != nil &&
		h.tasks != nil &&
		h.openAI != nil &&
		h.openAI.gatewayService != nil &&
		h.openAI.apiKeyService != nil
}

func (h *BackgroundResponseHandler) settleBackgroundResponseUsageByID(taskID string) {
	if !h.canSettleBackgroundResponseUsage() {
		return
	}
	task, err := h.tasks.GetByID(context.Background(), taskID)
	if err != nil {
		logger.L().Warn("background_response.usage_task_load_failed",
			zap.String("response_id", taskID),
			zap.Error(err),
		)
		return
	}
	h.settleBackgroundResponseUsage(task, nil)
}

// settleBackgroundResponseUsage records the terminal provider usage and then
// closes the Redis settlement flag. Billing is protected by the stable
// background_response:<proxy id> SQL dedup key. Therefore a crash after SQL
// commit but before MarkUsageRecorded is safe: restart recovery repeats the
// call, the SQL claim is a no-op, and the durable Redis flag is then closed.
func (h *BackgroundResponseHandler) settleBackgroundResponseUsage(task *service.BackgroundResponseTaskRecord, heldLease *service.BackgroundResponseTaskLease) {
	if !h.canSettleBackgroundResponseUsage() || task == nil ||
		task.Status != service.BackgroundResponseStatusCompleted ||
		task.UsageSettlementVersion <= 0 ||
		task.UsageRecorded {
		return
	}

	var acquiredLease service.BackgroundResponseTaskLease
	if heldLease == nil {
		leaseCtx, cancel := context.WithTimeout(context.Background(), backgroundResponseWorkerLeaseIOTimeout)
		lease, acquired, err := h.tasks.AcquireFencedLease(
			leaseCtx,
			task.ID,
			"background-usage-"+uuid.NewString(),
			backgroundResponseUsageLeaseTTL,
		)
		cancel()
		if err != nil {
			logger.L().Warn("background_response.usage_lease_acquire_failed",
				zap.String("response_id", task.ID),
				zap.Error(err),
			)
			return
		}
		if !acquired {
			return
		}
		acquiredLease = lease
		defer func() {
			releaseCtx, releaseCancel := context.WithTimeout(context.Background(), backgroundResponseWorkerLeaseIOTimeout)
			defer releaseCancel()
			if err := h.tasks.ReleaseFencedLease(releaseCtx, task.ID, acquiredLease); err != nil {
				logger.L().Warn("background_response.usage_lease_release_failed",
					zap.String("response_id", task.ID),
					zap.Error(err),
				)
			}
		}()

		// Refresh after acquiring the distributed lease: another instance may
		// have settled and closed this record between SCAN and lease acquisition.
		current, err := h.tasks.GetByID(context.Background(), task.ID)
		if err != nil {
			logger.L().Warn("background_response.usage_task_reload_failed",
				zap.String("response_id", task.ID),
				zap.Error(err),
			)
			return
		}
		task = current
		if task.Status != service.BackgroundResponseStatusCompleted ||
			task.UsageSettlementVersion <= 0 ||
			task.UsageRecorded {
			return
		}
	}

	settlementCtx, settlementCancel := context.WithTimeout(context.Background(), backgroundResponseUsageSettlementTimeout)
	err := h.openAI.gatewayService.RecordBackgroundResponseUsage(
		settlementCtx,
		h.openAI.apiKeyService,
		&service.OpenAIBackgroundResponseUsageInput{
			ProxyResponseID:    task.ID,
			UserID:             task.UserID,
			APIKeyID:           task.APIKeyID,
			AccountID:          task.UpstreamAccountID,
			SubscriptionID:     task.SubscriptionID,
			Model:              task.Model,
			Result:             task.Result,
			RequestFingerprint: task.RequestFingerprint,
			ElapsedSeconds:     task.ElapsedSeconds,
		},
	)
	settlementCancel()

	attemptError := ""
	if err != nil {
		attemptError = backgroundResponseRedactErrorText(err.Error())
	}
	if attemptErr := h.tasks.RecordUsageAttempt(context.Background(), task.ID, attemptError); attemptErr != nil {
		logger.L().Warn("background_response.usage_attempt_store_failed",
			zap.String("response_id", task.ID),
			zap.Error(attemptErr),
		)
	}
	if err != nil {
		logger.L().Warn("background_response.usage_settlement_failed",
			zap.String("response_id", task.ID),
			zap.Int64("api_key_id", task.APIKeyID),
			zap.Int64("account_id", task.UpstreamAccountID),
			zap.String("error", attemptError),
		)
		return
	}
	if err := h.tasks.MarkUsageRecorded(context.Background(), task.ID); err != nil {
		logger.L().Warn("background_response.usage_recorded_store_failed",
			zap.String("response_id", task.ID),
			zap.Error(err),
		)
		return
	}
	logger.L().Info("background_response.usage_settled",
		zap.String("response_id", task.ID),
		zap.Int64("api_key_id", task.APIKeyID),
		zap.Int64("account_id", task.UpstreamAccountID),
	)
}

func (h *BackgroundResponseHandler) failTaskWithLease(taskID string, lease service.BackgroundResponseTaskLease, statusCode int, taskErr json.RawMessage) {
	task, _ := h.tasks.GetByID(context.Background(), taskID)
	taskErr = backgroundResponseAttachAuditFields(task, taskErr, "")
	if err := h.tasks.FailWithLease(context.Background(), taskID, lease, statusCode, taskErr); err != nil {
		logger.L().Warn("background_response.failure_fenced_store_failed",
			zap.String("response_id", taskID),
			zap.Int64("lease_fence", lease.Fence),
			zap.Error(err),
		)
	}
}

func (h *BackgroundResponseHandler) failTaskWithLeaseAudit(taskID string, lease service.BackgroundResponseTaskLease, statusCode int, errorType, message, upstreamRequestID string) {
	task, _ := h.tasks.GetByID(context.Background(), taskID)
	taskErr := backgroundResponseAuditedErrorPayload(task, errorType, message, upstreamRequestID)
	if err := h.tasks.FailWithLease(context.Background(), taskID, lease, statusCode, taskErr); err != nil {
		logger.L().Warn("background_response.failure_fenced_store_failed",
			zap.String("response_id", taskID),
			zap.Int64("lease_fence", lease.Fence),
			zap.Error(err),
		)
	}
}

func (h *BackgroundResponseHandler) cancelUpstreamTaskWithLeaseAudit(taskID string, lease service.BackgroundResponseTaskLease, statusCode int, message, upstreamRequestID string) {
	task, _ := h.tasks.GetByID(context.Background(), taskID)
	taskErr := backgroundResponseAuditedErrorPayload(task, "upstream_task_cancelled", message, upstreamRequestID)
	if err := h.tasks.CancelUpstreamWithLease(context.Background(), taskID, lease, statusCode, taskErr); err != nil {
		logger.L().Warn("background_response.upstream_cancel_fenced_store_failed",
			zap.String("response_id", taskID),
			zap.Int64("lease_fence", lease.Fence),
			zap.Error(err),
		)
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
	if c != nil {
		if subscription, ok := middleware2.GetSubscriptionFromContext(c); ok && subscription != nil && subscription.ID > 0 {
			meta.SubscriptionID = subscription.ID
		}
	}
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

func backgroundResponseTaskIsCreateRejection(task *service.BackgroundResponseTaskRecord) bool {
	return task != nil &&
		task.Status == service.BackgroundResponseStatusFailed &&
		strings.TrimSpace(task.UpstreamResponseID) == "" &&
		task.HTTPStatus >= http.StatusBadRequest
}

func backgroundResponseWriteCreateRejection(c *gin.Context, task *service.BackgroundResponseTaskRecord, outcome backgroundResponseCreateOutcome) {
	if c == nil {
		return
	}
	statusCode := outcome.statusCode
	if statusCode < http.StatusBadRequest || statusCode > 599 {
		statusCode = http.StatusBadGateway
	}
	if retryAfter := strings.TrimSpace(outcome.retryAfter); retryAfter != "" {
		c.Header("Retry-After", retryAfter)
	}
	c.Header("Cache-Control", "no-store")
	var taskErr json.RawMessage
	if task != nil && json.Valid(task.Error) {
		taskErr = append(json.RawMessage(nil), task.Error...)
	}
	if !json.Valid(taskErr) {
		errorType := strings.TrimSpace(outcome.errorType)
		if errorType == "" {
			errorType = "upstream_error"
		}
		message := strings.TrimSpace(outcome.message)
		if message == "" {
			message = "upstream background response create failed"
		}
		taskErr = backgroundResponseAuditedErrorPayload(task, errorType, message, "")
	}
	c.JSON(statusCode, gin.H{"error": taskErr})
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
		normalized := backgroundResponseAttachAuditFields(task, task.Error, task.UpstreamRequestID)
		_ = json.Unmarshal(normalized, &responseErr)
	}
	if responseErr == nil {
		fallback := backgroundResponseAttachAuditFields(task, backgroundResponseErrorPayload("upstream_task_failed", "background response failed"), task.UpstreamRequestID)
		_ = json.Unmarshal(fallback, &responseErr)
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
	var taskErr any
	if task != nil && json.Valid(task.Error) {
		taskErr = json.RawMessage(task.Error)
	}
	return gin.H{
		"id":                 task.ID,
		"object":             "response",
		"created_at":         task.CreatedAt,
		"status":             service.BackgroundResponseStatusCancelled,
		"background":         true,
		"model":              task.Model,
		"output":             []any{},
		"error":              taskErr,
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
	case statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden:
		return backgroundResponseErrorPayload("upstream_auth_scope_mismatch", backgroundResponseFailureDetail(body, "upstream credentials do not match the scope that created this background response"))
	case statusCode == http.StatusNotFound:
		return backgroundResponseErrorPayload("upstream_response_not_found", backgroundResponseFailureDetail(body, "upstream background response was not found"))
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
	case upstreamStatus == http.StatusUnauthorized || upstreamStatus == http.StatusForbidden:
		return backgroundResponseErrorPayload("upstream_auth_scope_mismatch", upstreamMessage)
	case upstreamStatus == http.StatusNotFound:
		return backgroundResponseErrorPayload("upstream_response_not_found", upstreamMessage)
	case strings.Contains(strings.ToLower(upstreamText), "connection reset") || strings.Contains(strings.ToLower(upstreamText), "reset by peer"):
		return backgroundResponseErrorPayload("upstream_connection_reset", upstreamMessage)
	case strings.Contains(strings.ToLower(upstreamText), "unexpected eof"):
		return backgroundResponseErrorPayload("upstream_unexpected_eof", upstreamMessage)
	case upstreamStatus == http.StatusGatewayTimeout || strings.Contains(strings.ToLower(upstreamText), "timeout"):
		return backgroundResponseErrorPayload("upstream_timeout", upstreamMessage)
	case upstreamStatus > 0 && statusCode >= http.StatusBadGateway:
		return backgroundResponseErrorPayload("upstream_task_failed", upstreamMessage)
	default:
		return classifyBackgroundResponseFailure(statusCode, body, syntheticErr)
	}
}

func backgroundResponseErrorMessage(taskErr json.RawMessage, fallback string) string {
	if len(taskErr) > 0 && json.Valid(taskErr) {
		for _, path := range []string{"message", "detail", "error.message", "error.detail"} {
			if msg := strings.TrimSpace(gjson.GetBytes(taskErr, path).String()); msg != "" {
				return backgroundResponseRedactErrorText(msg)
			}
		}
	}
	return backgroundResponseRedactErrorText(fallback)
}

func backgroundResponseFailureDetail(body []byte, fallback string) string {
	if len(body) == 0 || !json.Valid(body) {
		return backgroundResponseRedactErrorText(fallback)
	}
	return backgroundResponseErrorMessage(json.RawMessage(body), fallback)
}

func backgroundResponseRedactErrorText(value string) string {
	value = backgroundResponseBearerCredentialPattern.ReplaceAllString(value, "Bearer ***")
	return logredact.RedactText(value,
		"authorization",
		"api_key",
		"apikey",
		"x-api-key",
		"token",
		"secret",
		"credential",
	)
}

func backgroundResponseErrorPayload(errorType, message string) json.RawMessage {
	errorType = strings.TrimSpace(errorType)
	if errorType == "" {
		errorType = "upstream_task_failed"
	}
	message = strings.TrimSpace(message)
	if message == "" {
		message = "background response failed"
	}
	data, _ := json.Marshal(gin.H{
		"type":      errorType,
		"code":      errorType,
		"message":   message,
		"detail":    message,
		"retryable": backgroundResponseErrorRetryable(errorType),
	})
	return data
}

func backgroundResponseErrorRetryable(errorType string) bool {
	switch strings.ToLower(strings.TrimSpace(errorType)) {
	case "upstream_rate_limited",
		"upstream_unexpected_eof",
		"upstream_connection_reset",
		"upstream_timeout",
		"upstream_error",
		"proxy_background_queue_full",
		"proxy_user_concurrency_timeout":
		return true
	default:
		return false
	}
}

func backgroundResponseAuditedErrorPayload(task *service.BackgroundResponseTaskRecord, errorType, message, upstreamRequestID string) json.RawMessage {
	payload := backgroundResponseErrorPayload(errorType, message)
	return backgroundResponseAttachAuditFields(task, payload, upstreamRequestID)
}

func backgroundResponseAttachAuditFields(task *service.BackgroundResponseTaskRecord, payload json.RawMessage, upstreamRequestID string) json.RawMessage {
	if !json.Valid(payload) {
		payload = backgroundResponseErrorPayload("upstream_task_failed", "background response failed")
	}
	payload = backgroundResponseNormalizeErrorEnvelope(payload)
	now := time.Now().UTC().Unix()
	updated := payload
	failedAt := now
	if task != nil {
		if task.FailedAt != nil && *task.FailedAt > 0 {
			failedAt = *task.FailedAt
		}
		updated, _ = sjson.SetBytes(updated, "proxy_response_id", task.ID)
		if task.ElapsedSeconds > 0 {
			updated, _ = sjson.SetBytes(updated, "elapsed_seconds", task.ElapsedSeconds)
		} else if task.CreatedAt > 0 && failedAt >= task.CreatedAt {
			updated, _ = sjson.SetBytes(updated, "elapsed_seconds", failedAt-task.CreatedAt)
		}
		updated, _ = sjson.SetBytes(updated, "upstream_response_id", task.UpstreamResponseID)
		if upstreamRequestID == "" {
			upstreamRequestID = task.UpstreamRequestID
		}
	}
	if !gjson.GetBytes(updated, "elapsed_seconds").Exists() {
		updated, _ = sjson.SetBytes(updated, "elapsed_seconds", 0)
	}
	if !gjson.GetBytes(updated, "failed_at").Exists() {
		updated, _ = sjson.SetBytes(updated, "failed_at", failedAt)
	}
	providerRequestID := strings.TrimSpace(upstreamRequestID)
	updated, _ = sjson.SetBytes(updated, "upstream_request_id", providerRequestID)
	updated, _ = sjson.SetBytes(updated, "provider_request_id", providerRequestID)
	return updated
}

func backgroundResponseNormalizeErrorEnvelope(payload json.RawMessage) json.RawMessage {
	if len(payload) == 0 || !json.Valid(payload) || !gjson.ParseBytes(payload).IsObject() {
		return backgroundResponseErrorPayload("upstream_task_failed", "background response failed")
	}
	errorType := ""
	for _, path := range []string{"type", "code", "error.type", "error.code"} {
		if value := strings.TrimSpace(gjson.GetBytes(payload, path).String()); value != "" {
			errorType = value
			break
		}
	}
	if errorType == "" {
		errorType = "upstream_task_failed"
	}
	message := backgroundResponseErrorMessage(payload, "background response failed")
	updated := payload
	if strings.TrimSpace(gjson.GetBytes(updated, "type").String()) == "" {
		updated, _ = sjson.SetBytes(updated, "type", errorType)
	}
	if strings.TrimSpace(gjson.GetBytes(updated, "code").String()) == "" {
		updated, _ = sjson.SetBytes(updated, "code", errorType)
	}
	if strings.TrimSpace(gjson.GetBytes(updated, "message").String()) == "" {
		updated, _ = sjson.SetBytes(updated, "message", message)
	}
	if strings.TrimSpace(gjson.GetBytes(updated, "detail").String()) == "" {
		updated, _ = sjson.SetBytes(updated, "detail", message)
	}
	if !gjson.GetBytes(updated, "retryable").Exists() {
		updated, _ = sjson.SetBytes(updated, "retryable", backgroundResponseErrorRetryable(errorType))
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
