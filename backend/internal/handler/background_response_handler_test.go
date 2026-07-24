package handler

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
)

type backgroundResponseHandlerMemoryStore struct {
	mu            sync.RWMutex
	tasks         map[string]*service.BackgroundResponseTaskRecord
	idempotencies map[string]string
	leases        map[string]backgroundResponseHandlerMemoryLease
	leaseFences   map[string]int64
}

type backgroundResponseHandlerMemoryLease struct {
	holder    string
	fence     int64
	expiresAt time.Time
}

func (s *backgroundResponseHandlerMemoryStore) Save(_ context.Context, task *service.BackgroundResponseTaskRecord, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	copy := *task
	copy.Result = append(json.RawMessage(nil), task.Result...)
	copy.Error = append(json.RawMessage(nil), task.Error...)
	s.tasks[task.ID] = &copy
	return nil
}

func (s *backgroundResponseHandlerMemoryStore) CreateWithIdempotency(
	_ context.Context,
	task *service.BackgroundResponseTaskRecord,
	owner service.BackgroundResponseOwner,
	keyHash string,
	_ time.Duration,
) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.idempotencies == nil {
		s.idempotencies = make(map[string]string)
	}
	key := backgroundResponseHandlerMemoryIdempotencyKey(owner, keyHash)
	if existing := s.idempotencies[key]; existing != "" {
		return existing, false, nil
	}
	s.idempotencies[key] = task.ID
	copy := *task
	copy.Result = append(json.RawMessage(nil), task.Result...)
	copy.Error = append(json.RawMessage(nil), task.Error...)
	s.tasks[task.ID] = &copy
	return "", true, nil
}

func (s *backgroundResponseHandlerMemoryStore) Get(_ context.Context, id string) (*service.BackgroundResponseTaskRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	task := s.tasks[id]
	if task == nil {
		return nil, service.ErrBackgroundResponseNotFound
	}
	copy := *task
	copy.Result = append(json.RawMessage(nil), task.Result...)
	copy.Error = append(json.RawMessage(nil), task.Error...)
	return &copy, nil
}

func (s *backgroundResponseHandlerMemoryStore) ListActive(_ context.Context, limit int) ([]*service.BackgroundResponseTaskRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*service.BackgroundResponseTaskRecord
	for _, task := range s.tasks {
		if task.Status != service.BackgroundResponseStatusQueued && task.Status != service.BackgroundResponseStatusInProgress {
			continue
		}
		copy := *task
		copy.Result = append(json.RawMessage(nil), task.Result...)
		copy.Error = append(json.RawMessage(nil), task.Error...)
		out = append(out, &copy)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (s *backgroundResponseHandlerMemoryStore) ListUsagePending(_ context.Context, limit int) ([]*service.BackgroundResponseTaskRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*service.BackgroundResponseTaskRecord
	for _, task := range s.tasks {
		if task.Status != service.BackgroundResponseStatusCompleted ||
			task.UsageSettlementVersion <= 0 ||
			task.UsageRecorded ||
			(task.UsageNextAttemptAt != nil && *task.UsageNextAttemptAt > time.Now().UTC().Unix()) {
			continue
		}
		copy := *task
		copy.Result = append(json.RawMessage(nil), task.Result...)
		copy.Error = append(json.RawMessage(nil), task.Error...)
		out = append(out, &copy)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (s *backgroundResponseHandlerMemoryStore) ReserveIdempotency(_ context.Context, owner service.BackgroundResponseOwner, keyHash, taskID string, _ time.Duration) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if keyHash == "" || taskID == "" {
		return "", true, nil
	}
	if s.idempotencies == nil {
		s.idempotencies = make(map[string]string)
	}
	key := backgroundResponseHandlerMemoryIdempotencyKey(owner, keyHash)
	if existing := s.idempotencies[key]; existing != "" {
		return existing, false, nil
	}
	s.idempotencies[key] = taskID
	return "", true, nil
}

func (s *backgroundResponseHandlerMemoryStore) ReleaseIdempotency(_ context.Context, owner service.BackgroundResponseOwner, keyHash, taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.idempotencies == nil {
		return nil
	}
	key := backgroundResponseHandlerMemoryIdempotencyKey(owner, keyHash)
	if s.idempotencies[key] == taskID {
		delete(s.idempotencies, key)
	}
	return nil
}

func (s *backgroundResponseHandlerMemoryStore) AcquireLease(_ context.Context, taskID, holder string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.leases == nil {
		s.leases = make(map[string]backgroundResponseHandlerMemoryLease)
	}
	now := time.Now()
	current := s.leases[taskID]
	if current.holder != "" && current.holder != holder && current.expiresAt.After(now) {
		return false, nil
	}
	s.leases[taskID] = backgroundResponseHandlerMemoryLease{holder: holder, expiresAt: now.Add(ttl)}
	return true, nil
}

func (s *backgroundResponseHandlerMemoryStore) RenewLease(_ context.Context, taskID, holder string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.leases == nil {
		return false, nil
	}
	current := s.leases[taskID]
	if current.holder != holder || !current.expiresAt.After(time.Now()) {
		delete(s.leases, taskID)
		return false, nil
	}
	current.expiresAt = time.Now().Add(ttl)
	s.leases[taskID] = current
	return true, nil
}

func (s *backgroundResponseHandlerMemoryStore) ReleaseLease(_ context.Context, taskID, holder string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.leases != nil && s.leases[taskID].holder == holder {
		delete(s.leases, taskID)
	}
	return nil
}

func (s *backgroundResponseHandlerMemoryStore) AcquireFencedLease(_ context.Context, taskID, holder string, ttl time.Duration) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.leases == nil {
		s.leases = make(map[string]backgroundResponseHandlerMemoryLease)
	}
	if s.leaseFences == nil {
		s.leaseFences = make(map[string]int64)
	}
	now := time.Now()
	current := s.leases[taskID]
	if current.holder != "" && current.expiresAt.After(now) {
		if current.holder != holder {
			return 0, false, nil
		}
		current.expiresAt = now.Add(ttl)
		s.leases[taskID] = current
		return current.fence, true, nil
	}
	s.leaseFences[taskID]++
	current = backgroundResponseHandlerMemoryLease{
		holder:    holder,
		fence:     s.leaseFences[taskID],
		expiresAt: now.Add(ttl),
	}
	s.leases[taskID] = current
	return current.fence, true, nil
}

func (s *backgroundResponseHandlerMemoryStore) RenewFencedLease(_ context.Context, taskID, holder string, fence int64, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.leases[taskID]
	if current.holder != holder || current.fence != fence || !current.expiresAt.After(time.Now()) {
		return false, nil
	}
	current.expiresAt = time.Now().Add(ttl)
	s.leases[taskID] = current
	return true, nil
}

func (s *backgroundResponseHandlerMemoryStore) ReleaseFencedLease(_ context.Context, taskID, holder string, fence int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.leases[taskID]
	if current.holder == holder && current.fence == fence {
		delete(s.leases, taskID)
	}
	return nil
}

func (s *backgroundResponseHandlerMemoryStore) SaveIfVersionAndLease(_ context.Context, task *service.BackgroundResponseTaskRecord, expectedVersion int64, holder string, fence int64, _ time.Duration) (bool, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	currentLease := s.leases[task.ID]
	if currentLease.holder != holder || currentLease.fence != fence || !currentLease.expiresAt.After(time.Now()) {
		return false, false, nil
	}
	currentTask := s.tasks[task.ID]
	if currentTask == nil || currentTask.Version != expectedVersion {
		return false, true, nil
	}
	copy := *task
	copy.Result = append(json.RawMessage(nil), task.Result...)
	copy.Error = append(json.RawMessage(nil), task.Error...)
	s.tasks[task.ID] = &copy
	return true, true, nil
}

func backgroundResponseHandlerMemoryIdempotencyKey(owner service.BackgroundResponseOwner, keyHash string) string {
	return strconv.FormatInt(owner.UserID, 10) + ":" + strconv.FormatInt(owner.APIKeyID, 10) + ":" + strings.TrimSpace(keyHash)
}

type backgroundResponseUsageAPIKeyRepo struct {
	service.APIKeyRepository
	apiKey *service.APIKey
}

func (r *backgroundResponseUsageAPIKeyRepo) GetByID(_ context.Context, _ int64) (*service.APIKey, error) {
	return r.apiKey, nil
}

type backgroundResponseUsageAccountRepo struct {
	service.AccountRepository
	account *service.Account
}

func (r *backgroundResponseUsageAccountRepo) GetByID(_ context.Context, _ int64) (*service.Account, error) {
	return r.account, nil
}

type backgroundResponseUsageLogRepo struct {
	service.UsageLogRepository
	calls   int
	lastLog *service.UsageLog
}

func (r *backgroundResponseUsageLogRepo) Create(_ context.Context, usage *service.UsageLog) (bool, error) {
	r.calls++
	r.lastLog = usage
	return true, nil
}

type backgroundResponseUsageBillingRepo struct {
	service.UsageBillingRepository
	calls   int
	lastCmd *service.UsageBillingCommand
}

func (r *backgroundResponseUsageBillingRepo) Apply(_ context.Context, command *service.UsageBillingCommand) (*service.UsageBillingApplyResult, error) {
	r.calls++
	r.lastCmd = command
	// The SQL claim is represented by this call. Applied=false avoids unrelated
	// cache side effects in this focused handler/recovery test.
	return &service.UsageBillingApplyResult{Applied: false}, nil
}

func TestBackgroundResponseTaskMetadataPersistsCreationSubscription(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	c.Set(string(middleware2.ContextKeySubscription), &service.UserSubscription{
		ID:      44,
		UserID:  7,
		GroupID: 3,
	})

	meta, _, err := backgroundResponseTaskMetadata(
		c,
		service.BackgroundResponseOwner{UserID: 7, APIKeyID: 9},
		[]byte(`{"model":"gpt-5.6-sol","background":true,"store":true}`),
	)
	require.NoError(t, err)
	require.Equal(t, int64(44), meta.SubscriptionID)
	require.NotEmpty(t, meta.RequestFingerprint)
}

func TestAttachNativeBackgroundUpstreamUsesCapturedRequestBinding(t *testing.T) {
	store := &backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)}
	tasks := service.NewBackgroundResponseTaskService(store)
	created, _, err := tasks.CreateWithMetadata(
		context.Background(),
		service.BackgroundResponseOwner{UserID: 7, APIKeyID: 9},
		"gpt-5.6-sol",
		service.BackgroundResponseTaskMetadata{},
	)
	require.NoError(t, err)

	gateway := service.NewOpenAIGatewayService(
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
	)
	h := &BackgroundResponseHandler{
		tasks:  tasks,
		openAI: &OpenAIGatewayHandler{gatewayService: gateway},
	}
	captured := service.BackgroundResponseUpstreamBinding{
		Version:             1,
		ExecutionMode:       service.BackgroundResponseExecutionModeNative,
		Pollable:            true,
		AccountID:           30,
		AccountType:         service.AccountTypeAPIKey,
		CredentialRef:       "account:30",
		IdentityFingerprint: strings.Repeat("a", 64),
		BaseURL:             "https://captured-upstream.example/v1/responses",
		Project:             "captured-project",
		Organization:        "captured-organization",
		RouteFingerprint:    strings.Repeat("b", 64),
	}
	ctx := service.WithOpenAIBackgroundUpstreamBinding(context.Background(), captured)

	require.NoError(t, h.attachNativeBackgroundUpstream(
		ctx,
		created.ID,
		captured.AccountID,
		"resp_upstream",
		"req_provider",
	))
	persisted, err := tasks.GetByID(context.Background(), created.ID)
	require.NoError(t, err)
	require.NotNil(t, persisted.UpstreamBinding)
	require.Equal(t, captured, *persisted.UpstreamBinding)
	require.Equal(t, "https://captured-upstream.example/v1/responses", persisted.UpstreamBinding.BaseURL)
	require.Equal(t, "captured-project", persisted.UpstreamBinding.Project)
	require.Equal(t, "captured-organization", persisted.UpstreamBinding.Organization)
}

func TestBackgroundResponseUsageReconcilerSettlesCompletedTaskOnce(t *testing.T) {
	store := &backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)}
	tasks := service.NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Hour)
	user := &service.User{ID: 7}
	apiKey := &service.APIKey{ID: 9, UserID: user.ID, User: user}
	account := &service.Account{
		ID:       30,
		Platform: service.PlatformOpenAI,
		Type:     service.AccountTypeAPIKey,
	}
	apiKeyRepo := &backgroundResponseUsageAPIKeyRepo{apiKey: apiKey}
	accountRepo := &backgroundResponseUsageAccountRepo{account: account}
	usageRepo := &backgroundResponseUsageLogRepo{}
	billingRepo := &backgroundResponseUsageBillingRepo{}
	cfg := &config.Config{}
	cfg.Default.RateMultiplier = 1
	apiKeyService := service.NewAPIKeyService(apiKeyRepo, nil, nil, nil, nil, nil, cfg)
	gateway := service.NewOpenAIGatewayService(
		accountRepo,
		usageRepo,
		billingRepo,
		nil,
		nil,
		nil,
		nil,
		cfg,
		nil,
		nil,
		service.NewBillingService(cfg, nil),
		nil,
		&service.BillingCacheService{},
		nil,
		&service.DeferredService{},
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	openAI := &OpenAIGatewayHandler{gatewayService: gateway, apiKeyService: apiKeyService}
	h := &BackgroundResponseHandler{tasks: tasks, openAI: openAI}

	created, _, err := tasks.CreateWithMetadata(
		context.Background(),
		service.BackgroundResponseOwner{UserID: user.ID, APIKeyID: apiKey.ID},
		"gpt-5.1",
		service.BackgroundResponseTaskMetadata{RequestFingerprint: strings.Repeat("a", 64)},
	)
	require.NoError(t, err)
	require.NoError(t, tasks.AttachUpstream(context.Background(), created.ID, account.ID, "resp_upstream", "req_provider"))
	require.NoError(t, tasks.Complete(
		context.Background(),
		created.ID,
		http.StatusOK,
		json.RawMessage(`{
			"id":"resp_upstream",
			"object":"response",
			"status":"completed",
			"model":"gpt-5.1",
			"output":[],
			"usage":{"input_tokens":11,"output_tokens":7}
		}`),
	))

	h.reconcileBackgroundResponseUsage()

	settled, err := tasks.GetByID(context.Background(), created.ID)
	require.NoError(t, err)
	require.True(t, settled.UsageRecorded)
	require.NotNil(t, settled.UsageRecordedAt)
	require.Equal(t, 1, settled.UsageRecordAttempts)
	require.Empty(t, settled.UsageRecordLastError)
	require.Equal(t, 1, billingRepo.calls)
	require.NotNil(t, billingRepo.lastCmd)
	require.Equal(t, "background_response:"+created.ID, billingRepo.lastCmd.RequestID)
	require.Equal(t, 11, billingRepo.lastCmd.InputTokens)
	require.Equal(t, 7, billingRepo.lastCmd.OutputTokens)
	require.Equal(t, 1, usageRepo.calls)
	require.NotNil(t, usageRepo.lastLog)

	// A second recovery pass sees the durable flag and performs no new billing.
	h.reconcileBackgroundResponseUsage()
	require.Equal(t, 1, billingRepo.calls)
	require.Equal(t, 1, usageRepo.calls)
}

func TestBackgroundResponseSubmitSurvivesDisconnectAndPollsFinalResult(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)}
	tasks := service.NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Minute)
	release := make(chan struct{})
	h := &BackgroundResponseHandler{tasks: tasks}
	h.execute = func(c *gin.Context) {
		body, err := io.ReadAll(c.Request.Body)
		require.NoError(t, err)
		require.True(t, jsonPathBool(body, "background"))
		require.False(t, jsonPathBool(body, "stream"))
		require.True(t, jsonPathBool(body, "store"))
		setOpsSelectedAccount(c, 30, service.PlatformOpenAI)
		c.JSON(http.StatusOK, gin.H{"id": "resp_upstream", "object": "response", "status": service.BackgroundResponseStatusInProgress})
	}
	h.fetchUpstream = func(ctx context.Context, _ *service.BackgroundResponseTaskRecord) (*backgroundUpstreamPollResult, error) {
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return &backgroundUpstreamPollResult{
			statusCode: http.StatusOK,
			body:       []byte(`{"id":"resp_upstream","object":"response","status":"completed","model":"gpt-5.6-terra","output":[],"usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}`),
		}, nil
	}

	router := gin.New()
	router.Use(func(c *gin.Context) {
		groupID := int64(3)
		c.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{ID: 9, UserID: 7, GroupID: &groupID, Group: &service.Group{ID: groupID, Platform: service.PlatformOpenAI}})
		c.Next()
	})
	router.POST("/v1/responses", func(c *gin.Context) {
		require.True(t, h.TrySubmit(c))
	})
	router.GET("/v1/responses/:response_id", h.Get)

	requestCtx, cancelRequest := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.6-terra","input":"reply ok","background":true,"store":true,"stream":true}`)).WithContext(requestCtx)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "3", w.Header().Get("Retry-After"))

	var accepted struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &accepted))
	require.Equal(t, service.BackgroundResponseStatusInProgress, accepted.Status)
	require.Equal(t, "/v1/responses/"+accepted.ID, w.Header().Get("Location"))

	cancelRequest()
	close(release)
	require.Eventually(t, func() bool {
		got, err := tasks.Get(context.Background(), service.BackgroundResponseOwner{UserID: 7, APIKeyID: 9}, accepted.ID)
		return err == nil && got.Status == service.BackgroundResponseStatusCompleted
	}, time.Second, 10*time.Millisecond)

	pollReq := httptest.NewRequest(http.MethodGet, "/v1/responses/"+accepted.ID, nil)
	pollWriter := httptest.NewRecorder()
	router.ServeHTTP(pollWriter, pollReq)
	require.Equal(t, http.StatusOK, pollWriter.Code)
	require.Contains(t, pollWriter.Body.String(), accepted.ID)
	require.NotContains(t, pollWriter.Body.String(), "resp_upstream")
}

func TestBackgroundResponseFinalResponseFromSSEPreservesWebSearchAddedSources(t *testing.T) {
	body := strings.Join([]string{
		`data: {"type":"response.output_item.added","output_index":0,"item":{"id":"ws_1","type":"web_search_call","status":"completed","action":{"type":"search","query":"OpenAI","sources":[{"type":"url","url":"https://developers.openai.com/api/docs/guides/tools-web-search","title":"Web search"}]}}}`,
		"",
		`data: {"type":"response.output_item.done","output_index":1,"item":{"id":"msg_1","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"ok"}]}}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_upstream","object":"response","status":"completed","output":[{"id":"msg_1","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}}`,
		"",
	}, "\n")

	finalResponse, ok := backgroundResponseFinalResponseFromSSE([]byte(body), "resp_bg_test")
	require.True(t, ok)
	require.Equal(t, "web_search_call", gjson.GetBytes(finalResponse, "output.0.type").String())
	require.Equal(t, "https://developers.openai.com/api/docs/guides/tools-web-search", gjson.GetBytes(finalResponse, "output.0.action.sources.0.url").String())
	require.Equal(t, "message", gjson.GetBytes(finalResponse, "output.1.type").String())
	require.Equal(t, int64(2), gjson.GetBytes(finalResponse, "output.#").Int())
}

func TestBackgroundResponseFinalResponseFromSSEPrefersCompletedWebSearchDoneSources(t *testing.T) {
	body := strings.Join([]string{
		`data: {"type":"response.output_item.added","output_index":0,"item":{"id":"ws_1","type":"web_search_call","status":"in_progress"}}`,
		"",
		`data: {"type":"response.output_item.done","output_index":0,"item":{"id":"ws_1","type":"web_search_call","status":"completed","action":{"type":"search","query":"OpenAI","sources":[{"type":"url","url":"https://platform.openai.com/docs/api-reference/responses-streaming","title":"Streaming"}]}}}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_upstream","object":"response","status":"completed","output":[{"id":"msg_1","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}}`,
		"",
	}, "\n")

	finalResponse, ok := backgroundResponseFinalResponseFromSSE([]byte(body), "resp_bg_test")
	require.True(t, ok)
	require.Equal(t, "web_search_call", gjson.GetBytes(finalResponse, "output.0.type").String())
	require.Equal(t, "completed", gjson.GetBytes(finalResponse, "output.0.status").String())
	require.Equal(t, "https://platform.openai.com/docs/api-reference/responses-streaming", gjson.GetBytes(finalResponse, "output.0.action.sources.0.url").String())
	require.Equal(t, "message", gjson.GetBytes(finalResponse, "output.1.type").String())
}

func TestBackgroundResponseStreamingProtocolFailureIsRejectedByOriginalPOST(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)}
	tasks := service.NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Minute)
	h := &BackgroundResponseHandler{tasks: tasks}
	h.execute = func(c *gin.Context) {
		c.Header("Content-Type", "text/event-stream")
		_, _ = c.Writer.Write([]byte("event: response.failed\n"))
		_, _ = c.Writer.Write([]byte(`data: {"type":"response.failed","response":{"error":{"type":"server_error","message":"upstream timeout"}}}` + "\n\n"))
	}

	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{ID: 9, UserID: 7})
		c.Next()
	})
	router.POST("/v1/responses", func(c *gin.Context) { require.True(t, h.TrySubmit(c)) })
	router.GET("/v1/responses/:response_id", h.Get)

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5","background":true,"store":true}`))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadGateway, w.Code)
	require.Empty(t, w.Header().Get("Location"))
	require.Contains(t, w.Body.String(), `"error"`)
	require.Equal(t, "upstream_timeout", gjson.GetBytes(w.Body.Bytes(), "error.code").String())
}

func TestBackgroundResponseTrySubmitRestoresNormalRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := &BackgroundResponseHandler{}
	router := gin.New()
	router.POST("/v1/responses", func(c *gin.Context) {
		require.False(t, h.TrySubmit(c))
		body, err := io.ReadAll(c.Request.Body)
		require.NoError(t, err)
		c.Data(http.StatusOK, "application/json", body)
	})

	original := `{"model":"gpt-5","input":"hello","stream":true}`
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(original))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, original, w.Body.String())
}

func TestBackgroundResponseTrySubmitDoesNotDetachSynchronousHeavyRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := &BackgroundResponseHandler{tasks: service.NewBackgroundResponseTaskService(&backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)})}
	router := gin.New()
	router.POST("/v1/responses", func(c *gin.Context) {
		require.False(t, h.TrySubmit(c))
		body, err := io.ReadAll(c.Request.Body)
		require.NoError(t, err)
		c.Data(http.StatusOK, "application/json", body)
	})

	original := heavyWebSearchRequestBody(false)
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(original))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, original, w.Body.String())
}

func TestBackgroundResponseCancelInProgressTask(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)}
	tasks := service.NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Minute)
	h := &BackgroundResponseHandler{tasks: tasks, heavyQueue: newBackgroundResponseHeavyQueue()}
	h.execute = func(c *gin.Context) {
		setOpsSelectedAccount(c, 30, service.PlatformOpenAI)
		c.JSON(http.StatusOK, gin.H{"id": "resp_upstream", "object": "response", "status": service.BackgroundResponseStatusInProgress})
	}
	h.fetchUpstream = func(ctx context.Context, _ *service.BackgroundResponseTaskRecord) (*backgroundUpstreamPollResult, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{ID: 9, UserID: 7})
		c.Next()
	})
	router.POST("/v1/responses", func(c *gin.Context) { require.True(t, h.TrySubmit(c)) })
	router.POST("/v1/responses/*subpath", func(c *gin.Context) { require.True(t, h.TryCancel(c)) })
	router.GET("/v1/responses/:response_id", h.Get)

	responseID := submitHeavyBackgroundForTest(t, router)
	require.NotEmpty(t, responseID)

	cancelReq := httptest.NewRequest(http.MethodPost, "/v1/responses/"+responseID+"/cancel", nil)
	cancelWriter := httptest.NewRecorder()
	router.ServeHTTP(cancelWriter, cancelReq)
	require.Equal(t, http.StatusOK, cancelWriter.Code)
	require.Contains(t, cancelWriter.Body.String(), `"status":"cancelled"`)

	require.Eventually(t, func() bool {
		got, err := tasks.Get(context.Background(), service.BackgroundResponseOwner{UserID: 7, APIKeyID: 9}, responseID)
		return err == nil && got.Status == service.BackgroundResponseStatusCancelled
	}, time.Second, 10*time.Millisecond)
}

func TestBackgroundResponseRejectsStoreFalse(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)}
	h := &BackgroundResponseHandler{tasks: service.NewBackgroundResponseTaskService(store), execute: func(*gin.Context) {}}
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{ID: 9, UserID: 7})
		c.Next()
	})
	router.POST("/v1/responses", func(c *gin.Context) { require.True(t, h.TrySubmit(c)) })

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5","background":true,"store":false}`))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "store=true")
	require.Empty(t, store.tasks)
}

func TestBackgroundResponseSubmitHeavyWebSearchBackgroundRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)}
	tasks := service.NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Minute)
	bodyCh := make(chan []byte, 1)
	h := &BackgroundResponseHandler{tasks: tasks, heavyQueue: newBackgroundResponseHeavyQueue()}
	h.execute = func(c *gin.Context) {
		body, err := io.ReadAll(c.Request.Body)
		require.NoError(t, err)
		bodyCh <- body
		c.JSON(http.StatusOK, gin.H{
			"id":     "resp_upstream",
			"object": "response",
			"status": service.BackgroundResponseStatusCompleted,
			"model":  "gpt-5.6-sol",
			"output": []gin.H{{
				"id":     "ws_1",
				"type":   "web_search_call",
				"status": service.BackgroundResponseStatusCompleted,
				"action": gin.H{
					"type":  "search",
					"query": "OpenAI",
					"sources": []gin.H{{
						"type":  "url",
						"url":   "https://developers.openai.com/api/docs/guides/tools-web-search",
						"title": "Web search",
					}},
				},
			}},
			"usage": gin.H{"input_tokens": 1, "output_tokens": 2, "total_tokens": 3},
		})
	}

	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{ID: 9, UserID: 7})
		c.Next()
	})
	router.POST("/v1/responses", func(c *gin.Context) { require.True(t, h.TrySubmit(c)) })

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(heavyWebSearchBackgroundRequestBody()))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.Empty(t, w.Header().Get("X-SubtoProxy-Auto-Background"))
	require.Equal(t, "heavy_web_search", w.Header().Get("X-SubtoProxy-Background-Queue"))
	require.Equal(t, "3", w.Header().Get("Retry-After"))

	var accepted struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &accepted))
	require.Equal(t, service.BackgroundResponseStatusCompleted, accepted.Status)
	require.Equal(t, "/v1/responses/"+accepted.ID, w.Header().Get("Location"))

	var executionBody []byte
	require.Eventually(t, func() bool {
		select {
		case executionBody = <-bodyCh:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
	require.True(t, gjson.GetBytes(executionBody, "background").Bool())
	require.False(t, gjson.GetBytes(executionBody, "stream").Bool())
	require.True(t, gjson.GetBytes(executionBody, "store").Bool())
	require.Equal(t, "web_search", gjson.GetBytes(executionBody, "tools.0.type").String())
	require.Equal(t, "high", gjson.GetBytes(executionBody, "tools.0.search_context_size").String())
	require.Equal(t, "required", gjson.GetBytes(executionBody, "tool_choice").String())
	require.Equal(t, "web_search_call.action.sources", gjson.GetBytes(executionBody, "include.0").String())
	require.Equal(t, "high", gjson.GetBytes(executionBody, "reasoning.effort").String())
	require.True(t, gjson.GetBytes(executionBody, "text.format.strict").Bool())

	require.Eventually(t, func() bool {
		got, err := tasks.Get(context.Background(), service.BackgroundResponseOwner{UserID: 7, APIKeyID: 9}, accepted.ID)
		return err == nil && got.Status == service.BackgroundResponseStatusCompleted
	}, time.Second, 10*time.Millisecond)
}

func TestBackgroundResponseDoesNotAutoSubmitLightWebSearchRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := &BackgroundResponseHandler{tasks: service.NewBackgroundResponseTaskService(&backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)})}
	router := gin.New()
	router.POST("/v1/responses", func(c *gin.Context) {
		require.False(t, h.TrySubmit(c))
		body, err := io.ReadAll(c.Request.Body)
		require.NoError(t, err)
		c.Data(http.StatusOK, "application/json", body)
	})

	original := strings.Replace(heavyWebSearchRequestBody(false), `"max_output_tokens":8000`, `"max_output_tokens":1200`, 1)
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(original))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, original, w.Body.String())
}

func TestBackgroundResponseHeavyQueueSerializesByUser(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)}
	tasks := service.NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Minute)
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	h := &BackgroundResponseHandler{tasks: tasks, heavyQueue: newBackgroundResponseHeavyQueue()}
	h.execute = func(c *gin.Context) {
		started <- struct{}{}
		<-release
		c.JSON(http.StatusOK, gin.H{
			"id":     "resp_upstream",
			"object": "response",
			"status": service.BackgroundResponseStatusCompleted,
			"model":  "gpt-5.6-sol",
			"output": []gin.H{{"type": "message", "role": "assistant", "content": []gin.H{{"type": "output_text", "text": "ok"}}}},
		})
	}

	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{ID: 9, UserID: 7})
		c.Next()
	})
	router.POST("/v1/responses", func(c *gin.Context) { require.True(t, h.TrySubmit(c)) })

	type submitResult struct {
		id   string
		code int
	}
	submitAsync := func() <-chan submitResult {
		done := make(chan submitResult, 1)
		go func() {
			req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(heavyWebSearchBackgroundRequestBody()))
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			done <- submitResult{id: gjson.GetBytes(w.Body.Bytes(), "id").String(), code: w.Code}
		}()
		return done
	}

	firstDone := submitAsync()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first heavy background request did not start")
	}

	secondDone := submitAsync()
	require.Eventually(t, func() bool { return h.heavyQueue.waitingCount() == 1 }, time.Second, 10*time.Millisecond)
	select {
	case <-started:
		t.Fatal("second create started before first released the per-user queue")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	first := <-firstDone
	second := <-secondDone
	require.Equal(t, http.StatusOK, first.code)
	require.Equal(t, http.StatusOK, second.code)
	require.NotEmpty(t, first.id)
	require.NotEmpty(t, second.id)
	require.NotEqual(t, first.id, second.id)
	for _, responseID := range []string{first.id, second.id} {
		got, err := tasks.Get(context.Background(), service.BackgroundResponseOwner{UserID: 7, APIKeyID: 9}, responseID)
		require.NoError(t, err)
		require.Equal(t, service.BackgroundResponseStatusCompleted, got.Status)
	}
}

func TestBackgroundResponseHeavyQueueFullAndCancel(t *testing.T) {
	q := newBackgroundResponseHeavyQueueWithOptions(1, 1, 1)
	firstRelease, _, err := q.acquire(context.Background(), 7)
	require.NoError(t, err)
	defer firstRelease()

	secondCtx, secondCancel := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() {
		release, _, acquireErr := q.acquire(secondCtx, 7)
		if release != nil {
			release()
		}
		secondDone <- acquireErr
	}()
	require.Eventually(t, func() bool { return q.waitingCount() == 1 }, time.Second, 10*time.Millisecond)

	_, _, err = q.acquire(context.Background(), 7)
	require.ErrorIs(t, err, errBackgroundHeavyQueueFull)

	secondCancel()
	require.ErrorIs(t, <-secondDone, context.Canceled)
	require.Eventually(t, func() bool { return q.waitingCount() == 0 }, time.Second, 10*time.Millisecond)
}

func TestResponsesHeavySchedulerCancelsAndSkipsUserSlot(t *testing.T) {
	oldQueue := defaultBackgroundResponseHeavyQueue
	defaultBackgroundResponseHeavyQueue = newBackgroundResponseHeavyQueueWithOptions(1, 1, 4)
	defer func() { defaultBackgroundResponseHeavyQueue = oldQueue }()

	gin.SetMode(gin.TestMode)
	requestCtx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(heavyWebSearchRequestBody(false))).WithContext(requestCtx)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = req

	h := &OpenAIGatewayHandler{}
	release, ok := h.acquireResponsesHeavyWebSearchSlot(c, []byte(heavyWebSearchRequestBody(false)), 7, 9, zap.NewNop())
	require.True(t, ok)
	require.NotNil(t, release)
	skip, _ := c.Get(responsesSkipUserSlotKey)
	require.True(t, skip.(bool))

	streamStarted := false
	userRelease, acquired := h.acquireResponsesUserSlot(c, 7, 1, false, &streamStarted, zap.NewNop())
	require.True(t, acquired)
	require.Nil(t, userRelease)

	cancel()
	require.Eventually(t, func() bool {
		nextRelease, _, err := defaultBackgroundResponseHeavyQueue.acquire(context.Background(), 7)
		if err != nil {
			return false
		}
		nextRelease()
		return true
	}, time.Second, 10*time.Millisecond)
	release()
	require.Equal(t, int64(0), responsesOrphanActiveTasksForTest())
}

func TestResponsesIngressCountsEntryPollAndCancelSeparately(t *testing.T) {
	resetResponsesIngressMetricsForTest()
	gin.SetMode(gin.TestMode)
	store := &backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)}
	tasks := service.NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Minute)
	h := &BackgroundResponseHandler{tasks: tasks}
	h.execute = func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"id": "resp_upstream", "object": "response", "status": service.BackgroundResponseStatusCompleted, "output": []gin.H{}})
	}
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{ID: 9, UserID: 7})
		c.Next()
	})
	router.POST("/v1/responses", func(c *gin.Context) { require.True(t, h.TrySubmit(c)) })
	router.POST("/v1/responses/*subpath", func(c *gin.Context) { require.True(t, h.TryCancel(c)) })
	router.GET("/v1/responses/:response_id", h.Get)

	id := submitHeavyBackgroundForTest(t, router)
	pollReq := httptest.NewRequest(http.MethodGet, "/v1/responses/"+id, nil)
	pollWriter := httptest.NewRecorder()
	router.ServeHTTP(pollWriter, pollReq)
	require.Equal(t, http.StatusOK, pollWriter.Code)

	cancelReq := httptest.NewRequest(http.MethodPost, "/v1/responses/"+id+"/cancel", nil)
	cancelWriter := httptest.NewRecorder()
	router.ServeHTTP(cancelWriter, cancelReq)
	require.Equal(t, http.StatusOK, cancelWriter.Code)

	require.Equal(t, uint64(1), responsesIngressKindCountForTest("responses_post"))
	require.Equal(t, uint64(1), responsesIngressKindCountForTest("background_poll"))
	require.Equal(t, uint64(1), responsesIngressKindCountForTest("background_cancel"))
	require.Equal(t, uint64(0), responsesIngressKindCountForTest("internal_background_execute"))
}

func TestBackgroundResponseFailureClassification(t *testing.T) {
	rateLimited := classifyBackgroundResponseFailure(http.StatusTooManyRequests, []byte(`{"error":{"message":"slow down"}}`), json.RawMessage(`{"type":"rate_limit_error","message":"slow down"}`))
	require.Equal(t, "upstream_rate_limited", gjson.GetBytes(rateLimited, "type").String())
	require.Equal(t, "slow down", gjson.GetBytes(rateLimited, "message").String())

	eof := classifyBackgroundResponseFailure(http.StatusBadGateway, []byte("unexpected EOF"), backgroundResponseErrorPayload("api_error", "unexpected EOF"))
	require.Equal(t, "upstream_unexpected_eof", gjson.GetBytes(eof, "type").String())

	reset := classifyBackgroundResponseFailure(http.StatusBadGateway, []byte("connection reset by peer"), backgroundResponseErrorPayload("api_error", "connection reset by peer"))
	require.Equal(t, "upstream_connection_reset", gjson.GetBytes(reset, "type").String())

	deadline := classifyBackgroundResponseFailure(http.StatusGatewayTimeout, []byte("context deadline exceeded"), backgroundResponseErrorPayload("api_error", "context deadline exceeded"))
	require.Equal(t, "proxy_task_deadline_exceeded", gjson.GetBytes(deadline, "type").String())
}

func TestBackgroundResponseNativeCreatePollsPastTwentyMinutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)}
	tasks := service.NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Hour)
	allowPoll := make(chan struct{})
	h := &BackgroundResponseHandler{tasks: tasks, heavyQueue: newBackgroundResponseHeavyQueue(), pollInterval: time.Millisecond}
	h.execute = func(c *gin.Context) {
		setOpsSelectedAccount(c, 30, service.PlatformOpenAI)
		body, err := io.ReadAll(c.Request.Body)
		require.NoError(t, err)
		require.True(t, gjson.GetBytes(body, "background").Bool())
		require.True(t, gjson.GetBytes(body, "store").Bool())
		require.False(t, gjson.GetBytes(body, "stream").Bool())
		c.Header("x-request-id", "req_create")
		c.JSON(http.StatusOK, gin.H{"id": "resp_up_long", "object": "response", "status": service.BackgroundResponseStatusInProgress, "model": "gpt-5.6-sol"})
	}
	h.fetchUpstream = func(ctx context.Context, task *service.BackgroundResponseTaskRecord) (*backgroundUpstreamPollResult, error) {
		select {
		case <-allowPoll:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return &backgroundUpstreamPollResult{statusCode: http.StatusOK, upstreamRequestID: "req_poll", body: []byte(`{"id":"resp_up_long","object":"response","status":"completed","model":"gpt-5.6-sol","output":[],"usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}`)}, nil
	}

	router := backgroundResponseTestRouter(h)
	id := submitHeavyBackgroundForTest(t, router)
	store.mu.Lock()
	store.tasks[id].CreatedAt = time.Now().Add(-1300 * time.Second).UTC().Unix()
	store.mu.Unlock()
	close(allowPoll)

	require.Eventually(t, func() bool {
		got, err := tasks.Get(context.Background(), service.BackgroundResponseOwner{UserID: 7, APIKeyID: 9}, id)
		return err == nil && got.Status == service.BackgroundResponseStatusCompleted && got.ElapsedSeconds >= 1300
	}, time.Second, 10*time.Millisecond)
}

func TestBackgroundResponseNativeSSEIDIsNotPolled(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)}
	tasks := service.NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Hour)
	h := &BackgroundResponseHandler{tasks: tasks, heavyQueue: newBackgroundResponseHeavyQueue(), pollInterval: time.Millisecond}
	h.execute = func(c *gin.Context) {
		c.Status(http.StatusBadGateway)
		_, _ = c.Writer.Write([]byte(`data: {"type":"response.created","response":{"id":"resp_up_recover","status":"in_progress"}}` + "\n\nunexpected EOF"))
	}
	fetchCalls := 0
	h.fetchUpstream = func(_ context.Context, task *service.BackgroundResponseTaskRecord) (*backgroundUpstreamPollResult, error) {
		fetchCalls++
		return nil, nil
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(heavyWebSearchBackgroundRequestBody()))
	w := httptest.NewRecorder()
	backgroundResponseTestRouter(h).ServeHTTP(w, req)
	require.Equal(t, http.StatusBadGateway, w.Code)
	require.Equal(t, "upstream_background_unsupported", gjson.GetBytes(w.Body.Bytes(), "error.code").String())
	require.Empty(t, w.Header().Get("Location"))
	require.Zero(t, fetchCalls)
}

func TestBackgroundResponseUnsupportedNativeBackgroundFailsWithoutStreamingFallback(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)}
	tasks := service.NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Hour)
	var bodies [][]byte
	h := &BackgroundResponseHandler{tasks: tasks, heavyQueue: newBackgroundResponseHeavyQueue(), pollInterval: time.Millisecond}
	h.execute = func(c *gin.Context) {
		body, err := io.ReadAll(c.Request.Body)
		require.NoError(t, err)
		bodies = append(bodies, append([]byte(nil), body...))
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Unsupported parameter: background"})
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(heavyWebSearchBackgroundRequestBody()))
	w := httptest.NewRecorder()
	backgroundResponseTestRouter(h).ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Equal(t, "upstream_background_unsupported", gjson.GetBytes(w.Body.Bytes(), "error.code").String())
	require.False(t, gjson.GetBytes(w.Body.Bytes(), "error.retryable").Bool())
	require.Empty(t, w.Header().Get("Location"))
	require.Empty(t, gjson.GetBytes(w.Body.Bytes(), "id").String())
	require.Len(t, bodies, 1)
	require.True(t, gjson.GetBytes(bodies[0], "background").Bool())
	require.True(t, gjson.GetBytes(bodies[0], "store").Bool())
	require.False(t, gjson.GetBytes(bodies[0], "stream").Bool())
}

func TestBackgroundResponseUnsupportedNativeBackgroundFromMappedGatewayErrorFailsWithoutFallback(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)}
	tasks := service.NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Hour)
	var bodies [][]byte
	h := &BackgroundResponseHandler{tasks: tasks, heavyQueue: newBackgroundResponseHeavyQueue(), pollInterval: time.Millisecond}
	h.execute = func(c *gin.Context) {
		body, err := io.ReadAll(c.Request.Body)
		require.NoError(t, err)
		bodies = append(bodies, append([]byte(nil), body...))
		service.SetOpsUpstreamError(c, http.StatusBadRequest, "Unsupported parameter: background", `{"detail":"Unsupported parameter: background"}`)
		c.JSON(http.StatusBadGateway, gin.H{"error": gin.H{"type": "upstream_error", "message": "Upstream request failed"}})
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(heavyWebSearchBackgroundRequestBody()))
	w := httptest.NewRecorder()
	backgroundResponseTestRouter(h).ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Equal(t, "upstream_background_unsupported", gjson.GetBytes(w.Body.Bytes(), "error.code").String())
	require.Empty(t, w.Header().Get("Location"))
	require.Len(t, bodies, 1)
	require.True(t, gjson.GetBytes(bodies[0], "background").Bool())
}

func TestBackgroundResponseTemporaryNotFoundRecoversToCompleted(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)}
	tasks := service.NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Hour)
	pollCalls := 0
	h := &BackgroundResponseHandler{tasks: tasks, heavyQueue: newBackgroundResponseHeavyQueue(), pollInterval: time.Millisecond}
	h.execute = func(c *gin.Context) {
		setOpsSelectedAccount(c, 30, service.PlatformOpenAI)
		c.JSON(http.StatusOK, gin.H{"id": "resp_up_eventual", "object": "response", "status": service.BackgroundResponseStatusInProgress})
	}
	h.fetchUpstream = func(_ context.Context, task *service.BackgroundResponseTaskRecord) (*backgroundUpstreamPollResult, error) {
		pollCalls++
		require.Equal(t, int64(30), task.UpstreamAccountID)
		require.Equal(t, "resp_up_eventual", task.UpstreamResponseID)
		if pollCalls <= 2 {
			return &backgroundUpstreamPollResult{
				statusCode:        http.StatusNotFound,
				body:              []byte(`{"detail":"Not Found"}`),
				upstreamRequestID: "req_not_found",
			}, nil
		}
		return &backgroundUpstreamPollResult{
			statusCode:        http.StatusOK,
			upstreamRequestID: "req_completed",
			body:              []byte(`{"id":"resp_up_eventual","object":"response","status":"completed","model":"gpt-5.6-sol","output":[],"usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}`),
		}, nil
	}

	id := submitHeavyBackgroundForTest(t, backgroundResponseTestRouter(h))
	require.Eventually(t, func() bool {
		got, err := tasks.Get(context.Background(), service.BackgroundResponseOwner{UserID: 7, APIKeyID: 9}, id)
		return err == nil && got.Status == service.BackgroundResponseStatusCompleted
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, 3, pollCalls)
}

func TestBackgroundResponsePermanentNotFoundHasStructuredTerminalError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)}
	tasks := service.NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Hour)
	pollCalls := 0
	h := &BackgroundResponseHandler{tasks: tasks, heavyQueue: newBackgroundResponseHeavyQueue(), pollInterval: time.Millisecond, notFoundGrace: 3 * time.Millisecond}
	h.execute = func(c *gin.Context) {
		setOpsSelectedAccount(c, 30, service.PlatformOpenAI)
		c.JSON(http.StatusOK, gin.H{"id": "resp_up_missing", "object": "response", "status": service.BackgroundResponseStatusInProgress})
	}
	h.fetchUpstream = func(_ context.Context, _ *service.BackgroundResponseTaskRecord) (*backgroundUpstreamPollResult, error) {
		pollCalls++
		return &backgroundUpstreamPollResult{
			statusCode:        http.StatusNotFound,
			body:              []byte(`{"detail":"Not Found"}`),
			upstreamRequestID: "req_permanent_404",
		}, nil
	}

	id := submitHeavyBackgroundForTest(t, backgroundResponseTestRouter(h))
	var got *service.BackgroundResponseTaskRecord
	require.Eventually(t, func() bool {
		var err error
		got, err = tasks.Get(context.Background(), service.BackgroundResponseOwner{UserID: 7, APIKeyID: 9}, id)
		return err == nil && got.Status == service.BackgroundResponseStatusFailed
	}, time.Second, 10*time.Millisecond)
	require.GreaterOrEqual(t, pollCalls, 2)
	require.Equal(t, "upstream_response_not_found", gjson.GetBytes(got.Error, "type").String())
	require.Equal(t, "upstream_response_not_found", gjson.GetBytes(got.Error, "code").String())
	require.Equal(t, "Not Found", gjson.GetBytes(got.Error, "detail").String())
	require.False(t, gjson.GetBytes(got.Error, "retryable").Bool())
	require.Equal(t, id, gjson.GetBytes(got.Error, "proxy_response_id").String())
	require.Equal(t, "resp_up_missing", gjson.GetBytes(got.Error, "upstream_response_id").String())
	require.Equal(t, "req_permanent_404", gjson.GetBytes(got.Error, "provider_request_id").String())
}

func TestBackgroundResponseAuthScopeMismatchIsDistinctFromNotFound(t *testing.T) {
	for _, statusCode := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(strconv.Itoa(statusCode), func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			store := &backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)}
			tasks := service.NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Hour)
			h := &BackgroundResponseHandler{tasks: tasks, heavyQueue: newBackgroundResponseHeavyQueue(), pollInterval: time.Millisecond}
			h.execute = func(c *gin.Context) {
				setOpsSelectedAccount(c, 30, service.PlatformOpenAI)
				c.JSON(http.StatusOK, gin.H{"id": "resp_up_scope", "object": "response", "status": service.BackgroundResponseStatusInProgress})
			}
			h.fetchUpstream = func(_ context.Context, _ *service.BackgroundResponseTaskRecord) (*backgroundUpstreamPollResult, error) {
				return &backgroundUpstreamPollResult{
					statusCode:        statusCode,
					body:              []byte(`{"error":{"message":"project scope mismatch"}}`),
					upstreamRequestID: "req_scope",
				}, nil
			}

			id := submitHeavyBackgroundForTest(t, backgroundResponseTestRouter(h))
			require.Eventually(t, func() bool {
				got, err := tasks.Get(context.Background(), service.BackgroundResponseOwner{UserID: 7, APIKeyID: 9}, id)
				return err == nil && got.Status == service.BackgroundResponseStatusFailed &&
					gjson.GetBytes(got.Error, "code").String() == "upstream_auth_scope_mismatch"
			}, time.Second, 10*time.Millisecond)
		})
	}
}

func TestBackgroundResponseMissingMappingHasStructuredError(t *testing.T) {
	store := &backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)}
	tasks := service.NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Hour)
	now := time.Now().UTC().Unix()
	task := &service.BackgroundResponseTaskRecord{
		ID:                    "resp_bg_missing_mapping",
		UserID:                7,
		APIKeyID:              9,
		Model:                 "gpt-5.6-sol",
		Status:                service.BackgroundResponseStatusInProgress,
		CreatedAt:             now,
		ExpiresAt:             now + 3600,
		UpstreamExecutionMode: "native_background",
		UpstreamPollable:      true,
	}
	require.NoError(t, store.Save(context.Background(), task, time.Hour))
	h := &BackgroundResponseHandler{tasks: tasks, pollInterval: time.Millisecond}
	h.pollNativeBackground(task.ID, service.BackgroundResponseOwner{UserID: 7, APIKeyID: 9})

	got, err := tasks.Get(context.Background(), service.BackgroundResponseOwner{UserID: 7, APIKeyID: 9}, task.ID)
	require.NoError(t, err)
	require.Equal(t, service.BackgroundResponseStatusFailed, got.Status)
	require.Equal(t, "proxy_mapping_missing", gjson.GetBytes(got.Error, "code").String())
	require.Equal(t, task.ID, gjson.GetBytes(got.Error, "proxy_response_id").String())
}

func TestBackgroundResponseErrorEnvelopeContainsRequiredAuditFields(t *testing.T) {
	failedAt := time.Now().UTC().Unix()
	task := &service.BackgroundResponseTaskRecord{
		ID:                 "resp_bg_audit",
		CreatedAt:          failedAt - 123,
		FailedAt:           &failedAt,
		ElapsedSeconds:     123,
		UpstreamResponseID: "resp_up_audit",
		UpstreamRequestID:  "req_audit",
	}
	payload := backgroundResponseAttachAuditFields(task, json.RawMessage(`{"type":"server_error","message":"boom"}`), "")
	require.Equal(t, "server_error", gjson.GetBytes(payload, "type").String())
	require.Equal(t, "server_error", gjson.GetBytes(payload, "code").String())
	require.Equal(t, "boom", gjson.GetBytes(payload, "detail").String())
	require.True(t, gjson.GetBytes(payload, "retryable").Exists())
	require.Equal(t, task.ID, gjson.GetBytes(payload, "proxy_response_id").String())
	require.Equal(t, task.UpstreamResponseID, gjson.GetBytes(payload, "upstream_response_id").String())
	require.Equal(t, task.UpstreamRequestID, gjson.GetBytes(payload, "provider_request_id").String())
	require.Equal(t, int64(123), gjson.GetBytes(payload, "elapsed_seconds").Int())
}

func TestBackgroundResponseUpstreamErrorTextRedactsCredentials(t *testing.T) {
	raw := json.RawMessage(`{"error":{"message":"Authorization: Bearer sk-live-secret api_key=top-secret token=abc123"}}`)
	message := backgroundResponseErrorMessage(raw, "")
	require.NotContains(t, message, "sk-live-secret")
	require.NotContains(t, message, "top-secret")
	require.NotContains(t, message, "abc123")
	require.Contains(t, message, "Authorization: ***")
	require.Contains(t, message, "api_key=***")
	require.Contains(t, message, "token=***")

	payload := backgroundResponseAuditedErrorPayload(
		&service.BackgroundResponseTaskRecord{ID: "resp_bg_redact", CreatedAt: time.Now().Add(-time.Second).Unix()},
		"upstream_task_failed",
		message,
		"req_redact",
	)
	require.NotContains(t, string(payload), "sk-live-secret")
	require.NotContains(t, string(payload), "top-secret")
	require.NotContains(t, string(payload), "abc123")
}

func TestBackgroundResponseEOFBeforeIDDoesNotReplayCreate(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)}
	tasks := service.NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Hour)
	var seenKeys []string
	h := &BackgroundResponseHandler{tasks: tasks, heavyQueue: newBackgroundResponseHeavyQueue(), pollInterval: time.Millisecond}
	h.execute = func(c *gin.Context) {
		seenKeys = append(seenKeys, c.GetHeader("Idempotency-Key"))
		setOpsSelectedAccount(c, 30, service.PlatformOpenAI)
		c.Status(http.StatusBadGateway)
		_, _ = c.Writer.Write([]byte("unexpected EOF"))
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(heavyWebSearchBackgroundRequestBody()))
	req.Header.Set("Idempotency-Key", "same-key")
	w := httptest.NewRecorder()
	backgroundResponseTestRouter(h).ServeHTTP(w, req)
	require.Equal(t, http.StatusBadGateway, w.Code)
	require.Equal(t, []string{"same-key"}, seenKeys)
	require.Equal(t, "upstream_unexpected_eof", gjson.GetBytes(w.Body.Bytes(), "error.code").String())
	require.True(t, gjson.GetBytes(w.Body.Bytes(), "error.retryable").Bool())
	require.Empty(t, w.Header().Get("Location"))
}

func TestBackgroundResponseStartupRecoveryPollsMappedUpstreamTask(t *testing.T) {
	store := &backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)}
	tasks := service.NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Hour)
	now := time.Now().UTC().Unix()
	task := &service.BackgroundResponseTaskRecord{
		ID:                    "resp_bg_restart",
		UserID:                7,
		APIKeyID:              9,
		Model:                 "gpt-5.6-sol",
		Status:                service.BackgroundResponseStatusInProgress,
		CreatedAt:             now,
		ExpiresAt:             now + 3600,
		UpstreamAccountID:     123,
		UpstreamResponseID:    "resp_up_restart",
		UpstreamExecutionMode: "native_background",
		UpstreamPollable:      true,
	}
	require.NoError(t, store.Save(context.Background(), task, time.Hour))
	h := &BackgroundResponseHandler{tasks: tasks, pollInterval: time.Millisecond}
	h.fetchUpstream = func(_ context.Context, task *service.BackgroundResponseTaskRecord) (*backgroundUpstreamPollResult, error) {
		require.Equal(t, "resp_up_restart", task.UpstreamResponseID)
		return &backgroundUpstreamPollResult{statusCode: http.StatusOK, body: []byte(`{"id":"resp_up_restart","object":"response","status":"completed","model":"gpt-5.6-sol","output":[]}`)}, nil
	}
	h.resumePolling(task)
	got, err := tasks.Get(context.Background(), service.BackgroundResponseOwner{UserID: 7, APIKeyID: 9}, task.ID)
	require.NoError(t, err)
	require.Equal(t, service.BackgroundResponseStatusCompleted, got.Status)
}

func TestBackgroundResponseRecoveryClassifiesLegacyStreamingTaskAsNonPollable(t *testing.T) {
	store := &backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)}
	tasks := service.NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Hour)
	now := time.Now().UTC()
	task := &service.BackgroundResponseTaskRecord{
		ID:                    "resp_bg_legacy_stream",
		UserID:                7,
		APIKeyID:              9,
		Model:                 "gpt-5.6-sol",
		Status:                service.BackgroundResponseStatusInProgress,
		CreatedAt:             now.Add(-10 * time.Minute).Unix(),
		DeadlineAt:            now.Add(time.Hour).Unix(),
		ExpiresAt:             now.Add(time.Hour).Unix(),
		UpstreamAccountID:     30,
		UpstreamResponseID:    "resp_stream_only",
		UpstreamExecutionMode: "streaming_fallback",
		UpstreamPollable:      false,
	}
	require.NoError(t, store.Save(context.Background(), task, time.Hour))
	h := &BackgroundResponseHandler{tasks: tasks}
	h.reconcileActiveBackgroundTasks()

	got, err := tasks.GetByID(context.Background(), task.ID)
	require.NoError(t, err)
	require.Equal(t, service.BackgroundResponseStatusFailed, got.Status)
	require.Equal(t, "legacy_non_pollable", gjson.GetBytes(got.Error, "code").String())
}

func TestBackgroundResponsePollUsesPersistedDeadlineAfterRestart(t *testing.T) {
	store := &backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)}
	tasks := service.NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Hour)
	now := time.Now().UTC()
	task := &service.BackgroundResponseTaskRecord{
		ID:                    "resp_bg_expired_deadline",
		UserID:                7,
		APIKeyID:              9,
		Model:                 "gpt-5.6-sol",
		Status:                service.BackgroundResponseStatusInProgress,
		CreatedAt:             now.Add(-20 * time.Minute).Unix(),
		DeadlineAt:            now.Add(-time.Second).Unix(),
		ExpiresAt:             now.Add(time.Hour).Unix(),
		UpstreamAccountID:     30,
		UpstreamResponseID:    "resp_up_expired",
		UpstreamExecutionMode: service.BackgroundResponseExecutionModeNative,
		UpstreamPollable:      true,
	}
	require.NoError(t, store.Save(context.Background(), task, time.Hour))
	fetchCalls := 0
	h := &BackgroundResponseHandler{tasks: tasks, pollInterval: time.Millisecond}
	h.fetchUpstream = func(_ context.Context, _ *service.BackgroundResponseTaskRecord) (*backgroundUpstreamPollResult, error) {
		fetchCalls++
		return nil, nil
	}
	h.pollNativeBackground(task.ID, service.BackgroundResponseOwner{UserID: 7, APIKeyID: 9})

	got, err := tasks.GetByID(context.Background(), task.ID)
	require.NoError(t, err)
	require.Equal(t, service.BackgroundResponseStatusFailed, got.Status)
	require.Equal(t, "proxy_task_deadline_exceeded", gjson.GetBytes(got.Error, "code").String())
	require.Zero(t, fetchCalls)
}

func TestBackgroundResponseWorkerLeasePreventsDuplicateMultiInstancePolling(t *testing.T) {
	store := &backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)}
	tasks1 := service.NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Hour)
	tasks2 := service.NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Hour)
	now := time.Now().UTC().Unix()
	task := &service.BackgroundResponseTaskRecord{
		ID:                    "resp_bg_leased",
		UserID:                7,
		APIKeyID:              9,
		Model:                 "gpt-5.6-sol",
		Status:                service.BackgroundResponseStatusInProgress,
		CreatedAt:             now,
		ExpiresAt:             now + 3600,
		UpstreamAccountID:     30,
		UpstreamResponseID:    "resp_up_leased",
		UpstreamExecutionMode: service.BackgroundResponseExecutionModeNative,
		UpstreamPollable:      true,
	}
	require.NoError(t, store.Save(context.Background(), task, time.Hour))

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var callsMu sync.Mutex
	pollCalls := 0
	fetch := func(ctx context.Context, _ *service.BackgroundResponseTaskRecord) (*backgroundUpstreamPollResult, error) {
		callsMu.Lock()
		pollCalls++
		callsMu.Unlock()
		select {
		case started <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return &backgroundUpstreamPollResult{
			statusCode: http.StatusOK,
			body:       []byte(`{"id":"resp_up_leased","object":"response","status":"completed","model":"gpt-5.6-sol","output":[]}`),
		}, nil
	}
	h1 := &BackgroundResponseHandler{tasks: tasks1, pollInterval: time.Millisecond, fetchUpstream: fetch}
	h2 := &BackgroundResponseHandler{tasks: tasks2, pollInterval: time.Millisecond, fetchUpstream: fetch}
	owner := service.BackgroundResponseOwner{UserID: 7, APIKeyID: 9}
	go h1.pollNativeBackground(task.ID, owner)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first worker did not start polling")
	}
	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		h2.pollNativeBackground(task.ID, owner)
	}()
	time.Sleep(20 * time.Millisecond)
	callsMu.Lock()
	require.Equal(t, 1, pollCalls)
	callsMu.Unlock()
	close(release)

	require.Eventually(t, func() bool {
		got, err := tasks1.Get(context.Background(), owner, task.ID)
		return err == nil && got.Status == service.BackgroundResponseStatusCompleted
	}, time.Second, 10*time.Millisecond)
	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("peer worker did not exit after observing terminal task")
	}
}

func TestBackgroundResponseWorkerTakesOverAfterPeerLeaseExpires(t *testing.T) {
	store := &backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)}
	tasks := service.NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Hour)
	now := time.Now().UTC()
	task := &service.BackgroundResponseTaskRecord{
		ID:                    "resp_bg_takeover",
		UserID:                7,
		APIKeyID:              9,
		Model:                 "gpt-5.6-sol",
		Status:                service.BackgroundResponseStatusInProgress,
		CreatedAt:             now.Unix(),
		DeadlineAt:            now.Add(time.Hour).Unix(),
		ExpiresAt:             now.Add(time.Hour).Unix(),
		UpstreamAccountID:     30,
		UpstreamResponseID:    "resp_up_takeover",
		UpstreamExecutionMode: service.BackgroundResponseExecutionModeNative,
		UpstreamPollable:      true,
	}
	require.NoError(t, store.Save(context.Background(), task, time.Hour))
	acquired, err := store.AcquireLease(context.Background(), task.ID, "dead-peer", 35*time.Millisecond)
	require.NoError(t, err)
	require.True(t, acquired)

	pollCalls := 0
	h := &BackgroundResponseHandler{tasks: tasks, pollInterval: 5 * time.Millisecond}
	h.fetchUpstream = func(_ context.Context, _ *service.BackgroundResponseTaskRecord) (*backgroundUpstreamPollResult, error) {
		pollCalls++
		return &backgroundUpstreamPollResult{
			statusCode: http.StatusOK,
			body:       []byte(`{"id":"resp_up_takeover","object":"response","status":"completed","model":"gpt-5.6-sol","output":[]}`),
		}, nil
	}

	startedAt := time.Now()
	h.pollNativeBackground(task.ID, service.BackgroundResponseOwner{UserID: 7, APIKeyID: 9})
	require.GreaterOrEqual(t, time.Since(startedAt), 30*time.Millisecond)
	require.Equal(t, 1, pollCalls)
	got, err := tasks.GetByID(context.Background(), task.ID)
	require.NoError(t, err)
	require.Equal(t, service.BackgroundResponseStatusCompleted, got.Status)
}

func TestBackgroundResponseCreate429StoresClassifiedAuditedFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)}
	tasks := service.NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Hour)
	h := &BackgroundResponseHandler{tasks: tasks, heavyQueue: newBackgroundResponseHeavyQueue(), pollInterval: time.Millisecond}
	h.execute = func(c *gin.Context) {
		c.Header("x-request-id", "req_429")
		c.Header("Retry-After", "12")
		c.JSON(http.StatusTooManyRequests, gin.H{"error": gin.H{"type": "rate_limit_error", "message": "slow down"}})
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(heavyWebSearchBackgroundRequestBody()))
	w := httptest.NewRecorder()
	backgroundResponseTestRouter(h).ServeHTTP(w, req)
	require.Equal(t, http.StatusTooManyRequests, w.Code)
	require.Equal(t, "12", w.Header().Get("Retry-After"))
	require.Equal(t, "upstream_rate_limited", gjson.GetBytes(w.Body.Bytes(), "error.type").String())
	require.Equal(t, "req_429", gjson.GetBytes(w.Body.Bytes(), "error.upstream_request_id").String())
	require.Equal(t, "12", gjson.GetBytes(w.Body.Bytes(), "error.retry_after").String())
	require.True(t, gjson.GetBytes(w.Body.Bytes(), "error.failed_at").Int() > 0)
}

func TestBackgroundResponseIdempotencyReplayDoesNotStartDuplicateWorker(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)}
	tasks := service.NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Hour)
	started := 0
	h := &BackgroundResponseHandler{tasks: tasks, heavyQueue: newBackgroundResponseHeavyQueue(), pollInterval: time.Millisecond}
	h.execute = func(c *gin.Context) {
		started++
		c.JSON(http.StatusOK, gin.H{"id": "resp_up_once", "object": "response", "status": service.BackgroundResponseStatusCompleted, "output": []gin.H{}})
	}
	router := backgroundResponseTestRouter(h)
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(heavyWebSearchBackgroundRequestBody()))
		req.Header.Set("Idempotency-Key", "replay-key")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code)
	}
	require.Eventually(t, func() bool { return started == 1 }, time.Second, 10*time.Millisecond)
}

func backgroundResponseTestRouter(h *BackgroundResponseHandler) http.Handler {
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{ID: 9, UserID: 7})
		c.Next()
	})
	router.POST("/v1/responses", func(c *gin.Context) {
		if !h.TrySubmit(c) {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "not handled"})
		}
	})
	router.GET("/v1/responses/:response_id", h.Get)
	return router
}

func submitHeavyBackgroundForTest(t *testing.T, router http.Handler) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(heavyWebSearchBackgroundRequestBody()))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var accepted struct {
		ID string `json:"id"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &accepted))
	require.NotEmpty(t, accepted.ID)
	return accepted.ID
}

func heavyWebSearchRequestBody(background bool) string {
	backgroundField := ""
	if background {
		backgroundField = `,"background":true`
	}
	return `{
		"model":"gpt-5.6-sol",
		"stream":false,
		"store":false,
		"reasoning":{"effort":"high"},
		"tools":[{"type":"web_search","search_context_size":"high"}],
		"tool_choice":"required",
		"include":["web_search_call.action.sources"],
		"text":{"format":{"type":"json_schema","name":"answer","strict":true,"schema":{"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"],"additionalProperties":false}}},
		"max_output_tokens":8000,
		"input":"Return strict JSON using a consulted source."` + backgroundField + `
	}`
}

func heavyWebSearchBackgroundRequestBody() string {
	return strings.Replace(heavyWebSearchRequestBody(true), `"store":false`, `"store":true`, 1)
}

func jsonPathBool(body []byte, path string) bool {
	var value map[string]any
	if json.Unmarshal(body, &value) != nil {
		return false
	}
	result, _ := value[path].(bool)
	return result
}
