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

func backgroundResponseHandlerMemoryIdempotencyKey(owner service.BackgroundResponseOwner, keyHash string) string {
	return strconv.FormatInt(owner.UserID, 10) + ":" + strconv.FormatInt(owner.APIKeyID, 10) + ":" + strings.TrimSpace(keyHash)
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
		<-release
		c.Header("Content-Type", "text/event-stream")
		_, _ = c.Writer.Write([]byte("event: response.output_text.delta\n"))
		_, _ = c.Writer.Write([]byte(`data: {"type":"response.output_text.delta","delta":"SUBTOPROXY_"}` + "\n\n"))
		_, _ = c.Writer.Write([]byte("event: response.output_text.delta\n"))
		_, _ = c.Writer.Write([]byte(`data: {"type":"response.output_text.delta","delta":"OK"}` + "\n\n"))
		_, _ = c.Writer.Write([]byte("event: response.completed\n"))
		_, _ = c.Writer.Write([]byte(`data: {"type":"response.completed","response":{"id":"resp_upstream","object":"response","status":"completed","model":"gpt-5.6-terra","output":[],"usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}}` + "\n\n"))
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
	require.Equal(t, service.BackgroundResponseStatusQueued, accepted.Status)
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
	require.Contains(t, pollWriter.Body.String(), "SUBTOPROXY_OK")
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

func TestBackgroundResponseStreamingFailedEventBecomesTerminalFailure(t *testing.T) {
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
	require.Equal(t, http.StatusOK, w.Code)

	var accepted struct {
		ID string `json:"id"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &accepted))
	require.Eventually(t, func() bool {
		got, err := tasks.Get(context.Background(), service.BackgroundResponseOwner{UserID: 7, APIKeyID: 9}, accepted.ID)
		return err == nil && got.Status == service.BackgroundResponseStatusFailed
	}, time.Second, 10*time.Millisecond)

	pollReq := httptest.NewRequest(http.MethodGet, "/v1/responses/"+accepted.ID, nil)
	pollWriter := httptest.NewRecorder()
	router.ServeHTTP(pollWriter, pollReq)
	require.Equal(t, http.StatusOK, pollWriter.Code)
	require.Contains(t, pollWriter.Body.String(), `"status":"failed"`)
	require.Contains(t, pollWriter.Body.String(), "upstream timeout")
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

func TestBackgroundResponseCancelQueuedTask(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)}
	tasks := service.NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Minute)
	block := make(chan struct{})
	started := make(chan struct{}, 1)
	h := &BackgroundResponseHandler{tasks: tasks, heavyQueue: newBackgroundResponseHeavyQueue()}
	h.execute = func(c *gin.Context) {
		started <- struct{}{}
		<-block
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

	firstID := submitHeavyBackgroundForTest(t, router)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first heavy background request did not start")
	}
	secondID := submitHeavyBackgroundForTest(t, router)
	require.NotEmpty(t, firstID)
	require.NotEmpty(t, secondID)

	cancelReq := httptest.NewRequest(http.MethodPost, "/v1/responses/"+secondID+"/cancel", nil)
	cancelWriter := httptest.NewRecorder()
	router.ServeHTTP(cancelWriter, cancelReq)
	require.Equal(t, http.StatusOK, cancelWriter.Code)
	require.Contains(t, cancelWriter.Body.String(), `"status":"cancelled"`)

	close(block)
	require.Eventually(t, func() bool {
		got, err := tasks.Get(context.Background(), service.BackgroundResponseOwner{UserID: 7, APIKeyID: 9}, secondID)
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
	require.Equal(t, service.BackgroundResponseStatusQueued, accepted.Status)
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

	firstID := submitHeavyBackgroundForTest(t, router)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first heavy background request did not start")
	}

	secondID := submitHeavyBackgroundForTest(t, router)
	require.Never(t, func() bool {
		got, err := tasks.Get(context.Background(), service.BackgroundResponseOwner{UserID: 7, APIKeyID: 9}, secondID)
		return err == nil && got.Status == service.BackgroundResponseStatusInProgress
	}, 150*time.Millisecond, 10*time.Millisecond)
	got, err := tasks.Get(context.Background(), service.BackgroundResponseOwner{UserID: 7, APIKeyID: 9}, secondID)
	require.NoError(t, err)
	require.Equal(t, service.BackgroundResponseStatusQueued, got.Status)

	close(release)
	require.Eventually(t, func() bool {
		got, err := tasks.Get(context.Background(), service.BackgroundResponseOwner{UserID: 7, APIKeyID: 9}, firstID)
		return err == nil && got.Status == service.BackgroundResponseStatusCompleted
	}, time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		got, err := tasks.Get(context.Background(), service.BackgroundResponseOwner{UserID: 7, APIKeyID: 9}, secondID)
		return err == nil && got.Status == service.BackgroundResponseStatusCompleted
	}, time.Second, 10*time.Millisecond)
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

func TestBackgroundResponseNativeCreatePollsPastNineHundredSeconds(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)}
	tasks := service.NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Hour)
	allowPoll := make(chan struct{})
	h := &BackgroundResponseHandler{tasks: tasks, heavyQueue: newBackgroundResponseHeavyQueue(), pollInterval: time.Millisecond}
	h.execute = func(c *gin.Context) {
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
	store.tasks[id].CreatedAt = time.Now().Add(-1100 * time.Second).UTC().Unix()
	store.mu.Unlock()
	close(allowPoll)

	require.Eventually(t, func() bool {
		got, err := tasks.Get(context.Background(), service.BackgroundResponseOwner{UserID: 7, APIKeyID: 9}, id)
		return err == nil && got.Status == service.BackgroundResponseStatusCompleted && got.ElapsedSeconds >= 1100
	}, time.Second, 10*time.Millisecond)
}

func TestBackgroundResponseSSECreatedEOFRecoversByPolling(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)}
	tasks := service.NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Hour)
	h := &BackgroundResponseHandler{tasks: tasks, heavyQueue: newBackgroundResponseHeavyQueue(), pollInterval: time.Millisecond}
	h.execute = func(c *gin.Context) {
		c.Status(http.StatusBadGateway)
		_, _ = c.Writer.Write([]byte(`data: {"type":"response.created","response":{"id":"resp_up_recover","status":"in_progress"}}` + "\n\nunexpected EOF"))
	}
	h.fetchUpstream = func(_ context.Context, task *service.BackgroundResponseTaskRecord) (*backgroundUpstreamPollResult, error) {
		require.Equal(t, "resp_up_recover", task.UpstreamResponseID)
		return &backgroundUpstreamPollResult{statusCode: http.StatusOK, body: []byte(`{"id":"resp_up_recover","object":"response","status":"completed","model":"gpt-5.6-sol","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}]}`)}, nil
	}

	id := submitHeavyBackgroundForTest(t, backgroundResponseTestRouter(h))
	require.Eventually(t, func() bool {
		got, err := tasks.Get(context.Background(), service.BackgroundResponseOwner{UserID: 7, APIKeyID: 9}, id)
		return err == nil && got.Status == service.BackgroundResponseStatusCompleted && got.UpstreamResponseID == "resp_up_recover"
	}, time.Second, 10*time.Millisecond)
}

func TestBackgroundResponseUnsupportedNativeBackgroundFallsBackToStreamingStoreTrue(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)}
	tasks := service.NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Hour)
	var bodies [][]byte
	h := &BackgroundResponseHandler{tasks: tasks, heavyQueue: newBackgroundResponseHeavyQueue(), pollInterval: time.Millisecond}
	h.execute = func(c *gin.Context) {
		body, err := io.ReadAll(c.Request.Body)
		require.NoError(t, err)
		bodies = append(bodies, append([]byte(nil), body...))
		if len(bodies) == 1 {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "Unsupported parameter: background"})
			return
		}
		require.False(t, gjson.GetBytes(body, "background").Exists())
		require.True(t, gjson.GetBytes(body, "stream").Bool())
		require.True(t, gjson.GetBytes(body, "store").Bool())
		c.Header("Content-Type", "text/event-stream")
		_, _ = c.Writer.Write([]byte(`data: {"type":"response.completed","response":{"id":"resp_up_stream","object":"response","status":"completed","model":"gpt-5.6-sol","output":[],"usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}}` + "\n\n"))
	}

	id := submitHeavyBackgroundForTest(t, backgroundResponseTestRouter(h))
	require.Eventually(t, func() bool {
		got, err := tasks.Get(context.Background(), service.BackgroundResponseOwner{UserID: 7, APIKeyID: 9}, id)
		return err == nil && got.Status == service.BackgroundResponseStatusCompleted
	}, time.Second, 10*time.Millisecond)
	require.Len(t, bodies, 2)
	require.True(t, gjson.GetBytes(bodies[0], "background").Bool())
}

func TestBackgroundResponseUnsupportedNativeStreamingCreatedEOFStillPolls(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)}
	tasks := service.NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Hour)
	calls := 0
	h := &BackgroundResponseHandler{tasks: tasks, heavyQueue: newBackgroundResponseHeavyQueue(), pollInterval: time.Millisecond}
	h.execute = func(c *gin.Context) {
		calls++
		if calls == 1 {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "Unsupported parameter: background"})
			return
		}
		c.Status(http.StatusBadGateway)
		_, _ = c.Writer.Write([]byte(`data: {"type":"response.created","response":{"id":"resp_up_stream_recover","status":"in_progress"}}` + "\n\nunexpected EOF"))
	}
	h.fetchUpstream = func(_ context.Context, task *service.BackgroundResponseTaskRecord) (*backgroundUpstreamPollResult, error) {
		require.Equal(t, "resp_up_stream_recover", task.UpstreamResponseID)
		return &backgroundUpstreamPollResult{statusCode: http.StatusOK, body: []byte(`{"id":"resp_up_stream_recover","object":"response","status":"completed","model":"gpt-5.6-sol","output":[]}`)}, nil
	}

	id := submitHeavyBackgroundForTest(t, backgroundResponseTestRouter(h))
	require.Eventually(t, func() bool {
		got, err := tasks.Get(context.Background(), service.BackgroundResponseOwner{UserID: 7, APIKeyID: 9}, id)
		return err == nil && got.Status == service.BackgroundResponseStatusCompleted && got.UpstreamResponseID == "resp_up_stream_recover"
	}, time.Second, 10*time.Millisecond)
}

func TestBackgroundResponseEOFBeforeIDRetriesWithSameIdempotencyKey(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)}
	tasks := service.NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Hour)
	var seenKeys []string
	h := &BackgroundResponseHandler{tasks: tasks, heavyQueue: newBackgroundResponseHeavyQueue(), pollInterval: time.Millisecond}
	h.execute = func(c *gin.Context) {
		seenKeys = append(seenKeys, c.GetHeader("Idempotency-Key"))
		if len(seenKeys) == 1 {
			c.Status(http.StatusBadGateway)
			_, _ = c.Writer.Write([]byte("unexpected EOF"))
			return
		}
		c.JSON(http.StatusOK, gin.H{"id": "resp_up_retry", "object": "response", "status": service.BackgroundResponseStatusInProgress})
	}
	h.fetchUpstream = func(_ context.Context, _ *service.BackgroundResponseTaskRecord) (*backgroundUpstreamPollResult, error) {
		return &backgroundUpstreamPollResult{statusCode: http.StatusOK, body: []byte(`{"id":"resp_up_retry","object":"response","status":"completed","model":"gpt-5.6-sol","output":[]}`)}, nil
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(heavyWebSearchBackgroundRequestBody()))
	req.Header.Set("Idempotency-Key", "same-key")
	w := httptest.NewRecorder()
	backgroundResponseTestRouter(h).ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.Eventually(t, func() bool { return len(seenKeys) == 2 }, time.Second, 10*time.Millisecond)
	require.Equal(t, []string{"same-key", "same-key"}, seenKeys)
}

func TestBackgroundResponseStartupRecoveryPollsMappedUpstreamTask(t *testing.T) {
	store := &backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)}
	tasks := service.NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Hour)
	now := time.Now().UTC().Unix()
	task := &service.BackgroundResponseTaskRecord{ID: "resp_bg_restart", UserID: 7, APIKeyID: 9, Model: "gpt-5.6-sol", Status: service.BackgroundResponseStatusInProgress, CreatedAt: now, ExpiresAt: now + 3600, UpstreamAccountID: 123, UpstreamResponseID: "resp_up_restart"}
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
	id := submitHeavyBackgroundForTest(t, backgroundResponseTestRouter(h))
	require.Eventually(t, func() bool {
		got, err := tasks.Get(context.Background(), service.BackgroundResponseOwner{UserID: 7, APIKeyID: 9}, id)
		return err == nil && got.Status == service.BackgroundResponseStatusFailed
	}, time.Second, 10*time.Millisecond)
	got, err := tasks.Get(context.Background(), service.BackgroundResponseOwner{UserID: 7, APIKeyID: 9}, id)
	require.NoError(t, err)
	require.Equal(t, "upstream_rate_limited", gjson.GetBytes(got.Error, "type").String())
	require.Equal(t, "req_429", gjson.GetBytes(got.Error, "upstream_request_id").String())
	require.Equal(t, "12", gjson.GetBytes(got.Error, "retry_after").String())
	require.True(t, gjson.GetBytes(got.Error, "failed_at").Int() > 0)
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
