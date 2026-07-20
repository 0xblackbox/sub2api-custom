package handler

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type backgroundResponseHandlerMemoryStore struct {
	mu    sync.RWMutex
	tasks map[string]*service.BackgroundResponseTaskRecord
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

func TestBackgroundResponseSubmitSurvivesDisconnectAndPollsFinalResult(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &backgroundResponseHandlerMemoryStore{tasks: make(map[string]*service.BackgroundResponseTaskRecord)}
	tasks := service.NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Minute)
	release := make(chan struct{})
	h := &BackgroundResponseHandler{tasks: tasks}
	h.execute = func(c *gin.Context) {
		body, err := io.ReadAll(c.Request.Body)
		require.NoError(t, err)
		require.False(t, jsonPathBool(body, "background"))
		require.True(t, jsonPathBool(body, "stream"))
		require.False(t, jsonPathBool(body, "store"))
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

func TestBackgroundResponseAutoSubmitsHeavyWebSearchRequest(t *testing.T) {
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

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(heavyWebSearchRequestBody(false)))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "true", w.Header().Get("X-SubtoProxy-Auto-Background"))
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
	require.False(t, gjson.GetBytes(executionBody, "background").Exists())
	require.True(t, gjson.GetBytes(executionBody, "stream").Bool())
	require.False(t, gjson.GetBytes(executionBody, "store").Bool())
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

func submitHeavyBackgroundForTest(t *testing.T, router http.Handler) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(heavyWebSearchRequestBody(false)))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "true", w.Header().Get("X-SubtoProxy-Auto-Background"))
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

func jsonPathBool(body []byte, path string) bool {
	var value map[string]any
	if json.Unmarshal(body, &value) != nil {
		return false
	}
	result, _ := value[path].(bool)
	return result
}
