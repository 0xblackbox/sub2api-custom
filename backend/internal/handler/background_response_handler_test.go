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

func jsonPathBool(body []byte, path string) bool {
	var value map[string]any
	if json.Unmarshal(body, &value) != nil {
		return false
	}
	result, _ := value[path].(bool)
	return result
}
