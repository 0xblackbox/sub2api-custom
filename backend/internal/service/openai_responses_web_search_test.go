//go:build unit

package service

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/openai_compat"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestOpenAIResponsesNativeContractDetectsWebSearchSources(t *testing.T) {
	body := []byte(`{
		"model":"gpt-5.6-sol",
		"reasoning":{"effort":"high"},
		"tools":[{"type":"web_search","search_context_size":"high"}],
		"tool_choice":"required",
		"include":["web_search_call.action.sources"],
		"text":{"format":{"type":"json_schema","name":"answer","strict":true,"schema":{"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"],"additionalProperties":false}}},
		"input":"latest OpenAI web search docs"
	}`)

	summary := OpenAIResponsesNativeContract(body)
	require.True(t, summary.HasHostedWebSearch)
	require.True(t, summary.RequestsWebSearchSources)
	require.True(t, summary.RequiresNativeResponses)
	require.Equal(t, "required", summary.ToolChoice)
}

func TestForwardResponses_WebSearchUsesNativeResponsesAndPreservesRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)

	body := []byte(`{
		"model":"gpt-5.6-sol",
		"stream":false,
		"store":false,
		"reasoning":{"effort":"high"},
		"tools":[{"type":"web_search","search_context_size":"high"}],
		"tool_choice":"required",
		"include":["web_search_call.action.sources"],
		"text":{"format":{"type":"json_schema","name":"answer","strict":true,"schema":{"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"],"additionalProperties":false}}},
		"max_output_tokens":8000,
		"input":"Return strict JSON using a consulted source."
	}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}, "x-request-id": []string{"rid_web_search_native"}},
		Body: io.NopCloser(strings.NewReader(`{
			"id":"resp_ws_1",
			"object":"response",
			"model":"gpt-5.6-sol",
			"status":"completed",
			"output":[
				{"id":"ws_1","type":"web_search_call","status":"completed","action":{"type":"search","query":"OpenAI Web Search","sources":[{"type":"url","url":"https://developers.openai.com/api/docs/guides/tools-web-search","title":"Web search"}]}},
				{"id":"msg_1","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"{\"answer\":\"ok\"}"}]}
			],
			"usage":{"input_tokens":10,"output_tokens":5,"total_tokens":15}
		}`)),
	}}
	svc := &OpenAIGatewayService{
		cfg:          rawChatCompletionsTestConfig(),
		httpUpstream: upstream,
	}
	account := rawChatCompletionsTestAccount()
	account.Extra = map[string]any{
		openai_compat.ExtraKeyResponsesMode:      string(openai_compat.ResponsesSupportModeAuto),
		openai_compat.ExtraKeyResponsesSupported: true,
	}

	result, err := svc.Forward(context.Background(), c, account, body)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "http://upstream.example/v1/responses", upstream.lastReq.URL.String())
	require.Equal(t, "web_search", gjson.GetBytes(upstream.lastBody, "tools.0.type").String())
	require.Equal(t, "high", gjson.GetBytes(upstream.lastBody, "tools.0.search_context_size").String())
	require.Equal(t, "required", gjson.GetBytes(upstream.lastBody, "tool_choice").String())
	require.Equal(t, "web_search_call.action.sources", gjson.GetBytes(upstream.lastBody, "include.0").String())
	require.Equal(t, "high", gjson.GetBytes(upstream.lastBody, "reasoning.effort").String())
	require.True(t, gjson.GetBytes(upstream.lastBody, "text.format.strict").Bool())
	require.Equal(t, int64(8000), gjson.GetBytes(upstream.lastBody, "max_output_tokens").Int())
	require.Equal(t, "web_search_call", gjson.Get(rec.Body.String(), "output.0.type").String())
	require.Equal(t, "https://developers.openai.com/api/docs/guides/tools-web-search", gjson.Get(rec.Body.String(), "output.0.action.sources.0.url").String())
	require.Equal(t, 10, result.Usage.InputTokens)
	require.Equal(t, 5, result.Usage.OutputTokens)
}

func TestForwardResponses_ChatFallbackRejectsWebSearchSourcesInsteadOfFaking(t *testing.T) {
	gin.SetMode(gin.TestMode)

	body := []byte(`{"model":"gpt-5.6-sol","tools":[{"type":"web_search","search_context_size":"high"}],"tool_choice":"required","include":["web_search_call.action.sources"],"input":"search"}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstream := &httpUpstreamRecorder{}
	svc := &OpenAIGatewayService{
		cfg:          rawChatCompletionsTestConfig(),
		httpUpstream: upstream,
	}

	result, err := svc.Forward(context.Background(), c, forceChatResponsesFallbackAccount(), body)
	require.Error(t, err)
	require.Nil(t, result)
	require.Nil(t, upstream.lastReq)
	require.Equal(t, http.StatusBadGateway, rec.Code)
	require.Equal(t, "upstream_configuration_error", gjson.Get(rec.Body.String(), "error.type").String())
}

func TestReconstructResponseOutputFromSSEPreservesWebSearchCallSourcesFromAdded(t *testing.T) {
	bodyText := strings.Join([]string{
		`data: {"type":"response.output_item.added","output_index":0,"item":{"id":"ws_1","type":"web_search_call","status":"completed","action":{"type":"search","query":"OpenAI","sources":[{"type":"url","url":"https://developers.openai.com/api/docs/guides/tools-web-search","title":"Web search"}]}}}`,
		"",
		`data: {"type":"response.output_item.done","output_index":1,"item":{"id":"msg_1","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"{\"answer\":\"ok\"}"}]}}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_ws_1","object":"response","status":"completed","output":[],"usage":{"input_tokens":4,"output_tokens":3,"total_tokens":7}}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")

	outputJSON, ok := reconstructResponseOutputFromSSE(bodyText)
	require.True(t, ok)
	require.Equal(t, "web_search_call", gjson.GetBytes(outputJSON, "0.type").String())
	require.Equal(t, "https://developers.openai.com/api/docs/guides/tools-web-search", gjson.GetBytes(outputJSON, "0.action.sources.0.url").String())
	require.Equal(t, "message", gjson.GetBytes(outputJSON, "1.type").String())
}

func TestHandleSSEToJSONPreservesWebSearchCallSources(t *testing.T) {
	gin.SetMode(gin.TestMode)
	bodyText := strings.Join([]string{
		`data: {"type":"response.output_item.added","output_index":0,"item":{"id":"ws_1","type":"web_search_call","status":"completed","action":{"type":"search","query":"OpenAI","sources":[{"type":"url","url":"https://developers.openai.com/api/docs/guides/tools-web-search","title":"Web search"}]}}}`,
		"",
		`data: {"type":"response.output_item.done","output_index":1,"item":{"id":"msg_1","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"{\"answer\":\"ok\"}"}]}}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_ws_1","object":"response","status":"completed","output":[],"usage":{"input_tokens":4,"output_tokens":3,"total_tokens":7}}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_sse_ws"}},
	}
	svc := &OpenAIGatewayService{cfg: rawChatCompletionsTestConfig()}

	result, err := svc.handleSSEToJSON(resp, c, []byte(bodyText), "gpt-5.6-sol", "gpt-5.6-sol")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "web_search_call", gjson.Get(rec.Body.String(), "output.0.type").String())
	require.Equal(t, "https://developers.openai.com/api/docs/guides/tools-web-search", gjson.Get(rec.Body.String(), "output.0.action.sources.0.url").String())
	require.NotNil(t, result.usage)
	require.Equal(t, 4, result.usage.InputTokens)
	require.Equal(t, 3, result.usage.OutputTokens)
}
