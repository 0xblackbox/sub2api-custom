package service

import (
	"strings"

	"github.com/tidwall/gjson"
)

// OpenAIResponsesNativeContractSummary is a redacted, prompt-free summary of
// Responses API features that cannot be faithfully represented through the
// Chat Completions fallback bridge.
type OpenAIResponsesNativeContractSummary struct {
	HasHostedWebSearch       bool
	RequestsWebSearchSources bool
	Background               bool
	ToolChoice               string
	RequiresNativeResponses  bool
}

// OpenAIResponsesNativeContract returns a small diagnostic summary for routing
// and logging. It deliberately inspects only structural fields (tools,
// tool_choice, include, background) and never logs or stores user prompt text.
func OpenAIResponsesNativeContract(body []byte) OpenAIResponsesNativeContractSummary {
	summary := OpenAIResponsesNativeContractSummary{
		HasHostedWebSearch:       openAIResponsesRequestHasHostedWebSearch(body),
		RequestsWebSearchSources: openAIResponsesRequestRequestsWebSearchSources(body),
		Background:               gjson.GetBytes(body, "background").Bool(),
		ToolChoice:               openAIResponsesToolChoiceSummary(body),
	}
	summary.RequiresNativeResponses = summary.HasHostedWebSearch || summary.RequestsWebSearchSources || summary.Background
	return summary
}

// OpenAIResponsesRequestRequiresNativeResponses reports whether the request
// must be forwarded to an upstream /v1/responses implementation. Returning true
// means the raw Chat Completions fallback would lose official Responses
// semantics (for example hosted web_search_call.action.sources) and must not be
// used as a silent degradation path.
func OpenAIResponsesRequestRequiresNativeResponses(body []byte) bool {
	return OpenAIResponsesNativeContract(body).RequiresNativeResponses
}

func openAIResponsesRequestHasHostedWebSearch(body []byte) bool {
	tools := gjson.GetBytes(body, "tools")
	if tools.IsArray() {
		for _, tool := range tools.Array() {
			if isOpenAIHostedWebSearchTool(tool) {
				return true
			}
		}
	}
	toolChoice := gjson.GetBytes(body, "tool_choice")
	return isOpenAIHostedWebSearchTool(toolChoice) ||
		isOpenAIHostedWebSearchName(toolChoice.String()) ||
		isOpenAIHostedWebSearchName(toolChoice.Get("type").String()) ||
		isOpenAIHostedWebSearchName(toolChoice.Get("name").String())
}

func openAIResponsesRequestRequestsWebSearchSources(body []byte) bool {
	include := gjson.GetBytes(body, "include")
	if !include.IsArray() {
		return false
	}
	for _, entry := range include.Array() {
		v := strings.TrimSpace(entry.String())
		if v == "web_search_call.action.sources" ||
			v == "web_search_call.results" ||
			strings.HasPrefix(v, "web_search_call.") {
			return true
		}
	}
	return false
}

func isOpenAIHostedWebSearchTool(tool gjson.Result) bool {
	if !tool.Exists() {
		return false
	}
	if isOpenAIHostedWebSearchName(tool.Get("type").String()) {
		return true
	}
	return isOpenAIHostedWebSearchName(tool.Get("name").String())
}

func isOpenAIHostedWebSearchName(value string) bool {
	value = strings.TrimSpace(strings.ToLower(value))
	return value == "web_search" ||
		value == "web_search_preview" ||
		strings.HasPrefix(value, "web_search_") ||
		strings.HasPrefix(value, "web_search-preview")
}

func openAIResponsesToolChoiceSummary(body []byte) string {
	toolChoice := gjson.GetBytes(body, "tool_choice")
	if !toolChoice.Exists() {
		return ""
	}
	if toolChoice.Type == gjson.String {
		return strings.TrimSpace(toolChoice.String())
	}
	if toolChoice.IsObject() {
		toolType := strings.TrimSpace(toolChoice.Get("type").String())
		name := strings.TrimSpace(toolChoice.Get("name").String())
		if name == "" {
			name = strings.TrimSpace(toolChoice.Get("tool.name").String())
		}
		switch {
		case toolType != "" && name != "":
			return toolType + ":" + name
		case toolType != "":
			return toolType
		case name != "":
			return name
		}
	}
	return "non_string"
}
