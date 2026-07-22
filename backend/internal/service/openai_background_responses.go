package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// OpenAIBackgroundResponseFetchResult is the short JSON polling result for a
// native upstream Responses background task.
type OpenAIBackgroundResponseFetchResult struct {
	StatusCode        int
	Body              []byte
	UpstreamRequestID string
	RetryAfter        string
}

func (s *OpenAIGatewayService) FetchOpenAIBackgroundResponse(ctx context.Context, accountID int64, upstreamResponseID string) (*OpenAIBackgroundResponseFetchResult, error) {
	return s.openAIBackgroundResponseRequest(ctx, http.MethodGet, accountID, upstreamResponseID, "")
}

func (s *OpenAIGatewayService) CancelOpenAIBackgroundResponse(ctx context.Context, accountID int64, upstreamResponseID string) (*OpenAIBackgroundResponseFetchResult, error) {
	return s.openAIBackgroundResponseRequest(ctx, http.MethodPost, accountID, upstreamResponseID, "/cancel")
}

func (s *OpenAIGatewayService) openAIBackgroundResponseRequest(ctx context.Context, method string, accountID int64, upstreamResponseID, action string) (*OpenAIBackgroundResponseFetchResult, error) {
	if s == nil || s.accountRepo == nil || s.httpUpstream == nil {
		return nil, fmt.Errorf("openai background response gateway unavailable")
	}
	upstreamResponseID = strings.TrimSpace(upstreamResponseID)
	if accountID <= 0 || upstreamResponseID == "" {
		return nil, fmt.Errorf("missing upstream background response mapping")
	}
	account, err := s.accountRepo.GetByID(ctx, accountID)
	if err != nil {
		return nil, err
	}
	if account == nil {
		return nil, fmt.Errorf("upstream account not found")
	}
	token, _, err := s.GetAccessToken(ctx, account)
	if err != nil {
		return nil, err
	}
	targetURL, err := s.openAIBackgroundResponseURL(account, upstreamResponseID, action)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, targetURL, nil)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(WithHTTPUpstreamProfile(req.Context(), HTTPUpstreamProfileOpenAI))
	authHeaders, err := s.buildOpenAIAuthenticationHeaders(ctx, account, token)
	if err != nil {
		return nil, fmt.Errorf("build openai authentication headers: %w", err)
	}
	for key, values := range authHeaders {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	req.Header.Set("accept", "application/json")
	if account.Type == AccountTypeOAuth {
		req.Host = "chatgpt.com"
		if err := resolveAndSetOpenAIChatGPTAccountHeaders(ctx, s.accountRepo, req.Header, account); err != nil {
			return nil, fmt.Errorf("resolve chatgpt account headers: %w", err)
		}
		req.Header.Set("OpenAI-Beta", "responses=experimental")
		req.Header.Set("originator", "codex_cli_rs")
		req.Header.Set("user-agent", DefaultOpenAICodexUserAgent)
		enforceCodexIdentityHeaders(req.Header)
	}
	account.ApplyHeaderOverrides(req.Header)

	proxyURL := ""
	if account.ProxyID != nil && account.Proxy != nil {
		proxyURL = account.Proxy.URL()
	}
	resp, err := s.httpUpstream.Do(req, proxyURL, account.ID, account.Concurrency)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxBackgroundResponseResultBytes+1))
	if readErr != nil {
		return nil, readErr
	}
	if len(body) > maxBackgroundResponseResultBytes {
		return nil, fmt.Errorf("background response exceeded storage limit")
	}
	return &OpenAIBackgroundResponseFetchResult{
		StatusCode:        resp.StatusCode,
		Body:              body,
		UpstreamRequestID: strings.TrimSpace(resp.Header.Get("x-request-id")),
		RetryAfter:        strings.TrimSpace(resp.Header.Get("Retry-After")),
	}, nil
}

func (s *OpenAIGatewayService) openAIBackgroundResponseURL(account *Account, upstreamResponseID, action string) (string, error) {
	base := openaiPlatformAPIURL
	switch account.Type {
	case AccountTypeOAuth:
		base = chatgptCodexURL
	case AccountTypeAPIKey:
		if rawBase := account.GetOpenAIBaseURL(); rawBase != "" {
			validated, err := s.validateUpstreamBaseURL(rawBase)
			if err != nil {
				return "", err
			}
			base = buildOpenAIResponsesURL(validated)
		}
	}
	return strings.TrimRight(base, "/") + "/" + url.PathEscape(strings.TrimSpace(upstreamResponseID)) + action, nil
}
