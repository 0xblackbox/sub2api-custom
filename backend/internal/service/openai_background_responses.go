package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

const (
	// BackgroundResponseExecutionModeNative identifies a response that was
	// created with the provider's native background=true contract and can
	// therefore be queried with short GET requests.
	BackgroundResponseExecutionModeNative = "native_background"

	backgroundResponseUpstreamBindingVersion = 1
)

var (
	// ErrOpenAIBackgroundUpstreamBindingInvalid means a worker did not receive a
	// complete, pollable creation-time binding. It is intentionally distinct
	// from identity drift so callers can classify a missing proxy mapping.
	ErrOpenAIBackgroundUpstreamBindingInvalid = errors.New("openai background upstream binding is invalid")

	// ErrOpenAIBackgroundUpstreamIdentityMismatch means the account or route
	// currently stored under an account id is no longer the same logical
	// upstream identity that created the background response.
	ErrOpenAIBackgroundUpstreamIdentityMismatch = errors.New("openai background upstream identity mismatch")
)

// BackgroundResponseUpstreamBinding is the immutable, non-secret snapshot a
// worker must persist with a proxy response id. CredentialRef references the
// selected account (or its credential-bearing parent for a shadow account);
// it never contains an API key, OAuth token, proxy password, or Authorization
// header. IdentityFingerprint includes secret-derived one-way digests so an API
// key replacement is detected, while volatile OAuth access/refresh/id tokens
// and expiry timestamps are deliberately excluded to allow normal refresh.
type BackgroundResponseUpstreamBinding struct {
	Version             int    `json:"version"`
	ExecutionMode       string `json:"execution_mode"`
	Pollable            bool   `json:"pollable"`
	AccountID           int64  `json:"account_id"`
	AccountType         string `json:"account_type"`
	CredentialRef       string `json:"credential_ref"`
	IdentityFingerprint string `json:"identity_fingerprint"`
	BaseURL             string `json:"base_url"`
	Project             string `json:"project,omitempty"`
	Organization        string `json:"organization,omitempty"`
	RouteFingerprint    string `json:"route_fingerprint"`
}

type openAIBackgroundUpstreamBindingContextKey struct{}

// WithOpenAIBackgroundUpstreamBinding attaches an immutable creation-time
// upstream identity snapshot to a request context. The value is copied before
// storage so later account or caller mutations cannot change the captured
// identity.
func WithOpenAIBackgroundUpstreamBinding(ctx context.Context, binding BackgroundResponseUpstreamBinding) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	copy := binding
	return context.WithValue(ctx, openAIBackgroundUpstreamBindingContextKey{}, copy)
}

// OpenAIBackgroundUpstreamBindingFromContext returns a defensive copy of the
// exact upstream identity selected by the native background create attempt.
func OpenAIBackgroundUpstreamBindingFromContext(ctx context.Context) (*BackgroundResponseUpstreamBinding, bool) {
	if ctx == nil {
		return nil, false
	}
	binding, ok := ctx.Value(openAIBackgroundUpstreamBindingContextKey{}).(BackgroundResponseUpstreamBinding)
	if !ok {
		return nil, false
	}
	copy := binding
	return &copy, true
}

// OpenAIBackgroundUpstreamIdentityMismatchError names the non-secret binding
// component that drifted. It unwraps to
// ErrOpenAIBackgroundUpstreamIdentityMismatch for stable classification.
type OpenAIBackgroundUpstreamIdentityMismatchError struct {
	Field string
}

func (e *OpenAIBackgroundUpstreamIdentityMismatchError) Error() string {
	field := strings.TrimSpace(e.Field)
	if field == "" {
		field = "identity"
	}
	return "openai background upstream identity changed: " + field
}

func (e *OpenAIBackgroundUpstreamIdentityMismatchError) Unwrap() error {
	return ErrOpenAIBackgroundUpstreamIdentityMismatch
}

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

// FetchOpenAIBackgroundResponseBound verifies that polling still uses the
// account, credential identity, base URL, project/organization scope, and route
// captured at creation. Validation happens before token lookup and before any
// network request.
func (s *OpenAIGatewayService) FetchOpenAIBackgroundResponseBound(ctx context.Context, binding BackgroundResponseUpstreamBinding, upstreamResponseID string) (*OpenAIBackgroundResponseFetchResult, error) {
	return s.openAIBackgroundResponseRequestBound(ctx, http.MethodGet, binding, upstreamResponseID, "")
}

func (s *OpenAIGatewayService) CancelOpenAIBackgroundResponse(ctx context.Context, accountID int64, upstreamResponseID string) (*OpenAIBackgroundResponseFetchResult, error) {
	return s.openAIBackgroundResponseRequest(ctx, http.MethodPost, accountID, upstreamResponseID, "/cancel")
}

// CancelOpenAIBackgroundResponseBound applies the same creation-time identity
// check as polling before sending the provider cancellation request.
func (s *OpenAIGatewayService) CancelOpenAIBackgroundResponseBound(ctx context.Context, binding BackgroundResponseUpstreamBinding, upstreamResponseID string) (*OpenAIBackgroundResponseFetchResult, error) {
	return s.openAIBackgroundResponseRequestBound(ctx, http.MethodPost, binding, upstreamResponseID, "/cancel")
}

// CaptureOpenAIBackgroundUpstreamBinding builds the non-secret binding that
// must be stored immediately after the native background create selected an
// account. For shadow accounts, CredentialRef points at the credential-bearing
// parent while AccountID continues to identify the scheduled route account.
func (s *OpenAIGatewayService) CaptureOpenAIBackgroundUpstreamBinding(ctx context.Context, accountID int64) (BackgroundResponseUpstreamBinding, error) {
	if s == nil || s.accountRepo == nil {
		return BackgroundResponseUpstreamBinding{}, fmt.Errorf("%w: account repository unavailable", ErrOpenAIBackgroundUpstreamBindingInvalid)
	}
	if accountID <= 0 {
		return BackgroundResponseUpstreamBinding{}, fmt.Errorf("%w: missing account id", ErrOpenAIBackgroundUpstreamBindingInvalid)
	}
	account, err := s.accountRepo.GetByID(ctx, accountID)
	if err != nil {
		return BackgroundResponseUpstreamBinding{}, err
	}
	if account == nil {
		return BackgroundResponseUpstreamBinding{}, fmt.Errorf("%w: upstream account not found", ErrOpenAIBackgroundUpstreamBindingInvalid)
	}
	return s.CaptureOpenAIBackgroundUpstreamBindingForAccount(ctx, account)
}

// CaptureOpenAIBackgroundUpstreamBindingForAccount is the exact-account form
// used when the creation path still has the selected Account object. It avoids
// selecting a different scheduler account and only resolves a shadow account's
// credential-bearing parent.
func (s *OpenAIGatewayService) CaptureOpenAIBackgroundUpstreamBindingForAccount(ctx context.Context, account *Account) (BackgroundResponseUpstreamBinding, error) {
	return s.captureOpenAIBackgroundUpstreamBindingForAccount(ctx, account)
}

// ValidateOpenAIBackgroundUpstreamBinding compares two non-secret snapshots and
// returns a classifiable mismatch without including either value in the error.
func ValidateOpenAIBackgroundUpstreamBinding(expected, current BackgroundResponseUpstreamBinding) error {
	if expected.Version != backgroundResponseUpstreamBindingVersion ||
		expected.AccountID <= 0 ||
		strings.TrimSpace(expected.ExecutionMode) == "" ||
		strings.TrimSpace(expected.AccountType) == "" ||
		strings.TrimSpace(expected.CredentialRef) == "" ||
		strings.TrimSpace(expected.IdentityFingerprint) == "" ||
		strings.TrimSpace(expected.BaseURL) == "" ||
		strings.TrimSpace(expected.RouteFingerprint) == "" {
		return ErrOpenAIBackgroundUpstreamBindingInvalid
	}
	if expected.ExecutionMode != BackgroundResponseExecutionModeNative || !expected.Pollable {
		return ErrOpenAIBackgroundUpstreamBindingInvalid
	}
	for _, candidate := range []struct {
		field   string
		matches bool
	}{
		{field: "account_id", matches: expected.AccountID == current.AccountID},
		{field: "account_type", matches: expected.AccountType == current.AccountType},
		{field: "credential_ref", matches: expected.CredentialRef == current.CredentialRef},
		{field: "base_url", matches: expected.BaseURL == current.BaseURL},
		{field: "project", matches: expected.Project == current.Project},
		{field: "organization", matches: expected.Organization == current.Organization},
		{field: "route", matches: expected.RouteFingerprint == current.RouteFingerprint},
		{field: "identity", matches: expected.IdentityFingerprint == current.IdentityFingerprint},
	} {
		if !candidate.matches {
			return &OpenAIBackgroundUpstreamIdentityMismatchError{Field: candidate.field}
		}
	}
	return nil
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
	return s.openAIBackgroundResponseRequestWithAccount(ctx, method, account, upstreamResponseID, action)
}

func (s *OpenAIGatewayService) openAIBackgroundResponseRequestBound(ctx context.Context, method string, binding BackgroundResponseUpstreamBinding, upstreamResponseID, action string) (*OpenAIBackgroundResponseFetchResult, error) {
	if s == nil || s.accountRepo == nil || s.httpUpstream == nil {
		return nil, fmt.Errorf("openai background response gateway unavailable")
	}
	upstreamResponseID = strings.TrimSpace(upstreamResponseID)
	if upstreamResponseID == "" {
		return nil, fmt.Errorf("%w: missing upstream response id", ErrOpenAIBackgroundUpstreamBindingInvalid)
	}
	if binding.AccountID <= 0 {
		return nil, fmt.Errorf("%w: missing account id", ErrOpenAIBackgroundUpstreamBindingInvalid)
	}
	account, err := s.accountRepo.GetByID(ctx, binding.AccountID)
	if err != nil {
		return nil, fmt.Errorf("%w: bound account lookup failed", ErrOpenAIBackgroundUpstreamBindingInvalid)
	}
	if account == nil {
		return nil, fmt.Errorf("%w: upstream account not found", ErrOpenAIBackgroundUpstreamBindingInvalid)
	}
	current, err := s.captureOpenAIBackgroundUpstreamBindingForAccount(ctx, account)
	if err != nil {
		return nil, fmt.Errorf("%w: bound credential reference unavailable", ErrOpenAIBackgroundUpstreamBindingInvalid)
	}
	if err := ValidateOpenAIBackgroundUpstreamBinding(binding, current); err != nil {
		return nil, err
	}
	return s.openAIBackgroundResponseRequestWithAccount(ctx, method, account, upstreamResponseID, action)
}

func (s *OpenAIGatewayService) openAIBackgroundResponseRequestWithAccount(ctx context.Context, method string, account *Account, upstreamResponseID, action string) (*OpenAIBackgroundResponseFetchResult, error) {
	if account == nil {
		return nil, fmt.Errorf("upstream account not found")
	}
	upstreamResponseID = strings.TrimSpace(upstreamResponseID)
	if account.ID <= 0 || upstreamResponseID == "" {
		return nil, fmt.Errorf("missing upstream background response mapping")
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

func (s *OpenAIGatewayService) captureOpenAIBackgroundUpstreamBindingForAccount(ctx context.Context, account *Account) (BackgroundResponseUpstreamBinding, error) {
	if account == nil || account.ID <= 0 {
		return BackgroundResponseUpstreamBinding{}, fmt.Errorf("%w: upstream account not found", ErrOpenAIBackgroundUpstreamBindingInvalid)
	}
	switch account.Type {
	case AccountTypeAPIKey, AccountTypeOAuth:
	default:
		return BackgroundResponseUpstreamBinding{}, fmt.Errorf("%w: unsupported account type", ErrOpenAIBackgroundUpstreamBindingInvalid)
	}
	credentialAccount, err := resolveCredentialAccount(ctx, s.accountRepo, account)
	if err != nil {
		return BackgroundResponseUpstreamBinding{}, err
	}
	if credentialAccount == nil || credentialAccount.ID <= 0 {
		return BackgroundResponseUpstreamBinding{}, fmt.Errorf("%w: credential account not found", ErrOpenAIBackgroundUpstreamBindingInvalid)
	}
	base, err := s.openAIBackgroundResponseBaseURL(account)
	if err != nil {
		return BackgroundResponseUpstreamBinding{}, err
	}
	project, organization := openAIBackgroundProjectOrganization(account, credentialAccount)
	credentialRef := "account:" + strconv.FormatInt(credentialAccount.ID, 10)
	routeFingerprint := openAIBackgroundRouteFingerprint(account)
	baseForStorage := openAIBackgroundSanitizedURL(base)
	if baseForStorage == "" {
		return BackgroundResponseUpstreamBinding{}, fmt.Errorf("%w: invalid base url", ErrOpenAIBackgroundUpstreamBindingInvalid)
	}

	identityParts := []string{
		"binding-v1",
		strconv.FormatInt(account.ID, 10),
		account.Platform,
		account.Type,
		credentialRef,
		base,
		project,
		organization,
		routeFingerprint,
		openAIBackgroundCredentialIdentity(credentialAccount),
		openAIBackgroundHeaderOverridesFingerprint(account),
	}
	return BackgroundResponseUpstreamBinding{
		Version:             backgroundResponseUpstreamBindingVersion,
		ExecutionMode:       BackgroundResponseExecutionModeNative,
		Pollable:            true,
		AccountID:           account.ID,
		AccountType:         account.Type,
		CredentialRef:       credentialRef,
		IdentityFingerprint: openAIBackgroundFingerprint(identityParts...),
		BaseURL:             baseForStorage,
		Project:             project,
		Organization:        organization,
		RouteFingerprint:    routeFingerprint,
	}, nil
}

func openAIBackgroundProjectOrganization(account, credentialAccount *Account) (string, string) {
	project := ""
	organization := ""
	if credentialAccount != nil {
		project = strings.TrimSpace(credentialAccount.GetCredential("project_id"))
		organization = strings.TrimSpace(credentialAccount.GetCredential("organization_id"))
	}
	if account != nil {
		overrides := account.GetHeaderOverrides()
		if value := strings.TrimSpace(overrides["openai-project"]); value != "" {
			project = value
		}
		if value := strings.TrimSpace(overrides["openai-organization"]); value != "" {
			organization = value
		}
	}
	return project, organization
}

func openAIBackgroundCredentialIdentity(account *Account) string {
	if account == nil {
		return ""
	}
	switch account.Type {
	case AccountTypeAPIKey:
		return openAIBackgroundFingerprint("apikey", account.GetOpenAIApiKey())
	case AccountTypeOAuth:
		authMode := strings.TrimSpace(account.GetCredential(openAIAuthModeCredentialKey))
		if account.IsOpenAIAgentIdentity() {
			// Agent Identity assertions are derived from these stable logical
			// credentials. Hash the private key rather than persisting it; task id
			// and runtime id must remain identical for GET/cancel to address the
			// response created by the original assertion scope.
			return openAIBackgroundFingerprint(
				"agent_identity",
				strconv.FormatInt(account.ID, 10),
				authMode,
				openAIBackgroundFingerprint("agent_private_key", account.GetCredential("agent_private_key")),
				strings.TrimSpace(account.GetCredential("agent_runtime_id")),
				strings.TrimSpace(account.GetCredential("task_id")),
				strings.TrimSpace(account.GetChatGPTAccountID()),
				strings.TrimSpace(account.GetChatGPTUserID()),
				strings.TrimSpace(account.GetCredential("organization_id")),
				strconv.FormatBool(account.IsChatGPTAccountFedRAMP()),
			)
		}
		// OAuth bearer tokens and their expiry are transport credentials, not the
		// logical owner of an already-created response. Stable subject/account
		// identifiers remain part of the binding and detect scope drift.
		return openAIBackgroundFingerprint(
			"oauth",
			strconv.FormatInt(account.ID, 10),
			authMode,
			strings.TrimSpace(account.GetChatGPTAccountID()),
			strings.TrimSpace(account.GetChatGPTUserID()),
			strings.TrimSpace(account.GetCredential("organization_id")),
			strconv.FormatBool(account.IsChatGPTAccountFedRAMP()),
		)
	default:
		return ""
	}
}

func openAIBackgroundHeaderOverridesFingerprint(account *Account) string {
	if account == nil {
		return openAIBackgroundFingerprint("headers")
	}
	overrides := account.GetHeaderOverrides()
	keys := make([]string, 0, len(overrides))
	for key := range overrides {
		keys = append(keys, strings.ToLower(strings.TrimSpace(key)))
	}
	sort.Strings(keys)
	parts := make([]string, 0, 1+2*len(keys))
	parts = append(parts, "headers")
	for _, key := range keys {
		parts = append(parts, key, overrides[key])
	}
	return openAIBackgroundFingerprint(parts...)
}

func openAIBackgroundRouteFingerprint(account *Account) string {
	if account == nil || account.ProxyID == nil {
		return openAIBackgroundFingerprint("route", "direct")
	}
	parts := []string{"route", "proxy", strconv.FormatInt(*account.ProxyID, 10)}
	if account.Proxy == nil {
		parts = append(parts, "unresolved")
	} else {
		parts = append(parts,
			strings.ToLower(strings.TrimSpace(account.Proxy.Protocol)),
			strings.ToLower(strings.TrimSpace(account.Proxy.Host)),
			strconv.Itoa(account.Proxy.Port),
			openAIBackgroundFingerprint(
				"proxy_credentials",
				account.Proxy.Username,
				account.Proxy.Password,
			),
		)
	}
	return openAIBackgroundFingerprint(parts...)
}

func openAIBackgroundFingerprint(parts ...string) string {
	hash := sha256.New()
	for _, part := range parts {
		value := strings.TrimSpace(part)
		_, _ = hash.Write([]byte(strconv.Itoa(len(value))))
		_, _ = hash.Write([]byte{':'})
		_, _ = hash.Write([]byte(value))
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func openAIBackgroundSanitizedURL(raw string) string {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return ""
	}
	parsed.User = nil
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return strings.TrimRight(parsed.String(), "/")
}

func (s *OpenAIGatewayService) openAIBackgroundResponseURL(account *Account, upstreamResponseID, action string) (string, error) {
	base, err := s.openAIBackgroundResponseBaseURL(account)
	if err != nil {
		return "", err
	}
	return strings.TrimRight(base, "/") + "/" + url.PathEscape(strings.TrimSpace(upstreamResponseID)) + action, nil
}

func (s *OpenAIGatewayService) openAIBackgroundResponseBaseURL(account *Account) (string, error) {
	if account == nil {
		return "", fmt.Errorf("upstream account not found")
	}
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
	return strings.TrimRight(base, "/"), nil
}
