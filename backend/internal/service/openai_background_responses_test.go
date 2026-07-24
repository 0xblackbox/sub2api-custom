package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type openAIBackgroundAccountRepo struct {
	AccountRepository
	accounts map[int64]*Account
}

func (r *openAIBackgroundAccountRepo) GetByID(_ context.Context, id int64) (*Account, error) {
	account := r.accounts[id]
	if account == nil {
		return nil, errors.New("account not found")
	}
	return account, nil
}

func TestCaptureOpenAIBackgroundUpstreamBindingContainsNoSecrets(t *testing.T) {
	proxyID := int64(77)
	account := &Account{
		ID:          101,
		Platform:    PlatformOpenAI,
		Type:        AccountTypeAPIKey,
		Concurrency: 1,
		Credentials: map[string]any{
			"api_key":         "sk-binding-secret",
			"base_url":        "http://upstream.example/v1",
			"project_id":      "project-alpha",
			"organization_id": "org-alpha",
		},
		ProxyID: &proxyID,
		Proxy: &Proxy{
			ID:       proxyID,
			Protocol: "http",
			Host:     "proxy.example",
			Port:     8443,
			Username: "proxy-user-secret",
			Password: "proxy-password-secret",
		},
	}
	repo := &openAIBackgroundAccountRepo{accounts: map[int64]*Account{account.ID: account}}
	svc := &OpenAIGatewayService{
		cfg:         openAIBackgroundTestConfig(),
		accountRepo: repo,
	}

	binding, err := svc.CaptureOpenAIBackgroundUpstreamBinding(context.Background(), account.ID)
	require.NoError(t, err)
	require.Equal(t, backgroundResponseUpstreamBindingVersion, binding.Version)
	require.Equal(t, BackgroundResponseExecutionModeNative, binding.ExecutionMode)
	require.True(t, binding.Pollable)
	require.Equal(t, account.ID, binding.AccountID)
	require.Equal(t, AccountTypeAPIKey, binding.AccountType)
	require.Equal(t, "account:101", binding.CredentialRef)
	require.Equal(t, "http://upstream.example/v1/responses", binding.BaseURL)
	require.Equal(t, "project-alpha", binding.Project)
	require.Equal(t, "org-alpha", binding.Organization)
	require.Len(t, binding.IdentityFingerprint, 64)
	require.Len(t, binding.RouteFingerprint, 64)

	serialized, err := json.Marshal(binding)
	require.NoError(t, err)
	for _, secret := range []string{
		"sk-binding-secret",
		"proxy-user-secret",
		"proxy-password-secret",
		"Authorization",
	} {
		require.NotContains(t, string(serialized), secret)
	}
}

func TestFetchOpenAIBackgroundResponseBoundUsesCapturedAPIKeyIdentity(t *testing.T) {
	account := openAIBackgroundAPIKeyTestAccount()
	repo := &openAIBackgroundAccountRepo{accounts: map[int64]*Account{account.ID: account}}
	upstream := &httpUpstreamRecorder{resp: openAIBackgroundTestResponse(
		http.StatusOK,
		"req_bound_fetch",
		`{"id":"resp_upstream","status":"in_progress"}`,
	)}
	svc := &OpenAIGatewayService{
		cfg:          openAIBackgroundTestConfig(),
		accountRepo:  repo,
		httpUpstream: upstream,
	}
	binding, err := svc.CaptureOpenAIBackgroundUpstreamBinding(context.Background(), account.ID)
	require.NoError(t, err)

	result, err := svc.FetchOpenAIBackgroundResponseBound(context.Background(), binding, "resp_upstream")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, result.StatusCode)
	require.Equal(t, "req_bound_fetch", result.UpstreamRequestID)
	require.NotNil(t, upstream.lastReq)
	require.Equal(t, http.MethodGet, upstream.lastReq.Method)
	require.Equal(t, "http://upstream.example/v1/responses/resp_upstream", upstream.lastReq.URL.String())
	require.Equal(t, "Bearer sk-original", upstream.lastReq.Header.Get("Authorization"))
}

func TestCancelOpenAIBackgroundResponseBoundUsesCapturedIdentity(t *testing.T) {
	account := openAIBackgroundAPIKeyTestAccount()
	repo := &openAIBackgroundAccountRepo{accounts: map[int64]*Account{account.ID: account}}
	upstream := &httpUpstreamRecorder{resp: openAIBackgroundTestResponse(
		http.StatusOK,
		"req_bound_cancel",
		`{"id":"resp_upstream","status":"cancelled"}`,
	)}
	svc := &OpenAIGatewayService{
		cfg:          openAIBackgroundTestConfig(),
		accountRepo:  repo,
		httpUpstream: upstream,
	}
	binding, err := svc.CaptureOpenAIBackgroundUpstreamBinding(context.Background(), account.ID)
	require.NoError(t, err)

	result, err := svc.CancelOpenAIBackgroundResponseBound(context.Background(), binding, "resp_upstream")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, result.StatusCode)
	require.Equal(t, http.MethodPost, upstream.lastReq.Method)
	require.Equal(t, "http://upstream.example/v1/responses/resp_upstream/cancel", upstream.lastReq.URL.String())
}

func TestFetchOpenAIBackgroundResponseBoundRejectsIdentityDriftBeforeNetwork(t *testing.T) {
	tests := []struct {
		name          string
		mutate        func(*Account)
		expectedField string
	}{
		{
			name: "api key replacement",
			mutate: func(account *Account) {
				account.Credentials["api_key"] = "sk-replaced"
			},
			expectedField: "identity",
		},
		{
			name: "base URL",
			mutate: func(account *Account) {
				account.Credentials["base_url"] = "http://other-upstream.example"
			},
			expectedField: "base_url",
		},
		{
			name: "project",
			mutate: func(account *Account) {
				account.Credentials["project_id"] = "project-other"
			},
			expectedField: "project",
		},
		{
			name: "organization",
			mutate: func(account *Account) {
				account.Credentials["organization_id"] = "org-other"
			},
			expectedField: "organization",
		},
		{
			name: "route",
			mutate: func(account *Account) {
				proxyID := int64(88)
				account.ProxyID = &proxyID
				account.Proxy = &Proxy{ID: proxyID, Protocol: "http", Host: "proxy-other.example", Port: 8080}
			},
			expectedField: "route",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			account := openAIBackgroundAPIKeyTestAccount()
			repo := &openAIBackgroundAccountRepo{accounts: map[int64]*Account{account.ID: account}}
			upstream := &httpUpstreamRecorder{resp: openAIBackgroundTestResponse(http.StatusOK, "", `{}`)}
			svc := &OpenAIGatewayService{
				cfg:          openAIBackgroundTestConfig(),
				accountRepo:  repo,
				httpUpstream: upstream,
			}
			binding, err := svc.CaptureOpenAIBackgroundUpstreamBinding(context.Background(), account.ID)
			require.NoError(t, err)
			tt.mutate(account)

			result, err := svc.FetchOpenAIBackgroundResponseBound(context.Background(), binding, "resp_upstream")
			require.Nil(t, result)
			require.ErrorIs(t, err, ErrOpenAIBackgroundUpstreamIdentityMismatch)
			var mismatch *OpenAIBackgroundUpstreamIdentityMismatchError
			require.True(t, errors.As(err, &mismatch))
			require.Equal(t, tt.expectedField, mismatch.Field)
			require.Nil(t, upstream.lastReq, "identity drift must be detected before any provider request")
			require.NotContains(t, err.Error(), "sk-original")
			require.NotContains(t, err.Error(), "sk-replaced")
		})
	}
}

func TestFetchOpenAIBackgroundResponseBoundRejectsProxyCredentialDriftBeforeNetwork(t *testing.T) {
	account := openAIBackgroundAPIKeyTestAccount()
	proxyID := int64(77)
	account.ProxyID = &proxyID
	account.Proxy = &Proxy{
		ID:       proxyID,
		Protocol: "http",
		Host:     "proxy.example",
		Port:     8443,
		Username: "proxy-user-original",
		Password: "proxy-password-original",
	}
	repo := &openAIBackgroundAccountRepo{accounts: map[int64]*Account{account.ID: account}}
	upstream := &httpUpstreamRecorder{resp: openAIBackgroundTestResponse(http.StatusOK, "", `{}`)}
	svc := &OpenAIGatewayService{
		cfg:          openAIBackgroundTestConfig(),
		accountRepo:  repo,
		httpUpstream: upstream,
	}

	binding, err := svc.CaptureOpenAIBackgroundUpstreamBinding(context.Background(), account.ID)
	require.NoError(t, err)
	serialized, err := json.Marshal(binding)
	require.NoError(t, err)
	require.NotContains(t, string(serialized), "proxy-user-original")
	require.NotContains(t, string(serialized), "proxy-password-original")

	// Keep the proxy id and endpoint identical while rotating its credentials.
	// A response created through the old authenticated route must not be polled
	// through the new route identity.
	account.Proxy.Username = "proxy-user-replaced"
	account.Proxy.Password = "proxy-password-replaced"

	result, err := svc.FetchOpenAIBackgroundResponseBound(context.Background(), binding, "resp_upstream")
	require.Nil(t, result)
	require.ErrorIs(t, err, ErrOpenAIBackgroundUpstreamIdentityMismatch)
	var mismatch *OpenAIBackgroundUpstreamIdentityMismatchError
	require.True(t, errors.As(err, &mismatch))
	require.Equal(t, "route", mismatch.Field)
	require.Nil(t, upstream.lastReq, "proxy credential drift must be detected before any provider request")
	for _, credential := range []string{
		"proxy-user-original",
		"proxy-password-original",
		"proxy-user-replaced",
		"proxy-password-replaced",
	} {
		require.NotContains(t, err.Error(), credential)
		require.NotContains(t, string(serialized), credential)
	}
}

func TestOpenAIBackgroundOAuthTokenRefreshPreservesLogicalIdentity(t *testing.T) {
	account := &Account{
		ID:          202,
		Platform:    PlatformOpenAI,
		Type:        AccountTypeOAuth,
		Concurrency: 1,
		Credentials: map[string]any{
			"access_token":       "oauth-access-old",
			"refresh_token":      "oauth-refresh-old",
			"id_token":           "oauth-id-old",
			"expires_at":         time.Now().Add(time.Hour).UTC().Format(time.RFC3339),
			"chatgpt_account_id": "chatgpt-account-stable",
			"chatgpt_user_id":    "chatgpt-user-stable",
			"organization_id":    "org-stable",
		},
	}
	repo := &openAIBackgroundAccountRepo{accounts: map[int64]*Account{account.ID: account}}
	upstream := &httpUpstreamRecorder{resp: openAIBackgroundTestResponse(
		http.StatusOK,
		"req_oauth_refresh",
		`{"id":"resp_oauth","status":"in_progress"}`,
	)}
	svc := &OpenAIGatewayService{
		cfg:          openAIBackgroundTestConfig(),
		accountRepo:  repo,
		httpUpstream: upstream,
	}
	binding, err := svc.CaptureOpenAIBackgroundUpstreamBinding(context.Background(), account.ID)
	require.NoError(t, err)

	account.Credentials["access_token"] = "oauth-access-new"
	account.Credentials["refresh_token"] = "oauth-refresh-new"
	account.Credentials["id_token"] = "oauth-id-new"
	account.Credentials["expires_at"] = time.Now().Add(2 * time.Hour).UTC().Format(time.RFC3339)

	current, err := svc.CaptureOpenAIBackgroundUpstreamBinding(context.Background(), account.ID)
	require.NoError(t, err)
	require.NoError(t, ValidateOpenAIBackgroundUpstreamBinding(binding, current))
	require.Equal(t, binding.IdentityFingerprint, current.IdentityFingerprint)

	result, err := svc.FetchOpenAIBackgroundResponseBound(context.Background(), binding, "resp_oauth")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, result.StatusCode)
	require.Equal(t, "Bearer oauth-access-new", upstream.lastReq.Header.Get("Authorization"))
	require.Equal(t, "chatgpt-account-stable", upstream.lastReq.Header.Get("chatgpt-account-id"))
	require.Equal(t, chatgptCodexURL+"/resp_oauth", upstream.lastReq.URL.String())

	serialized, err := json.Marshal(binding)
	require.NoError(t, err)
	for _, token := range []string{
		"oauth-access-old",
		"oauth-refresh-old",
		"oauth-id-old",
		"oauth-access-new",
		"oauth-refresh-new",
		"oauth-id-new",
	} {
		require.NotContains(t, string(serialized), token)
	}
}

func TestOpenAIBackgroundShadowBindingUsesCredentialParentAndAllowsParentTokenRefresh(t *testing.T) {
	parentID := int64(302)
	parent := &Account{
		ID:          parentID,
		Platform:    PlatformOpenAI,
		Type:        AccountTypeOAuth,
		Concurrency: 1,
		Credentials: map[string]any{
			"access_token":       "parent-access-old",
			"refresh_token":      "parent-refresh-old",
			"chatgpt_account_id": "parent-chatgpt-account",
			"chatgpt_user_id":    "parent-chatgpt-user",
			"organization_id":    "parent-org",
		},
	}
	shadowID := int64(303)
	shadow := &Account{
		ID:              shadowID,
		Platform:        PlatformOpenAI,
		Type:            AccountTypeOAuth,
		Concurrency:     1,
		ParentAccountID: &parentID,
	}
	repo := &openAIBackgroundAccountRepo{accounts: map[int64]*Account{
		parent.ID: parent,
		shadow.ID: shadow,
	}}
	upstream := &httpUpstreamRecorder{resp: openAIBackgroundTestResponse(
		http.StatusOK,
		"req_shadow_refresh",
		`{"id":"resp_shadow","status":"in_progress"}`,
	)}
	svc := &OpenAIGatewayService{
		cfg:          openAIBackgroundTestConfig(),
		accountRepo:  repo,
		httpUpstream: upstream,
	}

	binding, err := svc.CaptureOpenAIBackgroundUpstreamBinding(context.Background(), shadow.ID)
	require.NoError(t, err)
	require.Equal(t, shadow.ID, binding.AccountID)
	require.Equal(t, "account:302", binding.CredentialRef)
	require.Equal(t, "parent-org", binding.Organization)

	parent.Credentials["access_token"] = "parent-access-new"
	parent.Credentials["refresh_token"] = "parent-refresh-new"
	result, err := svc.FetchOpenAIBackgroundResponseBound(context.Background(), binding, "resp_shadow")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, result.StatusCode)
	require.Equal(t, "Bearer parent-access-new", upstream.lastReq.Header.Get("Authorization"))
	require.Equal(t, "parent-chatgpt-account", upstream.lastReq.Header.Get("chatgpt-account-id"))

	serialized, err := json.Marshal(binding)
	require.NoError(t, err)
	for _, token := range []string{"parent-access-old", "parent-refresh-old", "parent-access-new", "parent-refresh-new"} {
		require.NotContains(t, string(serialized), token)
	}
}

func TestFetchOpenAIBackgroundResponseBoundRejectsOAuthScopeDrift(t *testing.T) {
	account := &Account{
		ID:          203,
		Platform:    PlatformOpenAI,
		Type:        AccountTypeOAuth,
		Concurrency: 1,
		Credentials: map[string]any{
			"access_token":       "oauth-access",
			"chatgpt_account_id": "chatgpt-account-original",
			"chatgpt_user_id":    "chatgpt-user-original",
			"organization_id":    "org-original",
		},
	}
	repo := &openAIBackgroundAccountRepo{accounts: map[int64]*Account{account.ID: account}}
	upstream := &httpUpstreamRecorder{resp: openAIBackgroundTestResponse(http.StatusOK, "", `{}`)}
	svc := &OpenAIGatewayService{
		cfg:          openAIBackgroundTestConfig(),
		accountRepo:  repo,
		httpUpstream: upstream,
	}
	binding, err := svc.CaptureOpenAIBackgroundUpstreamBinding(context.Background(), account.ID)
	require.NoError(t, err)
	account.Credentials["chatgpt_account_id"] = "chatgpt-account-other"

	result, err := svc.FetchOpenAIBackgroundResponseBound(context.Background(), binding, "resp_oauth")
	require.Nil(t, result)
	require.ErrorIs(t, err, ErrOpenAIBackgroundUpstreamIdentityMismatch)
	var mismatch *OpenAIBackgroundUpstreamIdentityMismatchError
	require.True(t, errors.As(err, &mismatch))
	require.Equal(t, "identity", mismatch.Field)
	require.Nil(t, upstream.lastReq)
}

func TestFetchOpenAIBackgroundResponseBoundRejectsNonPollableBinding(t *testing.T) {
	account := openAIBackgroundAPIKeyTestAccount()
	repo := &openAIBackgroundAccountRepo{accounts: map[int64]*Account{account.ID: account}}
	upstream := &httpUpstreamRecorder{resp: openAIBackgroundTestResponse(http.StatusOK, "", `{}`)}
	svc := &OpenAIGatewayService{
		cfg:          openAIBackgroundTestConfig(),
		accountRepo:  repo,
		httpUpstream: upstream,
	}
	binding, err := svc.CaptureOpenAIBackgroundUpstreamBinding(context.Background(), account.ID)
	require.NoError(t, err)
	binding.Pollable = false
	binding.ExecutionMode = "streaming_fallback"

	result, err := svc.FetchOpenAIBackgroundResponseBound(context.Background(), binding, "resp_stream")
	require.Nil(t, result)
	require.ErrorIs(t, err, ErrOpenAIBackgroundUpstreamBindingInvalid)
	require.Nil(t, upstream.lastReq)
}

func TestOpenAIGatewayBackgroundCreateForwardsIdempotencyAndCapturesExactSelectedAccount(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, passthrough := range []bool{false, true} {
		name := "managed"
		if passthrough {
			name = "passthrough"
		}
		t.Run(name, func(t *testing.T) {
			selected := openAIBackgroundAPIKeyTestAccount()
			selected.Extra = map[string]any{
				"use_responses_api":  true,
				"openai_passthrough": passthrough,
			}

			// Simulate the repository changing after scheduling selected the exact
			// account object. The creation binding must come from selected, not
			// from a second account-id lookup after the provider responds.
			current := *selected
			current.Credentials = map[string]any{
				"api_key":         "sk-different-current",
				"base_url":        "http://different-current.example",
				"project_id":      "project-current",
				"organization_id": "org-current",
			}
			repo := &openAIBackgroundAccountRepo{accounts: map[int64]*Account{selected.ID: &current}}
			upstream := &httpUpstreamRecorder{resp: openAIBackgroundTestResponse(
				http.StatusOK,
				"req_native_create",
				`{"id":"resp_native","object":"response","status":"in_progress","output":[]}`,
			)}
			svc := &OpenAIGatewayService{
				cfg:          openAIBackgroundTestConfig(),
				accountRepo:  repo,
				httpUpstream: upstream,
			}

			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(nil))
			c.Request.Header.Set("Content-Type", "application/json")
			c.Request.Header.Set("Idempotency-Key", "native-background-idempotency")
			SetOpenAIClientTransport(c, OpenAIClientTransportHTTP)
			body := []byte(`{"model":"gpt-5.6-sol","input":"test","background":true,"store":true,"stream":false}`)

			result, err := svc.Forward(context.Background(), c, selected, body)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.NotNil(t, upstream.lastReq)
			require.Equal(t, "native-background-idempotency", upstream.lastReq.Header.Get("Idempotency-Key"))
			require.Equal(t, "resp_native", gjson.Get(rec.Body.String(), "id").String())
			require.Equal(t, BackgroundResponseStatusInProgress, gjson.Get(rec.Body.String(), "status").String())
			require.Zero(t, result.Usage.InputTokens)
			require.Zero(t, result.Usage.OutputTokens)

			captured, ok := OpenAIBackgroundUpstreamBindingFromContext(c.Request.Context())
			require.True(t, ok)
			require.NotNil(t, captured)
			expected, err := svc.CaptureOpenAIBackgroundUpstreamBindingForAccount(context.Background(), selected)
			require.NoError(t, err)
			require.Equal(t, expected, *captured)

			currentBinding, err := svc.CaptureOpenAIBackgroundUpstreamBindingForAccount(context.Background(), &current)
			require.NoError(t, err)
			require.NotEqual(t, currentBinding.IdentityFingerprint, captured.IdentityFingerprint)

			// Returned bindings are defensive copies.
			captured.BaseURL = "http://mutated.invalid"
			again, ok := OpenAIBackgroundUpstreamBindingFromContext(c.Request.Context())
			require.True(t, ok)
			require.Equal(t, expected.BaseURL, again.BaseURL)
		})
	}
}

func TestHandleNonStreamingResponseAcceptsOnlyNativeBackgroundAcknowledgementWithoutUsage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &OpenAIGatewayService{cfg: openAIBackgroundTestConfig()}
	account := openAIBackgroundAPIKeyTestAccount()

	for _, status := range []string{BackgroundResponseStatusQueued, BackgroundResponseStatusInProgress} {
		t.Run(status, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			req := httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(nil))
			req = req.WithContext(WithOpenAIBackgroundUpstreamBinding(
				req.Context(),
				BackgroundResponseUpstreamBinding{AccountID: account.ID},
			))
			c.Request = req
			body := fmt.Sprintf(`{"id":"resp_native_%s","object":"response","status":%q,"output":[]}`, status, status)
			resp := openAIBackgroundTestResponse(http.StatusOK, "req_ack", body)

			result, err := svc.handleNonStreamingResponse(
				context.Background(),
				resp,
				c,
				account,
				"gpt-5.6-sol",
				"gpt-5.6-sol",
			)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, "resp_native_"+status, result.responseID)
			require.NotNil(t, result.usage)
			require.Zero(t, result.usage.InputTokens)
			require.Zero(t, result.usage.OutputTokens)
		})
	}

	t.Run("ordinary response still requires usage", func(t *testing.T) {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(nil))
		resp := openAIBackgroundTestResponse(
			http.StatusOK,
			"req_ordinary",
			`{"id":"resp_ordinary","object":"response","status":"completed","output":[]}`,
		)

		result, err := svc.handleNonStreamingResponse(
			context.Background(),
			resp,
			c,
			account,
			"gpt-5.6-sol",
			"gpt-5.6-sol",
		)
		require.Nil(t, result)
		require.EqualError(t, err, "parse response: invalid json response")
	})
}

func TestOpenAIBackgroundAgentIdentityBindingIncludesLogicalCredentialsWithoutLeaks(t *testing.T) {
	newAccount := func() *Account {
		return &Account{
			ID:          404,
			Platform:    PlatformOpenAI,
			Type:        AccountTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				openAIAuthModeCredentialKey: OpenAIAuthModeAgentIdentity,
				"agent_private_key":         "agent-private-key-secret",
				"agent_runtime_id":          "runtime-original",
				"task_id":                   "task-original",
				"chatgpt_account_id":        "chatgpt-account-stable",
				"chatgpt_user_id":           "chatgpt-user-stable",
				"organization_id":           "org-stable",
				"access_token":              "rotating-access-token",
			},
		}
	}

	for _, tt := range []struct {
		name   string
		mutate func(*Account)
	}{
		{
			name: "auth mode",
			mutate: func(account *Account) {
				account.Credentials[openAIAuthModeCredentialKey] = ""
			},
		},
		{
			name: "private key",
			mutate: func(account *Account) {
				account.Credentials["agent_private_key"] = "agent-private-key-replaced"
			},
		},
		{
			name: "runtime id",
			mutate: func(account *Account) {
				account.Credentials["agent_runtime_id"] = "runtime-replaced"
			},
		},
		{
			name: "task id",
			mutate: func(account *Account) {
				account.Credentials["task_id"] = "task-replaced"
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			account := newAccount()
			svc := &OpenAIGatewayService{cfg: openAIBackgroundTestConfig()}
			expected, err := svc.CaptureOpenAIBackgroundUpstreamBindingForAccount(context.Background(), account)
			require.NoError(t, err)

			serialized, err := json.Marshal(expected)
			require.NoError(t, err)
			require.NotContains(t, string(serialized), "agent-private-key-secret")
			require.NotContains(t, string(serialized), "rotating-access-token")

			tt.mutate(account)
			current, err := svc.CaptureOpenAIBackgroundUpstreamBindingForAccount(context.Background(), account)
			require.NoError(t, err)
			err = ValidateOpenAIBackgroundUpstreamBinding(expected, current)
			require.ErrorIs(t, err, ErrOpenAIBackgroundUpstreamIdentityMismatch)
			var mismatch *OpenAIBackgroundUpstreamIdentityMismatchError
			require.True(t, errors.As(err, &mismatch))
			require.Equal(t, "identity", mismatch.Field)
			require.NotContains(t, err.Error(), "agent-private-key")
		})
	}
}

func TestFetchOpenAIBackgroundResponseBoundClassifiesMissingCredentialReference(t *testing.T) {
	account := openAIBackgroundAPIKeyTestAccount()
	repo := &openAIBackgroundAccountRepo{accounts: map[int64]*Account{account.ID: account}}
	upstream := &httpUpstreamRecorder{resp: openAIBackgroundTestResponse(http.StatusOK, "", `{}`)}
	svc := &OpenAIGatewayService{
		cfg:          openAIBackgroundTestConfig(),
		accountRepo:  repo,
		httpUpstream: upstream,
	}
	binding, err := svc.CaptureOpenAIBackgroundUpstreamBindingForAccount(context.Background(), account)
	require.NoError(t, err)
	delete(repo.accounts, account.ID)

	result, err := svc.FetchOpenAIBackgroundResponseBound(context.Background(), binding, "resp_missing_account")
	require.Nil(t, result)
	require.ErrorIs(t, err, ErrOpenAIBackgroundUpstreamBindingInvalid)
	require.Nil(t, upstream.lastReq)
}

func openAIBackgroundAPIKeyTestAccount() *Account {
	return &Account{
		ID:          101,
		Platform:    PlatformOpenAI,
		Type:        AccountTypeAPIKey,
		Concurrency: 1,
		Credentials: map[string]any{
			"api_key":         "sk-original",
			"base_url":        "http://upstream.example",
			"project_id":      "project-original",
			"organization_id": "org-original",
		},
	}
}

func openAIBackgroundTestConfig() *config.Config {
	return &config.Config{
		Security: config.SecurityConfig{
			URLAllowlist: config.URLAllowlistConfig{
				Enabled:           false,
				AllowInsecureHTTP: true,
			},
		},
	}
}

func openAIBackgroundTestResponse(status int, requestID, body string) *http.Response {
	headers := make(http.Header)
	headers.Set("Content-Type", "application/json")
	headers.Set("x-request-id", requestID)
	return &http.Response{
		StatusCode: status,
		Header:     headers,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}
