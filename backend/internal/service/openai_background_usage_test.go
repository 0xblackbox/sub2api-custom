package service

import (
	"context"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/stretchr/testify/require"
)

type openAIBackgroundUsageAPIKeyServiceStub struct {
	apiKey         *APIKey
	err            error
	getCalls       int
	quotaCalls     int
	rateLimitCalls int
}

func (s *openAIBackgroundUsageAPIKeyServiceStub) GetByID(_ context.Context, _ int64) (*APIKey, error) {
	s.getCalls++
	return s.apiKey, s.err
}

func (s *openAIBackgroundUsageAPIKeyServiceStub) UpdateQuotaUsed(_ context.Context, _ int64, _ float64) error {
	s.quotaCalls++
	return nil
}

func (s *openAIBackgroundUsageAPIKeyServiceStub) UpdateRateLimitUsage(_ context.Context, _ int64, _ float64) error {
	s.rateLimitCalls++
	return nil
}

type openAIBackgroundUsageSubRepoStub struct {
	UserSubscriptionRepository
	subscription *UserSubscription
	err          error
	getCalls     int
}

func (s *openAIBackgroundUsageSubRepoStub) GetByIDIncludeDeleted(_ context.Context, _ int64) (*UserSubscription, error) {
	s.getCalls++
	return s.subscription, s.err
}

func TestRecordBackgroundResponseUsageHydratesDurablePrincipalsAndForcesStableDedupID(t *testing.T) {
	groupID := int64(41)
	user := &User{ID: 11}
	apiKey := &APIKey{
		ID:      22,
		UserID:  user.ID,
		User:    user,
		GroupID: &groupID,
		Group: &Group{
			ID:               groupID,
			RateMultiplier:   1,
			SubscriptionType: SubscriptionTypeSubscription,
		},
	}
	account := &Account{
		ID:       33,
		Platform: PlatformOpenAI,
		Type:     AccountTypeAPIKey,
	}
	subscription := &UserSubscription{ID: 44, UserID: user.ID, GroupID: groupID}
	apiKeys := &openAIBackgroundUsageAPIKeyServiceStub{apiKey: apiKey}
	subRepo := &openAIBackgroundUsageSubRepoStub{subscription: subscription}
	usageRepo := &openAIRecordUsageLogRepoStub{inserted: true}
	billingRepo := &openAIRecordUsageBillingRepoStub{result: &UsageBillingApplyResult{Applied: true}}
	svc := newOpenAIRecordUsageServiceWithBillingRepoForTest(
		usageRepo,
		billingRepo,
		&openAIRecordUsageUserRepoStub{},
		subRepo,
		nil,
	)
	svc.accountRepo = &openAIRecordUsageAccountRepoStub{account: account}

	// A transient HTTP request id from a poll must not replace the durable
	// background task id used by billing deduplication.
	ctx := context.WithValue(context.Background(), ctxkey.ClientRequestID, "transient-poll-request")
	err := svc.RecordBackgroundResponseUsage(ctx, apiKeys, &OpenAIBackgroundResponseUsageInput{
		ProxyResponseID:    "resp_bg_usage_once",
		UserID:             user.ID,
		APIKeyID:           apiKey.ID,
		AccountID:          account.ID,
		SubscriptionID:     subscription.ID,
		Model:              "gpt-5.6-sol",
		RequestFingerprint: "sha256:request-fingerprint",
		ElapsedSeconds:     1207,
		Result: []byte(`{
			"id":"resp_upstream_completed",
			"object":"response",
			"status":"completed",
			"model":"gpt-5.6",
			"reasoning":{"effort":"high"},
			"service_tier":"priority",
			"output":[
				{"type":"web_search_call","status":"completed","action":{"sources":[{"type":"url","url":"https://example.test/source"}]}}
			],
			"usage":{
				"input_tokens":120,
				"output_tokens":34,
				"input_tokens_details":{"cached_tokens":20}
			}
		}`),
	})
	require.NoError(t, err)
	require.Equal(t, 1, apiKeys.getCalls)
	require.Equal(t, 1, subRepo.getCalls)
	require.Equal(t, 1, billingRepo.calls)
	require.NotNil(t, billingRepo.lastCmd)
	require.Equal(t, "background_response:resp_bg_usage_once", billingRepo.lastCmd.RequestID)
	require.Equal(t, "sha256:request-fingerprint", billingRepo.lastCmd.RequestPayloadHash)
	require.Equal(t, 100, billingRepo.lastCmd.InputTokens)
	require.Equal(t, 34, billingRepo.lastCmd.OutputTokens)
	require.Equal(t, 20, billingRepo.lastCmd.CacheReadTokens)
	require.NotNil(t, billingRepo.lastCmd.SubscriptionID)
	require.Equal(t, subscription.ID, *billingRepo.lastCmd.SubscriptionID)

	require.Equal(t, 1, usageRepo.calls)
	require.NotNil(t, usageRepo.lastLog)
	require.Equal(t, "background_response:resp_bg_usage_once", usageRepo.lastLog.RequestID)
	require.Equal(t, "gpt-5.6-sol", usageRepo.lastLog.Model)
	require.NotNil(t, usageRepo.lastLog.UpstreamModel)
	require.Equal(t, "gpt-5.6", *usageRepo.lastLog.UpstreamModel)
	require.Equal(t, 100, usageRepo.lastLog.InputTokens)
	require.Equal(t, 34, usageRepo.lastLog.OutputTokens)
	require.Equal(t, 20, usageRepo.lastLog.CacheReadTokens)
	require.NotNil(t, usageRepo.lastLog.DurationMs)
	require.Equal(t, 1_207_000, *usageRepo.lastLog.DurationMs)
}

func TestRecordBackgroundResponseUsageRejectsCreateAcknowledgementWithoutUsage(t *testing.T) {
	apiKeys := &openAIBackgroundUsageAPIKeyServiceStub{}
	billingRepo := &openAIRecordUsageBillingRepoStub{}
	svc := newOpenAIRecordUsageServiceWithBillingRepoForTest(
		&openAIRecordUsageLogRepoStub{},
		billingRepo,
		&openAIRecordUsageUserRepoStub{},
		&openAIBackgroundUsageSubRepoStub{},
		nil,
	)

	err := svc.RecordBackgroundResponseUsage(context.Background(), apiKeys, &OpenAIBackgroundResponseUsageInput{
		ProxyResponseID: "resp_bg_no_usage",
		UserID:          1,
		APIKeyID:        2,
		AccountID:       3,
		Model:           "gpt-5.6-sol",
		Result:          []byte(`{"id":"resp_upstream","object":"response","status":"completed","usage":null}`),
	})
	require.EqualError(t, err, "openai background completed response has no usage")
	require.Zero(t, apiKeys.getCalls)
	require.Zero(t, billingRepo.calls)
}

func TestRecordBackgroundResponseUsageRejectsHydratedIdentityMismatch(t *testing.T) {
	apiKeys := &openAIBackgroundUsageAPIKeyServiceStub{
		apiKey: &APIKey{ID: 2, UserID: 999, User: &User{ID: 999}},
	}
	svc := &OpenAIGatewayService{}
	err := svc.RecordBackgroundResponseUsage(context.Background(), apiKeys, &OpenAIBackgroundResponseUsageInput{
		ProxyResponseID: "resp_bg_identity",
		UserID:          1,
		APIKeyID:        2,
		AccountID:       3,
		Model:           "gpt-5.6-sol",
		Result:          []byte(`{"id":"resp_upstream","object":"response","status":"completed","usage":{"input_tokens":1,"output_tokens":1}}`),
	})
	require.EqualError(t, err, "background usage API key identity mismatch")
}
