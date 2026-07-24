package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/tidwall/gjson"
)

const openAIBackgroundUsageRequestIDPrefix = "background_response:"

type usageBillingRequestIDOverrideContextKey struct{}

// OpenAIBackgroundUsageAPIKeyService is the narrow API-key dependency needed
// to settle a detached Responses task after the original HTTP request and its
// authentication context are gone.
type OpenAIBackgroundUsageAPIKeyService interface {
	APIKeyQuotaUpdater
	GetByID(ctx context.Context, id int64) (*APIKey, error)
}

// OpenAIBackgroundResponseUsageInput contains only durable identifiers and the
// final upstream Response. It intentionally excludes API key material,
// Authorization headers, and the original prompt.
type OpenAIBackgroundResponseUsageInput struct {
	ProxyResponseID    string
	UserID             int64
	APIKeyID           int64
	AccountID          int64
	SubscriptionID     int64
	Model              string
	Result             json.RawMessage
	RequestFingerprint string
	ElapsedSeconds     int64
	ChannelUsageFields
}

// RecordBackgroundResponseUsage reconstructs the billing principals for a
// completed detached Response and records its real terminal usage. The stable
// proxy response id is forced as the billing request id, so a worker crash
// after SQL billing but before the Redis UsageRecorded CAS can safely replay
// this method without charging twice.
func (s *OpenAIGatewayService) RecordBackgroundResponseUsage(
	ctx context.Context,
	apiKeys OpenAIBackgroundUsageAPIKeyService,
	input *OpenAIBackgroundResponseUsageInput,
) error {
	if s == nil {
		return errors.New("openai background usage service is nil")
	}
	if input == nil {
		return errors.New("openai background usage input is nil")
	}
	proxyResponseID := strings.TrimSpace(input.ProxyResponseID)
	if proxyResponseID == "" || input.UserID <= 0 || input.APIKeyID <= 0 || input.AccountID <= 0 {
		return errors.New("openai background usage identifiers are incomplete")
	}
	if apiKeys == nil {
		return errors.New("openai background usage API key service is unavailable")
	}
	if len(input.Result) == 0 || !json.Valid(input.Result) {
		return errors.New("openai background usage result is not valid JSON")
	}
	if status := strings.TrimSpace(gjson.GetBytes(input.Result, "status").String()); status != BackgroundResponseStatusCompleted {
		return fmt.Errorf("openai background usage result is not completed: %s", status)
	}
	usage, ok := extractOpenAIUsageFromJSONBytes(input.Result)
	if !ok {
		// A create acknowledgement has no usage by contract. Never turn it into a
		// zero-token billing row; settlement must use the completed response.
		return errors.New("openai background completed response has no usage")
	}

	apiKey, err := apiKeys.GetByID(ctx, input.APIKeyID)
	if err != nil {
		return fmt.Errorf("load background usage API key: %w", err)
	}
	if apiKey == nil || apiKey.ID != input.APIKeyID || apiKey.UserID != input.UserID {
		return errors.New("background usage API key identity mismatch")
	}
	user := apiKey.User
	if user == nil && s.userRepo != nil {
		user, err = s.userRepo.GetByID(ctx, input.UserID)
		if err != nil {
			return fmt.Errorf("load background usage user: %w", err)
		}
	}
	if user == nil || user.ID != input.UserID {
		return errors.New("background usage user identity mismatch")
	}
	if s.accountRepo == nil {
		return errors.New("background usage account repository is unavailable")
	}
	account, err := s.accountRepo.GetByID(ctx, input.AccountID)
	if err != nil {
		return fmt.Errorf("load background usage account: %w", err)
	}
	if account == nil || account.ID != input.AccountID {
		return errors.New("background usage account identity mismatch")
	}

	var subscription *UserSubscription
	if input.SubscriptionID > 0 {
		if s.userSubRepo == nil {
			return errors.New("background usage subscription repository is unavailable")
		}
		subscription, err = s.userSubRepo.GetByIDIncludeDeleted(ctx, input.SubscriptionID)
		if err != nil {
			return fmt.Errorf("load background usage subscription: %w", err)
		}
		if subscription == nil ||
			subscription.ID != input.SubscriptionID ||
			subscription.UserID != input.UserID ||
			apiKey.GroupID == nil ||
			subscription.GroupID != *apiKey.GroupID {
			return errors.New("background usage subscription identity mismatch")
		}
	}
	// Detached settlement is replayed after worker crashes. The standard mode
	// must therefore have the transactional usage_billing_dedup repository;
	// falling back to the legacy non-idempotent post-billing path here could
	// charge twice if a process dies between billing and UsageRecorded CAS.
	if (s.cfg == nil || s.cfg.RunMode != config.RunModeSimple) && s.usageBillingRepo == nil {
		return errors.New("background usage idempotent billing repository is unavailable")
	}

	requestedModel := strings.TrimSpace(input.Model)
	upstreamModel := strings.TrimSpace(gjson.GetBytes(input.Result, "model").String())
	if requestedModel == "" {
		requestedModel = upstreamModel
	}
	if requestedModel == "" {
		return errors.New("openai background completed response has no model")
	}
	if upstreamModel == requestedModel {
		upstreamModel = ""
	}
	elapsed := input.ElapsedSeconds
	if elapsed < 0 {
		elapsed = 0
	}

	result := &OpenAIForwardResult{
		RequestID:        openAIBackgroundUsageRequestIDPrefix + proxyResponseID,
		ResponseID:       extractOpenAIResponseIDFromJSONBytes(input.Result),
		Usage:            usage,
		Model:            requestedModel,
		UpstreamModel:    upstreamModel,
		ServiceTier:      extractOpenAIServiceTierFromBody(input.Result),
		ReasoningEffort:  extractOpenAIReasoningEffortFromBody(input.Result, upstreamModel, requestedModel),
		Duration:         time.Duration(elapsed) * time.Second,
		ImageCount:       countOpenAIResponseImageOutputsFromJSONBytes(input.Result),
		ImageOutputSizes: collectOpenAIResponseImageOutputSizesFromJSONBytes(input.Result),
	}
	if len(result.ImageOutputSizes) > 0 {
		result.ImageOutputSize = result.ImageOutputSizes[0]
	}

	// Force the stable durable request id ahead of transient HTTP request ids
	// that may be present on a manual GET poll or recovery worker context.
	billingCtx := context.WithValue(
		ctx,
		usageBillingRequestIDOverrideContextKey{},
		openAIBackgroundUsageRequestIDPrefix+proxyResponseID,
	)
	return s.RecordUsage(billingCtx, &OpenAIRecordUsageInput{
		Result:             result,
		APIKey:             apiKey,
		User:               user,
		Account:            account,
		Subscription:       subscription,
		InboundEndpoint:    "/v1/responses",
		UpstreamEndpoint:   "/v1/responses",
		RequestPayloadHash: strings.TrimSpace(input.RequestFingerprint),
		APIKeyService:      apiKeys,
		QuotaPlatform:      PlatformFromAPIKey(apiKey),
		ChannelUsageFields: input.ChannelUsageFields,
	})
}
