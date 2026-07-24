package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"time"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/util/logredact"
	"github.com/google/uuid"
)

const (
	BackgroundResponseStatusQueued     = "queued"
	BackgroundResponseStatusInProgress = "in_progress"
	BackgroundResponseStatusCompleted  = "completed"
	BackgroundResponseStatusFailed     = "failed"
	BackgroundResponseStatusCancelled  = "cancelled"

	defaultBackgroundResponseTTL              = 24 * time.Hour
	defaultBackgroundResponseExecutionTimeout = 2 * time.Hour
	defaultBackgroundResponseLeaseTTL         = 30 * time.Second
	minBackgroundResponseRetention            = 24 * time.Hour
	backgroundResponseExecutionRetentionSlack = time.Hour
	backgroundResponseCASMaxAttempts          = 16
	maxBackgroundResponseResultBytes          = 32 << 20
	maxBackgroundResponseUsageErrorBytes      = 256
	backgroundResponseUsageSettlementVersion  = 1
	backgroundResponseUsageRetryBaseDelay     = 15 * time.Second
	backgroundResponseUsageRetryMaxDelay      = time.Hour
)

var (
	ErrBackgroundResponseNotFound        = infraerrors.New(http.StatusNotFound, "BACKGROUND_RESPONSE_NOT_FOUND", "response not found")
	ErrBackgroundResponseForbidden       = infraerrors.New(http.StatusForbidden, "BACKGROUND_RESPONSE_FORBIDDEN", "response does not belong to this API key")
	ErrBackgroundResponseUnavailable     = infraerrors.New(http.StatusServiceUnavailable, "BACKGROUND_RESPONSE_UNAVAILABLE", "background response storage is unavailable")
	ErrBackgroundResponseBindingConflict = infraerrors.New(http.StatusConflict, "BACKGROUND_RESPONSE_BINDING_CONFLICT", "background response upstream binding is immutable")
	ErrBackgroundResponseLeaseLost       = infraerrors.New(http.StatusConflict, "BACKGROUND_RESPONSE_LEASE_LOST", "background response worker lease is no longer current")
	ErrBackgroundResponseTaskExpired     = infraerrors.New(http.StatusGone, "PROXY_TASK_EXPIRED", "background response retention expired")
	ErrBackgroundResponseMappingMissing  = infraerrors.New(http.StatusInternalServerError, "PROXY_MAPPING_MISSING", "background response mapping is missing")
	ErrBackgroundResponseUsageNotReady   = infraerrors.New(http.StatusConflict, "BACKGROUND_RESPONSE_USAGE_NOT_READY", "background response usage is not ready to record")
	errBackgroundResponseVersionConflict = errors.New("background response version conflict")
)

// BackgroundResponseTaskRecord is the private Redis representation of a
// detached Responses API request. UserID/APIKeyID/account identifiers are never
// returned to callers and prevent one key from reading another key's response.
type BackgroundResponseTaskRecord struct {
	Version                int64                              `json:"version"`
	ID                     string                             `json:"id"`
	UserID                 int64                              `json:"user_id"`
	APIKeyID               int64                              `json:"api_key_id"`
	Model                  string                             `json:"model"`
	Status                 string                             `json:"status"`
	HTTPStatus             int                                `json:"http_status,omitempty"`
	Result                 json.RawMessage                    `json:"result,omitempty"`
	Error                  json.RawMessage                    `json:"error,omitempty"`
	CreatedAt              int64                              `json:"created_at"`
	StartedAt              *int64                             `json:"started_at,omitempty"`
	CompletedAt            *int64                             `json:"completed_at,omitempty"`
	FailedAt               *int64                             `json:"failed_at,omitempty"`
	ElapsedSeconds         int64                              `json:"elapsed_seconds,omitempty"`
	DeadlineAt             int64                              `json:"deadline_at"`
	ExpiresAt              int64                              `json:"expires_at"`
	UpstreamResponseID     string                             `json:"upstream_response_id,omitempty"`
	UpstreamRequestID      string                             `json:"upstream_request_id,omitempty"`
	UpstreamAccountID      int64                              `json:"upstream_account_id,omitempty"`
	UpstreamExecutionMode  string                             `json:"upstream_execution_mode,omitempty"`
	UpstreamPollable       bool                               `json:"upstream_pollable,omitempty"`
	UpstreamBinding        *BackgroundResponseUpstreamBinding `json:"upstream_binding,omitempty"`
	RequestFingerprint     string                             `json:"request_fingerprint,omitempty"`
	IdempotencyKeyHash     string                             `json:"idempotency_key_hash,omitempty"`
	SubscriptionID         int64                              `json:"subscription_id,omitempty"`
	UsageSettlementVersion int                                `json:"usage_settlement_version,omitempty"`
	UsageRecorded          bool                               `json:"usage_recorded,omitempty"`
	UsageRecordedAt        *int64                             `json:"usage_recorded_at,omitempty"`
	UsageRecordAttempts    int                                `json:"usage_record_attempts,omitempty"`
	UsageConsecutiveErrors int                                `json:"usage_consecutive_errors,omitempty"`
	UsageNextAttemptAt     *int64                             `json:"usage_next_attempt_at,omitempty"`
	UsageRecordLastError   string                             `json:"usage_record_last_error,omitempty"`
}

type BackgroundResponseOwner struct {
	UserID   int64
	APIKeyID int64
}

type BackgroundResponseTaskMetadata struct {
	RequestFingerprint string
	IdempotencyKeyHash string
	SubscriptionID     int64
}

// BackgroundResponseTaskIndex is a small, non-secret tombstone retained after
// the full task body expires. It lets an authenticated owner distinguish a
// task whose retention elapsed from an id that never existed, without retaining
// prompts, output, credentials, or upstream routing details.
type BackgroundResponseTaskIndex struct {
	ID        string `json:"id"`
	UserID    int64  `json:"user_id"`
	APIKeyID  int64  `json:"api_key_id"`
	ExpiresAt int64  `json:"expires_at"`
}

// BackgroundResponseTaskLease is a fencing token issued by the persistence
// layer. Fence increases on every successful takeover. A terminal write made by
// an older worker is rejected atomically even if it races immediately after its
// lease expires.
type BackgroundResponseTaskLease struct {
	Holder string
	Fence  int64
}

type BackgroundResponseTaskStore interface {
	Save(ctx context.Context, task *BackgroundResponseTaskRecord, ttl time.Duration) error
	Get(ctx context.Context, id string) (*BackgroundResponseTaskRecord, error)
	ListActive(ctx context.Context, limit int) ([]*BackgroundResponseTaskRecord, error)
	ReserveIdempotency(ctx context.Context, owner BackgroundResponseOwner, keyHash, taskID string, ttl time.Duration) (existingTaskID string, reserved bool, err error)
	ReleaseIdempotency(ctx context.Context, owner BackgroundResponseOwner, keyHash, taskID string) error
}

// BackgroundResponseTaskAtomicCreateStore creates the durable task body, its
// owner-scoped idempotency reservation, and the task index in one storage
// transaction. Production stores must implement this interface. Without that
// atomic boundary, a concurrent replay can observe a reservation before the
// task body exists, incorrectly reclaim it, and submit the same upstream
// background request twice.
type BackgroundResponseTaskAtomicCreateStore interface {
	CreateWithIdempotency(
		ctx context.Context,
		task *BackgroundResponseTaskRecord,
		owner BackgroundResponseOwner,
		keyHash string,
		ttl time.Duration,
	) (existingTaskID string, created bool, err error)
}

// BackgroundResponseTaskCASStore atomically replaces a task only when its
// persisted version still matches expectedVersion. Implementations must treat a
// legacy record with no version field as version zero.
type BackgroundResponseTaskCASStore interface {
	SaveIfVersion(ctx context.Context, task *BackgroundResponseTaskRecord, expectedVersion int64, ttl time.Duration) (bool, error)
}

// BackgroundResponseTaskLeaseStore provides a short, renewable distributed
// worker lease. The holder value is an opaque process/worker identifier and
// must only be released or renewed by the same holder.
type BackgroundResponseTaskLeaseStore interface {
	AcquireLease(ctx context.Context, taskID, holder string, ttl time.Duration) (bool, error)
	RenewLease(ctx context.Context, taskID, holder string, ttl time.Duration) (bool, error)
	ReleaseLease(ctx context.Context, taskID, holder string) error
}

// BackgroundResponseTaskFencedLeaseStore couples a monotonically increasing
// lease token with task CAS. SaveIfVersionAndLease must check both task version
// and the exact current lease in the same atomic storage operation.
type BackgroundResponseTaskFencedLeaseStore interface {
	AcquireFencedLease(ctx context.Context, taskID, holder string, ttl time.Duration) (fence int64, acquired bool, err error)
	RenewFencedLease(ctx context.Context, taskID, holder string, fence int64, ttl time.Duration) (bool, error)
	ReleaseFencedLease(ctx context.Context, taskID, holder string, fence int64) error
	SaveIfVersionAndLease(ctx context.Context, task *BackgroundResponseTaskRecord, expectedVersion int64, holder string, fence int64, ttl time.Duration) (saved bool, leaseCurrent bool, err error)
}

// BackgroundResponseTaskIndexStore exposes the small task index used to
// classify expired and unexpectedly missing mappings.
type BackgroundResponseTaskIndexStore interface {
	GetTaskIndex(ctx context.Context, id string) (*BackgroundResponseTaskIndex, error)
}

// BackgroundResponseTaskUsageStore lists completed tasks created under the
// durable usage-settlement contract that still need billing reconciliation.
// Legacy records (settlement version zero) must not be returned because some
// were already billed by the pre-fix create path under a different request id.
type BackgroundResponseTaskUsageStore interface {
	ListUsagePending(ctx context.Context, limit int) ([]*BackgroundResponseTaskRecord, error)
}

type BackgroundResponseTaskService struct {
	store            BackgroundResponseTaskStore
	ttl              time.Duration
	executionTimeout time.Duration
}

func NewBackgroundResponseTaskService(store BackgroundResponseTaskStore) *BackgroundResponseTaskService {
	return NewBackgroundResponseTaskServiceWithOptions(store, defaultBackgroundResponseTTL, defaultBackgroundResponseExecutionTimeout)
}

func NewBackgroundResponseTaskServiceWithOptions(store BackgroundResponseTaskStore, ttl, executionTimeout time.Duration) *BackgroundResponseTaskService {
	if ttl <= 0 {
		ttl = defaultBackgroundResponseTTL
	}
	if executionTimeout <= 0 {
		executionTimeout = defaultBackgroundResponseExecutionTimeout
	}
	return &BackgroundResponseTaskService{store: store, ttl: ttl, executionTimeout: executionTimeout}
}

func (s *BackgroundResponseTaskService) Enabled() bool {
	return s != nil && s.store != nil
}

func (s *BackgroundResponseTaskService) ExecutionTimeout() time.Duration {
	if s == nil || s.executionTimeout <= 0 {
		return defaultBackgroundResponseExecutionTimeout
	}
	return s.executionTimeout
}

func (s *BackgroundResponseTaskService) Create(ctx context.Context, owner BackgroundResponseOwner, model string) (*BackgroundResponseTaskRecord, error) {
	task, _, err := s.CreateWithMetadata(ctx, owner, model, BackgroundResponseTaskMetadata{})
	return task, err
}

func (s *BackgroundResponseTaskService) CreateWithMetadata(ctx context.Context, owner BackgroundResponseOwner, model string, meta BackgroundResponseTaskMetadata) (*BackgroundResponseTaskRecord, bool, error) {
	if !s.Enabled() {
		return nil, false, ErrBackgroundResponseUnavailable
	}
	now := time.Now().UTC()
	retention := s.retentionTTL()
	task := &BackgroundResponseTaskRecord{
		Version:                1,
		ID:                     "resp_bg_" + strings.ReplaceAll(uuid.NewString(), "-", ""),
		UserID:                 owner.UserID,
		APIKeyID:               owner.APIKeyID,
		Model:                  strings.TrimSpace(model),
		Status:                 BackgroundResponseStatusQueued,
		CreatedAt:              now.Unix(),
		DeadlineAt:             now.Add(s.ExecutionTimeout()).Unix(),
		ExpiresAt:              now.Add(retention).Unix(),
		RequestFingerprint:     strings.TrimSpace(meta.RequestFingerprint),
		IdempotencyKeyHash:     strings.TrimSpace(meta.IdempotencyKeyHash),
		SubscriptionID:         meta.SubscriptionID,
		UsageSettlementVersion: backgroundResponseUsageSettlementVersion,
	}
	if task.IdempotencyKeyHash != "" {
		atomicStore, ok := s.store.(BackgroundResponseTaskAtomicCreateStore)
		if !ok {
			// Idempotent background creation is a billable, non-replayable POST.
			// A process-local reserve+save fallback cannot provide the required
			// cross-instance atomicity, so fail closed instead of risking two
			// upstream tasks.
			return nil, false, ErrBackgroundResponseUnavailable.WithCause(errors.New("background response store does not support atomic idempotent create"))
		}
		existingID, created, err := atomicStore.CreateWithIdempotency(
			ctx,
			task,
			owner,
			task.IdempotencyKeyHash,
			retention,
		)
		if err != nil {
			return nil, false, ErrBackgroundResponseUnavailable.WithCause(err)
		}
		if created {
			return task, true, nil
		}
		existing, getErr := s.store.Get(ctx, strings.TrimSpace(existingID))
		if getErr != nil {
			// A live reservation without a task body can only come from a legacy
			// non-atomic creator during a rolling upgrade. Leave it untouched
			// until its original TTL expires.
			return nil, false, ErrBackgroundResponseUnavailable.WithCause(getErr)
		}
		if existing.UserID != owner.UserID || existing.APIKeyID != owner.APIKeyID {
			return nil, false, ErrBackgroundResponseNotFound
		}
		if task.RequestFingerprint != "" && existing.RequestFingerprint != "" && task.RequestFingerprint != existing.RequestFingerprint {
			return nil, false, ErrIdempotencyKeyConflict
		}
		return existing, false, nil
	}
	if err := s.store.Save(ctx, task, retention); err != nil {
		return nil, false, ErrBackgroundResponseUnavailable.WithCause(err)
	}
	return task, true, nil
}

func (s *BackgroundResponseTaskService) Get(ctx context.Context, owner BackgroundResponseOwner, id string) (*BackgroundResponseTaskRecord, error) {
	if !s.Enabled() {
		return nil, ErrBackgroundResponseUnavailable
	}
	id = strings.TrimSpace(id)
	task, err := s.store.Get(ctx, id)
	if err != nil {
		if errors.Is(err, ErrBackgroundResponseNotFound) {
			return nil, s.classifyMissingTask(ctx, id, &owner)
		}
		return nil, ErrBackgroundResponseUnavailable.WithCause(err)
	}
	if task.UserID != owner.UserID || task.APIKeyID != owner.APIKeyID {
		// Deliberately hide the existence of another caller's response.
		return nil, ErrBackgroundResponseNotFound
	}
	return task, nil
}

func (s *BackgroundResponseTaskService) GetByID(ctx context.Context, id string) (*BackgroundResponseTaskRecord, error) {
	if !s.Enabled() {
		return nil, ErrBackgroundResponseUnavailable
	}
	id = strings.TrimSpace(id)
	task, err := s.store.Get(ctx, id)
	if err != nil {
		if errors.Is(err, ErrBackgroundResponseNotFound) {
			return nil, s.classifyMissingTask(ctx, id, nil)
		}
		return nil, ErrBackgroundResponseUnavailable.WithCause(err)
	}
	return task, nil
}

// ExecutionDeadline returns the absolute persisted task deadline. Legacy
// records derive it from CreatedAt exactly once at read time; callers must not
// grant a fresh duration when a worker restarts.
func (s *BackgroundResponseTaskService) ExecutionDeadline(task *BackgroundResponseTaskRecord) time.Time {
	if task == nil {
		return time.Time{}
	}
	if task.DeadlineAt > 0 {
		return time.Unix(task.DeadlineAt, 0).UTC()
	}
	if task.CreatedAt > 0 {
		return time.Unix(task.CreatedAt, 0).UTC().Add(s.ExecutionTimeout())
	}
	return time.Time{}
}

// ExecutionDeadlineExceeded treats a missing/corrupt timestamp as already
// expired. This fails closed instead of silently extending an unbounded task.
func (s *BackgroundResponseTaskService) ExecutionDeadlineExceeded(task *BackgroundResponseTaskRecord, now time.Time) bool {
	deadline := s.ExecutionDeadline(task)
	if deadline.IsZero() {
		return true
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return !now.Before(deadline)
}

func (s *BackgroundResponseTaskService) ListActive(ctx context.Context, limit int) ([]*BackgroundResponseTaskRecord, error) {
	if !s.Enabled() {
		return nil, ErrBackgroundResponseUnavailable
	}
	if limit <= 0 {
		limit = 1000
	}
	tasks, err := s.store.ListActive(ctx, limit)
	if err != nil {
		return nil, ErrBackgroundResponseUnavailable.WithCause(err)
	}
	return tasks, nil
}

func (s *BackgroundResponseTaskService) ListUsagePending(ctx context.Context, limit int) ([]*BackgroundResponseTaskRecord, error) {
	if !s.Enabled() {
		return nil, ErrBackgroundResponseUnavailable
	}
	if limit <= 0 {
		limit = 1000
	}
	store, ok := s.store.(BackgroundResponseTaskUsageStore)
	if !ok {
		return nil, ErrBackgroundResponseUnavailable.WithCause(errors.New("background response store does not support usage settlement recovery"))
	}
	tasks, err := store.ListUsagePending(ctx, limit)
	if err != nil {
		return nil, ErrBackgroundResponseUnavailable.WithCause(err)
	}
	return tasks, nil
}

func (s *BackgroundResponseTaskService) MarkInProgress(ctx context.Context, id string) error {
	return s.update(ctx, id, func(task *BackgroundResponseTaskRecord) bool {
		if backgroundResponseTaskTerminal(task.Status) {
			return false
		}
		now := time.Now().UTC().Unix()
		if task.StartedAt == nil {
			task.StartedAt = &now
		}
		task.Status = BackgroundResponseStatusInProgress
		return true
	})
}

func (s *BackgroundResponseTaskService) AttachUpstream(ctx context.Context, id string, accountID int64, upstreamResponseID, upstreamRequestID string) error {
	bindingConflict := false
	err := s.update(ctx, id, func(task *BackgroundResponseTaskRecord) bool {
		if backgroundResponseTaskTerminal(task.Status) {
			return false
		}
		if accountID > 0 {
			if task.UpstreamAccountID > 0 && task.UpstreamAccountID != accountID {
				bindingConflict = true
				return false
			}
			task.UpstreamAccountID = accountID
		}
		if upstreamResponseID = strings.TrimSpace(upstreamResponseID); upstreamResponseID != "" {
			if task.UpstreamResponseID != "" && task.UpstreamResponseID != upstreamResponseID {
				bindingConflict = true
				return false
			}
			task.UpstreamResponseID = upstreamResponseID
		}
		if upstreamRequestID = strings.TrimSpace(upstreamRequestID); upstreamRequestID != "" {
			task.UpstreamRequestID = upstreamRequestID
		}
		return accountID > 0 || upstreamResponseID != "" || upstreamRequestID != ""
	})
	if err != nil {
		return err
	}
	if bindingConflict {
		return ErrBackgroundResponseBindingConflict
	}
	return nil
}

// AttachNativeUpstreamMapping persists the complete creation-time mapping in a
// single CAS transition. There is no intermediate state where a recovery worker
// can observe an upstream response id without the identity and route that must
// be used to poll it.
func (s *BackgroundResponseTaskService) AttachNativeUpstreamMapping(
	ctx context.Context,
	id string,
	accountID int64,
	upstreamResponseID string,
	upstreamRequestID string,
	binding BackgroundResponseUpstreamBinding,
) error {
	upstreamResponseID = strings.TrimSpace(upstreamResponseID)
	upstreamRequestID = strings.TrimSpace(upstreamRequestID)
	if accountID <= 0 || upstreamResponseID == "" ||
		binding.AccountID != accountID ||
		binding.ExecutionMode != BackgroundResponseExecutionModeNative ||
		!binding.Pollable {
		return ErrBackgroundResponseBindingConflict.WithCause(ErrOpenAIBackgroundUpstreamBindingInvalid)
	}
	if err := ValidateOpenAIBackgroundUpstreamBinding(binding, binding); err != nil {
		return ErrBackgroundResponseBindingConflict.WithCause(err)
	}
	bindingCopy, err := cloneOpenAIBackgroundUpstreamBinding(binding)
	if err != nil {
		return ErrBackgroundResponseUnavailable.WithCause(err)
	}

	bindingConflict := false
	err = s.update(ctx, id, func(task *BackgroundResponseTaskRecord) bool {
		if backgroundResponseTaskTerminal(task.Status) {
			return false
		}
		if (task.UpstreamAccountID > 0 && task.UpstreamAccountID != accountID) ||
			(task.UpstreamResponseID != "" && task.UpstreamResponseID != upstreamResponseID) ||
			(task.UpstreamExecutionMode != "" &&
				(task.UpstreamExecutionMode != BackgroundResponseExecutionModeNative || !task.UpstreamPollable)) ||
			(task.UpstreamBinding != nil && !reflect.DeepEqual(task.UpstreamBinding, bindingCopy)) {
			bindingConflict = true
			return false
		}
		task.UpstreamAccountID = accountID
		task.UpstreamResponseID = upstreamResponseID
		task.UpstreamRequestID = upstreamRequestID
		task.UpstreamExecutionMode = BackgroundResponseExecutionModeNative
		task.UpstreamPollable = true
		task.UpstreamBinding = bindingCopy
		return true
	})
	if err != nil {
		return err
	}
	if bindingConflict {
		return ErrBackgroundResponseBindingConflict
	}
	return nil
}

// SetExecutionMode records how the upstream response was created. Pollability
// is explicit so a response.created id captured from an SSE fallback is never
// accidentally treated as a native background response id after restart.
func (s *BackgroundResponseTaskService) SetExecutionMode(ctx context.Context, id, mode string, pollable bool) error {
	mode = strings.TrimSpace(mode)
	bindingConflict := false
	err := s.update(ctx, id, func(task *BackgroundResponseTaskRecord) bool {
		if backgroundResponseTaskTerminal(task.Status) {
			return false
		}
		if task.UpstreamBinding != nil &&
			(task.UpstreamBinding.ExecutionMode != mode || task.UpstreamBinding.Pollable != pollable) {
			bindingConflict = true
			return false
		}
		if task.UpstreamExecutionMode == mode && task.UpstreamPollable == pollable {
			return true // An active heartbeat also renews retention.
		}
		task.UpstreamExecutionMode = mode
		task.UpstreamPollable = pollable
		return true
	})
	if err != nil {
		return err
	}
	if bindingConflict {
		return ErrBackgroundResponseBindingConflict
	}
	return nil
}

// AttachUpstreamBinding persists an immutable, non-secret snapshot of the
// upstream identity and route used for creation. Later polling must use this
// binding rather than resolving a newly edited account configuration.
func (s *BackgroundResponseTaskService) AttachUpstreamBinding(ctx context.Context, id string, binding BackgroundResponseUpstreamBinding) error {
	if err := ValidateOpenAIBackgroundUpstreamBinding(binding, binding); err != nil {
		return ErrBackgroundResponseBindingConflict.WithCause(err)
	}
	bindingCopy, err := cloneOpenAIBackgroundUpstreamBinding(binding)
	if err != nil {
		return ErrBackgroundResponseUnavailable.WithCause(err)
	}
	bindingConflict := false
	err = s.update(ctx, id, func(task *BackgroundResponseTaskRecord) bool {
		if backgroundResponseTaskTerminal(task.Status) {
			return false
		}
		if task.UpstreamAccountID > 0 && bindingCopy.AccountID > 0 && task.UpstreamAccountID != bindingCopy.AccountID {
			bindingConflict = true
			return false
		}
		if task.UpstreamExecutionMode != "" &&
			(task.UpstreamExecutionMode != bindingCopy.ExecutionMode || task.UpstreamPollable != bindingCopy.Pollable) {
			bindingConflict = true
			return false
		}
		if task.UpstreamBinding != nil {
			if !reflect.DeepEqual(task.UpstreamBinding, bindingCopy) {
				bindingConflict = true
			}
			return false
		}
		task.UpstreamBinding = bindingCopy
		task.UpstreamExecutionMode = bindingCopy.ExecutionMode
		task.UpstreamPollable = bindingCopy.Pollable
		if task.UpstreamAccountID == 0 && bindingCopy.AccountID > 0 {
			task.UpstreamAccountID = bindingCopy.AccountID
		}
		return true
	})
	if err != nil {
		return err
	}
	if bindingConflict {
		return ErrBackgroundResponseBindingConflict
	}
	return nil
}

func (s *BackgroundResponseTaskService) Complete(ctx context.Context, id string, statusCode int, result json.RawMessage) error {
	if !json.Valid(result) {
		return s.Fail(ctx, id, http.StatusBadGateway, backgroundResponseErrorJSON("api_error", "upstream returned a non-JSON response"))
	}
	if len(result) > maxBackgroundResponseResultBytes {
		return s.Fail(ctx, id, http.StatusBadGateway, backgroundResponseErrorJSON("response_too_large", "background response exceeded the storage limit"))
	}
	return s.finish(ctx, id, BackgroundResponseStatusCompleted, statusCode, result, nil)
}

func (s *BackgroundResponseTaskService) CompleteWithLease(ctx context.Context, id string, lease BackgroundResponseTaskLease, statusCode int, result json.RawMessage) error {
	if !json.Valid(result) {
		return s.FailWithLease(ctx, id, lease, http.StatusBadGateway, backgroundResponseErrorJSON("api_error", "upstream returned a non-JSON response"))
	}
	if len(result) > maxBackgroundResponseResultBytes {
		return s.FailWithLease(ctx, id, lease, http.StatusBadGateway, backgroundResponseErrorJSON("response_too_large", "background response exceeded the storage limit"))
	}
	return s.finishWithLease(ctx, id, lease, BackgroundResponseStatusCompleted, statusCode, result, nil)
}

func (s *BackgroundResponseTaskService) Fail(ctx context.Context, id string, statusCode int, taskErr json.RawMessage) error {
	if !json.Valid(taskErr) {
		taskErr = backgroundResponseErrorJSON("api_error", "background response failed")
	}
	return s.finish(ctx, id, BackgroundResponseStatusFailed, statusCode, nil, taskErr)
}

func (s *BackgroundResponseTaskService) FailWithLease(ctx context.Context, id string, lease BackgroundResponseTaskLease, statusCode int, taskErr json.RawMessage) error {
	if !json.Valid(taskErr) {
		taskErr = backgroundResponseErrorJSON("api_error", "background response failed")
	}
	return s.finishWithLease(ctx, id, lease, BackgroundResponseStatusFailed, statusCode, nil, taskErr)
}

// CancelUpstreamWithLease records an upstream terminal cancellation together
// with its classified audit payload. It is separate from a caller-requested
// cancellation so upstream_task_cancelled is not silently collapsed.
func (s *BackgroundResponseTaskService) CancelUpstreamWithLease(ctx context.Context, id string, lease BackgroundResponseTaskLease, statusCode int, taskErr json.RawMessage) error {
	if !json.Valid(taskErr) {
		taskErr = backgroundResponseErrorJSON("upstream_task_cancelled", "upstream background response was cancelled")
	}
	return s.finishWithLease(ctx, id, lease, BackgroundResponseStatusCancelled, statusCode, nil, taskErr)
}

// MarkUsageRecorded closes durable background usage settlement only after the
// idempotent SQL billing path has returned success. Calling it repeatedly is a
// no-op. It deliberately rejects non-completed and legacy tasks.
func (s *BackgroundResponseTaskService) MarkUsageRecorded(ctx context.Context, id string) error {
	notReady := false
	err := s.update(ctx, id, func(task *BackgroundResponseTaskRecord) bool {
		if task.UsageRecorded {
			return false
		}
		if task.Status != BackgroundResponseStatusCompleted ||
			task.UsageSettlementVersion < backgroundResponseUsageSettlementVersion {
			notReady = true
			return false
		}
		now := time.Now().UTC().Unix()
		task.UsageRecorded = true
		task.UsageRecordedAt = &now
		task.UsageConsecutiveErrors = 0
		task.UsageNextAttemptAt = nil
		task.UsageRecordLastError = ""
		return true
	})
	if err != nil {
		return err
	}
	if notReady {
		return ErrBackgroundResponseUsageNotReady
	}
	return nil
}

// RecordUsageAttempt persists the outcome of one completed billing attempt.
// Callers pass an empty message for a successful attempt immediately before
// MarkUsageRecorded, or a sanitized error source on failure. Billing itself is
// still protected by usage_billing_dedup; this counter is operational audit.
func (s *BackgroundResponseTaskService) RecordUsageAttempt(ctx context.Context, id, errMsg string) error {
	notReady := false
	err := s.update(ctx, id, func(task *BackgroundResponseTaskRecord) bool {
		if task.UsageRecorded {
			return false
		}
		if task.Status != BackgroundResponseStatusCompleted ||
			task.UsageSettlementVersion < backgroundResponseUsageSettlementVersion {
			notReady = true
			return false
		}
		task.UsageRecordAttempts++
		errMsg = sanitizeBackgroundResponseUsageError(errMsg)
		task.UsageRecordLastError = errMsg
		if errMsg == "" {
			task.UsageConsecutiveErrors = 0
			task.UsageNextAttemptAt = nil
			return true
		}
		task.UsageConsecutiveErrors++
		nextAttempt := time.Now().UTC().Add(backgroundResponseUsageRetryDelay(task.UsageConsecutiveErrors)).Unix()
		task.UsageNextAttemptAt = &nextAttempt
		return true
	})
	if err != nil {
		return err
	}
	if notReady {
		return ErrBackgroundResponseUsageNotReady
	}
	return nil
}

func (s *BackgroundResponseTaskService) Cancel(ctx context.Context, owner BackgroundResponseOwner, id string) (*BackgroundResponseTaskRecord, error) {
	task, err := s.Get(ctx, owner, id)
	if err != nil {
		return nil, err
	}
	if backgroundResponseTaskTerminal(task.Status) {
		return task, nil
	}
	if err := s.finish(ctx, id, BackgroundResponseStatusCancelled, http.StatusOK, nil, nil); err != nil {
		return nil, err
	}
	return s.Get(ctx, owner, id)
}

func (s *BackgroundResponseTaskService) CancelByID(ctx context.Context, id string) error {
	return s.finish(ctx, id, BackgroundResponseStatusCancelled, http.StatusOK, nil, nil)
}

func (s *BackgroundResponseTaskService) FailActiveOnStartup(ctx context.Context, limit int) (int, error) {
	if !s.Enabled() {
		return 0, ErrBackgroundResponseUnavailable
	}
	if limit <= 0 {
		limit = 1000
	}
	tasks, err := s.store.ListActive(ctx, limit)
	if err != nil {
		return 0, ErrBackgroundResponseUnavailable.WithCause(err)
	}
	failed := 0
	for _, task := range tasks {
		if task == nil || backgroundResponseTaskTerminal(task.Status) || strings.TrimSpace(task.UpstreamResponseID) != "" {
			continue
		}
		if err := s.Fail(ctx, task.ID, http.StatusServiceUnavailable, backgroundResponseErrorJSON("background_task_failed", "background task did not survive proxy restart before upstream response id was stored")); err != nil {
			return failed, err
		}
		failed++
	}
	return failed, nil
}

func (s *BackgroundResponseTaskService) update(ctx context.Context, id string, mutate func(*BackgroundResponseTaskRecord) bool) error {
	if !s.Enabled() {
		return ErrBackgroundResponseUnavailable
	}
	id = strings.TrimSpace(id)
	for attempt := 0; attempt < backgroundResponseCASMaxAttempts; attempt++ {
		task, err := s.store.Get(ctx, id)
		if err != nil {
			if errors.Is(err, ErrBackgroundResponseNotFound) {
				return s.classifyMissingTask(ctx, id, nil)
			}
			return ErrBackgroundResponseUnavailable.WithCause(err)
		}
		expectedVersion := task.Version
		if !mutate(task) {
			return nil
		}
		task.Version = expectedVersion + 1
		if task.Version <= 0 {
			task.Version = 1
		}
		ttl := s.retentionTTL()
		task.ExpiresAt = time.Now().UTC().Add(ttl).Unix()
		if casStore, ok := s.store.(BackgroundResponseTaskCASStore); ok {
			saved, err := casStore.SaveIfVersion(ctx, task, expectedVersion, ttl)
			if err != nil {
				if errors.Is(err, ErrBackgroundResponseNotFound) {
					return s.classifyMissingTask(ctx, id, nil)
				}
				return ErrBackgroundResponseUnavailable.WithCause(err)
			}
			if !saved {
				continue
			}
			return nil
		}
		// Compatibility for in-memory/testing stores. Production Redis implements
		// BackgroundResponseTaskCASStore and therefore never takes this path.
		if err := s.store.Save(ctx, task, ttl); err != nil {
			return ErrBackgroundResponseUnavailable.WithCause(err)
		}
		return nil
	}
	return ErrBackgroundResponseUnavailable.WithCause(errBackgroundResponseVersionConflict)
}

func (s *BackgroundResponseTaskService) updateWithLease(ctx context.Context, id string, lease BackgroundResponseTaskLease, mutate func(*BackgroundResponseTaskRecord) bool) error {
	if !s.Enabled() {
		return ErrBackgroundResponseUnavailable
	}
	id = strings.TrimSpace(id)
	lease.Holder = strings.TrimSpace(lease.Holder)
	if id == "" || lease.Holder == "" || lease.Fence <= 0 {
		return ErrBackgroundResponseLeaseLost
	}
	store, ok := s.store.(BackgroundResponseTaskFencedLeaseStore)
	if !ok {
		return ErrBackgroundResponseUnavailable.WithCause(errors.New("background response store does not support fenced leases"))
	}
	for attempt := 0; attempt < backgroundResponseCASMaxAttempts; attempt++ {
		task, err := s.store.Get(ctx, id)
		if err != nil {
			if errors.Is(err, ErrBackgroundResponseNotFound) {
				return s.classifyMissingTask(ctx, id, nil)
			}
			return ErrBackgroundResponseUnavailable.WithCause(err)
		}
		expectedVersion := task.Version
		if !mutate(task) {
			return nil
		}
		task.Version = expectedVersion + 1
		if task.Version <= 0 {
			task.Version = 1
		}
		ttl := s.retentionTTL()
		task.ExpiresAt = time.Now().UTC().Add(ttl).Unix()
		saved, leaseCurrent, err := store.SaveIfVersionAndLease(ctx, task, expectedVersion, lease.Holder, lease.Fence, ttl)
		if err != nil {
			if errors.Is(err, ErrBackgroundResponseNotFound) {
				return s.classifyMissingTask(ctx, id, nil)
			}
			return ErrBackgroundResponseUnavailable.WithCause(err)
		}
		if !leaseCurrent {
			return ErrBackgroundResponseLeaseLost
		}
		if !saved {
			continue
		}
		return nil
	}
	return ErrBackgroundResponseUnavailable.WithCause(errBackgroundResponseVersionConflict)
}

func (s *BackgroundResponseTaskService) finish(ctx context.Context, id, status string, statusCode int, result, taskErr json.RawMessage) error {
	return s.update(ctx, id, func(task *BackgroundResponseTaskRecord) bool {
		return applyBackgroundResponseTerminal(task, status, statusCode, result, taskErr)
	})
}

func (s *BackgroundResponseTaskService) finishWithLease(ctx context.Context, id string, lease BackgroundResponseTaskLease, status string, statusCode int, result, taskErr json.RawMessage) error {
	return s.updateWithLease(ctx, id, lease, func(task *BackgroundResponseTaskRecord) bool {
		return applyBackgroundResponseTerminal(task, status, statusCode, result, taskErr)
	})
}

func (s *BackgroundResponseTaskService) retentionTTL() time.Duration {
	ttl := s.ttl
	if ttl < minBackgroundResponseRetention {
		ttl = minBackgroundResponseRetention
	}
	minForExecution := s.ExecutionTimeout() + backgroundResponseExecutionRetentionSlack
	if ttl < minForExecution {
		ttl = minForExecution
	}
	return ttl
}

func (s *BackgroundResponseTaskService) AcquireFencedLease(ctx context.Context, taskID, holder string, ttl time.Duration) (BackgroundResponseTaskLease, bool, error) {
	if !s.Enabled() {
		return BackgroundResponseTaskLease{}, false, ErrBackgroundResponseUnavailable
	}
	store, ok := s.store.(BackgroundResponseTaskFencedLeaseStore)
	if !ok {
		return BackgroundResponseTaskLease{}, false, ErrBackgroundResponseUnavailable.WithCause(errors.New("background response store does not support fenced leases"))
	}
	taskID = strings.TrimSpace(taskID)
	holder = strings.TrimSpace(holder)
	if taskID == "" || holder == "" {
		return BackgroundResponseTaskLease{}, false, ErrBackgroundResponseLeaseLost
	}
	fence, acquired, err := store.AcquireFencedLease(ctx, taskID, holder, normalizeBackgroundResponseLeaseTTL(ttl))
	if err != nil {
		return BackgroundResponseTaskLease{}, false, ErrBackgroundResponseUnavailable.WithCause(err)
	}
	if !acquired || fence <= 0 {
		return BackgroundResponseTaskLease{}, false, nil
	}
	return BackgroundResponseTaskLease{Holder: holder, Fence: fence}, true, nil
}

func (s *BackgroundResponseTaskService) RenewFencedLease(ctx context.Context, taskID string, lease BackgroundResponseTaskLease, ttl time.Duration) (bool, error) {
	if !s.Enabled() {
		return false, ErrBackgroundResponseUnavailable
	}
	store, ok := s.store.(BackgroundResponseTaskFencedLeaseStore)
	if !ok {
		return false, ErrBackgroundResponseUnavailable.WithCause(errors.New("background response store does not support fenced leases"))
	}
	renewed, err := store.RenewFencedLease(ctx, strings.TrimSpace(taskID), strings.TrimSpace(lease.Holder), lease.Fence, normalizeBackgroundResponseLeaseTTL(ttl))
	if err != nil {
		return false, ErrBackgroundResponseUnavailable.WithCause(err)
	}
	return renewed, nil
}

func (s *BackgroundResponseTaskService) ReleaseFencedLease(ctx context.Context, taskID string, lease BackgroundResponseTaskLease) error {
	if !s.Enabled() {
		return ErrBackgroundResponseUnavailable
	}
	store, ok := s.store.(BackgroundResponseTaskFencedLeaseStore)
	if !ok {
		return ErrBackgroundResponseUnavailable.WithCause(errors.New("background response store does not support fenced leases"))
	}
	if err := store.ReleaseFencedLease(ctx, strings.TrimSpace(taskID), strings.TrimSpace(lease.Holder), lease.Fence); err != nil {
		return ErrBackgroundResponseUnavailable.WithCause(err)
	}
	return nil
}

func (s *BackgroundResponseTaskService) AcquireLease(ctx context.Context, taskID, holder string, ttl time.Duration) (bool, error) {
	if !s.Enabled() {
		return false, ErrBackgroundResponseUnavailable
	}
	leaseStore, ok := s.store.(BackgroundResponseTaskLeaseStore)
	if !ok {
		// Backward-compatible single-process stores used by embedders and tests.
		return true, nil
	}
	ttl = normalizeBackgroundResponseLeaseTTL(ttl)
	acquired, err := leaseStore.AcquireLease(ctx, strings.TrimSpace(taskID), strings.TrimSpace(holder), ttl)
	if err != nil {
		return false, ErrBackgroundResponseUnavailable.WithCause(err)
	}
	return acquired, nil
}

func (s *BackgroundResponseTaskService) RenewLease(ctx context.Context, taskID, holder string, ttl time.Duration) (bool, error) {
	if !s.Enabled() {
		return false, ErrBackgroundResponseUnavailable
	}
	leaseStore, ok := s.store.(BackgroundResponseTaskLeaseStore)
	if !ok {
		return true, nil
	}
	ttl = normalizeBackgroundResponseLeaseTTL(ttl)
	renewed, err := leaseStore.RenewLease(ctx, strings.TrimSpace(taskID), strings.TrimSpace(holder), ttl)
	if err != nil {
		return false, ErrBackgroundResponseUnavailable.WithCause(err)
	}
	return renewed, nil
}

func (s *BackgroundResponseTaskService) ReleaseLease(ctx context.Context, taskID, holder string) error {
	if !s.Enabled() {
		return ErrBackgroundResponseUnavailable
	}
	leaseStore, ok := s.store.(BackgroundResponseTaskLeaseStore)
	if !ok {
		return nil
	}
	if err := leaseStore.ReleaseLease(ctx, strings.TrimSpace(taskID), strings.TrimSpace(holder)); err != nil {
		return ErrBackgroundResponseUnavailable.WithCause(err)
	}
	return nil
}

func normalizeBackgroundResponseLeaseTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return defaultBackgroundResponseLeaseTTL
	}
	return ttl
}

func (s *BackgroundResponseTaskService) classifyMissingTask(ctx context.Context, id string, owner *BackgroundResponseOwner) error {
	indexStore, ok := s.store.(BackgroundResponseTaskIndexStore)
	if !ok {
		return ErrBackgroundResponseNotFound
	}
	index, err := indexStore.GetTaskIndex(ctx, strings.TrimSpace(id))
	if err != nil {
		if errors.Is(err, ErrBackgroundResponseNotFound) {
			return ErrBackgroundResponseNotFound
		}
		return ErrBackgroundResponseUnavailable.WithCause(err)
	}
	if index == nil {
		return ErrBackgroundResponseNotFound
	}
	if owner != nil && (index.UserID != owner.UserID || index.APIKeyID != owner.APIKeyID) {
		return ErrBackgroundResponseNotFound
	}
	if index.ExpiresAt > 0 && time.Now().UTC().Unix() >= index.ExpiresAt {
		return ErrBackgroundResponseTaskExpired
	}
	return ErrBackgroundResponseMappingMissing
}

func applyBackgroundResponseTerminal(task *BackgroundResponseTaskRecord, status string, statusCode int, result, taskErr json.RawMessage) bool {
	if task == nil || backgroundResponseTaskTerminal(task.Status) {
		return false
	}
	now := time.Now().UTC()
	completedAt := now.Unix()
	task.Status = status
	task.HTTPStatus = statusCode
	task.Result = result
	task.Error = taskErr
	task.CompletedAt = &completedAt
	if status == BackgroundResponseStatusFailed {
		task.FailedAt = &completedAt
	}
	if task.CreatedAt > 0 && completedAt >= task.CreatedAt {
		task.ElapsedSeconds = completedAt - task.CreatedAt
	}
	return true
}

func sanitizeBackgroundResponseUsageError(message string) string {
	lower := strings.ToLower(message)
	for _, marker := range []string{
		"authorization",
		"bearer ",
		"api_key",
		"apikey",
		"access_token",
		"refresh_token",
		"client_secret",
		"credential=",
		"credential:",
	} {
		if strings.Contains(lower, marker) {
			return "<redacted usage settlement error>"
		}
	}
	message = logredact.RedactText(
		strings.TrimSpace(message),
		"authorization",
		"api_key",
		"apikey",
		"token",
		"secret",
		"credential",
	)
	message = strings.Join(strings.Fields(message), " ")
	return truncateUTF8(message, maxBackgroundResponseUsageErrorBytes)
}

func backgroundResponseUsageRetryDelay(consecutiveErrors int) time.Duration {
	if consecutiveErrors <= 1 {
		return backgroundResponseUsageRetryBaseDelay
	}
	delay := backgroundResponseUsageRetryBaseDelay
	for attempt := 1; attempt < consecutiveErrors && delay < backgroundResponseUsageRetryMaxDelay; attempt++ {
		if delay >= backgroundResponseUsageRetryMaxDelay/2 {
			return backgroundResponseUsageRetryMaxDelay
		}
		delay *= 2
	}
	if delay > backgroundResponseUsageRetryMaxDelay {
		return backgroundResponseUsageRetryMaxDelay
	}
	return delay
}

func cloneOpenAIBackgroundUpstreamBinding(binding BackgroundResponseUpstreamBinding) (*BackgroundResponseUpstreamBinding, error) {
	data, err := json.Marshal(binding)
	if err != nil {
		return nil, err
	}
	var cloned BackgroundResponseUpstreamBinding
	if err := json.Unmarshal(data, &cloned); err != nil {
		return nil, err
	}
	return &cloned, nil
}

func backgroundResponseTaskTerminal(status string) bool {
	switch status {
	case BackgroundResponseStatusCompleted, BackgroundResponseStatusFailed, BackgroundResponseStatusCancelled:
		return true
	default:
		return false
	}
}

func backgroundResponseErrorJSON(errorType, message string) json.RawMessage {
	data, _ := json.Marshal(map[string]string{"type": errorType, "code": errorType, "message": message})
	return data
}
