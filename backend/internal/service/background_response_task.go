package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
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
	maxBackgroundResponseResultBytes          = 32 << 20
)

var (
	ErrBackgroundResponseNotFound    = infraerrors.New(http.StatusNotFound, "BACKGROUND_RESPONSE_NOT_FOUND", "response not found")
	ErrBackgroundResponseForbidden   = infraerrors.New(http.StatusForbidden, "BACKGROUND_RESPONSE_FORBIDDEN", "response does not belong to this API key")
	ErrBackgroundResponseUnavailable = infraerrors.New(http.StatusServiceUnavailable, "BACKGROUND_RESPONSE_UNAVAILABLE", "background response storage is unavailable")
)

// BackgroundResponseTaskRecord is the private Redis representation of a
// detached Responses API request. UserID/APIKeyID/account identifiers are never
// returned to callers and prevent one key from reading another key's response.
type BackgroundResponseTaskRecord struct {
	ID                 string          `json:"id"`
	UserID             int64           `json:"user_id"`
	APIKeyID           int64           `json:"api_key_id"`
	Model              string          `json:"model"`
	Status             string          `json:"status"`
	HTTPStatus         int             `json:"http_status,omitempty"`
	Result             json.RawMessage `json:"result,omitempty"`
	Error              json.RawMessage `json:"error,omitempty"`
	CreatedAt          int64           `json:"created_at"`
	StartedAt          *int64          `json:"started_at,omitempty"`
	CompletedAt        *int64          `json:"completed_at,omitempty"`
	FailedAt           *int64          `json:"failed_at,omitempty"`
	ElapsedSeconds     int64           `json:"elapsed_seconds,omitempty"`
	ExpiresAt          int64           `json:"expires_at"`
	UpstreamResponseID string          `json:"upstream_response_id,omitempty"`
	UpstreamRequestID  string          `json:"upstream_request_id,omitempty"`
	UpstreamAccountID  int64           `json:"upstream_account_id,omitempty"`
	RequestFingerprint string          `json:"request_fingerprint,omitempty"`
	IdempotencyKeyHash string          `json:"idempotency_key_hash,omitempty"`
}

type BackgroundResponseOwner struct {
	UserID   int64
	APIKeyID int64
}

type BackgroundResponseTaskMetadata struct {
	RequestFingerprint string
	IdempotencyKeyHash string
}

type BackgroundResponseTaskStore interface {
	Save(ctx context.Context, task *BackgroundResponseTaskRecord, ttl time.Duration) error
	Get(ctx context.Context, id string) (*BackgroundResponseTaskRecord, error)
	ListActive(ctx context.Context, limit int) ([]*BackgroundResponseTaskRecord, error)
	ReserveIdempotency(ctx context.Context, owner BackgroundResponseOwner, keyHash, taskID string, ttl time.Duration) (existingTaskID string, reserved bool, err error)
	ReleaseIdempotency(ctx context.Context, owner BackgroundResponseOwner, keyHash, taskID string) error
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
	task := &BackgroundResponseTaskRecord{
		ID:                 "resp_bg_" + strings.ReplaceAll(uuid.NewString(), "-", ""),
		UserID:             owner.UserID,
		APIKeyID:           owner.APIKeyID,
		Model:              strings.TrimSpace(model),
		Status:             BackgroundResponseStatusQueued,
		CreatedAt:          now.Unix(),
		ExpiresAt:          now.Add(s.ttl).Unix(),
		RequestFingerprint: strings.TrimSpace(meta.RequestFingerprint),
		IdempotencyKeyHash: strings.TrimSpace(meta.IdempotencyKeyHash),
	}
	if task.IdempotencyKeyHash != "" {
		existingID, reserved, err := s.store.ReserveIdempotency(ctx, owner, task.IdempotencyKeyHash, task.ID, s.ttl)
		if err != nil {
			return nil, false, ErrBackgroundResponseUnavailable.WithCause(err)
		}
		if !reserved {
			existing, getErr := s.store.Get(ctx, strings.TrimSpace(existingID))
			if getErr != nil {
				if errors.Is(getErr, ErrBackgroundResponseNotFound) {
					// Stale reservation from a failed create; reclaim it with the new task id.
					_ = s.store.ReleaseIdempotency(ctx, owner, task.IdempotencyKeyHash, existingID)
					existingID, reserved, err = s.store.ReserveIdempotency(ctx, owner, task.IdempotencyKeyHash, task.ID, s.ttl)
					if err != nil {
						return nil, false, ErrBackgroundResponseUnavailable.WithCause(err)
					}
					if !reserved {
						existing, getErr = s.store.Get(ctx, strings.TrimSpace(existingID))
					}
				} else {
					return nil, false, ErrBackgroundResponseUnavailable.WithCause(getErr)
				}
			}
			if !reserved {
				if getErr != nil {
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
		}
	}
	if err := s.store.Save(ctx, task, s.ttl); err != nil {
		if task.IdempotencyKeyHash != "" {
			_ = s.store.ReleaseIdempotency(ctx, owner, task.IdempotencyKeyHash, task.ID)
		}
		return nil, false, ErrBackgroundResponseUnavailable.WithCause(err)
	}
	return task, true, nil
}

func (s *BackgroundResponseTaskService) Get(ctx context.Context, owner BackgroundResponseOwner, id string) (*BackgroundResponseTaskRecord, error) {
	if !s.Enabled() {
		return nil, ErrBackgroundResponseUnavailable
	}
	task, err := s.store.Get(ctx, strings.TrimSpace(id))
	if err != nil {
		if errors.Is(err, ErrBackgroundResponseNotFound) {
			return nil, ErrBackgroundResponseNotFound
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
	task, err := s.store.Get(ctx, strings.TrimSpace(id))
	if err != nil {
		if errors.Is(err, ErrBackgroundResponseNotFound) {
			return nil, ErrBackgroundResponseNotFound
		}
		return nil, ErrBackgroundResponseUnavailable.WithCause(err)
	}
	return task, nil
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

func (s *BackgroundResponseTaskService) MarkInProgress(ctx context.Context, id string) error {
	return s.update(ctx, id, func(task *BackgroundResponseTaskRecord) {
		if backgroundResponseTaskTerminal(task.Status) {
			return
		}
		now := time.Now().UTC().Unix()
		if task.StartedAt == nil {
			task.StartedAt = &now
		}
		task.Status = BackgroundResponseStatusInProgress
	})
}

func (s *BackgroundResponseTaskService) AttachUpstream(ctx context.Context, id string, accountID int64, upstreamResponseID, upstreamRequestID string) error {
	return s.update(ctx, id, func(task *BackgroundResponseTaskRecord) {
		if backgroundResponseTaskTerminal(task.Status) {
			return
		}
		if accountID > 0 {
			task.UpstreamAccountID = accountID
		}
		if upstreamResponseID = strings.TrimSpace(upstreamResponseID); upstreamResponseID != "" {
			task.UpstreamResponseID = upstreamResponseID
		}
		if upstreamRequestID = strings.TrimSpace(upstreamRequestID); upstreamRequestID != "" {
			task.UpstreamRequestID = upstreamRequestID
		}
	})
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

func (s *BackgroundResponseTaskService) Fail(ctx context.Context, id string, statusCode int, taskErr json.RawMessage) error {
	if !json.Valid(taskErr) {
		taskErr = backgroundResponseErrorJSON("api_error", "background response failed")
	}
	return s.finish(ctx, id, BackgroundResponseStatusFailed, statusCode, nil, taskErr)
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

func (s *BackgroundResponseTaskService) update(ctx context.Context, id string, mutate func(*BackgroundResponseTaskRecord)) error {
	if !s.Enabled() {
		return ErrBackgroundResponseUnavailable
	}
	task, err := s.store.Get(ctx, strings.TrimSpace(id))
	if err != nil {
		if errors.Is(err, ErrBackgroundResponseNotFound) {
			return ErrBackgroundResponseNotFound
		}
		return ErrBackgroundResponseUnavailable.WithCause(err)
	}
	mutate(task)
	ttl := time.Until(time.Unix(task.ExpiresAt, 0))
	if ttl <= 0 {
		ttl = s.ttl
	}
	if err := s.store.Save(ctx, task, ttl); err != nil {
		return ErrBackgroundResponseUnavailable.WithCause(err)
	}
	return nil
}

func (s *BackgroundResponseTaskService) finish(ctx context.Context, id, status string, statusCode int, result, taskErr json.RawMessage) error {
	return s.update(ctx, id, func(task *BackgroundResponseTaskRecord) {
		if backgroundResponseTaskTerminal(task.Status) {
			return
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
		task.ExpiresAt = now.Add(s.ttl).Unix()
	})
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
