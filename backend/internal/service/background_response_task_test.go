package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type backgroundResponseMemoryStore struct {
	task          *BackgroundResponseTaskRecord
	ttl           time.Duration
	saveErr       error
	getErr        error
	idempotencies map[string]string
}

func (s *backgroundResponseMemoryStore) Save(_ context.Context, task *BackgroundResponseTaskRecord, ttl time.Duration) error {
	if s.saveErr != nil {
		return s.saveErr
	}
	copy := *task
	copy.Result = append(json.RawMessage(nil), task.Result...)
	copy.Error = append(json.RawMessage(nil), task.Error...)
	s.task = &copy
	s.ttl = ttl
	return nil
}

func (s *backgroundResponseMemoryStore) Get(_ context.Context, _ string) (*BackgroundResponseTaskRecord, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	if s.task == nil {
		return nil, ErrBackgroundResponseNotFound
	}
	copy := *s.task
	copy.Result = append(json.RawMessage(nil), s.task.Result...)
	copy.Error = append(json.RawMessage(nil), s.task.Error...)
	return &copy, nil
}

func (s *backgroundResponseMemoryStore) ListActive(_ context.Context, limit int) ([]*BackgroundResponseTaskRecord, error) {
	if s.task == nil {
		return nil, nil
	}
	if s.task.Status != BackgroundResponseStatusQueued && s.task.Status != BackgroundResponseStatusInProgress {
		return nil, nil
	}
	copy := *s.task
	copy.Result = append(json.RawMessage(nil), s.task.Result...)
	copy.Error = append(json.RawMessage(nil), s.task.Error...)
	return []*BackgroundResponseTaskRecord{&copy}[:min(1, max(1, limit))], nil
}

func (s *backgroundResponseMemoryStore) ReserveIdempotency(_ context.Context, owner BackgroundResponseOwner, keyHash, taskID string, _ time.Duration) (string, bool, error) {
	if keyHash == "" || taskID == "" {
		return "", true, nil
	}
	if s.idempotencies == nil {
		s.idempotencies = make(map[string]string)
	}
	key := backgroundResponseMemoryIdempotencyKey(owner, keyHash)
	if existing := s.idempotencies[key]; existing != "" {
		return existing, false, nil
	}
	s.idempotencies[key] = taskID
	return "", true, nil
}

func (s *backgroundResponseMemoryStore) ReleaseIdempotency(_ context.Context, owner BackgroundResponseOwner, keyHash, taskID string) error {
	if s.idempotencies == nil {
		return nil
	}
	key := backgroundResponseMemoryIdempotencyKey(owner, keyHash)
	if s.idempotencies[key] == taskID {
		delete(s.idempotencies, key)
	}
	return nil
}

func backgroundResponseMemoryIdempotencyKey(owner BackgroundResponseOwner, keyHash string) string {
	return strconv.FormatInt(owner.UserID, 10) + ":" + strconv.FormatInt(owner.APIKeyID, 10) + ":" + keyHash
}

func TestBackgroundResponseTaskLifecycleAndOwnership(t *testing.T) {
	store := &backgroundResponseMemoryStore{}
	svc := NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, 10*time.Minute)
	owner := BackgroundResponseOwner{UserID: 7, APIKeyID: 9}

	created, err := svc.Create(context.Background(), owner, "gpt-5.6-terra")
	require.NoError(t, err)
	require.True(t, svc.Enabled())
	require.Equal(t, BackgroundResponseStatusQueued, created.Status)
	require.Equal(t, "gpt-5.6-terra", created.Model)
	require.Contains(t, created.ID, "resp_bg_")
	require.Equal(t, owner.UserID, store.task.UserID)
	require.Equal(t, owner.APIKeyID, store.task.APIKeyID)

	_, err = svc.Get(context.Background(), BackgroundResponseOwner{UserID: 7, APIKeyID: 10}, created.ID)
	require.ErrorIs(t, err, ErrBackgroundResponseNotFound)

	require.NoError(t, svc.MarkInProgress(context.Background(), created.ID))
	processing, err := svc.Get(context.Background(), owner, created.ID)
	require.NoError(t, err)
	require.Equal(t, BackgroundResponseStatusInProgress, processing.Status)
	require.NotNil(t, processing.StartedAt)

	require.NoError(t, svc.AttachUpstream(context.Background(), created.ID, 123, "resp_upstream", "req_upstream"))
	mapped, err := svc.Get(context.Background(), owner, created.ID)
	require.NoError(t, err)
	require.Equal(t, int64(123), mapped.UpstreamAccountID)
	require.Equal(t, "resp_upstream", mapped.UpstreamResponseID)
	require.Equal(t, "req_upstream", mapped.UpstreamRequestID)

	result := json.RawMessage(`{"id":"resp_upstream","object":"response","status":"completed","output":[]}`)
	require.NoError(t, svc.Complete(context.Background(), created.ID, http.StatusOK, result))
	completed, err := svc.Get(context.Background(), owner, created.ID)
	require.NoError(t, err)
	require.Equal(t, BackgroundResponseStatusCompleted, completed.Status)
	require.Equal(t, http.StatusOK, completed.HTTPStatus)
	require.JSONEq(t, string(result), string(completed.Result))
	require.NotNil(t, completed.CompletedAt)
}

func TestBackgroundResponseTaskIdempotencyReplaysExistingTask(t *testing.T) {
	store := &backgroundResponseMemoryStore{}
	svc := NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Minute)
	owner := BackgroundResponseOwner{UserID: 7, APIKeyID: 9}
	meta := BackgroundResponseTaskMetadata{RequestFingerprint: "fp", IdempotencyKeyHash: "hash"}

	created, fresh, err := svc.CreateWithMetadata(context.Background(), owner, "gpt-5.6-sol", meta)
	require.NoError(t, err)
	require.True(t, fresh)

	replayed, fresh, err := svc.CreateWithMetadata(context.Background(), owner, "gpt-5.6-sol", meta)
	require.NoError(t, err)
	require.False(t, fresh)
	require.Equal(t, created.ID, replayed.ID)

	_, _, err = svc.CreateWithMetadata(context.Background(), owner, "gpt-5.6-sol", BackgroundResponseTaskMetadata{RequestFingerprint: "different", IdempotencyKeyHash: "hash"})
	require.ErrorIs(t, err, ErrIdempotencyKeyConflict)
}

func TestBackgroundResponseTaskCancel(t *testing.T) {
	store := &backgroundResponseMemoryStore{}
	svc := NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Minute)
	owner := BackgroundResponseOwner{UserID: 7, APIKeyID: 9}

	created, err := svc.Create(context.Background(), owner, "gpt-5.6-sol")
	require.NoError(t, err)
	cancelled, err := svc.Cancel(context.Background(), owner, created.ID)
	require.NoError(t, err)
	require.Equal(t, BackgroundResponseStatusCancelled, cancelled.Status)
	require.NotNil(t, cancelled.CompletedAt)

	require.NoError(t, svc.Fail(context.Background(), created.ID, http.StatusBadGateway, json.RawMessage(`{"type":"api_error","message":"late"}`)))
	got, err := svc.Get(context.Background(), owner, created.ID)
	require.NoError(t, err)
	require.Equal(t, BackgroundResponseStatusCancelled, got.Status)
}

func TestBackgroundResponseTaskFailActiveOnStartup(t *testing.T) {
	store := &backgroundResponseMemoryStore{}
	svc := NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Minute)
	owner := BackgroundResponseOwner{UserID: 7, APIKeyID: 9}
	created, err := svc.Create(context.Background(), owner, "gpt-5.6-sol")
	require.NoError(t, err)
	require.NoError(t, svc.MarkInProgress(context.Background(), created.ID))

	failed, err := svc.FailActiveOnStartup(context.Background(), 100)
	require.NoError(t, err)
	require.Equal(t, 1, failed)

	got, err := svc.Get(context.Background(), owner, created.ID)
	require.NoError(t, err)
	require.Equal(t, BackgroundResponseStatusFailed, got.Status)
	require.Equal(t, "background_task_failed", gjson.GetBytes(got.Error, "type").String())
}

func TestBackgroundResponseTaskInvalidResultBecomesFailed(t *testing.T) {
	store := &backgroundResponseMemoryStore{}
	svc := NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Minute)
	owner := BackgroundResponseOwner{UserID: 1, APIKeyID: 2}
	created, err := svc.Create(context.Background(), owner, "gpt-5")
	require.NoError(t, err)

	require.NoError(t, svc.Complete(context.Background(), created.ID, http.StatusOK, json.RawMessage(`not-json`)))
	got, err := svc.Get(context.Background(), owner, created.ID)
	require.NoError(t, err)
	require.Equal(t, BackgroundResponseStatusFailed, got.Status)
	require.Equal(t, http.StatusBadGateway, got.HTTPStatus)
	require.Contains(t, string(got.Error), "non-JSON")
}

func TestBackgroundResponseTaskMapsStoreFailures(t *testing.T) {
	store := &backgroundResponseMemoryStore{saveErr: errors.New("redis down")}
	svc := NewBackgroundResponseTaskService(store)

	_, err := svc.Create(context.Background(), BackgroundResponseOwner{UserID: 1, APIKeyID: 2}, "gpt-5")
	require.ErrorIs(t, err, ErrBackgroundResponseUnavailable)
}
