package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type backgroundResponseMemoryStore struct {
	task    *BackgroundResponseTaskRecord
	ttl     time.Duration
	saveErr error
	getErr  error
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

	result := json.RawMessage(`{"id":"resp_upstream","object":"response","status":"completed","output":[]}`)
	require.NoError(t, svc.Complete(context.Background(), created.ID, http.StatusOK, result))
	completed, err := svc.Get(context.Background(), owner, created.ID)
	require.NoError(t, err)
	require.Equal(t, BackgroundResponseStatusCompleted, completed.Status)
	require.Equal(t, http.StatusOK, completed.HTTPStatus)
	require.JSONEq(t, string(result), string(completed.Result))
	require.NotNil(t, completed.CompletedAt)
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
