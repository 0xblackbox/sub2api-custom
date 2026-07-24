package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type backgroundResponseMemoryStore struct {
	mu            sync.Mutex
	task          *BackgroundResponseTaskRecord
	ttl           time.Duration
	saveErr       error
	getErr        error
	idempotencies map[string]string
	leases        map[string]backgroundResponseMemoryLease
	nextFence     int64
	index         *BackgroundResponseTaskIndex
}

type backgroundResponseMemoryLease struct {
	holder    string
	fence     int64
	expiresAt time.Time
}

type backgroundResponseCASRaceStore struct {
	*backgroundResponseMemoryStore
	failureReady   chan struct{}
	completionDone chan struct{}
	failureOnce    sync.Once
	completionOnce sync.Once
}

// backgroundResponseNonAtomicStore deliberately exposes only the legacy base
// interface so idempotent creation must fail closed.
type backgroundResponseNonAtomicStore struct {
	inner *backgroundResponseMemoryStore
}

func (s *backgroundResponseNonAtomicStore) Save(ctx context.Context, task *BackgroundResponseTaskRecord, ttl time.Duration) error {
	return s.inner.Save(ctx, task, ttl)
}

func (s *backgroundResponseNonAtomicStore) Get(ctx context.Context, id string) (*BackgroundResponseTaskRecord, error) {
	return s.inner.Get(ctx, id)
}

func (s *backgroundResponseNonAtomicStore) ListActive(ctx context.Context, limit int) ([]*BackgroundResponseTaskRecord, error) {
	return s.inner.ListActive(ctx, limit)
}

func (s *backgroundResponseNonAtomicStore) ReserveIdempotency(ctx context.Context, owner BackgroundResponseOwner, keyHash, taskID string, ttl time.Duration) (string, bool, error) {
	return s.inner.ReserveIdempotency(ctx, owner, keyHash, taskID, ttl)
}

func (s *backgroundResponseNonAtomicStore) ReleaseIdempotency(ctx context.Context, owner BackgroundResponseOwner, keyHash, taskID string) error {
	return s.inner.ReleaseIdempotency(ctx, owner, keyHash, taskID)
}

func (s *backgroundResponseMemoryStore) Save(_ context.Context, task *BackgroundResponseTaskRecord, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saveErr != nil {
		return s.saveErr
	}
	s.task = cloneBackgroundResponseTaskRecordForTest(task)
	s.ttl = ttl
	s.index = backgroundResponseTaskIndexForTest(task)
	return nil
}

func (s *backgroundResponseMemoryStore) CreateWithIdempotency(
	_ context.Context,
	task *BackgroundResponseTaskRecord,
	owner BackgroundResponseOwner,
	keyHash string,
	ttl time.Duration,
) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saveErr != nil {
		return "", false, s.saveErr
	}
	if s.idempotencies == nil {
		s.idempotencies = make(map[string]string)
	}
	key := backgroundResponseMemoryIdempotencyKey(owner, keyHash)
	if existing := s.idempotencies[key]; existing != "" {
		return existing, false, nil
	}
	s.idempotencies[key] = task.ID
	s.task = cloneBackgroundResponseTaskRecordForTest(task)
	s.ttl = ttl
	s.index = backgroundResponseTaskIndexForTest(task)
	return "", true, nil
}

func (s *backgroundResponseMemoryStore) Get(_ context.Context, _ string) (*BackgroundResponseTaskRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.getErr != nil {
		return nil, s.getErr
	}
	if s.task == nil {
		return nil, ErrBackgroundResponseNotFound
	}
	return cloneBackgroundResponseTaskRecordForTest(s.task), nil
}

func (s *backgroundResponseMemoryStore) ListActive(_ context.Context, limit int) ([]*BackgroundResponseTaskRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.task == nil {
		return nil, nil
	}
	if s.task.Status != BackgroundResponseStatusQueued && s.task.Status != BackgroundResponseStatusInProgress {
		return nil, nil
	}
	copy := cloneBackgroundResponseTaskRecordForTest(s.task)
	return []*BackgroundResponseTaskRecord{copy}[:min(1, max(1, limit))], nil
}

func (s *backgroundResponseMemoryStore) ListUsagePending(_ context.Context, limit int) ([]*BackgroundResponseTaskRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.task == nil ||
		s.task.Status != BackgroundResponseStatusCompleted ||
		s.task.UsageSettlementVersion < backgroundResponseUsageSettlementVersion ||
		s.task.UsageRecorded ||
		(s.task.UsageNextAttemptAt != nil && *s.task.UsageNextAttemptAt > time.Now().UTC().Unix()) {
		return nil, nil
	}
	copy := cloneBackgroundResponseTaskRecordForTest(s.task)
	return []*BackgroundResponseTaskRecord{copy}[:min(1, max(1, limit))], nil
}

func (s *backgroundResponseMemoryStore) ReserveIdempotency(_ context.Context, owner BackgroundResponseOwner, keyHash, taskID string, _ time.Duration) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.idempotencies == nil {
		return nil
	}
	key := backgroundResponseMemoryIdempotencyKey(owner, keyHash)
	if s.idempotencies[key] == taskID {
		delete(s.idempotencies, key)
	}
	return nil
}

func (s *backgroundResponseMemoryStore) SaveIfVersion(_ context.Context, task *BackgroundResponseTaskRecord, expectedVersion int64, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saveErr != nil {
		return false, s.saveErr
	}
	if s.task == nil {
		return false, ErrBackgroundResponseNotFound
	}
	if s.task.Version != expectedVersion {
		return false, nil
	}
	s.task = cloneBackgroundResponseTaskRecordForTest(task)
	s.ttl = ttl
	s.index = backgroundResponseTaskIndexForTest(task)
	return true, nil
}

func (s *backgroundResponseMemoryStore) GetTaskIndex(_ context.Context, _ string) (*BackgroundResponseTaskIndex, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.index == nil {
		return nil, ErrBackgroundResponseNotFound
	}
	copy := *s.index
	return &copy, nil
}

func (s *backgroundResponseCASRaceStore) SaveIfVersion(ctx context.Context, task *BackgroundResponseTaskRecord, expectedVersion int64, ttl time.Duration) (bool, error) {
	switch task.Status {
	case BackgroundResponseStatusFailed:
		s.failureOnce.Do(func() { close(s.failureReady) })
		<-s.completionDone
	case BackgroundResponseStatusCompleted:
		<-s.failureReady
		saved, err := s.backgroundResponseMemoryStore.SaveIfVersion(ctx, task, expectedVersion, ttl)
		s.completionOnce.Do(func() { close(s.completionDone) })
		return saved, err
	}
	return s.backgroundResponseMemoryStore.SaveIfVersion(ctx, task, expectedVersion, ttl)
}

func (s *backgroundResponseMemoryStore) AcquireLease(_ context.Context, taskID, holder string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.leases == nil {
		s.leases = make(map[string]backgroundResponseMemoryLease)
	}
	now := time.Now()
	if lease, ok := s.leases[taskID]; ok && lease.expiresAt.After(now) {
		return false, nil
	}
	s.leases[taskID] = backgroundResponseMemoryLease{holder: holder, expiresAt: now.Add(ttl)}
	return true, nil
}

func (s *backgroundResponseMemoryStore) RenewLease(_ context.Context, taskID, holder string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lease, ok := s.leases[taskID]
	if !ok || lease.holder != holder || !lease.expiresAt.After(time.Now()) {
		return false, nil
	}
	lease.expiresAt = time.Now().Add(ttl)
	s.leases[taskID] = lease
	return true, nil
}

func (s *backgroundResponseMemoryStore) ReleaseLease(_ context.Context, taskID, holder string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if lease, ok := s.leases[taskID]; ok && lease.holder == holder {
		delete(s.leases, taskID)
	}
	return nil
}

func (s *backgroundResponseMemoryStore) AcquireFencedLease(_ context.Context, taskID, holder string, ttl time.Duration) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.leases == nil {
		s.leases = make(map[string]backgroundResponseMemoryLease)
	}
	now := time.Now()
	if lease, ok := s.leases[taskID]; ok && lease.expiresAt.After(now) {
		return 0, false, nil
	}
	s.nextFence++
	s.leases[taskID] = backgroundResponseMemoryLease{holder: holder, fence: s.nextFence, expiresAt: now.Add(ttl)}
	return s.nextFence, true, nil
}

func (s *backgroundResponseMemoryStore) RenewFencedLease(_ context.Context, taskID, holder string, fence int64, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lease, ok := s.leases[taskID]
	if !ok || lease.holder != holder || lease.fence != fence || !lease.expiresAt.After(time.Now()) {
		return false, nil
	}
	lease.expiresAt = time.Now().Add(ttl)
	s.leases[taskID] = lease
	return true, nil
}

func (s *backgroundResponseMemoryStore) ReleaseFencedLease(_ context.Context, taskID, holder string, fence int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if lease, ok := s.leases[taskID]; ok && lease.holder == holder && lease.fence == fence {
		delete(s.leases, taskID)
	}
	return nil
}

func (s *backgroundResponseMemoryStore) SaveIfVersionAndLease(_ context.Context, task *BackgroundResponseTaskRecord, expectedVersion int64, holder string, fence int64, ttl time.Duration) (bool, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lease, ok := s.leases[task.ID]
	if !ok || lease.holder != holder || lease.fence != fence || !lease.expiresAt.After(time.Now()) {
		return false, false, nil
	}
	if s.task == nil {
		return false, true, ErrBackgroundResponseNotFound
	}
	if s.task.Version != expectedVersion {
		return false, true, nil
	}
	s.task = cloneBackgroundResponseTaskRecordForTest(task)
	s.ttl = ttl
	s.index = backgroundResponseTaskIndexForTest(task)
	return true, true, nil
}

func cloneBackgroundResponseTaskRecordForTest(task *BackgroundResponseTaskRecord) *BackgroundResponseTaskRecord {
	if task == nil {
		return nil
	}
	copy := *task
	copy.Result = append(json.RawMessage(nil), task.Result...)
	copy.Error = append(json.RawMessage(nil), task.Error...)
	if task.UpstreamBinding != nil {
		bindingCopy := *task.UpstreamBinding
		copy.UpstreamBinding = &bindingCopy
	}
	return &copy
}

func backgroundResponseTaskIndexForTest(task *BackgroundResponseTaskRecord) *BackgroundResponseTaskIndex {
	if task == nil {
		return nil
	}
	return &BackgroundResponseTaskIndex{
		ID:        task.ID,
		UserID:    task.UserID,
		APIKeyID:  task.APIKeyID,
		ExpiresAt: task.ExpiresAt,
	}
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
	require.Equal(t, int64(1), store.task.Version)

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
	completedVersion := completed.Version

	require.NoError(t, svc.Fail(context.Background(), created.ID, http.StatusNotFound, json.RawMessage(`{"detail":"late not found"}`)))
	stillCompleted, err := svc.Get(context.Background(), owner, created.ID)
	require.NoError(t, err)
	require.Equal(t, BackgroundResponseStatusCompleted, stillCompleted.Status)
	require.Equal(t, completedVersion, stillCompleted.Version)
	require.JSONEq(t, string(result), string(stillCompleted.Result))
}

func TestBackgroundResponseTaskIdempotencyReplaysExistingTask(t *testing.T) {
	store := &backgroundResponseMemoryStore{}
	svc := NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Minute)
	owner := BackgroundResponseOwner{UserID: 7, APIKeyID: 9}
	meta := BackgroundResponseTaskMetadata{RequestFingerprint: "fp", IdempotencyKeyHash: "hash", SubscriptionID: 41}

	created, fresh, err := svc.CreateWithMetadata(context.Background(), owner, "gpt-5.6-sol", meta)
	require.NoError(t, err)
	require.True(t, fresh)
	require.Equal(t, int64(41), created.SubscriptionID)
	require.Equal(t, backgroundResponseUsageSettlementVersion, created.UsageSettlementVersion)
	require.False(t, created.UsageRecorded)

	replayed, fresh, err := svc.CreateWithMetadata(context.Background(), owner, "gpt-5.6-sol", meta)
	require.NoError(t, err)
	require.False(t, fresh)
	require.Equal(t, created.ID, replayed.ID)

	_, _, err = svc.CreateWithMetadata(context.Background(), owner, "gpt-5.6-sol", BackgroundResponseTaskMetadata{RequestFingerprint: "different", IdempotencyKeyHash: "hash"})
	require.ErrorIs(t, err, ErrIdempotencyKeyConflict)
}

func TestBackgroundResponseTaskIdempotentCreateFailsClosedWithoutAtomicStore(t *testing.T) {
	inner := &backgroundResponseMemoryStore{}
	svc := NewBackgroundResponseTaskService(&backgroundResponseNonAtomicStore{inner: inner})

	task, created, err := svc.CreateWithMetadata(
		context.Background(),
		BackgroundResponseOwner{UserID: 7, APIKeyID: 9},
		"gpt-5.6-sol",
		BackgroundResponseTaskMetadata{RequestFingerprint: "fp", IdempotencyKeyHash: "hash"},
	)
	require.Nil(t, task)
	require.False(t, created)
	require.ErrorIs(t, err, ErrBackgroundResponseUnavailable)
	require.Nil(t, inner.task)
	require.Empty(t, inner.idempotencies)
}

func TestBackgroundResponseTaskUsageSettlementSurvivesRestart(t *testing.T) {
	store := &backgroundResponseMemoryStore{}
	owner := BackgroundResponseOwner{UserID: 41, APIKeyID: 42}
	firstService := NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Hour)
	task, created, err := firstService.CreateWithMetadata(
		context.Background(),
		owner,
		"gpt-5.6-sol",
		BackgroundResponseTaskMetadata{RequestFingerprint: "request-fingerprint", SubscriptionID: 43},
	)
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, int64(43), task.SubscriptionID)
	require.NoError(t, firstService.Complete(context.Background(), task.ID, http.StatusOK, json.RawMessage(`{"status":"completed","usage":{"input_tokens":10,"output_tokens":4}}`)))

	restartedService := NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, 2*time.Hour)
	pending, err := restartedService.ListUsagePending(context.Background(), 10)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, task.ID, pending[0].ID)

	require.NoError(t, restartedService.RecordUsageAttempt(
		context.Background(),
		task.ID,
		`billing failed authorization="Bearer super-secret-token"`,
	))
	attempted, err := restartedService.Get(context.Background(), owner, task.ID)
	require.NoError(t, err)
	require.Equal(t, 1, attempted.UsageRecordAttempts)
	require.Equal(t, 1, attempted.UsageConsecutiveErrors)
	require.NotNil(t, attempted.UsageNextAttemptAt)
	require.NotContains(t, attempted.UsageRecordLastError, "super-secret-token")
	require.LessOrEqual(t, len(attempted.UsageRecordLastError), maxBackgroundResponseUsageErrorBytes)

	require.NoError(t, restartedService.RecordUsageAttempt(context.Background(), task.ID, ""))
	require.NoError(t, restartedService.MarkUsageRecorded(context.Background(), task.ID))
	require.NoError(t, restartedService.MarkUsageRecorded(context.Background(), task.ID))
	recorded, err := restartedService.Get(context.Background(), owner, task.ID)
	require.NoError(t, err)
	require.True(t, recorded.UsageRecorded)
	require.NotNil(t, recorded.UsageRecordedAt)
	require.Equal(t, 2, recorded.UsageRecordAttempts)
	require.Zero(t, recorded.UsageConsecutiveErrors)
	require.Nil(t, recorded.UsageNextAttemptAt)
	require.Empty(t, recorded.UsageRecordLastError)
	pending, err = restartedService.ListUsagePending(context.Background(), 10)
	require.NoError(t, err)
	require.Empty(t, pending)
}

func TestBackgroundResponseUsageRetryDelayIsExponentialAndCapped(t *testing.T) {
	require.Equal(t, 15*time.Second, backgroundResponseUsageRetryDelay(1))
	require.Equal(t, 30*time.Second, backgroundResponseUsageRetryDelay(2))
	require.Equal(t, time.Minute, backgroundResponseUsageRetryDelay(3))
	require.Equal(t, time.Hour, backgroundResponseUsageRetryDelay(20))
	require.Equal(t, time.Hour, backgroundResponseUsageRetryDelay(1000))
}

func TestBackgroundResponseTaskUsageSettlementRejectsNonCompletedAndLegacy(t *testing.T) {
	store := &backgroundResponseMemoryStore{}
	svc := NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Hour)
	owner := BackgroundResponseOwner{UserID: 44, APIKeyID: 45}
	task, err := svc.Create(context.Background(), owner, "gpt-5.6-sol")
	require.NoError(t, err)

	require.ErrorIs(t, svc.RecordUsageAttempt(context.Background(), task.ID, "not ready"), ErrBackgroundResponseUsageNotReady)
	require.ErrorIs(t, svc.MarkUsageRecorded(context.Background(), task.ID), ErrBackgroundResponseUsageNotReady)

	require.NoError(t, svc.Complete(context.Background(), task.ID, http.StatusOK, json.RawMessage(`{"status":"completed","usage":{}}`)))
	store.mu.Lock()
	store.task.UsageSettlementVersion = 0
	store.mu.Unlock()
	pending, err := svc.ListUsagePending(context.Background(), 10)
	require.NoError(t, err)
	require.Empty(t, pending)
	require.ErrorIs(t, svc.MarkUsageRecorded(context.Background(), task.ID), ErrBackgroundResponseUsageNotReady)
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

func TestBackgroundResponseTaskActiveUpdatesRenewMinimumRetention(t *testing.T) {
	store := &backgroundResponseMemoryStore{}
	svc := NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, 30*time.Hour)
	owner := BackgroundResponseOwner{UserID: 11, APIKeyID: 12}

	task, err := svc.Create(context.Background(), owner, "gpt-5.6-sol")
	require.NoError(t, err)
	require.InDelta(t, (31 * time.Hour).Seconds(), store.ttl.Seconds(), 1)
	require.GreaterOrEqual(t, task.ExpiresAt-time.Now().UTC().Unix(), int64((31*time.Hour - time.Second).Seconds()))
	require.InDelta(t, time.Now().UTC().Add(30*time.Hour).Unix(), task.DeadlineAt, 1)
	originalDeadline := task.DeadlineAt

	require.NoError(t, svc.MarkInProgress(context.Background(), task.ID))
	active, err := svc.Get(context.Background(), owner, task.ID)
	require.NoError(t, err)
	require.Equal(t, BackgroundResponseStatusInProgress, active.Status)
	require.Equal(t, originalDeadline, active.DeadlineAt)
	require.InDelta(t, (31 * time.Hour).Seconds(), store.ttl.Seconds(), 1)
	require.GreaterOrEqual(t, active.ExpiresAt-time.Now().UTC().Unix(), int64((31*time.Hour - time.Second).Seconds()))

	shortStore := &backgroundResponseMemoryStore{}
	shortSvc := NewBackgroundResponseTaskServiceWithOptions(shortStore, time.Hour, time.Hour)
	_, err = shortSvc.Create(context.Background(), owner, "gpt-5.6-sol")
	require.NoError(t, err)
	require.InDelta(t, (24 * time.Hour).Seconds(), shortStore.ttl.Seconds(), 1)
}

func TestBackgroundResponseTaskDeadlineSurvivesWorkerRestart(t *testing.T) {
	store := &backgroundResponseMemoryStore{}
	owner := BackgroundResponseOwner{UserID: 13, APIKeyID: 14}
	originalService := NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, 45*time.Minute)

	task, err := originalService.Create(context.Background(), owner, "gpt-5.6-sol")
	require.NoError(t, err)
	require.InDelta(t, time.Now().UTC().Add(45*time.Minute).Unix(), task.DeadlineAt, 1)
	originalDeadline := task.DeadlineAt

	// A replacement worker can have a different local timeout configuration,
	// but resuming the persisted task must not reset its absolute deadline.
	restartedService := NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, 2*time.Hour)
	require.NoError(t, restartedService.MarkInProgress(context.Background(), task.ID))
	resumed, err := restartedService.Get(context.Background(), owner, task.ID)
	require.NoError(t, err)
	require.Equal(t, originalDeadline, resumed.DeadlineAt)
	require.Greater(t, time.Until(time.Unix(resumed.DeadlineAt, 0)), 20*time.Minute)
	require.LessOrEqual(t, time.Until(time.Unix(resumed.DeadlineAt, 0)), 45*time.Minute)
}

func TestBackgroundResponseTaskExecutionDeadlineSemantics(t *testing.T) {
	svc := NewBackgroundResponseTaskServiceWithOptions(&backgroundResponseMemoryStore{}, time.Hour, 45*time.Minute)
	now := time.Now().UTC().Truncate(time.Second)

	persisted := &BackgroundResponseTaskRecord{
		CreatedAt:  now.Add(-time.Hour).Unix(),
		DeadlineAt: now.Add(10 * time.Minute).Unix(),
	}
	require.Equal(t, now.Add(10*time.Minute), svc.ExecutionDeadline(persisted))
	require.False(t, svc.ExecutionDeadlineExceeded(persisted, now))
	require.True(t, svc.ExecutionDeadlineExceeded(persisted, now.Add(10*time.Minute)))

	legacy := &BackgroundResponseTaskRecord{CreatedAt: now.Add(-20 * time.Minute).Unix()}
	require.Equal(t, now.Add(25*time.Minute), svc.ExecutionDeadline(legacy))
	require.False(t, svc.ExecutionDeadlineExceeded(legacy, now))
	require.True(t, svc.ExecutionDeadlineExceeded(&BackgroundResponseTaskRecord{}, now))
}

func TestBackgroundResponseTaskUpstreamBindingIsImmutable(t *testing.T) {
	store := &backgroundResponseMemoryStore{}
	svc := NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Hour)
	owner := BackgroundResponseOwner{UserID: 21, APIKeyID: 22}
	task, err := svc.Create(context.Background(), owner, "gpt-5.6-sol")
	require.NoError(t, err)

	binding := BackgroundResponseUpstreamBinding{
		Version:             1,
		ExecutionMode:       BackgroundResponseExecutionModeNative,
		Pollable:            true,
		AccountID:           123,
		AccountType:         AccountTypeAPIKey,
		CredentialRef:       "account:123",
		IdentityFingerprint: "identity",
		BaseURL:             "https://api.example.test/v1/responses",
		Project:             "project-fingerprint",
		Organization:        "organization-fingerprint",
		RouteFingerprint:    "route",
	}
	require.NoError(t, svc.AttachUpstreamBinding(context.Background(), task.ID, binding))
	require.NoError(t, svc.SetExecutionMode(context.Background(), task.ID, BackgroundResponseExecutionModeNative, true))

	binding.BaseURL = "https://different.example.test/v1/responses"
	err = svc.AttachUpstreamBinding(context.Background(), task.ID, binding)
	require.ErrorIs(t, err, ErrBackgroundResponseBindingConflict)

	got, err := svc.Get(context.Background(), owner, task.ID)
	require.NoError(t, err)
	require.NotNil(t, got.UpstreamBinding)
	require.Equal(t, "https://api.example.test/v1/responses", got.UpstreamBinding.BaseURL)
	require.Equal(t, BackgroundResponseExecutionModeNative, got.UpstreamExecutionMode)
	require.True(t, got.UpstreamPollable)
}

func TestBackgroundResponseTaskAttachesNativeMappingAtomically(t *testing.T) {
	store := &backgroundResponseMemoryStore{}
	svc := NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Hour)
	owner := BackgroundResponseOwner{UserID: 23, APIKeyID: 24}
	task, err := svc.Create(context.Background(), owner, "gpt-5.6-sol")
	require.NoError(t, err)
	originalVersion := task.Version

	binding := BackgroundResponseUpstreamBinding{
		Version:             1,
		ExecutionMode:       BackgroundResponseExecutionModeNative,
		Pollable:            true,
		AccountID:           456,
		AccountType:         AccountTypeAPIKey,
		CredentialRef:       "account:456",
		IdentityFingerprint: "identity",
		BaseURL:             "https://api.example.test/v1/responses",
		Project:             "project-fingerprint",
		Organization:        "organization-fingerprint",
		RouteFingerprint:    "route",
	}
	require.NoError(t, svc.AttachNativeUpstreamMapping(context.Background(), task.ID, 456, "resp_upstream", "req_create", binding))

	got, err := svc.Get(context.Background(), owner, task.ID)
	require.NoError(t, err)
	require.Equal(t, originalVersion+1, got.Version)
	require.Equal(t, int64(456), got.UpstreamAccountID)
	require.Equal(t, "resp_upstream", got.UpstreamResponseID)
	require.Equal(t, "req_create", got.UpstreamRequestID)
	require.Equal(t, BackgroundResponseExecutionModeNative, got.UpstreamExecutionMode)
	require.True(t, got.UpstreamPollable)
	require.Equal(t, binding, *got.UpstreamBinding)

	conflicting := binding
	conflicting.Project = "other-project"
	err = svc.AttachNativeUpstreamMapping(context.Background(), task.ID, 456, "resp_upstream", "req_other", conflicting)
	require.ErrorIs(t, err, ErrBackgroundResponseBindingConflict)
	unchanged, err := svc.Get(context.Background(), owner, task.ID)
	require.NoError(t, err)
	require.Equal(t, got.Version, unchanged.Version)
	require.Equal(t, "req_create", unchanged.UpstreamRequestID)
	require.Equal(t, binding, *unchanged.UpstreamBinding)
}

func TestBackgroundResponseTaskLeaseOwnership(t *testing.T) {
	store := &backgroundResponseMemoryStore{}
	svc := NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Hour)

	acquired, err := svc.AcquireLease(context.Background(), "resp_bg_lease", "worker-a", time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)
	acquired, err = svc.AcquireLease(context.Background(), "resp_bg_lease", "worker-b", time.Minute)
	require.NoError(t, err)
	require.False(t, acquired)

	renewed, err := svc.RenewLease(context.Background(), "resp_bg_lease", "worker-b", time.Minute)
	require.NoError(t, err)
	require.False(t, renewed)
	renewed, err = svc.RenewLease(context.Background(), "resp_bg_lease", "worker-a", time.Minute)
	require.NoError(t, err)
	require.True(t, renewed)

	require.NoError(t, svc.ReleaseLease(context.Background(), "resp_bg_lease", "worker-b"))
	acquired, err = svc.AcquireLease(context.Background(), "resp_bg_lease", "worker-b", time.Minute)
	require.NoError(t, err)
	require.False(t, acquired)

	require.NoError(t, svc.ReleaseLease(context.Background(), "resp_bg_lease", "worker-a"))
	acquired, err = svc.AcquireLease(context.Background(), "resp_bg_lease", "worker-b", time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)
}

func TestBackgroundResponseTaskFencingRejectsStaleFailureBeforeCompletion(t *testing.T) {
	store := &backgroundResponseMemoryStore{}
	svc := NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Hour)
	owner := BackgroundResponseOwner{UserID: 33, APIKeyID: 34}
	task, err := svc.Create(context.Background(), owner, "gpt-5.6-sol")
	require.NoError(t, err)
	require.NoError(t, svc.MarkInProgress(context.Background(), task.ID))

	staleLease, acquired, err := svc.AcquireFencedLease(context.Background(), task.ID, "worker-a", time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, svc.ReleaseFencedLease(context.Background(), task.ID, staleLease))
	currentLease, acquired, err := svc.AcquireFencedLease(context.Background(), task.ID, "worker-b", time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)
	require.Greater(t, currentLease.Fence, staleLease.Fence)

	err = svc.FailWithLease(context.Background(), task.ID, staleLease, http.StatusNotFound, json.RawMessage(`{"code":"upstream_response_not_found"}`))
	require.ErrorIs(t, err, ErrBackgroundResponseLeaseLost)

	result := json.RawMessage(`{"id":"resp_upstream","status":"completed","output":[]}`)
	require.NoError(t, svc.CompleteWithLease(context.Background(), task.ID, currentLease, http.StatusOK, result))
	got, err := svc.Get(context.Background(), owner, task.ID)
	require.NoError(t, err)
	require.Equal(t, BackgroundResponseStatusCompleted, got.Status)
	require.JSONEq(t, string(result), string(got.Result))
	require.Empty(t, got.Error)
}

func TestBackgroundResponseTaskUpstreamCancelledKeepsAudit(t *testing.T) {
	store := &backgroundResponseMemoryStore{}
	svc := NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Hour)
	owner := BackgroundResponseOwner{UserID: 35, APIKeyID: 36}
	task, err := svc.Create(context.Background(), owner, "gpt-5.6-sol")
	require.NoError(t, err)
	lease, acquired, err := svc.AcquireFencedLease(context.Background(), task.ID, "worker-a", time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)

	audit := json.RawMessage(`{"code":"upstream_task_cancelled","detail":"upstream background response was cancelled"}`)
	require.NoError(t, svc.CancelUpstreamWithLease(context.Background(), task.ID, lease, http.StatusOK, audit))
	got, err := svc.Get(context.Background(), owner, task.ID)
	require.NoError(t, err)
	require.Equal(t, BackgroundResponseStatusCancelled, got.Status)
	require.JSONEq(t, string(audit), string(got.Error))
	require.NotNil(t, got.CompletedAt)
}

func TestBackgroundResponseTaskMissingClassificationUsesOwnerScopedIndex(t *testing.T) {
	store := &backgroundResponseMemoryStore{}
	svc := NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Hour)
	owner := BackgroundResponseOwner{UserID: 37, APIKeyID: 38}
	task, err := svc.Create(context.Background(), owner, "gpt-5.6-sol")
	require.NoError(t, err)

	store.mu.Lock()
	store.task = nil
	store.index.ExpiresAt = time.Now().UTC().Add(time.Hour).Unix()
	store.mu.Unlock()
	_, err = svc.Get(context.Background(), owner, task.ID)
	require.ErrorIs(t, err, ErrBackgroundResponseMappingMissing)
	_, err = svc.Get(context.Background(), BackgroundResponseOwner{UserID: owner.UserID, APIKeyID: owner.APIKeyID + 1}, task.ID)
	require.ErrorIs(t, err, ErrBackgroundResponseNotFound)

	store.mu.Lock()
	store.index.ExpiresAt = time.Now().UTC().Add(-time.Second).Unix()
	store.mu.Unlock()
	_, err = svc.Get(context.Background(), owner, task.ID)
	require.ErrorIs(t, err, ErrBackgroundResponseTaskExpired)
}

func TestBackgroundResponseTaskCASPreventsLateFailureOverwritingCompletion(t *testing.T) {
	store := &backgroundResponseCASRaceStore{
		backgroundResponseMemoryStore: &backgroundResponseMemoryStore{},
		failureReady:                  make(chan struct{}),
		completionDone:                make(chan struct{}),
	}
	svc := NewBackgroundResponseTaskServiceWithOptions(store, time.Hour, time.Hour)
	owner := BackgroundResponseOwner{UserID: 31, APIKeyID: 32}
	task, err := svc.Create(context.Background(), owner, "gpt-5.6-sol")
	require.NoError(t, err)
	require.NoError(t, svc.MarkInProgress(context.Background(), task.ID))

	result := json.RawMessage(`{"id":"resp_upstream","status":"completed","output":[]}`)
	errs := make(chan error, 2)
	go func() {
		errs <- svc.Complete(context.Background(), task.ID, http.StatusOK, result)
	}()
	go func() {
		errs <- svc.Fail(context.Background(), task.ID, http.StatusNotFound, json.RawMessage(`{"detail":"Not Found"}`))
	}()
	require.NoError(t, <-errs)
	require.NoError(t, <-errs)

	got, err := svc.Get(context.Background(), owner, task.ID)
	require.NoError(t, err)
	require.Equal(t, BackgroundResponseStatusCompleted, got.Status)
	require.JSONEq(t, string(result), string(got.Result))
	require.Empty(t, got.Error)
}
