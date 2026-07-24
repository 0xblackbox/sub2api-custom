package repository

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestBackgroundResponseTaskStoreRoundTripAndTTL(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	store := NewBackgroundResponseTaskStore(rdb)
	task := &service.BackgroundResponseTaskRecord{
		ID:        "resp_bg_123",
		UserID:    7,
		APIKeyID:  9,
		Model:     "gpt-5",
		Status:    service.BackgroundResponseStatusInProgress,
		CreatedAt: 100,
		ExpiresAt: 200,
	}

	require.NoError(t, store.Save(context.Background(), task, 24*time.Hour))
	got, err := store.Get(context.Background(), task.ID)
	require.NoError(t, err)
	require.Equal(t, task, got)
	require.Equal(t, 24*time.Hour, mr.TTL(backgroundResponseTaskKey(task.ID)))
	indexStore, ok := store.(service.BackgroundResponseTaskIndexStore)
	require.True(t, ok)
	index, err := indexStore.GetTaskIndex(context.Background(), task.ID)
	require.NoError(t, err)
	require.Equal(t, task.UserID, index.UserID)
	require.Equal(t, task.APIKeyID, index.APIKeyID)
	require.Equal(t, task.ExpiresAt, index.ExpiresAt)
	require.Equal(t, 8*24*time.Hour, mr.TTL(backgroundResponseTaskIndexKey(task.ID)))
}

func TestBackgroundResponseTaskStoreMissing(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	store := NewBackgroundResponseTaskStore(rdb)

	_, err := store.Get(context.Background(), "resp_bg_missing")
	require.ErrorIs(t, err, service.ErrBackgroundResponseNotFound)
}

func TestBackgroundResponseTaskStoreListActive(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	store := NewBackgroundResponseTaskStore(rdb)

	require.NoError(t, store.Save(context.Background(), &service.BackgroundResponseTaskRecord{
		ID:        "resp_bg_active",
		UserID:    7,
		APIKeyID:  9,
		Model:     "gpt-5",
		Status:    service.BackgroundResponseStatusInProgress,
		CreatedAt: 100,
		ExpiresAt: 200,
	}, time.Hour))
	require.NoError(t, store.Save(context.Background(), &service.BackgroundResponseTaskRecord{
		ID:        "resp_bg_done",
		UserID:    7,
		APIKeyID:  9,
		Model:     "gpt-5",
		Status:    service.BackgroundResponseStatusCompleted,
		CreatedAt: 100,
		ExpiresAt: 200,
	}, time.Hour))

	active, err := store.ListActive(context.Background(), 10)
	require.NoError(t, err)
	require.Len(t, active, 1)
	require.Equal(t, "resp_bg_active", active[0].ID)
}

func TestBackgroundResponseTaskStoreIdempotencyReservation(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	store := NewBackgroundResponseTaskStore(rdb)
	owner := service.BackgroundResponseOwner{UserID: 7, APIKeyID: 9}

	existing, reserved, err := store.ReserveIdempotency(context.Background(), owner, "hash", "resp_bg_1", time.Hour)
	require.NoError(t, err)
	require.True(t, reserved)
	require.Empty(t, existing)

	existing, reserved, err = store.ReserveIdempotency(context.Background(), owner, "hash", "resp_bg_2", time.Hour)
	require.NoError(t, err)
	require.False(t, reserved)
	require.Equal(t, "resp_bg_1", existing)

	require.NoError(t, store.ReleaseIdempotency(context.Background(), owner, "hash", "resp_bg_other"))
	existing, reserved, err = store.ReserveIdempotency(context.Background(), owner, "hash", "resp_bg_2", time.Hour)
	require.NoError(t, err)
	require.False(t, reserved)
	require.Equal(t, "resp_bg_1", existing)

	require.NoError(t, store.ReleaseIdempotency(context.Background(), owner, "hash", "resp_bg_1"))
	_, reserved, err = store.ReserveIdempotency(context.Background(), owner, "hash", "resp_bg_2", time.Hour)
	require.NoError(t, err)
	require.True(t, reserved)
}

func TestBackgroundResponseTaskServiceConcurrentIdempotentCreateIsAtomic(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	store := NewBackgroundResponseTaskStore(rdb)
	tasks := service.NewBackgroundResponseTaskService(store)
	owner := service.BackgroundResponseOwner{UserID: 7, APIKeyID: 9}
	meta := service.BackgroundResponseTaskMetadata{
		RequestFingerprint: "sha256:same-request",
		IdempotencyKeyHash: "sha256:same-key",
	}

	const workers = 32
	start := make(chan struct{})
	type createResult struct {
		task    *service.BackgroundResponseTaskRecord
		created bool
		err     error
	}
	results := make(chan createResult, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			task, created, err := tasks.CreateWithMetadata(context.Background(), owner, "gpt-5.6-sol", meta)
			results <- createResult{task: task, created: created, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	createdCount := 0
	taskID := ""
	for result := range results {
		require.NoError(t, result.err)
		require.NotNil(t, result.task)
		if taskID == "" {
			taskID = result.task.ID
		}
		require.Equal(t, taskID, result.task.ID)
		if result.created {
			createdCount++
		}
	}
	require.Equal(t, 1, createdCount)
	require.NotEmpty(t, taskID)

	keys := mr.Keys()
	taskBodies := 0
	for _, key := range keys {
		if len(key) >= len(backgroundResponseTaskKeyPrefix) &&
			key[:len(backgroundResponseTaskKeyPrefix)] == backgroundResponseTaskKeyPrefix {
			taskBodies++
		}
	}
	require.Equal(t, 1, taskBodies)
}

func TestBackgroundResponseTaskAtomicCreateNeverReclaimsLiveLegacyReservation(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	store := NewBackgroundResponseTaskStore(rdb)
	tasks := service.NewBackgroundResponseTaskService(store)
	owner := service.BackgroundResponseOwner{UserID: 7, APIKeyID: 9}
	const keyHash = "sha256:legacy-key"

	_, reserved, err := store.ReserveIdempotency(context.Background(), owner, keyHash, "resp_bg_legacy_creator", 24*time.Hour)
	require.NoError(t, err)
	require.True(t, reserved)

	_, created, err := tasks.CreateWithMetadata(context.Background(), owner, "gpt-5.6-sol", service.BackgroundResponseTaskMetadata{
		RequestFingerprint: "sha256:same-request",
		IdempotencyKeyHash: keyHash,
	})
	require.Error(t, err)
	require.False(t, created)
	reservation, getErr := mr.Get(backgroundResponseIdempotencyKey(owner, keyHash))
	require.NoError(t, getErr)
	require.Equal(t, "resp_bg_legacy_creator", reservation)

	// A long pause is not proof that the legacy creator is dead. Keep the live
	// reservation until its own TTL expires rather than risk a duplicate submit.
	mr.FastForward(23 * time.Hour)
	_, created, err = tasks.CreateWithMetadata(context.Background(), owner, "gpt-5.6-sol", service.BackgroundResponseTaskMetadata{
		RequestFingerprint: "sha256:same-request",
		IdempotencyKeyHash: keyHash,
	})
	require.Error(t, err)
	require.False(t, created)
	reservation, getErr = mr.Get(backgroundResponseIdempotencyKey(owner, keyHash))
	require.NoError(t, getErr)
	require.Equal(t, "resp_bg_legacy_creator", reservation)

	mr.FastForward(time.Hour + time.Second)
	task, created, err := tasks.CreateWithMetadata(context.Background(), owner, "gpt-5.6-sol", service.BackgroundResponseTaskMetadata{
		RequestFingerprint: "sha256:same-request",
		IdempotencyKeyHash: keyHash,
	})
	require.NoError(t, err)
	require.True(t, created)
	require.NotNil(t, task)
	reservation, getErr = mr.Get(backgroundResponseIdempotencyKey(owner, keyHash))
	require.NoError(t, getErr)
	require.Equal(t, task.ID, reservation)
	persisted, err := store.Get(context.Background(), task.ID)
	require.NoError(t, err)
	require.Equal(t, task.ID, persisted.ID)
}

func TestBackgroundResponseTaskCASRenewsIdempotencyReservationBeyondOriginalTTL(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	store := NewBackgroundResponseTaskStore(rdb)
	tasks := service.NewBackgroundResponseTaskService(store)
	owner := service.BackgroundResponseOwner{UserID: 7, APIKeyID: 9}
	meta := service.BackgroundResponseTaskMetadata{
		RequestFingerprint: "sha256:same-request",
		IdempotencyKeyHash: "sha256:same-key",
	}

	task, created, err := tasks.CreateWithMetadata(context.Background(), owner, "gpt-5.6-sol", meta)
	require.NoError(t, err)
	require.True(t, created)
	idempotencyKey := backgroundResponseIdempotencyKey(owner, meta.IdempotencyKeyHash)

	mr.FastForward(23 * time.Hour)
	require.NoError(t, tasks.MarkInProgress(context.Background(), task.ID))
	require.Equal(t, 24*time.Hour, mr.TTL(backgroundResponseTaskKey(task.ID)))
	require.Equal(t, 24*time.Hour, mr.TTL(idempotencyKey))

	// The original reservation would now be expired. The CAS heartbeat renewed
	// it together with the task, so a replay still resolves to the same id.
	mr.FastForward(2 * time.Hour)
	replayed, fresh, err := tasks.CreateWithMetadata(context.Background(), owner, "gpt-5.6-sol", meta)
	require.NoError(t, err)
	require.False(t, fresh)
	require.Equal(t, task.ID, replayed.ID)
}

func TestBackgroundResponseTaskFencedTerminalWriteRenewsIdempotencyReservation(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	store := NewBackgroundResponseTaskStore(rdb)
	tasks := service.NewBackgroundResponseTaskService(store)
	owner := service.BackgroundResponseOwner{UserID: 17, APIKeyID: 19}
	meta := service.BackgroundResponseTaskMetadata{
		RequestFingerprint: "sha256:fenced-request",
		IdempotencyKeyHash: "sha256:fenced-key",
	}
	task, created, err := tasks.CreateWithMetadata(context.Background(), owner, "gpt-5.6-sol", meta)
	require.NoError(t, err)
	require.True(t, created)

	mr.FastForward(23 * time.Hour)
	lease, acquired, err := tasks.AcquireFencedLease(context.Background(), task.ID, "worker-a", time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, tasks.CompleteWithLease(
		context.Background(),
		task.ID,
		lease,
		200,
		[]byte(`{"id":"resp_upstream","status":"completed","usage":{"input_tokens":1,"output_tokens":1}}`),
	))
	idempotencyKey := backgroundResponseIdempotencyKey(owner, meta.IdempotencyKeyHash)
	require.Equal(t, 24*time.Hour, mr.TTL(backgroundResponseTaskKey(task.ID)))
	require.Equal(t, 24*time.Hour, mr.TTL(idempotencyKey))

	mr.FastForward(2 * time.Hour)
	replayed, fresh, err := tasks.CreateWithMetadata(context.Background(), owner, "gpt-5.6-sol", meta)
	require.NoError(t, err)
	require.False(t, fresh)
	require.Equal(t, task.ID, replayed.ID)
	require.Equal(t, service.BackgroundResponseStatusCompleted, replayed.Status)
}

func TestBackgroundResponseTaskCASFailsClosedWhenIdempotencyReservationDrifts(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	store := NewBackgroundResponseTaskStore(rdb)
	tasks := service.NewBackgroundResponseTaskService(store)
	owner := service.BackgroundResponseOwner{UserID: 7, APIKeyID: 9}
	meta := service.BackgroundResponseTaskMetadata{
		RequestFingerprint: "sha256:same-request",
		IdempotencyKeyHash: "sha256:same-key",
	}
	task, created, err := tasks.CreateWithMetadata(context.Background(), owner, "gpt-5.6-sol", meta)
	require.NoError(t, err)
	require.True(t, created)

	idempotencyKey := backgroundResponseIdempotencyKey(owner, meta.IdempotencyKeyHash)
	require.NoError(t, rdb.Set(context.Background(), idempotencyKey, "resp_bg_other", 24*time.Hour).Err())
	require.Error(t, tasks.MarkInProgress(context.Background(), task.ID))

	persisted, err := store.Get(context.Background(), task.ID)
	require.NoError(t, err)
	require.Equal(t, service.BackgroundResponseStatusQueued, persisted.Status)
	require.Equal(t, int64(1), persisted.Version)
}

func TestBackgroundResponseTaskStoreSaveIfVersion(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	store := NewBackgroundResponseTaskStore(rdb)
	casStore, ok := store.(service.BackgroundResponseTaskCASStore)
	require.True(t, ok)
	task := &service.BackgroundResponseTaskRecord{
		Version:   1,
		ID:        "resp_bg_cas",
		UserID:    7,
		APIKeyID:  9,
		Model:     "gpt-5.6-sol",
		Status:    service.BackgroundResponseStatusInProgress,
		CreatedAt: 100,
		ExpiresAt: 200,
	}
	require.NoError(t, store.Save(context.Background(), task, time.Hour))

	completed := *task
	completed.Version = 2
	completed.Status = service.BackgroundResponseStatusCompleted
	saved, err := casStore.SaveIfVersion(context.Background(), &completed, 1, 24*time.Hour)
	require.NoError(t, err)
	require.True(t, saved)

	staleFailure := *task
	staleFailure.Version = 2
	staleFailure.Status = service.BackgroundResponseStatusFailed
	saved, err = casStore.SaveIfVersion(context.Background(), &staleFailure, 1, 24*time.Hour)
	require.NoError(t, err)
	require.False(t, saved)

	got, err := store.Get(context.Background(), task.ID)
	require.NoError(t, err)
	require.Equal(t, int64(2), got.Version)
	require.Equal(t, service.BackgroundResponseStatusCompleted, got.Status)
	require.Equal(t, 24*time.Hour, mr.TTL(backgroundResponseTaskKey(task.ID)))
}

func TestBackgroundResponseTaskStoreSaveIfVersionSupportsLegacyRecord(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	store := NewBackgroundResponseTaskStore(rdb)
	casStore, ok := store.(service.BackgroundResponseTaskCASStore)
	require.True(t, ok)
	key := backgroundResponseTaskKey("resp_bg_legacy")
	require.NoError(t, rdb.Set(context.Background(), key, `{"id":"resp_bg_legacy","user_id":7,"api_key_id":9,"status":"in_progress"}`, time.Hour).Err())

	legacy, err := store.Get(context.Background(), "resp_bg_legacy")
	require.NoError(t, err)
	require.Zero(t, legacy.Version)
	legacy.Version = 1
	legacy.Status = service.BackgroundResponseStatusCompleted
	saved, err := casStore.SaveIfVersion(context.Background(), legacy, 0, 24*time.Hour)
	require.NoError(t, err)
	require.True(t, saved)

	got, err := store.Get(context.Background(), "resp_bg_legacy")
	require.NoError(t, err)
	require.Equal(t, int64(1), got.Version)
	require.Equal(t, service.BackgroundResponseStatusCompleted, got.Status)
}

func TestBackgroundResponseTaskStoreLeaseOwnershipAndExpiry(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	store := NewBackgroundResponseTaskStore(rdb)
	leaseStore, ok := store.(service.BackgroundResponseTaskLeaseStore)
	require.True(t, ok)
	ctx := context.Background()

	acquired, err := leaseStore.AcquireLease(ctx, "resp_bg_lease", "worker-a", time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)
	acquired, err = leaseStore.AcquireLease(ctx, "resp_bg_lease", "worker-b", time.Minute)
	require.NoError(t, err)
	require.False(t, acquired)

	renewed, err := leaseStore.RenewLease(ctx, "resp_bg_lease", "worker-b", 2*time.Minute)
	require.NoError(t, err)
	require.False(t, renewed)
	renewed, err = leaseStore.RenewLease(ctx, "resp_bg_lease", "worker-a", 2*time.Minute)
	require.NoError(t, err)
	require.True(t, renewed)
	require.Equal(t, 2*time.Minute, mr.TTL(backgroundResponseLeaseKey("resp_bg_lease")))

	require.NoError(t, leaseStore.ReleaseLease(ctx, "resp_bg_lease", "worker-b"))
	require.True(t, mr.Exists(backgroundResponseLeaseKey("resp_bg_lease")))
	require.NoError(t, leaseStore.ReleaseLease(ctx, "resp_bg_lease", "worker-a"))
	require.False(t, mr.Exists(backgroundResponseLeaseKey("resp_bg_lease")))

	acquired, err = leaseStore.AcquireLease(ctx, "resp_bg_lease", "worker-b", time.Second)
	require.NoError(t, err)
	require.True(t, acquired)
	mr.FastForward(2 * time.Second)
	acquired, err = leaseStore.AcquireLease(ctx, "resp_bg_lease", "worker-a", time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)
}

func TestBackgroundResponseTaskStoreFencedLeaseRejectsStaleTerminalWrite(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	store := NewBackgroundResponseTaskStore(rdb)
	fencedStore, ok := store.(service.BackgroundResponseTaskFencedLeaseStore)
	require.True(t, ok)
	ctx := context.Background()
	task := &service.BackgroundResponseTaskRecord{
		Version:   1,
		ID:        "resp_bg_fenced",
		UserID:    7,
		APIKeyID:  9,
		Status:    service.BackgroundResponseStatusInProgress,
		CreatedAt: 100,
		ExpiresAt: 200,
	}
	require.NoError(t, store.Save(ctx, task, time.Hour))

	staleFence, acquired, err := fencedStore.AcquireFencedLease(ctx, task.ID, "worker-a", time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)
	require.Positive(t, staleFence)
	require.NoError(t, fencedStore.ReleaseFencedLease(ctx, task.ID, "worker-a", staleFence))

	currentFence, acquired, err := fencedStore.AcquireFencedLease(ctx, task.ID, "worker-b", time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)
	require.Greater(t, currentFence, staleFence)

	staleFailure := *task
	staleFailure.Version = 2
	staleFailure.Status = service.BackgroundResponseStatusFailed
	saved, leaseCurrent, err := fencedStore.SaveIfVersionAndLease(ctx, &staleFailure, 1, "worker-a", staleFence, 24*time.Hour)
	require.NoError(t, err)
	require.False(t, saved)
	require.False(t, leaseCurrent)

	completed := *task
	completed.Version = 2
	completed.Status = service.BackgroundResponseStatusCompleted
	saved, leaseCurrent, err = fencedStore.SaveIfVersionAndLease(ctx, &completed, 1, "worker-b", currentFence, 24*time.Hour)
	require.NoError(t, err)
	require.True(t, saved)
	require.True(t, leaseCurrent)

	got, err := store.Get(ctx, task.ID)
	require.NoError(t, err)
	require.Equal(t, service.BackgroundResponseStatusCompleted, got.Status)
}

func TestBackgroundResponseTaskStoreIndexOutlivesFullTask(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	store := NewBackgroundResponseTaskStore(rdb)
	indexStore, ok := store.(service.BackgroundResponseTaskIndexStore)
	require.True(t, ok)
	ctx := context.Background()
	task := &service.BackgroundResponseTaskRecord{
		Version:   1,
		ID:        "resp_bg_tombstone",
		UserID:    17,
		APIKeyID:  19,
		Status:    service.BackgroundResponseStatusCompleted,
		CreatedAt: 100,
		ExpiresAt: 200,
	}
	require.NoError(t, store.Save(ctx, task, time.Hour))

	mr.FastForward(time.Hour + time.Second)
	_, err := store.Get(ctx, task.ID)
	require.ErrorIs(t, err, service.ErrBackgroundResponseNotFound)
	index, err := indexStore.GetTaskIndex(ctx, task.ID)
	require.NoError(t, err)
	require.Equal(t, task.ID, index.ID)
	require.Equal(t, task.UserID, index.UserID)
	require.Equal(t, task.APIKeyID, index.APIKeyID)
}

func TestBackgroundResponseTaskStoreListUsagePendingSurvivesRestart(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	ctx := context.Background()
	store := NewBackgroundResponseTaskStore(rdb)

	require.NoError(t, store.Save(ctx, &service.BackgroundResponseTaskRecord{
		Version:                1,
		ID:                     "resp_bg_usage_pending",
		UserID:                 7,
		APIKeyID:               9,
		Status:                 service.BackgroundResponseStatusCompleted,
		UsageSettlementVersion: 1,
		UsageRecorded:          false,
		CreatedAt:              100,
		ExpiresAt:              200,
	}, time.Hour))
	require.NoError(t, store.Save(ctx, &service.BackgroundResponseTaskRecord{
		Version:                1,
		ID:                     "resp_bg_usage_recorded",
		UserID:                 7,
		APIKeyID:               9,
		Status:                 service.BackgroundResponseStatusCompleted,
		UsageSettlementVersion: 1,
		UsageRecorded:          true,
		CreatedAt:              100,
		ExpiresAt:              200,
	}, time.Hour))
	require.NoError(t, store.Save(ctx, &service.BackgroundResponseTaskRecord{
		Version:   0,
		ID:        "resp_bg_usage_legacy",
		UserID:    7,
		APIKeyID:  9,
		Status:    service.BackgroundResponseStatusCompleted,
		CreatedAt: 100,
		ExpiresAt: 200,
	}, time.Hour))
	require.NoError(t, store.Save(ctx, &service.BackgroundResponseTaskRecord{
		Version:                1,
		ID:                     "resp_bg_usage_active",
		UserID:                 7,
		APIKeyID:               9,
		Status:                 service.BackgroundResponseStatusInProgress,
		UsageSettlementVersion: 1,
		CreatedAt:              100,
		ExpiresAt:              200,
	}, time.Hour))

	// Constructing a new store over the same Redis instance models a worker
	// restart; pending settlement is derived from durable records, not memory.
	restarted := NewBackgroundResponseTaskStore(rdb)
	usageStore, ok := restarted.(service.BackgroundResponseTaskUsageStore)
	require.True(t, ok)
	pending, err := usageStore.ListUsagePending(ctx, 10)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, "resp_bg_usage_pending", pending[0].ID)
}

func TestBackgroundResponseTaskStoreActiveScanRotatesPastRunningBatch(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	store := NewBackgroundResponseTaskStore(rdb)
	ctx := context.Background()

	for i := 0; i < 7; i++ {
		require.NoError(t, store.Save(ctx, &service.BackgroundResponseTaskRecord{
			Version:   1,
			ID:        fmt.Sprintf("resp_bg_active_%02d", i),
			UserID:    7,
			APIKeyID:  9,
			Status:    service.BackgroundResponseStatusInProgress,
			CreatedAt: 100,
			ExpiresAt: 200,
		}, time.Hour))
	}

	first, err := store.ListActive(ctx, 3)
	require.NoError(t, err)
	require.Len(t, first, 3)
	firstIDs := make(map[string]struct{}, len(first))
	for _, task := range first {
		firstIDs[task.ID] = struct{}{}
	}

	// These first records model tasks already running in this process. The next
	// recovery tick must advance its persisted SCAN page and expose an orphan
	// outside that batch instead of returning the same first page forever.
	second, err := store.ListActive(ctx, 3)
	require.NoError(t, err)
	require.NotEmpty(t, second)
	sawNew := false
	for _, task := range second {
		if _, existed := firstIDs[task.ID]; !existed {
			sawNew = true
		}
	}
	require.True(t, sawNew)
}

func TestBackgroundResponseTaskStoreUsageScanEventuallyPassesMoreThanBatchFailures(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	store := NewBackgroundResponseTaskStore(rdb)
	ctx := context.Background()

	const failedCandidates = 105
	for i := 0; i < failedCandidates; i++ {
		require.NoError(t, store.Save(ctx, &service.BackgroundResponseTaskRecord{
			Version:                1,
			ID:                     fmt.Sprintf("resp_bg_usage_failed_%03d", i),
			UserID:                 7,
			APIKeyID:               9,
			Status:                 service.BackgroundResponseStatusCompleted,
			UsageSettlementVersion: 1,
			UsageRecordAttempts:    20,
			UsageConsecutiveErrors: 20,
			CreatedAt:              100,
			ExpiresAt:              200,
		}, time.Hour))
	}
	const goodID = "resp_bg_usage_good_after_failures"
	require.NoError(t, store.Save(ctx, &service.BackgroundResponseTaskRecord{
		Version:                1,
		ID:                     goodID,
		UserID:                 7,
		APIKeyID:               9,
		Status:                 service.BackgroundResponseStatusCompleted,
		UsageSettlementVersion: 1,
		CreatedAt:              100,
		ExpiresAt:              200,
	}, time.Hour))

	seen := make(map[string]struct{}, failedCandidates+1)
	for page := 0; page < 4 && len(seen) < failedCandidates+1; page++ {
		pending, err := store.(service.BackgroundResponseTaskUsageStore).ListUsagePending(ctx, 100)
		require.NoError(t, err)
		for _, task := range pending {
			seen[task.ID] = struct{}{}
		}
	}
	require.Len(t, seen, failedCandidates+1)
	require.Contains(t, seen, goodID)
}

func TestBackgroundResponseTaskStoreUsageScanSkipsPersistedBackoff(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	store := NewBackgroundResponseTaskStore(rdb)
	ctx := context.Background()
	future := time.Now().UTC().Add(time.Hour).Unix()

	require.NoError(t, store.Save(ctx, &service.BackgroundResponseTaskRecord{
		Version:                1,
		ID:                     "resp_bg_usage_backoff",
		UserID:                 7,
		APIKeyID:               9,
		Status:                 service.BackgroundResponseStatusCompleted,
		UsageSettlementVersion: 1,
		UsageConsecutiveErrors: 5,
		UsageNextAttemptAt:     &future,
		CreatedAt:              100,
		ExpiresAt:              200,
	}, time.Hour))
	require.NoError(t, store.Save(ctx, &service.BackgroundResponseTaskRecord{
		Version:                1,
		ID:                     "resp_bg_usage_due",
		UserID:                 7,
		APIKeyID:               9,
		Status:                 service.BackgroundResponseStatusCompleted,
		UsageSettlementVersion: 1,
		CreatedAt:              100,
		ExpiresAt:              200,
	}, time.Hour))

	pending, err := store.(service.BackgroundResponseTaskUsageStore).ListUsagePending(ctx, 10)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, "resp_bg_usage_due", pending[0].ID)
}
