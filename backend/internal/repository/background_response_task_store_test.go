package repository

import (
	"context"
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

	require.NoError(t, store.ReleaseIdempotency(context.Background(), owner, "hash", "resp_bg_1"))
	_, reserved, err = store.ReserveIdempotency(context.Background(), owner, "hash", "resp_bg_2", time.Hour)
	require.NoError(t, err)
	require.True(t, reserved)
}
