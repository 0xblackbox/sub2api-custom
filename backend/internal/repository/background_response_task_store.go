package repository

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

const backgroundResponseTaskKeyPrefix = "background_response_task:"
const backgroundResponseIdempotencyKeyPrefix = "background_response_idempotency:"

type backgroundResponseTaskStore struct {
	rdb *redis.Client
}

func NewBackgroundResponseTaskStore(rdb *redis.Client) service.BackgroundResponseTaskStore {
	return &backgroundResponseTaskStore{rdb: rdb}
}

func (s *backgroundResponseTaskStore) Save(ctx context.Context, task *service.BackgroundResponseTaskRecord, ttl time.Duration) error {
	data, err := json.Marshal(task)
	if err != nil {
		return err
	}
	return s.rdb.Set(ctx, backgroundResponseTaskKey(task.ID), data, ttl).Err()
}

func (s *backgroundResponseTaskStore) Get(ctx context.Context, id string) (*service.BackgroundResponseTaskRecord, error) {
	data, err := s.rdb.Get(ctx, backgroundResponseTaskKey(id)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, service.ErrBackgroundResponseNotFound
		}
		return nil, err
	}
	var task service.BackgroundResponseTaskRecord
	if err := json.Unmarshal(data, &task); err != nil {
		return nil, err
	}
	return &task, nil
}

func (s *backgroundResponseTaskStore) ListActive(ctx context.Context, limit int) ([]*service.BackgroundResponseTaskRecord, error) {
	if limit <= 0 {
		limit = 1000
	}
	var (
		cursor uint64
		out    []*service.BackgroundResponseTaskRecord
	)
	for {
		keys, next, err := s.rdb.Scan(ctx, cursor, backgroundResponseTaskKeyPrefix+"*", 100).Result()
		if err != nil {
			return nil, err
		}
		cursor = next
		for _, key := range keys {
			data, err := s.rdb.Get(ctx, key).Bytes()
			if err != nil {
				if err == redis.Nil {
					continue
				}
				return nil, err
			}
			var task service.BackgroundResponseTaskRecord
			if err := json.Unmarshal(data, &task); err != nil {
				continue
			}
			if task.Status == service.BackgroundResponseStatusQueued || task.Status == service.BackgroundResponseStatusInProgress {
				out = append(out, &task)
				if len(out) >= limit {
					return out, nil
				}
			}
		}
		if cursor == 0 {
			return out, nil
		}
	}
}

func (s *backgroundResponseTaskStore) ReserveIdempotency(ctx context.Context, owner service.BackgroundResponseOwner, keyHash, taskID string, ttl time.Duration) (string, bool, error) {
	key := backgroundResponseIdempotencyKey(owner, keyHash)
	if key == "" || strings.TrimSpace(taskID) == "" {
		return "", true, nil
	}
	reserved, err := s.rdb.SetNX(ctx, key, strings.TrimSpace(taskID), ttl).Result()
	if err != nil {
		return "", false, err
	}
	if reserved {
		return "", true, nil
	}
	existing, err := s.rdb.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return "", false, service.ErrBackgroundResponseNotFound
		}
		return "", false, err
	}
	return strings.TrimSpace(existing), false, nil
}

func (s *backgroundResponseTaskStore) ReleaseIdempotency(ctx context.Context, owner service.BackgroundResponseOwner, keyHash, taskID string) error {
	key := backgroundResponseIdempotencyKey(owner, keyHash)
	if key == "" || strings.TrimSpace(taskID) == "" {
		return nil
	}
	existing, err := s.rdb.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil
		}
		return err
	}
	if strings.TrimSpace(existing) != strings.TrimSpace(taskID) {
		return nil
	}
	return s.rdb.Del(ctx, key).Err()
}

func backgroundResponseTaskKey(id string) string {
	return backgroundResponseTaskKeyPrefix + strings.TrimSpace(id)
}

func backgroundResponseIdempotencyKey(owner service.BackgroundResponseOwner, keyHash string) string {
	keyHash = strings.TrimSpace(keyHash)
	if owner.UserID <= 0 || owner.APIKeyID <= 0 || keyHash == "" {
		return ""
	}
	return backgroundResponseIdempotencyKeyPrefix + strings.Join([]string{
		strconv.FormatInt(owner.UserID, 10),
		strconv.FormatInt(owner.APIKeyID, 10),
		keyHash,
	}, ":")
}
