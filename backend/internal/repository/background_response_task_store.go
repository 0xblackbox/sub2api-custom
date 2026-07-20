package repository

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

const backgroundResponseTaskKeyPrefix = "background_response_task:"

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

func backgroundResponseTaskKey(id string) string {
	return backgroundResponseTaskKeyPrefix + strings.TrimSpace(id)
}
