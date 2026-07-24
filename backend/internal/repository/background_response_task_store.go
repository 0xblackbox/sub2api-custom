package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

const backgroundResponseTaskKeyPrefix = "background_response_task:"
const backgroundResponseIdempotencyKeyPrefix = "background_response_idempotency:"
const backgroundResponseLeaseKeyPrefix = "background_response_lease:"
const backgroundResponseFenceKeyPrefix = "background_response_fence:"
const backgroundResponseTaskIndexKeyPrefix = "background_response_task_index:"

const backgroundResponseTaskIndexRetentionSlack = 7 * 24 * time.Hour
const backgroundResponseFenceRetention = 32 * 24 * time.Hour

// backgroundResponseCreateWithIdempotencyScript atomically publishes the
// idempotency reservation, task body, and owner-scoped task index. A replay can
// therefore never observe a reservation created by this version without also
// observing its task body.
//
// During a rolling upgrade an older process may already have published the
// reservation before saving the task. Never reclaim such a reservation while
// its TTL is live: the old creator may resume after an arbitrary pause, and
// guessing that it is stale could submit the same billable request twice.
// Natural reservation expiry is the only automatic stale-recovery boundary.
var backgroundResponseCreateWithIdempotencyScript = redis.NewScript(`
local existing = redis.call("GET", KEYS[1])
if existing then
	return {0, existing}
end
redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[3])
redis.call("SET", KEYS[2], ARGV[2], "PX", ARGV[3])
redis.call("SET", KEYS[3], ARGV[4], "PX", ARGV[5])
return {1, ARGV[1]}
`)

var backgroundResponseSaveIfVersionScript = redis.NewScript(`
local current = redis.call("GET", KEYS[1])
if not current then
	return -1
end
local ok, decoded = pcall(cjson.decode, current)
if not ok then
	return -2
end
local current_version = tonumber(decoded["version"] or 0)
local expected_version = tonumber(ARGV[1])
if current_version ~= expected_version then
	return 0
end
if ARGV[6] == "1" and redis.call("GET", KEYS[3]) ~= ARGV[7] then
	return -3
end
redis.call("SET", KEYS[1], ARGV[2], "PX", ARGV[3])
redis.call("SET", KEYS[2], ARGV[4], "PX", ARGV[5])
if ARGV[6] == "1" then
	redis.call("SET", KEYS[3], ARGV[7], "PX", ARGV[3])
end
return 1
`)

var backgroundResponseAcquireFencedLeaseScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 1 then
	return 0
end
local fence = redis.call("INCR", KEYS[2])
redis.call("SET", KEYS[1], tostring(fence) .. "|" .. ARGV[1], "PX", ARGV[2])
redis.call("PEXPIRE", KEYS[2], ARGV[3])
return fence
`)

var backgroundResponseSaveIfVersionAndLeaseScript = redis.NewScript(`
if redis.call("GET", KEYS[2]) ~= ARGV[4] then
	return -3
end
local current = redis.call("GET", KEYS[1])
if not current then
	return -1
end
local ok, decoded = pcall(cjson.decode, current)
if not ok then
	return -2
end
local current_version = tonumber(decoded["version"] or 0)
local expected_version = tonumber(ARGV[1])
if current_version ~= expected_version then
	return 0
end
if ARGV[7] == "1" and redis.call("GET", KEYS[4]) ~= ARGV[8] then
	return -4
end
redis.call("SET", KEYS[1], ARGV[2], "PX", ARGV[3])
redis.call("SET", KEYS[3], ARGV[5], "PX", ARGV[6])
if ARGV[7] == "1" then
	redis.call("SET", KEYS[4], ARGV[8], "PX", ARGV[3])
end
return 1
`)

var backgroundResponseRenewLeaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
	return 0
end
redis.call("PEXPIRE", KEYS[1], ARGV[2])
return 1
`)

var backgroundResponseCompareDeleteScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
	return 0
end
return redis.call("DEL", KEYS[1])
`)

type backgroundResponseTaskStore struct {
	rdb *redis.Client

	activeScanMu sync.Mutex
	activeScan   backgroundResponseRotatingScan
	usageScanMu  sync.Mutex
	usageScan    backgroundResponseRotatingScan
}

type backgroundResponseRotatingScan struct {
	cursor  uint64
	pending []string
	wrapped bool
}

func NewBackgroundResponseTaskStore(rdb *redis.Client) service.BackgroundResponseTaskStore {
	return &backgroundResponseTaskStore{rdb: rdb}
}

func (s *backgroundResponseTaskStore) Save(ctx context.Context, task *service.BackgroundResponseTaskRecord, ttl time.Duration) error {
	data, err := json.Marshal(task)
	if err != nil {
		return err
	}
	indexData, err := json.Marshal(backgroundResponseTaskIndex(task))
	if err != nil {
		return err
	}
	_, err = s.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, backgroundResponseTaskKey(task.ID), data, ttl)
		pipe.Set(ctx, backgroundResponseTaskIndexKey(task.ID), indexData, backgroundResponseTaskIndexTTL(ttl))
		return nil
	})
	return err
}

func (s *backgroundResponseTaskStore) CreateWithIdempotency(
	ctx context.Context,
	task *service.BackgroundResponseTaskRecord,
	owner service.BackgroundResponseOwner,
	keyHash string,
	ttl time.Duration,
) (string, bool, error) {
	if task == nil || strings.TrimSpace(task.ID) == "" {
		return "", false, fmt.Errorf("background response task id is required")
	}
	keyHash = strings.TrimSpace(keyHash)
	if owner.UserID <= 0 || owner.APIKeyID <= 0 || keyHash == "" {
		return "", false, fmt.Errorf("background response idempotency owner and key are required")
	}
	if task.UserID != owner.UserID || task.APIKeyID != owner.APIKeyID {
		return "", false, fmt.Errorf("background response task owner does not match idempotency owner")
	}
	data, err := json.Marshal(task)
	if err != nil {
		return "", false, err
	}
	indexData, err := json.Marshal(backgroundResponseTaskIndex(task))
	if err != nil {
		return "", false, err
	}
	result, err := backgroundResponseCreateWithIdempotencyScript.Run(
		ctx,
		s.rdb,
		[]string{
			backgroundResponseIdempotencyKey(owner, keyHash),
			backgroundResponseTaskKey(task.ID),
			backgroundResponseTaskIndexKey(task.ID),
		},
		strings.TrimSpace(task.ID),
		data,
		backgroundResponseTTLMillis(ttl),
		indexData,
		backgroundResponseTTLMillis(backgroundResponseTaskIndexTTL(ttl)),
	).Slice()
	if err != nil {
		return "", false, err
	}
	if len(result) != 2 {
		return "", false, fmt.Errorf("unexpected background response idempotent create result")
	}
	status, ok := result[0].(int64)
	if !ok {
		return "", false, fmt.Errorf("unexpected background response idempotent create status")
	}
	taskID, ok := result[1].(string)
	if !ok || strings.TrimSpace(taskID) == "" {
		return "", false, fmt.Errorf("unexpected background response idempotent create task id")
	}
	switch status {
	case 1:
		return "", true, nil
	case 0:
		return strings.TrimSpace(taskID), false, nil
	default:
		return "", false, fmt.Errorf("unexpected background response idempotent create status: %d", status)
	}
}

func (s *backgroundResponseTaskStore) SaveIfVersion(ctx context.Context, task *service.BackgroundResponseTaskRecord, expectedVersion int64, ttl time.Duration) (bool, error) {
	if task == nil || strings.TrimSpace(task.ID) == "" {
		return false, fmt.Errorf("background response task id is required")
	}
	data, err := json.Marshal(task)
	if err != nil {
		return false, err
	}
	indexData, err := json.Marshal(backgroundResponseTaskIndex(task))
	if err != nil {
		return false, err
	}
	idempotencyKey, hasIdempotency := backgroundResponseTaskIdempotencyKey(task)
	result, err := backgroundResponseSaveIfVersionScript.Run(
		ctx,
		s.rdb,
		[]string{
			backgroundResponseTaskKey(task.ID),
			backgroundResponseTaskIndexKey(task.ID),
			idempotencyKey,
		},
		expectedVersion,
		data,
		backgroundResponseTTLMillis(ttl),
		indexData,
		backgroundResponseTTLMillis(backgroundResponseTaskIndexTTL(ttl)),
		backgroundResponseLuaBool(hasIdempotency),
		strings.TrimSpace(task.ID),
	).Int64()
	if err != nil {
		return false, err
	}
	switch result {
	case 1:
		return true, nil
	case 0:
		return false, nil
	case -1:
		return false, service.ErrBackgroundResponseNotFound
	case -2:
		return false, fmt.Errorf("persisted background response task is not valid JSON")
	case -3:
		return false, fmt.Errorf("background response idempotency reservation is missing or belongs to another task")
	default:
		return false, fmt.Errorf("unexpected background response CAS result: %d", result)
	}
}

func (s *backgroundResponseTaskStore) SaveIfVersionAndLease(
	ctx context.Context,
	task *service.BackgroundResponseTaskRecord,
	expectedVersion int64,
	holder string,
	fence int64,
	ttl time.Duration,
) (bool, bool, error) {
	if task == nil || strings.TrimSpace(task.ID) == "" {
		return false, false, fmt.Errorf("background response task id is required")
	}
	holder = strings.TrimSpace(holder)
	if holder == "" || fence <= 0 {
		return false, false, fmt.Errorf("background response fenced lease is required")
	}
	data, err := json.Marshal(task)
	if err != nil {
		return false, false, err
	}
	indexData, err := json.Marshal(backgroundResponseTaskIndex(task))
	if err != nil {
		return false, false, err
	}
	idempotencyKey, hasIdempotency := backgroundResponseTaskIdempotencyKey(task)
	result, err := backgroundResponseSaveIfVersionAndLeaseScript.Run(
		ctx,
		s.rdb,
		[]string{
			backgroundResponseTaskKey(task.ID),
			backgroundResponseLeaseKey(task.ID),
			backgroundResponseTaskIndexKey(task.ID),
			idempotencyKey,
		},
		expectedVersion,
		data,
		backgroundResponseTTLMillis(ttl),
		backgroundResponseFencedLeaseValue(holder, fence),
		indexData,
		backgroundResponseTTLMillis(backgroundResponseTaskIndexTTL(ttl)),
		backgroundResponseLuaBool(hasIdempotency),
		strings.TrimSpace(task.ID),
	).Int64()
	if err != nil {
		return false, false, err
	}
	switch result {
	case 1:
		return true, true, nil
	case 0:
		return false, true, nil
	case -1:
		return false, true, service.ErrBackgroundResponseNotFound
	case -2:
		return false, true, fmt.Errorf("persisted background response task is not valid JSON")
	case -3:
		return false, false, nil
	case -4:
		return false, true, fmt.Errorf("background response idempotency reservation is missing or belongs to another task")
	default:
		return false, false, fmt.Errorf("unexpected background response fenced CAS result: %d", result)
	}
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

func (s *backgroundResponseTaskStore) GetTaskIndex(ctx context.Context, id string) (*service.BackgroundResponseTaskIndex, error) {
	data, err := s.rdb.Get(ctx, backgroundResponseTaskIndexKey(id)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, service.ErrBackgroundResponseNotFound
		}
		return nil, err
	}
	var index service.BackgroundResponseTaskIndex
	if err := json.Unmarshal(data, &index); err != nil {
		return nil, err
	}
	return &index, nil
}

func (s *backgroundResponseTaskStore) ListActive(ctx context.Context, limit int) ([]*service.BackgroundResponseTaskRecord, error) {
	if limit <= 0 {
		limit = 1000
	}
	return s.listBackgroundResponseTasksRotating(
		ctx,
		limit,
		&s.activeScanMu,
		&s.activeScan,
		func(task *service.BackgroundResponseTaskRecord, _ int64) bool {
			return task.Status == service.BackgroundResponseStatusQueued ||
				task.Status == service.BackgroundResponseStatusInProgress
		},
	)
}

func (s *backgroundResponseTaskStore) ListUsagePending(ctx context.Context, limit int) ([]*service.BackgroundResponseTaskRecord, error) {
	if limit <= 0 {
		limit = 1000
	}
	return s.listBackgroundResponseTasksRotating(
		ctx,
		limit,
		&s.usageScanMu,
		&s.usageScan,
		func(task *service.BackgroundResponseTaskRecord, now int64) bool {
			return task.Status == service.BackgroundResponseStatusCompleted &&
				task.UsageSettlementVersion >= 1 &&
				!task.UsageRecorded &&
				(task.UsageNextAttemptAt == nil || *task.UsageNextAttemptAt <= now)
		},
	)
}

func (s *backgroundResponseTaskStore) listBackgroundResponseTasksRotating(
	ctx context.Context,
	limit int,
	mu *sync.Mutex,
	state *backgroundResponseRotatingScan,
	include func(*service.BackgroundResponseTaskRecord, int64) bool,
) ([]*service.BackgroundResponseTaskRecord, error) {
	mu.Lock()
	defer mu.Unlock()

	out := make([]*service.BackgroundResponseTaskRecord, 0, limit)
	now := time.Now().UTC().Unix()
	for {
		if len(state.pending) == 0 {
			if state.wrapped {
				// Finish this complete SCAN cycle before starting another one.
				// This prevents the same small candidate set from appearing twice
				// in one call when fewer than limit records exist.
				state.wrapped = false
				return out, nil
			}
			keys, next, err := s.rdb.Scan(ctx, state.cursor, backgroundResponseTaskKeyPrefix+"*", 100).Result()
			if err != nil {
				return nil, err
			}
			state.cursor = next
			state.pending = keys
			state.wrapped = next == 0
			if len(state.pending) == 0 {
				continue
			}
		}

		key := state.pending[0]
		state.pending = state.pending[1:]
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
		if !include(&task, now) {
			continue
		}
		out = append(out, &task)
		if len(out) >= limit {
			if len(state.pending) == 0 && state.wrapped {
				state.wrapped = false
			}
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
	_, err := backgroundResponseCompareDeleteScript.Run(
		ctx,
		s.rdb,
		[]string{key},
		strings.TrimSpace(taskID),
	).Int64()
	return err
}

func (s *backgroundResponseTaskStore) AcquireLease(ctx context.Context, taskID, holder string, ttl time.Duration) (bool, error) {
	taskID = strings.TrimSpace(taskID)
	holder = strings.TrimSpace(holder)
	if taskID == "" || holder == "" {
		return false, fmt.Errorf("background response lease task id and holder are required")
	}
	return s.rdb.SetNX(ctx, backgroundResponseLeaseKey(taskID), holder, ttl).Result()
}

func (s *backgroundResponseTaskStore) AcquireFencedLease(ctx context.Context, taskID, holder string, ttl time.Duration) (int64, bool, error) {
	taskID = strings.TrimSpace(taskID)
	holder = strings.TrimSpace(holder)
	if taskID == "" || holder == "" {
		return 0, false, fmt.Errorf("background response lease task id and holder are required")
	}
	fence, err := backgroundResponseAcquireFencedLeaseScript.Run(
		ctx,
		s.rdb,
		[]string{backgroundResponseLeaseKey(taskID), backgroundResponseFenceKey(taskID)},
		holder,
		backgroundResponseTTLMillis(ttl),
		backgroundResponseTTLMillis(backgroundResponseFenceRetention),
	).Int64()
	if err != nil {
		return 0, false, err
	}
	if fence <= 0 {
		return 0, false, nil
	}
	return fence, true, nil
}

func (s *backgroundResponseTaskStore) RenewLease(ctx context.Context, taskID, holder string, ttl time.Duration) (bool, error) {
	taskID = strings.TrimSpace(taskID)
	holder = strings.TrimSpace(holder)
	if taskID == "" || holder == "" {
		return false, fmt.Errorf("background response lease task id and holder are required")
	}
	result, err := backgroundResponseRenewLeaseScript.Run(
		ctx,
		s.rdb,
		[]string{backgroundResponseLeaseKey(taskID)},
		holder,
		backgroundResponseTTLMillis(ttl),
	).Int64()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func (s *backgroundResponseTaskStore) RenewFencedLease(ctx context.Context, taskID, holder string, fence int64, ttl time.Duration) (bool, error) {
	taskID = strings.TrimSpace(taskID)
	holder = strings.TrimSpace(holder)
	if taskID == "" || holder == "" || fence <= 0 {
		return false, fmt.Errorf("background response fenced lease is required")
	}
	result, err := backgroundResponseRenewLeaseScript.Run(
		ctx,
		s.rdb,
		[]string{backgroundResponseLeaseKey(taskID)},
		backgroundResponseFencedLeaseValue(holder, fence),
		backgroundResponseTTLMillis(ttl),
	).Int64()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func (s *backgroundResponseTaskStore) ReleaseLease(ctx context.Context, taskID, holder string) error {
	taskID = strings.TrimSpace(taskID)
	holder = strings.TrimSpace(holder)
	if taskID == "" || holder == "" {
		return fmt.Errorf("background response lease task id and holder are required")
	}
	_, err := backgroundResponseCompareDeleteScript.Run(
		ctx,
		s.rdb,
		[]string{backgroundResponseLeaseKey(taskID)},
		holder,
	).Int64()
	return err
}

func (s *backgroundResponseTaskStore) ReleaseFencedLease(ctx context.Context, taskID, holder string, fence int64) error {
	taskID = strings.TrimSpace(taskID)
	holder = strings.TrimSpace(holder)
	if taskID == "" || holder == "" || fence <= 0 {
		return fmt.Errorf("background response fenced lease is required")
	}
	_, err := backgroundResponseCompareDeleteScript.Run(
		ctx,
		s.rdb,
		[]string{backgroundResponseLeaseKey(taskID)},
		backgroundResponseFencedLeaseValue(holder, fence),
	).Int64()
	return err
}

func backgroundResponseTaskKey(id string) string {
	return backgroundResponseTaskKeyPrefix + strings.TrimSpace(id)
}

func backgroundResponseTaskIndexKey(id string) string {
	return backgroundResponseTaskIndexKeyPrefix + strings.TrimSpace(id)
}

func backgroundResponseLeaseKey(id string) string {
	return backgroundResponseLeaseKeyPrefix + strings.TrimSpace(id)
}

func backgroundResponseFenceKey(id string) string {
	return backgroundResponseFenceKeyPrefix + strings.TrimSpace(id)
}

func backgroundResponseTaskIndex(task *service.BackgroundResponseTaskRecord) service.BackgroundResponseTaskIndex {
	if task == nil {
		return service.BackgroundResponseTaskIndex{}
	}
	return service.BackgroundResponseTaskIndex{
		ID:        strings.TrimSpace(task.ID),
		UserID:    task.UserID,
		APIKeyID:  task.APIKeyID,
		ExpiresAt: task.ExpiresAt,
	}
}

func backgroundResponseTaskIndexTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		ttl = time.Millisecond
	}
	return ttl + backgroundResponseTaskIndexRetentionSlack
}

func backgroundResponseFencedLeaseValue(holder string, fence int64) string {
	return strconv.FormatInt(fence, 10) + "|" + strings.TrimSpace(holder)
}

func backgroundResponseTTLMillis(ttl time.Duration) int64 {
	millis := ttl.Milliseconds()
	if millis <= 0 {
		return 1
	}
	return millis
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

func backgroundResponseTaskIdempotencyKey(task *service.BackgroundResponseTaskRecord) (string, bool) {
	if task == nil || strings.TrimSpace(task.IdempotencyKeyHash) == "" {
		// Redis scripts require a concrete key even when the guarded branch is
		// disabled. Keep the sentinel task-scoped so unrelated writes cannot
		// contend on one global key; the script never reads or writes it.
		taskID := ""
		if task != nil {
			taskID = strings.TrimSpace(task.ID)
		}
		return backgroundResponseIdempotencyKeyPrefix + "none:" + taskID, false
	}
	return backgroundResponseIdempotencyKey(
		service.BackgroundResponseOwner{UserID: task.UserID, APIKeyID: task.APIKeyID},
		task.IdempotencyKeyHash,
	), true
}

func backgroundResponseLuaBool(value bool) string {
	if value {
		return "1"
	}
	return "0"
}
