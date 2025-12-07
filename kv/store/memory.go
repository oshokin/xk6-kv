package store

import (
	"bytes"
	"fmt"
	"slices"
	"strconv"
	"sync"
)

// MaxShardCount is the maximum number of shards allowed.
// 65536 (2^16) is a reasonable upper bound that provides excellent hash distribution
// while keeping memory overhead minimal (~5MB for empty shard structures).
// This limit is well beyond any practical CPU core count, allowing fine-grained
// control for extreme concurrency scenarios.
const MaxShardCount = 65536

// MemoryStore is an in-memory key-value store implementation backed by sharded maps.
// Sharding improves scalability under concurrent workloads while preserving optional
// key tracking structures for deterministic scans and random selection.
type MemoryStore struct {
	// shardCount is the number of shards to use.
	shardCount int
	// trackKeys is a flag to enable key tracking.
	trackKeys bool
	// shards is the array of shards.
	shards []*memoryShard
	// hashFn is the function to use to hash the key to a shard.
	hashFn shardHashFunc
	// mutationGate allows writers to run concurrently while still supporting mutation blocking.
	// Uses RWMutex: writers acquire RLock (shared), blockers acquire Lock (exclusive).
	mutationGate sync.RWMutex
	// isMutationBlocked is a flag to indicate if mutations are blocked.
	// Checked while holding mutationGate lock to avoid races.
	isMutationBlocked bool
	// mutationBlockReason is the reason why mutations are blocked.
	// Stored so blocked operations can return a descriptive error.
	mutationBlockReason error
}

// NewMemoryStore creates a MemoryStore with the provided memory configuration.
func NewMemoryStore(memoryCfg *MemoryConfig) *MemoryStore {
	if memoryCfg == nil {
		memoryCfg = new(MemoryConfig)
	}

	shardCount := memoryCfg.GetShardCount()

	store := &MemoryStore{
		shardCount: shardCount,
		trackKeys:  memoryCfg.TrackKeys,
		shards:     make([]*memoryShard, shardCount),
		hashFn:     selectShardHashFunc(defaultShardHashStrategy),
	}

	for i := range store.shards {
		shard := &memoryShard{
			container: make(map[string][]byte),
		}

		if memoryCfg.TrackKeys {
			shard.keysList = []string{}
			shard.keysMap = make(map[string]int)
			shard.ost = NewOSTree()
		}

		store.shards[i] = shard
	}

	return store
}

// Open prepares the store for use (no-op for in-memory store).
func (s *MemoryStore) Open() error {
	return nil
}

// Close releases resources held by the store (no-op for in-memory store).
func (s *MemoryStore) Close() error {
	return nil
}

// Get returns the current value for the given key as raw []byte.
// The returned value is the stored bytes; serialization concerns are handled
// by the SerializedStore wrapper. If the key does not exist, an error is returned.
func (s *MemoryStore) Get(key string) (any, error) {
	shard := s.getShardByKey(key)

	shard.mu.RLock()
	value, ok := shard.container[key]
	shard.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrKeyNotFound, key)
	}

	// Return a defensive copy so callers cannot mutate the stored value.
	return slices.Clone(value), nil
}

// Set associates value with key, overwriting any previous value.
// The value must be a []byte or string; other types result in an error.
func (s *MemoryStore) Set(key string, value any) error {
	release, err := s.guardMutation()
	if err != nil {
		return err
	}
	defer release()

	// Convert value to bytes if it's not already.
	valueBytes, err := normalizeToBytes(value)
	if err != nil {
		return err
	}

	shard := s.getShardByKey(key)

	shard.mu.Lock()
	_, existed := shard.container[key]

	shard.container[key] = valueBytes
	// Only update tracking structures for new keys to avoid unnecessary work.
	if !existed {
		shard.addKeyTrackingLocked(key)
	}

	shard.mu.Unlock()

	return nil
}

// IncrementBy atomically adds delta to the integer value stored at key.
// Absent keys are treated as 0. Values must be decimal ASCII int64.
// Returns the new value as int64.
func (s *MemoryStore) IncrementBy(key string, delta int64) (int64, error) {
	release, err := s.guardMutation()
	if err != nil {
		return 0, err
	}
	defer release()

	shard := s.getShardByKey(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	var current int64

	if b, exists := shard.container[key]; exists && len(b) > 0 {
		v, err := strconv.ParseInt(string(b), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("%w: key %q: %w", ErrValueParseFailed, key, err)
		}

		current = v
	}

	current += delta
	shard.container[key] = []byte(strconv.FormatInt(current, 10))

	// Check keysMap existence before adding to tracking to avoid duplicate entries.
	// This handles the case where IncrementBy creates a new key.
	if _, existed := shard.keysMap[key]; s.trackKeys && !existed {
		shard.addKeyTrackingLocked(key)
	}

	return current, nil
}

// GetOrSet returns the existing value (loaded=true) if key is present,
// otherwise stores value and returns it (loaded=false).
func (s *MemoryStore) GetOrSet(key string, value any) (actual any, loaded bool, err error) {
	release, err := s.guardMutation()
	if err != nil {
		return nil, false, err
	}
	defer release()

	// Convert value to bytes.
	valueBytes, err := normalizeToBytes(value)
	if err != nil {
		return nil, false, err
	}

	shard := s.getShardByKey(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	if existing, exists := shard.container[key]; exists {
		cloned, err := normalizeToBytes(existing)
		if err != nil {
			return nil, true, err
		}

		return cloned, true, nil
	}

	shard.container[key] = valueBytes
	shard.addKeyTrackingLocked(key)

	return slices.Clone(valueBytes), false, nil
}

// Swap replaces the current value for key with value, returning the previous value
// (cloned) and whether it existed.
func (s *MemoryStore) Swap(key string, value any) (previous any, loaded bool, err error) {
	release, err := s.guardMutation()
	if err != nil {
		return nil, false, err
	}
	defer release()

	// Convert value to bytes.
	valueBytes, err := normalizeToBytes(value)
	if err != nil {
		return nil, false, err
	}

	shard := s.getShardByKey(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	if current, ok := shard.container[key]; ok {
		prev := slices.Clone(current)
		shard.container[key] = valueBytes

		return prev, true, nil
	}

	shard.container[key] = valueBytes
	shard.addKeyTrackingLocked(key)

	return nil, false, nil
}

// CompareAndSwap atomically replaces the current value with newValue only if the
// current value equals oldValue (or the key is absent when oldValue is nil).
func (s *MemoryStore) CompareAndSwap(key string, oldValue any, newValue any) (bool, error) {
	release, err := s.guardMutation()
	if err != nil {
		return false, err
	}
	defer release()

	expectAbsent := oldValue == nil

	var oldBytes []byte

	if !expectAbsent {
		oldBytes, err = normalizeToBytes(oldValue)
		if err != nil {
			return false, err
		}
	}

	// Convert new value to bytes.
	newBytes, err := normalizeToBytes(newValue)
	if err != nil {
		return false, err
	}

	shard := s.getShardByKey(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	current, exists := shard.container[key]

	switch {
	case expectAbsent:
		// CAS with nil oldValue: only succeed if key doesn't exist.
		if exists {
			return false, nil
		}

		shard.container[key] = newBytes
		shard.addKeyTrackingLocked(key)

		return true, nil
	default:
		// CAS with non-nil oldValue: compare current value byte-for-byte.
		if !exists || !bytes.Equal(current, oldBytes) {
			return false, nil
		}

		shard.container[key] = newBytes

		// Indexes do not change because the key already existed.
		// No need to update keysList/keysMap/OSTree for existing keys.
		return true, nil
	}
}

// Delete removes key if present.
func (s *MemoryStore) Delete(key string) error {
	release, err := s.guardMutation()
	if err != nil {
		return err
	}
	defer release()

	shard := s.getShardByKey(key)

	shard.mu.Lock()

	if _, existed := shard.container[key]; !existed {
		shard.mu.Unlock()
		return nil
	}

	delete(shard.container, key)
	shard.removeKeyTrackingLocked(key)
	shard.mu.Unlock()

	return nil
}

// Exists reports whether key is present in the store.
func (s *MemoryStore) Exists(key string) (bool, error) {
	shard := s.getShardByKey(key)

	shard.mu.RLock()
	_, ok := shard.container[key]
	shard.mu.RUnlock()

	return ok, nil
}

// Clear removes all keys and values from the store.
// When trackKeys is enabled, auxiliary indexes are fully reset as well.
func (s *MemoryStore) Clear() error {
	release, err := s.guardMutation()
	if err != nil {
		return err
	}
	defer release()

	s.clearAllShardsUnsafe()

	return nil
}

// DeleteIfExists deletes key only if present.
// Returns true if a deletion occurred.
func (s *MemoryStore) DeleteIfExists(key string) (bool, error) {
	release, err := s.guardMutation()
	if err != nil {
		return false, err
	}
	defer release()

	shard := s.getShardByKey(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	if _, exists := shard.container[key]; !exists {
		return false, nil
	}

	delete(shard.container, key)
	shard.removeKeyTrackingLocked(key)

	return true, nil
}

// CompareAndDelete deletes key only if the current value equals oldValue.
func (s *MemoryStore) CompareAndDelete(key string, oldValue any) (bool, error) {
	release, err := s.guardMutation()
	if err != nil {
		return false, err
	}
	defer release()

	oldBytes, err := normalizeToBytes(oldValue)
	if err != nil {
		return false, err
	}

	shard := s.getShardByKey(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	current, exists := shard.container[key]
	if !exists || !bytes.Equal(current, oldBytes) {
		return false, nil
	}

	delete(shard.container, key)
	shard.removeKeyTrackingLocked(key)

	return true, nil
}

// Size returns the number of keys in the store.
func (s *MemoryStore) Size() (int64, error) {
	var total int64

	for _, shard := range s.shards {
		shard.mu.RLock()
		total += int64(len(shard.container))
		shard.mu.RUnlock()
	}

	return total, nil
}

// List returns key-value pairs whose keys start with prefix, sorted lexicographically.
func (s *MemoryStore) List(prefix string, limit int64) ([]Entry, error) {
	page, err := s.Scan(prefix, "", limit)
	if err != nil {
		return nil, err
	}

	return page.Entries, nil
}

// RebuildKeyList reconstructs key tracking structures for every shard.
func (s *MemoryStore) RebuildKeyList() error {
	if !s.trackKeys {
		return nil
	}

	release, err := s.guardMutation()
	if err != nil {
		return err
	}
	defer release()

	for _, shard := range s.shards {
		shard.mu.Lock()
		shard.keysList = make([]string, 0, len(shard.container))
		shard.keysMap = make(map[string]int, len(shard.container))
		shard.ost = NewOSTree()

		for k := range shard.container {
			shard.addKeyTrackingLocked(k)
		}

		shard.mu.Unlock()
	}

	return nil
}
