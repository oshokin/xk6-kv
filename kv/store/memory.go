package store

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var errUnsupportedValueType = errors.New("unsupported value type (want []byte or string)")

// MemoryStore is an in-memory key-value store implementation.
//
// It is safe for concurrent use. When trackKeys is enabled, the store
// maintains auxiliary structures to support O(1) random selection without
// prefixes and O(log n) random selection with prefixes via an order-statistics
// tree (OSTree). When trackKeys is disabled, RandomKey falls back to an O(n)
// two-pass scan.
type MemoryStore struct {
	mu        sync.RWMutex
	container map[string][]byte

	// Optional key tracking structures (enabled via trackKeys flag)
	trackKeys bool           // Whether key tracking is enabled
	keysList  []string       // Slice of all keys for O(1) random access when no prefix is used
	keysMap   map[string]int // Map from key to index in keysList for O(1) deletes
	ost       *OSTree        // Order-statistics tree for prefix-based random selection
}

// NewMemoryStore creates a new MemoryStore.
//
// If trackKeys is true, auxiliary key indexes (keysList/keysMap/OSTree) are
// initialized to accelerate RandomKey() calls.
func NewMemoryStore(trackKeys bool) *MemoryStore {
	var idx *OSTree
	if trackKeys {
		idx = NewOSTree()
	}

	return &MemoryStore{
		mu:        sync.RWMutex{},
		container: map[string][]byte{},
		trackKeys: trackKeys,
		keysList:  []string{},
		keysMap:   make(map[string]int),
		ost:       idx,
	}
}

// Get returns the current value for the given key as raw []byte.
//
// The returned value is the stored bytes; serialization concerns are handled
// by the SerializedStore wrapper. If the key does not exist, an error is returned.
func (s *MemoryStore) Get(key string) (any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.container[key]
	if !ok {
		return nil, fmt.Errorf("key %q not found", key)
	}

	// Return the raw bytes - serialization will be handled by the SerializedStore wrapper.
	return value, nil
}

// Set associates value with key, overwriting any previous value.
//
// The value must be a []byte or string; other types result in an error.
func (s *MemoryStore) Set(key string, value any) error {
	// Convert value to bytes if it's not already.
	valueBytes, err := normalizeToBytes(value)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if key already exists.
	_, existed := s.container[key]
	s.container[key] = valueBytes

	if !existed {
		s.addKeyLocked(key)
	}

	return nil
}

// IncrementBy atomically adds delta to the integer value stored at key.
// Absent keys are treated as 0. Values must be decimal ASCII int64.
//
// Returns the new value as int64.
func (s *MemoryStore) IncrementBy(key string, delta int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Parse current value or start from 0.
	var current int64

	if b, exists := s.container[key]; exists && len(b) > 0 {
		// Parse current as base-10 int64.
		v, err := strconv.ParseInt(string(b), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("value at %q is not a valid integer: %w", key, err)
		}

		current = v
	}

	current += delta
	s.container[key] = []byte(strconv.FormatInt(current, 10))

	// If this is a new key, index it.
	if _, existed := s.keysMap[key]; s.trackKeys && !existed {
		// Note: keysMap lookup is safe since addKeyLocked re-checks existence.
		s.addKeyLocked(key)
	}

	return current, nil
}

// GetOrSet returns the existing value (loaded=true) if key is present,
// otherwise stores "value" and returns it (loaded=false).
//
// The stored value is always a copy of the provided bytes/string.
func (s *MemoryStore) GetOrSet(key string, value any) (actual any, loaded bool, err error) {
	// Convert value to bytes.
	valueBytes, err := normalizeToBytes(value)
	if err != nil {
		return nil, false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if key exists.
	if existing, exists := s.container[key]; exists {
		// Return the existing raw bytes as-is.
		return existing, true, nil
	}

	// Store new value.
	s.container[key] = valueBytes
	s.addKeyLocked(key)

	return valueBytes, false, nil
}

// Swap replaces the current value for key with "value", returning the previous
// value and whether it existed.
func (s *MemoryStore) Swap(key string, value any) (previous any, loaded bool, err error) {
	// Convert value to bytes.
	valueBytes, err := normalizeToBytes(value)
	if err != nil {
		return nil, false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// If key exists, replace and return previous value (optionally copy for safety).
	if current, ok := s.container[key]; ok {
		// Return a copy so callers can't mutate internal storage by accident.
		prev := append([]byte(nil), current...)
		s.container[key] = valueBytes

		return prev, true, nil
	}

	// First insert: add and return a true nil interface for previous.
	s.container[key] = valueBytes
	s.addKeyLocked(key)

	return nil, false, nil
}

// CompareAndSwap atomically replaces the current value with "new" only if the
// current value equals "old". Returns true if the swap occurred.
//
// Both "old" and "new" must be []byte or string.
func (s *MemoryStore) CompareAndSwap(key string, oldValue any, newValue any) (bool, error) {
	// Convert old value to bytes.
	oldBytes, err := normalizeToBytes(oldValue)
	if err != nil {
		return false, err
	}

	// Convert new value to bytes.
	newBytes, err := normalizeToBytes(newValue)
	if err != nil {
		return false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Get current value.
	current, exists := s.container[key]
	if !exists {
		return false, nil
	}
	// Compare current value with old value.
	if !bytes.Equal(current, oldBytes) {
		return false, nil
	}

	// Update value.
	s.container[key] = newBytes

	// Indexes do not change because the key already existed.
	return true, nil
}

// Delete removes key if present. It is not an error if the key does not exist.
//
// When trackKeys is enabled, auxiliary indexes are maintained consistently.
func (s *MemoryStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, existed := s.container[key]; !existed {
		return nil
	}

	delete(s.container, key)
	s.removeKeyLocked(key)

	return nil
}

// Exists reports whether key is present in the store.
func (s *MemoryStore) Exists(key string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.container[key]

	return ok, nil
}

// Clear removes all keys and values from the store.
//
// When trackKeys is enabled, auxiliary indexes are fully reset as well.
func (s *MemoryStore) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.container = make(map[string][]byte)

	if s.trackKeys {
		s.keysList = []string{}
		s.keysMap = make(map[string]int)
		s.ost = NewOSTree()
	}

	return nil
}

// DeleteIfExists deletes key only if present.
// Returns true if a deletion occurred.
func (s *MemoryStore) DeleteIfExists(key string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.container[key]; !exists {
		return false, nil
	}

	delete(s.container, key)
	s.removeKeyLocked(key)

	return true, nil
}

// CompareAndDelete deletes key only if the current value equals "oldValue".
// Returns true if the deletion occurred.
func (s *MemoryStore) CompareAndDelete(key string, oldValue any) (bool, error) {
	oldBytes, err := normalizeToBytes(oldValue)
	if err != nil {
		return false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Get current value.
	current, exists := s.container[key]

	// Compare current value with old value.
	if !exists || !bytes.Equal(current, oldBytes) {
		return false, nil
	}

	// Delete the key.
	delete(s.container, key)
	s.removeKeyLocked(key)

	return true, nil
}

// Size returns the number of keys in the store.
func (s *MemoryStore) Size() (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return int64(len(s.container)), nil
}

// List returns key-value pairs filtered by prefix and limited by "limit".
//
// The result is sorted lexicographically by key for deterministic ordering.
// Passing limit <= 0 means "no limit".
//
// NOTE: This is O(n log n) due to the sort and scans the whole map in memory.
// For very large datasets, prefer disk backend with cursors.
func (s *MemoryStore) List(prefix string, limit int64) ([]Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Collect matching keys (full scan)
	keys := make([]string, 0, len(s.container))

	for k := range s.container {
		if prefix != "" && !strings.HasPrefix(k, prefix) {
			continue
		}

		keys = append(keys, k)
	}

	// Lexicographical sort for deterministic ordering
	sort.Strings(keys)

	var (
		// Pre-allocate result capacity to reduce reallocations.
		maxEntries = len(keys)
		// Apply limit if set.
		hasLimit = limit > 0
	)

	if hasLimit && limit < int64(maxEntries) {
		maxEntries = int(limit)
	}

	var (
		entries = make([]Entry, 0, maxEntries)
		count   int64
	)

	for _, k := range keys {
		if hasLimit && count >= limit {
			break
		}

		entries = append(entries, Entry{
			Key:   k,
			Value: s.container[k],
		})

		count++
	}

	return entries, nil
}

// RandomKey returns a random key, optionally filtered by "prefix".
//
// Strategies:
//  1. trackKeys && prefix == ""  -> O(1) random from keysList
//  2. trackKeys && prefix != ""  -> O(log n) via OSTree (rank/select)
//  3. !trackKeys                 -> O(n) two-pass scan
//
// If no match exists (including empty store), it returns "" and a nil error.
func (s *MemoryStore) RandomKey(prefix string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.trackKeys {
		return s.randomKeyTracked(prefix), nil
	}

	return s.randomKeyScan(prefix), nil
}

// RebuildKeyList reconstructs key tracking structures from the container.
//
// This is useful after crashes or when indexes might be out of sync. It is an
// O(n) operation and should be used sparingly. No-op if trackKeys is disabled.
func (s *MemoryStore) RebuildKeyList() error {
	if !s.trackKeys {
		return nil
	}

	s.mu.Lock()

	defer s.mu.Unlock()

	// Reinitialize tracking structures
	s.keysList = make([]string, 0, len(s.container))
	s.keysMap = make(map[string]int, len(s.container))
	s.ost = NewOSTree()

	// Repopulate from container
	for k := range s.container {
		s.keysMap[k] = len(s.keysList)
		s.keysList = append(s.keysList, k)
		s.ost.Insert(k)
	}

	return nil
}

// Close releases resources associated with the store.
//
// For MemoryStore this is a no-op and always returns nil.
func (s *MemoryStore) Close() error {
	return nil
}

// normalizeToBytes converts "value" to an owned []byte copy.
//
// Supported inputs:
//   - []byte   => copied
//   - string   => converted to []byte
//
// Anything else is rejected so the store always contains raw bytes and
// SerializedStore can uniformly (de)serialize.
func normalizeToBytes(value any) ([]byte, error) {
	switch v := value.(type) {
	case []byte:
		// Make a copy to avoid external aliasing after Set/Swap/GetOrSet.
		return append([]byte(nil), v...), nil
	case string:
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("%w: %T", errUnsupportedValueType, value)
	}
}

// addKeyLocked inserts k into tracking indexes.
// Precondition: s.mu must be held by caller. No-op if key already indexed.
func (s *MemoryStore) addKeyLocked(k string) {
	if !s.trackKeys {
		return
	}

	if _, exists := s.keysMap[k]; exists {
		return
	}

	s.keysMap[k] = len(s.keysList)
	s.keysList = append(s.keysList, k)

	if s.ost != nil {
		s.ost.Insert(k)
	}
}

// removeKeyLocked removes k from tracking indexes with O(1) swap-delete.
// Precondition: s.mu must be held by caller.
func (s *MemoryStore) removeKeyLocked(k string) {
	if !s.trackKeys {
		return
	}

	idx, exists := s.keysMap[k]
	if !exists {
		return
	}

	// If the slice is empty or idx is stale, drop the stale map entry
	// and clean up the OST; do not attempt to slice [: -1].
	if len(s.keysList) == 0 || idx < 0 || idx >= len(s.keysList) {
		delete(s.keysMap, k)

		if s.ost != nil {
			s.ost.Delete(k)
		}

		return
	}

	last := len(s.keysList) - 1

	// O(1) swap-delete to keep keysList contiguous.
	if idx != last {
		moved := s.keysList[last]

		s.keysList[idx] = moved
		s.keysMap[moved] = idx
	}

	// Now drop the tail.
	s.keysList = s.keysList[:last]
	delete(s.keysMap, k)

	if s.ost != nil {
		s.ost.Delete(k)
	}
}

// randomKeyTracked returns a random key using indexes.
// Precondition: s.mu is held for reading; s.trackKeys == true.
func (s *MemoryStore) randomKeyTracked(prefix string) string {
	// No prefix: O(1) from keysList.
	if prefix == "" {
		if len(s.keysList) == 0 {
			return ""
		}

		idx := rand.IntN(len(s.keysList)) //nolint:gosec // acceptable for randomized selection in tests

		return s.keysList[idx]
	}

	// With prefix: OSTree rank/select over [l, r).
	if s.ost == nil || s.ost.Len() == 0 {
		return ""
	}

	l, r := s.ost.RangeBounds(prefix)
	if r <= l {
		return ""
	}

	position := l + rand.IntN(r-l) //nolint:gosec // math/rand/v2 is safe

	k, ok := s.ost.Kth(position)
	if !ok {
		// Defensive: should not happen if l/r came from RangeBounds.
		return ""
	}

	return k
}

// randomKeyScan returns a random key by scanning the map twice.
// Precondition: s.mu is held for reading; s.trackKeys == false.
//
// First pass counts matching keys; second pass selects the target-th match.
// We do this to keep memory usage minimal without building a temporary slice.
func (s *MemoryStore) randomKeyScan(prefix string) string {
	// First pass: count matches.
	matchCount := 0

	for k := range s.container {
		if prefix != "" && !strings.HasPrefix(k, prefix) {
			continue
		}

		matchCount++
	}

	if matchCount == 0 {
		return ""
	}

	// Second pass: pick the target-th match.
	target := rand.IntN(matchCount) //nolint:gosec // math/rand/v2 is safe

	for k := range s.container {
		if prefix != "" && !strings.HasPrefix(k, prefix) {
			continue
		}

		if target == 0 {
			return k
		}

		target--
	}

	// Unreachable if matchCount > 0.
	return ""
}
