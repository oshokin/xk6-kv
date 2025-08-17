package store

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
)

// MemoryStore is an in-memory key-value store.
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

// Get returns the value for a given key.
func (s *MemoryStore) Get(key string) (any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.container[key]
	if !ok {
		return nil, fmt.Errorf("key %s not found", key)
	}

	// Return the raw bytes - serialization will be handled by the SerializedStore wrapper
	return value, nil
}

// Set sets the value for a given key.
func (s *MemoryStore) Set(key string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Convert value to bytes if it's not already
	var valueBytes []byte
	switch v := value.(type) {
	case []byte:
		valueBytes = v
	case string:
		valueBytes = []byte(v)
	default:
		return fmt.Errorf("unsupported value type for memory store: %T", value)
	}

	// Check if key already exists
	_, existed := s.container[key]
	s.container[key] = valueBytes

	if s.trackKeys && !existed {
		// Add to keysList and record index
		s.keysMap[key] = len(s.keysList)
		s.keysList = append(s.keysList, key)

		// Update OST for prefix-based operations
		if s.ost != nil {
			s.ost.Insert(key)
		}
	}

	return nil
}

// Delete deletes the value for a given key.
func (s *MemoryStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, existed := s.container[key]; !existed {
		return nil
	}

	delete(s.container, key)

	if s.trackKeys {
		if idx, exists := s.keysMap[key]; exists {
			lastIndex := len(s.keysList) - 1
			lastKey := s.keysList[lastIndex]

			// Swap with last element for O(1) deletion
			if idx != lastIndex {
				s.keysList[idx] = lastKey
				s.keysMap[lastKey] = idx
			}

			// Swap with last element for O(1) deletion
			s.keysList = s.keysList[:lastIndex]
			delete(s.keysMap, key)
		}

		// Update OST for prefix-based operations
		if s.ost != nil {
			s.ost.Delete(key)
		}
	}

	return nil
}

// Exists checks if a given key exists.
func (s *MemoryStore) Exists(key string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.container[key]
	return ok, nil
}

// Clear clears the store.
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

// Size returns the size of the store.
func (s *MemoryStore) Size() (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return int64(len(s.container)), nil
}

// List returns key-value pairs filtered by prefix and limited by count.
// Keys are sorted lexicographically for consistent ordering.
// Note: Memory-inefficient for large datasets due to full scan and sorting.
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

	// Pre-allocate result capacity to silence `prealloc` linter and reduce reallocs.
	var (
		maxEntries = len(keys)
		// Apply limit if set
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

// RandomKey returns a random key, optionally filtered by prefix.
//
// Strategies:
// 1) trackKeys && prefix == ""  -> O(1) random from keysList
// 2) trackKeys && prefix != ""  -> O(log n) via OSTree (rank/select)
// 3) !trackKeys                 -> O(n) two-pass scan
//
// If no match exists, returns "" and nil error.
func (s *MemoryStore) RandomKey(prefix string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.trackKeys {
		return s.randomKeyTracked(prefix), nil
	}

	return s.randomKeyScan(prefix), nil
}

// randomKeyTracked assumes s.mu is already held (RLock) and s.trackKeys == true.
func (s *MemoryStore) randomKeyTracked(prefix string) string {
	// No prefix: O(1) from keysList
	if prefix == "" {
		if len(s.keysList) == 0 {
			return ""
		}
		idx := rand.IntN(len(s.keysList)) //nolint:gosec
		return s.keysList[idx]
	}

	// With prefix: rank/select in OST
	if s.ost == nil || s.ost.Len() == 0 {
		return ""
	}
	l, r := s.ost.RangeBounds(prefix)
	if r <= l {
		return ""
	}
	pos := l + rand.IntN(r-l) //nolint:gosec
	k, ok := s.ost.Kth(pos)
	if !ok {
		return ""
	}
	return k
}

// randomKeyScan assumes s.mu is already held (RLock) and s.trackKeys == false.
func (s *MemoryStore) randomKeyScan(prefix string) string {
	// First pass: count matches
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

	// Second pass: pick the target-th match
	target := rand.IntN(matchCount) //nolint:gosec
	for k := range s.container {
		if prefix != "" && !strings.HasPrefix(k, prefix) {
			continue
		}

		if target == 0 {
			return k
		}

		target--
	}

	return "" // should be unreachable
}

// RebuildKeyList reconstructs key tracking structures from container.
// Used for recovery when auxiliary structures might be out of sync.
// Note: Expensive O(n) operation - use judiciously.
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

// Close closes the store.
//
// This is a no-op for the MemoryStore.
func (s *MemoryStore) Close() error {
	return nil
}
