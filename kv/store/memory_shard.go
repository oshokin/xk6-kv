package store

import (
	"sync"

	xxhash "github.com/cespare/xxhash/v2"
)

type (
	// memoryShard encapsulates the data and optional indexes for a single shard.
	memoryShard struct {
		// container is the map of key-value pairs.
		container map[string][]byte
		// keysList is the list of keys in the shard.
		keysList []string
		// keysMap is the map of key-value pairs.
		keysMap map[string]int
		// ost is the ordered set tree of keys.
		ost *OSTree
		// mu is the mutex to protect the shard.
		mu sync.RWMutex
	}

	// shardHashStrategy is the strategy to use to hash the key to a shard.
	shardHashStrategy int

	// shardHashFunc is the function to use to hash the key to a shard.
	shardHashFunc func(string) uint64
)

// We need these constants to keep the fallback hash path deterministic
// and identical to the canonical algorithm while staying allocation-free.
// These match the FNV-1a specification exactly to ensure consistent hashing.
const (
	// fnv1aOffset64 is the standard 64-bit FNV-1a offset basis.
	// This is the initial hash value before processing any input bytes.
	fnv1aOffset64 = 1469598103934665603
	// fnv1aPrime64 is the standard 64-bit FNV-1a prime.
	// Used as the multiplier in each iteration of the hash computation.
	fnv1aPrime64 = 1099511628211
)

const (
	// Simple byte-sum is dangerously weak for sharding.
	shardHashSumBytes shardHashStrategy = iota
	// xxhash is nearly as fast while keeping the distribution uniform.
	shardHashXXHash
	// FNV is used in NATS but it's slower than xxhash.
	shardHashFNV

	// Simple byte-sum is dangerously weak for sharding.
	// Because it only depends on the multiset of characters (not order),
	// countless distinct keys collapse to the same checksum,
	// so collision probability spikes and shards skew badly as keyspace grows.
	// xxhash is nearly as fast while keeping the distribution uniform.
	defaultShardHashStrategy = shardHashXXHash
)

// selectShardHashFunc selects the function to use to hash the key to a shard.
func selectShardHashFunc(strategy shardHashStrategy) shardHashFunc {
	switch strategy {
	case shardHashSumBytes:
		return sumBytesShardHash
	case shardHashFNV:
		return fnvShardHash
	case shardHashXXHash:
		return xxhashShardHash
	default:
		return xxhashShardHash
	}
}

// sumBytesShardHash is the function to use to hash the key to a shard using sum of bytes.
func sumBytesShardHash(key string) uint64 {
	var sum uint64

	for i := 0; i < len(key); i++ {
		sum += uint64(key[i])
	}

	return sum
}

// xxhashShardHash is the function to use to hash the key to a shard using xxhash.
func xxhashShardHash(key string) uint64 {
	return xxhash.Sum64String(key)
}

// fnvShardHash is the function to use to hash the key to a shard using FNV-1a.
func fnvShardHash(key string) uint64 {
	hash := uint64(fnv1aOffset64)

	for i := 0; i < len(key); i++ {
		hash ^= uint64(key[i])
		hash *= fnv1aPrime64
	}

	return hash
}

// getShardByKey gets the shard by the key.
func (s *MemoryStore) getShardByKey(key string) *memoryShard {
	return s.shards[s.hashKey(key)]
}

// hashKey hashes the key to a shard.
func (s *MemoryStore) hashKey(key string) int {
	if s.shardCount == 1 {
		return 0
	}

	hashFn := s.hashFn
	if hashFn == nil {
		hashFn = selectShardHashFunc(defaultShardHashStrategy)
	}

	//nolint:gosec // shardCount is always >= 1, see NewMemoryStore.
	return int(hashFn(key) % uint64(s.shardCount))
}

// lockAllShardReaders locks all the shard readers.
func (s *MemoryStore) lockAllShardReaders() {
	for i := 0; i < s.shardCount; i++ {
		s.shards[i].mu.RLock()
	}
}

// unlockAllShardReaders unlocks all the shard readers.
// Unlocks in reverse order to match lock order (best practice for nested locks).
func (s *MemoryStore) unlockAllShardReaders() {
	for i := s.shardCount - 1; i >= 0; i-- {
		s.shards[i].mu.RUnlock()
	}
}

// clearAllShardsUnsafe clears all the shards.
func (s *MemoryStore) clearAllShardsUnsafe() {
	for _, shard := range s.shards {
		shard.mu.Lock()
		shard.clearLocked(s.trackKeys)
		shard.mu.Unlock()
	}
}

// entryCount safely reads the number of keys in a shard.
func (sh *memoryShard) entryCount() int {
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	return len(sh.container)
}

// clearLocked resets shard data structures.
// Caller must hold shard.mu.
func (sh *memoryShard) clearLocked(trackKeys bool) {
	if sh.container == nil {
		sh.container = make(map[string][]byte)
	} else {
		clear(sh.container)
	}

	if !trackKeys {
		sh.keysList = nil
		sh.keysMap = nil
		sh.ost = nil

		return
	}

	sh.clearTrackingLocked()
}

// clearTrackingLocked resets key tracking structures.
// Caller must hold shard.mu.
func (sh *memoryShard) clearTrackingLocked() {
	if sh.keysList != nil {
		sh.keysList = sh.keysList[:0]
	} else {
		sh.keysList = make([]string, 0)
	}

	if sh.keysMap == nil {
		sh.keysMap = make(map[string]int)
	} else {
		clear(sh.keysMap)
	}

	if sh.ost != nil {
		sh.ost.root = nil
	} else {
		sh.ost = NewOSTree()
	}
}

// addKeyTrackingLocked adds the key to the key tracking.
func (sh *memoryShard) addKeyTrackingLocked(key string) {
	if sh.keysMap == nil {
		return
	}

	if _, exists := sh.keysMap[key]; exists {
		return
	}

	sh.keysMap[key] = len(sh.keysList)
	sh.keysList = append(sh.keysList, key)

	if sh.ost != nil {
		sh.ost.Insert(key)
	}
}

// removeKeyTrackingLocked removes the key from the key tracking.
func (sh *memoryShard) removeKeyTrackingLocked(key string) {
	if sh.keysMap == nil {
		return
	}

	idx, exists := sh.keysMap[key]
	if !exists {
		return
	}

	if len(sh.keysList) == 0 || idx < 0 || idx >= len(sh.keysList) {
		delete(sh.keysMap, key)

		if sh.ost != nil {
			sh.ost.Delete(key)
		}

		return
	}

	// Swap-with-last deletion: O(1) instead of O(n) array shift.
	last := len(sh.keysList) - 1
	if idx != last {
		moved := sh.keysList[last]
		sh.keysList[idx] = moved
		// Update map entry for the moved key to point to its new index.
		sh.keysMap[moved] = idx
	}

	sh.keysList = sh.keysList[:last]
	delete(sh.keysMap, key)

	if sh.ost != nil {
		sh.ost.Delete(key)
	}
}

// rangeBounds returns the bounds of the range of keys in the shard.
// If the prefix is empty, it returns the bounds of the range of keys in the shard.
// If the prefix is not empty, it returns the bounds of the range of keys in the shard
// that have the prefix.
func (sh *memoryShard) rangeBounds(prefix string) (int, int) {
	if sh.ost == nil {
		return 0, 0
	}

	if prefix == "" {
		return 0, sh.ost.Len()
	}

	return sh.ost.RangeBounds(prefix)
}

// startIndex returns the index of the key to start iterating from.
func (sh *memoryShard) startIndex(l, r int, afterKey string) int {
	if afterKey == "" {
		return l
	}

	// Start from rank of afterKey, but ensure we're within the prefix range.
	idx := max(sh.ost.Rank(afterKey), l)
	// If the key at idx is <= afterKey, we need to advance to the next key.
	// This handles the "strictly after" semantics required by Scan.
	if idx < r {
		if keyAtIdx, ok := sh.ost.Kth(idx); ok && keyAtIdx <= afterKey {
			idx++
		}
	}

	if idx > r {
		return r
	}

	return idx
}

// getRandomKeyFromShard returns the key whose zero-based index matches target.
func (sh *memoryShard) getRandomKey(target int) (string, bool) {
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	if target < 0 || target >= len(sh.container) {
		return "", false
	}

	var idx int
	for key := range sh.container {
		if idx == target {
			return key, true
		}

		idx++
	}

	return "", false
}
