package store

import (
	"math/rand/v2"
	"sort"
	"strings"
)

// randomKeyMaxAttempts bounds how many times we retry selecting a random key
// before giving up due to concurrent churn.
// Retries handle cases where keys are deleted between counting and selection.
const randomKeyMaxAttempts = 8

// keyFromShardRangesLinearCutoff selects linear lookup for small shard range sets.
// For short slices, a tight loop is typically faster than binary search overhead.
const keyFromShardRangesLinearCutoff = 32

// Keep shard ranges as values ([]shardRandomRange), not pointers ([]*shardRandomRange):
// ranges are small immutable snapshots built per call, contiguous layout improves
// cache locality, and this avoids per-range heap allocations and pointer chasing.
// When a pointer is needed, we take it from the slice element directly.
type shardRandomRange struct {
	shard      *memoryShard
	left       int
	right      int
	cumulative int
}

// RandomKey returns a random key, optionally filtered by prefix.
func (s *MemoryStore) RandomKey(prefix string) (string, error) {
	if s.trackKeys {
		return s.randomKeyTracked(prefix), nil
	}

	return s.randomKeyScan(prefix), nil
}

// RandomKeys returns random key names matching prefix.
func (s *MemoryStore) RandomKeys(prefix string, count int64, unique bool) ([]string, error) {
	if err := validateRandomKeysCount(count); err != nil {
		return nil, err
	}

	if count <= 0 {
		return []string{}, nil
	}

	if s.trackKeys {
		return s.randomKeysTracked(prefix, count, unique), nil
	}

	keys := s.collectMatchingKeysUntracked(prefix)

	return sampleKeys(keys, count, unique), nil
}

func (s *MemoryStore) randomKeysTracked(prefix string, count int64, unique bool) []string {
	s.lockAllShardReaders()
	defer s.unlockAllShardReaders()

	ranges, total := buildShardRandomRanges(s.shards, prefix)
	if total == 0 {
		return []string{}
	}

	if unique {
		return randomUniqueKeysFromShardRanges(ranges, total, count)
	}

	return randomKeysWithReplacementFromShardRanges(ranges, total, count)
}

func (s *MemoryStore) collectMatchingKeysUntracked(prefix string) []string {
	keys := make([]string, 0)

	for _, shard := range s.shards {
		shard.mu.RLock()

		for key := range shard.container {
			if prefix == "" || strings.HasPrefix(key, prefix) {
				keys = append(keys, key)
			}
		}

		shard.mu.RUnlock()
	}

	return keys
}

// buildShardRandomRanges builds immutable per-call shard snapshots for indexed
// random sampling and cumulative lookup.
// Precondition: caller holds read locks for all shards.
func buildShardRandomRanges(shards []*memoryShard, prefix string) ([]shardRandomRange, int) {
	var (
		ranges = make([]shardRandomRange, 0, len(shards))
		total  int
	)

	for _, shard := range shards {
		if shard.ost == nil || shard.ost.Len() == 0 {
			continue
		}

		left, right := 0, shard.ost.Len()
		if prefix != "" {
			left, right = shard.rangeBounds(prefix)
		}

		count := right - left
		if count <= 0 {
			continue
		}

		total += count

		ranges = append(ranges, shardRandomRange{
			shard:      shard,
			left:       left,
			right:      right,
			cumulative: total,
		})
	}

	return ranges, total
}

func randomUniqueKeysFromShardRanges(ranges []shardRandomRange, total int, count int64) []string {
	if count >= int64(total) {
		keys := make([]string, 0, total)

		for _, item := range ranges {
			for i := item.left; i < item.right; i++ {
				key, ok := item.shard.ost.Kth(i)
				if ok {
					keys = append(keys, key)
				}
			}
		}

		return shuffleKeys(keys)
	}

	offsets := sampleUniqueOffsets(total, int(count))
	keys := make([]string, 0, len(offsets))

	for _, offset := range offsets {
		key, ok := keyFromShardRanges(ranges, offset)
		if ok {
			keys = append(keys, key)
		}
	}

	return keys
}

func randomKeysWithReplacementFromShardRanges(ranges []shardRandomRange, total int, count int64) []string {
	keys := make([]string, 0, count)

	for range count {
		//nolint:gosec // math/rand/v2 is enough for non-crypto sampling.
		offset := rand.IntN(total)

		key, ok := keyFromShardRanges(ranges, offset)
		if ok {
			keys = append(keys, key)
		}
	}

	return keys
}

// keyFromShardRanges maps a global offset in [0,total) into a shard-local key.
// For small range sets it uses linear scan; for larger sets it uses cumulative
// counts with binary search.
func keyFromShardRanges(ranges []shardRandomRange, globalOffset int) (string, bool) {
	if globalOffset < 0 || len(ranges) == 0 {
		return "", false
	}

	if len(ranges) <= keyFromShardRangesLinearCutoff {
		remaining := globalOffset

		for rangeIndex := range ranges {
			selectedRange := &ranges[rangeIndex]

			rangeWidth := selectedRange.right - selectedRange.left
			if remaining >= rangeWidth {
				remaining -= rangeWidth
				continue
			}

			return selectedRange.shard.ost.Kth(selectedRange.left + remaining)
		}

		return "", false
	}

	// Use cumulative counts to map a global offset to a shard in O(log S),
	// where S is the number of matching shards.
	rangeIndex := sort.Search(len(ranges), func(index int) bool {
		return globalOffset < ranges[index].cumulative
	})
	if rangeIndex >= len(ranges) {
		return "", false
	}

	var previousCumulative int
	if rangeIndex > 0 {
		previousCumulative = ranges[rangeIndex-1].cumulative
	}

	localOffset := globalOffset - previousCumulative
	selectedRange := &ranges[rangeIndex]

	return selectedRange.shard.ost.Kth(selectedRange.left + localOffset)
}

// randomKeyTracked returns a random key from the tracked shard.
func (s *MemoryStore) randomKeyTracked(prefix string) string {
	for range randomKeyMaxAttempts {
		if key, done := s.tryRandomKeyTracked(prefix); key != "" || done {
			return key
		}
	}

	key, _ := s.tryRandomKeyTracked(prefix)

	return key
}

// randomKeyScan returns a random key from shards without OST indexes.
func (s *MemoryStore) randomKeyScan(prefix string) string {
	for range randomKeyMaxAttempts {
		if key, done := s.tryRandomKeyScan(prefix); key != "" || done {
			return key
		}
	}

	key, _ := s.tryRandomKeyScan(prefix)

	return key
}

// tryRandomKeyTracked attempts to pick a random key when OST indexes are present.
// It returns the key and a boolean indicating whether no further retries are needed
// (either because a key was found or there are currently no matches).
func (s *MemoryStore) tryRandomKeyTracked(prefix string) (string, bool) {
	if prefix == "" {
		return s.tryRandomKeyTrackedNoPrefix()
	}

	return s.tryRandomKeyTrackedWithPrefix(prefix)
}

// tryRandomKeyTrackedNoPrefix picks a random key from all shards without prefix filtering.
func (s *MemoryStore) tryRandomKeyTrackedNoPrefix() (string, bool) {
	var total int

	for _, shard := range s.shards {
		shard.mu.RLock()
		total += len(shard.keysList)
		shard.mu.RUnlock()
	}

	if total == 0 {
		return "", true
	}

	// Select random index across all shards.
	idx := rand.IntN(total) //nolint:gosec // math/rand/v2 is enough for our use case.

	// Find which shard contains the selected index by subtracting shard sizes.
	for _, shard := range s.shards {
		shard.mu.RLock()
		length := len(shard.keysList)

		if idx < length {
			// length > 0 is guaranteed since idx < length and idx >= 0.
			key := shard.keysList[idx]
			shard.mu.RUnlock()

			return key, key != ""
		}

		shard.mu.RUnlock()

		// Adjust index for next shard by subtracting current shard's size.
		idx -= length
	}

	return "", false
}

// tryRandomKeyTrackedWithPrefix picks a random key matching the prefix using OST indexes.
func (s *MemoryStore) tryRandomKeyTrackedWithPrefix(prefix string) (string, bool) {
	counts, totalMatch := s.countPrefixMatchesPerShard(prefix)

	if totalMatch == 0 {
		return "", true
	}

	idx := rand.IntN(totalMatch) //nolint:gosec // math/rand/v2 is enough for our use case.

	return s.selectKeyFromShardByIndex(prefix, counts, idx)
}

// countPrefixMatchesPerShard counts keys matching prefix in each shard using OST range bounds.
func (s *MemoryStore) countPrefixMatchesPerShard(prefix string) ([]int, int) {
	counts := make([]int, len(s.shards))

	var totalMatch int

	for i, shard := range s.shards {
		shard.mu.RLock()

		if shard.ost != nil {
			l, r := shard.rangeBounds(prefix)
			counts[i] = r - l
			totalMatch += counts[i]
		}

		shard.mu.RUnlock()
	}

	return counts, totalMatch
}

// selectKeyFromShardByIndex selects a key at the given global index across all shards.
func (s *MemoryStore) selectKeyFromShardByIndex(prefix string, counts []int, idx int) (string, bool) {
	for i, count := range counts {
		if idx >= count {
			idx -= count

			continue
		}

		shard := s.shards[i]
		shard.mu.RLock()
		l, r := shard.rangeBounds(prefix)
		currentCount := r - l

		// Verify shard still has enough keys (may have changed due to concurrent deletes).
		if currentCount == 0 || idx >= currentCount {
			shard.mu.RUnlock()

			return "", false
		}

		// Convert global index to shard-local index by adding range start offset.
		key, ok := shard.ost.Kth(l + idx)
		shard.mu.RUnlock()

		if ok {
			return key, true
		}

		return "", false
	}

	return "", false
}

// tryRandomKeyScan attempts to pick a random key without relying on OST indexes.
// The boolean return follows the same semantics as tryRandomKeyTracked.
func (s *MemoryStore) tryRandomKeyScan(prefix string) (string, bool) {
	if prefix == "" {
		return s.tryRandomKeyScanNoPrefix()
	}

	return s.tryRandomKeyScanWithPrefix(prefix)
}

// tryRandomKeyScanNoPrefix selects a random key without prefix filtering by
// first choosing a shard proportionally to its key count and then sampling
// a random entry inside that shard.
func (s *MemoryStore) tryRandomKeyScanNoPrefix() (string, bool) {
	if len(s.shards) == 0 {
		return "", true
	}

	var (
		counts = make([]int, len(s.shards))
		total  int
	)

	for i, shard := range s.shards {
		count := shard.entryCount()
		if count == 0 {
			continue
		}

		counts[i] = count
		total += count
	}

	if total == 0 {
		return "", true
	}

	target := rand.IntN(total) //nolint:gosec // math/rand/v2 is enough for our use case.

	for i, count := range counts {
		if count == 0 {
			continue
		}

		if target < count {
			if key, ok := s.shards[i].getRandomKey(target); ok {
				return key, true
			}

			// Shard changed between counting and sampling; retry.
			return "", false
		}

		target -= count
	}

	return "", false
}

// tryRandomKeyScanWithPrefix performs a single-pass reservoir sample over all
// shards applying the requested prefix.
func (s *MemoryStore) tryRandomKeyScanWithPrefix(prefix string) (string, bool) {
	var (
		selected string
		seen     int
	)

	for _, shard := range s.shards {
		shard.mu.RLock()

		for key := range shard.container {
			if !strings.HasPrefix(key, prefix) {
				continue
			}

			seen++

			// Reservoir sampling: each key has 1/seen probability of being selected.
			// This ensures uniform distribution without knowing total count upfront.
			if rand.IntN(seen) == 0 { //nolint:gosec // math/rand/v2 is enough for our use case.
				selected = key
			}
		}

		shard.mu.RUnlock()
	}

	if seen == 0 {
		return "", true
	}

	return selected, true
}
