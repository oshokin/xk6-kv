package store

import (
	"strings"
	"time"
)

// ClaimNext leases the lexicographically smallest currently free matching key.
func (s *MemoryStore) ClaimNext(opts *ClaimOptions) (*EntryClaim, error) {
	release, err := s.guardMutation()
	if err != nil {
		return nil, err
	}
	defer release()

	normalized := normalizeClaimOptions(opts)
	if err := validateClaimOptions(normalized); err != nil {
		return nil, err
	}

	// Only ordered allocations need to serialize with one another.
	// Other claim/mutation APIs remain shard-concurrent.
	s.claimNextMu.Lock()
	defer s.claimNextMu.Unlock()

	for {
		now := time.Now().UnixMilli()

		key := s.nextClaimableMemoryKey(normalized.Prefix, now)
		if key == "" {
			return nil, nil //nolint:nilnil // exhausted pool is a normal allocation result.
		}

		claim, claimed := s.tryClaimMemoryKey(key, normalized, now)
		if claimed {
			return claim, nil
		}

		// Another non-ClaimNext mutation may have changed this candidate
		// after discovery but before the shard write lock was acquired.
		//
		// Retry from the current queue head.
	}
}

// nextClaimableMemoryKey finds the lexicographically smallest claimable key across shards.
func (s *MemoryStore) nextClaimableMemoryKey(prefix string, now int64) string {
	var best string

	for _, shard := range s.shards {
		shard.mu.RLock()
		candidate := nextClaimableMemoryKeyInShardLocked(shard, prefix, now)
		shard.mu.RUnlock()

		if candidate == "" {
			continue
		}

		if best == "" || candidate < best {
			best = candidate
		}
	}

	return best
}

// nextClaimableMemoryKeyInShardLocked finds the lexicographically smallest claimable key in one shard.
//
// Caller must hold shard.mu for reading or writing.
func nextClaimableMemoryKeyInShardLocked(shard *memoryShard, prefix string, now int64) string {
	if shard.ost != nil {
		return nextTrackedMemoryClaimKeyLocked(shard, prefix, now)
	}

	return nextUntrackedMemoryClaimKeyLocked(shard, prefix, now)
}

// nextTrackedMemoryClaimKeyLocked returns the lexicographically smallest claimable key in a tracked shard.
//
// Caller must hold shard.mu for reading or writing.
func nextTrackedMemoryClaimKeyLocked(shard *memoryShard, prefix string, now int64) string {
	if shard.ost == nil {
		return ""
	}

	var candidate string

	shard.ost.WalkMetaPrefix(prefix, func(key string, _ emptyOSTMeta) bool {
		// Be defensive about index-vs-container consistency.
		if _, exists := shard.container[key]; !exists {
			return true
		}

		record := shard.claims[key]
		if record != nil && record.ExpiresAt > now {
			return true
		}

		candidate = key

		// First claimable key is enough because the walk is ordered.
		return false
	})

	return candidate
}

// nextUntrackedMemoryClaimKeyLocked returns the lexicographically smallest claimable key in an untracked shard.
//
// Caller must hold shard.mu for reading or writing.
func nextUntrackedMemoryClaimKeyLocked(shard *memoryShard, prefix string, now int64) string {
	var candidate string

	for key := range shard.container {
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			continue
		}

		record := shard.claims[key]
		if record != nil && record.ExpiresAt > now {
			continue
		}

		if candidate == "" || key < candidate {
			candidate = key
		}
	}

	return candidate
}
