package store

import (
	"strings"
	"time"
)

// Stats returns a diagnostic snapshot of the in-memory store.
func (s *MemoryStore) Stats() (*StatsSnapshot, error) {
	now := time.Now().UnixMilli()

	s.lockAllShardReaders()
	defer s.unlockAllShardReaders()

	snapshot := &StatsSnapshot{
		Backend:   backendMemoryName,
		TrackKeys: s.trackKeys,
	}

	var (
		keyCount   int64
		claimsLive int64
		claimsExp  int64
	)

	indexStats := &IndexStats{
		Enabled:    s.trackKeys,
		Consistent: true,
	}

	for _, shard := range s.shards {
		shardCount := int64(len(shard.container))
		keyCount += shardCount

		for _, claim := range shard.claims {
			if claim.ExpiresAt <= now {
				claimsExp++
			} else {
				claimsLive++
			}
		}

		if s.trackKeys {
			shardKeysList := int64(len(shard.keysList))
			shardKeysMap := int64(len(shard.keysMap))

			var shardOST int64
			if shard.ost != nil {
				shardOST = int64(shard.ost.Len())
			}

			indexStats.KeysList += shardKeysList
			indexStats.KeysMap += shardKeysMap
			indexStats.OST += shardOST

			if shardKeysList != shardCount || shardKeysList != shardKeysMap || shardKeysList != shardOST {
				indexStats.Consistent = false
			}
		}
	}

	snapshot.Count = keyCount
	snapshot.Claims = ClaimStats{
		Live:    claimsLive,
		Expired: claimsExp,
	}

	if s.trackKeys {
		snapshot.Index = indexStats
	}

	return snapshot, nil
}

// AllocationStats returns a prefix-scoped allocation snapshot for memory backend.
// It is an on-demand diagnostic call and may scan matching keys.
func (s *MemoryStore) AllocationStats(prefix string) (*AllocationStats, error) {
	now := time.Now().UnixMilli()

	s.lockAllShardReaders()
	defer s.unlockAllShardReaders()

	snapshot := &AllocationStats{
		Prefix:    prefix,
		Backend:   backendMemoryName,
		TrackKeys: s.trackKeys,
	}

	if s.trackKeys {
		s.fillAllocationStatsTracked(snapshot, prefix, now)

		return snapshot, nil
	}

	for _, shard := range s.shards {
		fillAllocationStatsByScan(snapshot, shard, prefix, now)
	}

	return snapshot, nil
}

// fillAllocationStatsTracked uses trackKeys shard indexes as a fast path.
// If a shard index looks inconsistent, it falls back to container scan semantics for that shard.
func (s *MemoryStore) fillAllocationStatsTracked(snapshot *AllocationStats, prefix string, now int64) {
	for _, shard := range s.shards {
		if !isMemoryAllocationTrackingConsistent(shard) {
			fillAllocationStatsByScan(snapshot, shard, prefix, now)

			continue
		}

		left, right := shard.rangeBounds(prefix)
		if right <= left {
			continue
		}

		snapshot.Total += int64(right - left)

		for key, claim := range shard.claims {
			if claim == nil {
				continue
			}

			if prefix != "" && !strings.HasPrefix(key, prefix) {
				continue
			}

			if _, exists := shard.container[key]; !exists {
				continue
			}

			if claim.ExpiresAt <= now {
				snapshot.ClaimedExpired++

				continue
			}

			snapshot.ClaimedLive++
		}
	}

	snapshot.Claimable = max(snapshot.Total-snapshot.ClaimedLive, 0)
}

// fillAllocationStatsByScan is the baseline per-key scan path shared by trackKeys=false
// and tracked shards that fail consistency checks.
func fillAllocationStatsByScan(snapshot *AllocationStats, shard *memoryShard, prefix string, now int64) {
	for key := range shard.container {
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			continue
		}

		snapshot.Total++

		claim, hasClaim := shard.claims[key]
		if !hasClaim {
			snapshot.Claimable++

			continue
		}

		if claim.ExpiresAt <= now {
			snapshot.ClaimedExpired++
			snapshot.Claimable++

			continue
		}

		snapshot.ClaimedLive++
	}
}

// isMemoryAllocationTrackingConsistent reports whether shard index counters align.
func isMemoryAllocationTrackingConsistent(shard *memoryShard) bool {
	if shard == nil || shard.ost == nil || shard.keysMap == nil {
		return false
	}

	containerLen := len(shard.container)

	return len(shard.keysList) == containerLen &&
		len(shard.keysMap) == containerLen &&
		shard.ost.Len() == containerLen
}
