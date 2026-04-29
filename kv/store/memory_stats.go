package store

import "time"

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
