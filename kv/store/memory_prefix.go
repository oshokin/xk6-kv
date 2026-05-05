package store

import (
	"fmt"
	"sort"
)

// DeleteByPrefix deletes up to limit keys with the given non-empty prefix.
func (s *MemoryStore) DeleteByPrefix(prefix string, limit int64) (*DeleteByPrefixResult, error) {
	release, err := s.guardMutation()
	if err != nil {
		return nil, err
	}
	defer release()

	if prefix == "" {
		return nil, fmt.Errorf("%w: prefix must be non-empty", ErrKVOptionsInvalid)
	}

	if limit <= 0 {
		return nil, fmt.Errorf("%w: limit must be positive: %d", ErrKVOptionsInvalid, limit)
	}

	keys := s.selectKeysByPrefix(prefix, limit)
	result := &DeleteByPrefixResult{}

	for _, key := range keys {
		shard := s.getShardByKey(key)
		shard.mu.Lock()

		if _, exists := shard.container[key]; exists {
			delete(shard.container, key)
			shard.removeKeyTrackingLocked(key)
			delete(shard.claims, key)

			result.Deleted++
		}

		shard.mu.Unlock()
	}

	remaining, err := s.Count(prefix)
	if err != nil {
		return nil, err
	}

	result.Done = remaining == 0

	return result, nil
}

func (s *MemoryStore) selectKeysByPrefix(prefix string, limit int64) []string {
	var keys []string

	if s.trackKeys {
		keys = s.listKeysWithTracking(prefix)
	} else {
		keys = s.listKeysWithoutTracking(prefix)
	}

	sort.Strings(keys)

	if limit > 0 && int64(len(keys)) > limit {
		keys = keys[:limit]
	}

	if keys == nil {
		return []string{}
	}

	return keys
}
