package store

import (
	"fmt"
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

	if limit > MaxDeleteByPrefixLimit {
		return nil, fmt.Errorf(
			"%w: limit must be less than or equal to %d: %d",
			ErrKVOptionsInvalid,
			MaxDeleteByPrefixLimit,
			limit,
		)
	}

	keys, err := s.selectKeysByPrefix(prefix, limit)
	if err != nil {
		return nil, err
	}

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

// selectKeysByPrefix returns key names matching prefix up to limit via ScanKeys.
func (s *MemoryStore) selectKeysByPrefix(prefix string, limit int64) ([]string, error) {
	page, err := s.ScanKeys(prefix, "", limit)
	if err != nil {
		return nil, err
	}

	return page.Keys, nil
}
