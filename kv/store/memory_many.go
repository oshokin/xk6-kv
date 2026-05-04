package store

import (
	"fmt"
	"slices"
)

// GetMany returns entries in the same order as keys.
// Missing keys are represented as nil entries.
func (s *MemoryStore) GetMany(keys []string) ([]*Entry, error) {
	result := make([]*Entry, len(keys))

	for i, key := range keys {
		shard := s.getShardByKey(key)

		shard.mu.RLock()

		value, exists := shard.container[key]
		if exists {
			result[i] = &Entry{
				Key:   key,
				Value: slices.Clone(value),
			}
		}

		shard.mu.RUnlock()
	}

	return result, nil
}

// SetMany stores all entries in one logical batch.
//
// We normalize every value before mutating any shard so unsupported values fail
// fast without partial writes.
func (s *MemoryStore) SetMany(entries []Entry) (int64, error) {
	release, err := s.guardMutation()
	if err != nil {
		return 0, err
	}
	defer release()

	if len(entries) == 0 {
		return 0, nil
	}

	normalizedKeys := make([]string, len(entries))

	normalizedValues := make([][]byte, len(entries))
	for i := range entries {
		if entries[i].Key == "" {
			return 0, fmt.Errorf("%w: entries[%d]", ErrKeyEmpty, i)
		}

		valueBytes, normalizeErr := normalizeToBytes(entries[i].Value)
		if normalizeErr != nil {
			return 0, normalizeErr
		}

		normalizedKeys[i] = entries[i].Key
		normalizedValues[i] = valueBytes
	}

	for i := range normalizedKeys {
		key := normalizedKeys[i]
		valueBytes := normalizedValues[i]

		shard := s.getShardByKey(key)
		shard.mu.Lock()

		_, existed := shard.container[key]

		shard.container[key] = valueBytes
		if !existed {
			shard.addKeyTrackingLocked(key)
		}

		shard.mu.Unlock()
	}

	return int64(len(normalizedKeys)), nil
}
