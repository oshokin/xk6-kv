package store

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
