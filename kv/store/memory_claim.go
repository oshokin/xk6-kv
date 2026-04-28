package store

import (
	"slices"
	"strconv"
	"strings"
	"time"
)

type memoryClaimRecord struct {
	ID        string
	Owner     string
	Token     int64
	ExpiresAt int64
}

// PopRandom atomically selects and removes a random free matching entry.
// If there are no free matching entries, it returns nil, nil.
func (s *MemoryStore) PopRandom(prefix string) (*Entry, error) {
	release, err := s.guardMutation()
	if err != nil {
		return nil, err
	}
	defer release()

	now := time.Now().UnixMilli()

	for range randomKeyMaxAttempts {
		key, err := s.RandomKey(prefix)
		if err != nil {
			return nil, err
		}

		if key == "" {
			//nolint:nilnil // empty pool is expected and represented as nil result, nil error.
			return nil, nil
		}

		shard := s.getShardByKey(key)
		shard.mu.Lock()

		value, exists := shard.container[key]
		if !exists {
			shard.mu.Unlock()

			continue
		}

		if !claimAvailableInShardLocked(shard, key, now) {
			shard.mu.Unlock()

			continue
		}

		entry := &Entry{
			Key:   key,
			Value: slices.Clone(value),
		}

		delete(shard.container, key)
		shard.removeKeyTrackingLocked(key)
		delete(shard.claims, key)
		shard.mu.Unlock()

		return entry, nil
	}

	return s.popRandomByScan(prefix, now), nil
}

// ClaimRandom atomically leases a random matching entry.
// If no free (unclaimed) matching entry exists, it returns nil, nil.
func (s *MemoryStore) ClaimRandom(opts *ClaimOptions) (*EntryClaim, error) {
	release, err := s.guardMutation()
	if err != nil {
		return nil, err
	}
	defer release()

	normalized := normalizeClaimOptions(opts)
	now := time.Now().UnixMilli()

	for range randomKeyMaxAttempts {
		key, err := s.RandomKey(normalized.Prefix)
		if err != nil {
			return nil, err
		}

		if key == "" {
			return nil, nil //nolint:nilnil // No free key is a normal state, not a technical failure.
		}

		claim, claimed := s.tryClaimMemoryKey(key, normalized, now)
		if claimed {
			return claim, nil
		}
	}

	return s.claimRandomByScan(normalized, now), nil
}

// ReleaseClaim releases a live claim and makes the key claimable again.
func (s *MemoryStore) ReleaseClaim(ref *ClaimRef) (bool, error) {
	release, err := s.guardMutation()
	if err != nil {
		return false, err
	}
	defer release()

	if !isValidClaimRef(ref) {
		return false, nil
	}

	now := time.Now().UnixMilli()
	shard := s.getShardByKey(ref.Key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	record, exists := shard.claims[ref.Key]
	if !exists {
		return false, nil
	}

	if record.ExpiresAt <= now {
		delete(shard.claims, ref.Key)

		return false, nil
	}

	if _, keyExists := shard.container[ref.Key]; !keyExists {
		delete(shard.claims, ref.Key)

		return false, nil
	}

	if record.ID != ref.ID || record.Token != ref.Token {
		return false, nil
	}

	delete(shard.claims, ref.Key)

	return true, nil
}

// CompleteClaim completes a live claim and optionally deletes the underlying key.
func (s *MemoryStore) CompleteClaim(ref *ClaimRef, opts *CompleteClaimOptions) (bool, error) {
	release, err := s.guardMutation()
	if err != nil {
		return false, err
	}
	defer release()

	if !isValidClaimRef(ref) {
		return false, nil
	}

	normalized := normalizeCompleteClaimOptions(opts)
	now := time.Now().UnixMilli()

	shard := s.getShardByKey(ref.Key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	record, exists := shard.claims[ref.Key]
	if !exists {
		return false, nil
	}

	if record.ExpiresAt <= now {
		delete(shard.claims, ref.Key)

		return false, nil
	}

	if record.ID != ref.ID || record.Token != ref.Token {
		return false, nil
	}

	if _, keyExists := shard.container[ref.Key]; !keyExists {
		delete(shard.claims, ref.Key)

		return false, nil
	}

	delete(shard.claims, ref.Key)

	if normalized.DeleteKey {
		delete(shard.container, ref.Key)
		shard.removeKeyTrackingLocked(ref.Key)
	}

	return true, nil
}

func (s *MemoryStore) popRandomByScan(prefix string, now int64) *Entry {
	for _, shard := range s.shards {
		shard.mu.Lock()

		for key, value := range shard.container {
			if prefix != "" && !strings.HasPrefix(key, prefix) {
				continue
			}

			if !claimAvailableInShardLocked(shard, key, now) {
				continue
			}

			entry := &Entry{
				Key:   key,
				Value: slices.Clone(value),
			}

			delete(shard.container, key)
			shard.removeKeyTrackingLocked(key)
			delete(shard.claims, key)
			shard.mu.Unlock()

			return entry
		}

		shard.mu.Unlock()
	}

	return nil
}

func (s *MemoryStore) claimRandomByScan(opts *ClaimOptions, now int64) *EntryClaim {
	for _, shard := range s.shards {
		shard.mu.Lock()

		for key, value := range shard.container {
			if opts.Prefix != "" && !strings.HasPrefix(key, opts.Prefix) {
				continue
			}

			if !claimAvailableInShardLocked(shard, key, now) {
				continue
			}

			claim := s.createMemoryClaimLocked(shard, key, value, opts, now)
			shard.mu.Unlock()

			return claim
		}

		shard.mu.Unlock()
	}

	return nil
}

func (s *MemoryStore) tryClaimMemoryKey(key string, opts *ClaimOptions, now int64) (*EntryClaim, bool) {
	shard := s.getShardByKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	value, exists := shard.container[key]
	if !exists {
		return nil, false
	}

	if !claimAvailableInShardLocked(shard, key, now) {
		return nil, false
	}

	return s.createMemoryClaimLocked(shard, key, value, opts, now), true
}

func (s *MemoryStore) createMemoryClaimLocked(
	shard *memoryShard,
	key string,
	value []byte,
	opts *ClaimOptions,
	now int64,
) *EntryClaim {
	token := s.claimToken.Add(1)
	expiresAt := now + opts.TTLMs
	claimID := claimIDFromToken(token)

	shard.claims[key] = &memoryClaimRecord{
		ID:        claimID,
		Owner:     opts.Owner,
		Token:     token,
		ExpiresAt: expiresAt,
	}

	return &EntryClaim{
		ID:  claimID,
		Key: key,
		Entry: Entry{
			Key:   key,
			Value: slices.Clone(value),
		},
		Owner:     opts.Owner,
		Token:     token,
		ExpiresAt: expiresAt,
	}
}

func claimAvailableInShardLocked(shard *memoryShard, key string, now int64) bool {
	record, exists := shard.claims[key]
	if !exists {
		return true
	}

	if record.ExpiresAt <= now {
		delete(shard.claims, key)

		return true
	}

	return false
}

func isValidClaimRef(ref *ClaimRef) bool {
	return ref != nil && ref.ID != "" && ref.Key != "" && ref.Token > 0
}

func claimIDFromToken(token int64) string {
	return "claim-" + strconv.FormatInt(token, 10)
}
