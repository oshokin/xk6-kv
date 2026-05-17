package store

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"time"
)

// memoryClaimRecord is the internal representation of a memory claim record.
type memoryClaimRecord struct {
	// ID is the unique claim identifier.
	ID string
	// Owner is an optional logical owner identifier for diagnostics.
	Owner string
	// Token is a monotonically increasing fence token for stale-holder protection.
	Token int64
	// ExpiresAt is Unix epoch milliseconds when the lease expires.
	ExpiresAt int64
}

// PopRandom claims one random free matching entry and removes it.
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

		if !s.claimAvailableInShardLocked(shard, key, now) {
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

// PopRandomMany claims up to count random free matching entries and removes each claimed key.
// Completed deletes are not rolled back if a later completion fails.
func (s *MemoryStore) PopRandomMany(prefix string, count int64) ([]*Entry, error) {
	release, err := s.guardMutation()
	if err != nil {
		return nil, err
	}
	defer release()

	if count <= 0 {
		return nil, fmt.Errorf("%w: popRandomMany count must be positive", ErrKVOptionsInvalid)
	}

	if count > MaxRandomKeysCount {
		return nil, fmt.Errorf(
			"%w: popRandomMany count must be less than or equal to %d",
			ErrKVOptionsInvalid,
			MaxRandomKeysCount,
		)
	}

	now := time.Now().UnixMilli()
	remaining := int(count)
	entries := make([]*Entry, 0, remaining)
	shardOrder := rand.Perm(len(s.shards))

	for _, shardIndex := range shardOrder {
		if remaining <= 0 {
			break
		}

		shard := s.shards[shardIndex]
		shard.mu.Lock()

		freeKeys := make([]string, 0)

		for key := range shard.container {
			if prefix != "" && !strings.HasPrefix(key, prefix) {
				continue
			}

			if !s.claimAvailableInShardLocked(shard, key, now) {
				continue
			}

			freeKeys = append(freeKeys, key)
		}

		if len(freeKeys) == 0 {
			shard.mu.Unlock()
			continue
		}

		rand.Shuffle(len(freeKeys), func(i, j int) {
			freeKeys[i], freeKeys[j] = freeKeys[j], freeKeys[i]
		})

		take := min(remaining, len(freeKeys))

		for _, key := range freeKeys[:take] {
			value, exists := shard.container[key]
			if !exists || !s.claimAvailableInShardLocked(shard, key, now) {
				continue
			}

			entries = append(entries, &Entry{
				Key:   key,
				Value: slices.Clone(value),
			})

			delete(shard.container, key)
			shard.removeKeyTrackingLocked(key)
			delete(shard.claims, key)
		}

		remaining = int(count) - len(entries)
		shard.mu.Unlock()
	}

	return entries, nil
}

// ClaimRandom leases one random free matching entry.
// If no free (unclaimed) matching entry exists, it returns nil, nil.
func (s *MemoryStore) ClaimRandom(opts *ClaimOptions) (*EntryClaim, error) {
	release, err := s.guardMutation()
	if err != nil {
		return nil, err
	}
	defer release()

	normalized := normalizeClaimOptions(opts)
	if err := validateClaimOptions(normalized); err != nil {
		return nil, err
	}

	now := time.Now().UnixMilli()

	for range randomKeyMaxAttempts {
		key, err := s.RandomKey(normalized.Prefix)
		if err != nil {
			return nil, err
		}

		if key == "" {
			//nolint:nilnil // no free key is a normal state, not a technical failure.
			return nil, nil
		}

		claim, claimed := s.tryClaimMemoryKey(key, normalized, now)
		if claimed {
			return claim, nil
		}
	}

	return s.claimRandomByScan(normalized, now), nil
}

// ClaimKey leases one specific free key.
// If the key is missing or currently live-claimed, it returns nil, nil.
func (s *MemoryStore) ClaimKey(key string, opts *ClaimOptions) (*EntryClaim, error) {
	release, err := s.guardMutation()
	if err != nil {
		return nil, err
	}
	defer release()

	if err := validateNonEmptyKey(key); err != nil {
		return nil, err
	}

	normalized := normalizeClaimOptions(opts)
	if err := validateClaimOptions(normalized); err != nil {
		return nil, err
	}

	now := time.Now().UnixMilli()

	claim, claimed := s.tryClaimMemoryKey(key, normalized, now)
	if !claimed {
		return nil, nil //nolint:nilnil // key missing or currently claimed.
	}

	return claim, nil
}

// ClaimRandomMany leases up to Count unique random free matching entries.
func (s *MemoryStore) ClaimRandomMany(opts *ClaimManyOptions) ([]*EntryClaim, error) {
	release, err := s.guardMutation()
	if err != nil {
		return nil, err
	}
	defer release()

	normalized := normalizeClaimManyOptions(opts)
	if err := validateClaimManyOptions(normalized); err != nil {
		return nil, err
	}

	remaining := int(normalized.Count)
	if remaining <= 0 {
		return []*EntryClaim{}, nil
	}

	now := time.Now().UnixMilli()
	claims := make([]*EntryClaim, 0, remaining)
	shardOrder := rand.Perm(len(s.shards))

	for _, shardIndex := range shardOrder {
		if remaining <= 0 {
			break
		}

		shard := s.shards[shardIndex]
		shard.mu.Lock()

		freeKeys := make([]string, 0)

		for key := range shard.container {
			if normalized.Prefix != "" && !strings.HasPrefix(key, normalized.Prefix) {
				continue
			}

			if !s.claimAvailableInShardLocked(shard, key, now) {
				continue
			}

			freeKeys = append(freeKeys, key)
		}

		if len(freeKeys) == 0 {
			shard.mu.Unlock()
			continue
		}

		rand.Shuffle(len(freeKeys), func(i, j int) {
			freeKeys[i], freeKeys[j] = freeKeys[j], freeKeys[i]
		})

		take := min(remaining, len(freeKeys))

		claimOpts := &ClaimOptions{
			Prefix: normalized.Prefix,
			Owner:  normalized.Owner,
			TTLMs:  normalized.TTLMs,
		}

		for _, key := range freeKeys[:take] {
			value, exists := shard.container[key]
			if !exists || !s.claimAvailableInShardLocked(shard, key, now) {
				continue
			}

			claims = append(claims, s.createMemoryClaimLocked(shard, key, value, claimOpts, now))
		}

		remaining = int(normalized.Count) - len(claims)
		shard.mu.Unlock()
	}

	return claims, nil
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

// RenewClaim extends a live claim lease without changing claim token.
func (s *MemoryStore) RenewClaim(ref *ClaimRef, opts *RenewClaimOptions) (bool, error) {
	release, err := s.guardMutation()
	if err != nil {
		return false, err
	}
	defer release()

	if !isValidClaimRef(ref) {
		return false, nil
	}

	if err := validateRenewClaimOptions(opts); err != nil {
		return false, err
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

	record.ExpiresAt = now + opts.TTLMs

	return true, nil
}

// popRandomByScan pops a random entry by scanning the shards.
func (s *MemoryStore) popRandomByScan(prefix string, now int64) *Entry {
	for _, shard := range s.shards {
		shard.mu.Lock()

		for key, value := range shard.container {
			if prefix != "" && !strings.HasPrefix(key, prefix) {
				continue
			}

			if !s.claimAvailableInShardLocked(shard, key, now) {
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

// claimRandomByScan claims a random entry by scanning the shards.
func (s *MemoryStore) claimRandomByScan(opts *ClaimOptions, now int64) *EntryClaim {
	for _, shard := range s.shards {
		shard.mu.Lock()

		for key, value := range shard.container {
			if opts.Prefix != "" && !strings.HasPrefix(key, opts.Prefix) {
				continue
			}

			if !s.claimAvailableInShardLocked(shard, key, now) {
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

// tryClaimMemoryKey tries to claim a memory key.
func (s *MemoryStore) tryClaimMemoryKey(key string, opts *ClaimOptions, now int64) (*EntryClaim, bool) {
	shard := s.getShardByKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	value, exists := shard.container[key]
	if !exists {
		return nil, false
	}

	if !s.claimAvailableInShardLocked(shard, key, now) {
		return nil, false
	}

	return s.createMemoryClaimLocked(shard, key, value, opts, now), true
}

// createMemoryClaimLocked creates a memory claim record.
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

// claimAvailableInShardLocked checks if a key is available in a shard.
func (s *MemoryStore) claimAvailableInShardLocked(shard *memoryShard, key string, now int64) bool {
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
