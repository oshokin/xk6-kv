package store

import (
	"slices"
	"strings"
	"time"
)

// ClaimForOwner returns one sticky live claim for an exact (prefix, owner)
// pair or allocates a new random claim when the previous binding is stale.
func (s *MemoryStore) ClaimForOwner(opts *ClaimOptions) (*EntryClaim, error) {
	if opts == nil {
		return nil, validateClaimForOwnerOptions(nil)
	}

	normalized := normalizeClaimOptions(opts)
	if err := validateClaimForOwnerOptions(normalized); err != nil {
		return nil, err
	}

	return claimForOwnerWithRegistry(
		s.ownerBindings,
		normalized,
		s.lookupMemoryOwnerBoundClaim,
		s.ClaimRandom,
	)
}

func (s *MemoryStore) lookupMemoryOwnerBoundClaim(
	ref *ClaimRef,
	opts *ClaimOptions,
) (*EntryClaim, bool, error) {
	if !isValidClaimRef(ref) {
		return nil, false, nil
	}

	// ClaimForOwner is an allocation API. Even sticky reuse must respect mutation
	// blocking used by restore and related lifecycle flows.
	release, err := s.guardMutation()
	if err != nil {
		return nil, false, err
	}
	defer release()

	if opts.Prefix != "" && !strings.HasPrefix(ref.Key, opts.Prefix) {
		return nil, false, nil
	}

	now := time.Now().UnixMilli()
	shard := s.getShardByKey(ref.Key)

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	record, exists := shard.claims[ref.Key]
	if !exists || record == nil {
		return nil, false, nil
	}

	if record.ExpiresAt <= now {
		return nil, false, nil
	}

	if record.ID != ref.ID || record.Token != ref.Token {
		return nil, false, nil
	}

	if record.Owner != opts.Owner {
		return nil, false, nil
	}

	value, exists := shard.container[ref.Key]
	if !exists {
		return nil, false, nil
	}

	return &EntryClaim{
		ID:  record.ID,
		Key: ref.Key,
		Entry: Entry{
			Key:   ref.Key,
			Value: slices.Clone(value),
		},
		Owner:     record.Owner,
		Token:     record.Token,
		ExpiresAt: record.ExpiresAt,
	}, true, nil
}
