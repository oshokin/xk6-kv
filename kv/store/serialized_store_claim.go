package store

import (
	"errors"
	"fmt"
)

// serializedPopRandomClaimOwner is the owner of the pop random claim.
// It is used to identify the owner of the pop random claim.
const serializedPopRandomClaimOwner = "__xk6_kv_pop_random"

// PopRandom atomically selects and removes a random matching entry and deserializes its value.
func (s *SerializedStore) PopRandom(prefix string) (*Entry, error) {
	claim, err := s.store.ClaimRandom(&ClaimOptions{
		Prefix: prefix,
		Owner:  serializedPopRandomClaimOwner,
		TTLMs:  DefaultClaimTTLMs,
	})
	if err != nil || claim == nil {
		return nil, err
	}

	decoded, err := s.deserializeValue(claim.Entry.Value)
	if err != nil {
		released, releaseErr := s.store.ReleaseClaim(claim.Ref())
		if releaseErr != nil {
			return nil, errors.Join(err, releaseErr)
		}

		if !released {
			return nil, fmt.Errorf("%w: popRandom claim release failed after decode error", err)
		}

		return nil, err
	}

	completed, err := s.store.CompleteClaim(
		claim.Ref(),
		&CompleteClaimOptions{DeleteKey: true},
	)
	if err != nil {
		return nil, err
	}

	if !completed {
		return nil, fmt.Errorf("%w: popRandom", ErrClaimCompletionFailed)
	}

	return &Entry{
		Key:   claim.Entry.Key,
		Value: decoded,
	}, nil
}

// ClaimRandom atomically leases a random matching entry and deserializes its value.
func (s *SerializedStore) ClaimRandom(opts *ClaimOptions) (*EntryClaim, error) {
	claim, err := s.store.ClaimRandom(opts)
	if err != nil || claim == nil {
		return claim, err
	}

	decoded, err := s.deserializeValue(claim.Entry.Value)
	if err != nil {
		released, releaseErr := s.store.ReleaseClaim(claim.Ref())
		if releaseErr != nil {
			return nil, errors.Join(err, releaseErr)
		}

		if !released {
			return nil, fmt.Errorf("%w: claim release failed after decode error", err)
		}

		return nil, err
	}

	claim.Entry.Value = decoded

	return claim, nil
}

// ReleaseClaim delegates claim release to the underlying store.
func (s *SerializedStore) ReleaseClaim(ref *ClaimRef) (bool, error) {
	return s.store.ReleaseClaim(ref)
}

// CompleteClaim delegates claim completion to the underlying store.
func (s *SerializedStore) CompleteClaim(ref *ClaimRef, opts *CompleteClaimOptions) (bool, error) {
	return s.store.CompleteClaim(ref, opts)
}
