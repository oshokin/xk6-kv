package store

import (
	"errors"
	"fmt"
)

// PopRandom claims one random free matching entry, deserializes its value, then
// completes that claim with DeleteKey=true.
func (s *SerializedStore) PopRandom(prefix string) (*Entry, error) {
	claim, err := s.store.ClaimRandom(&ClaimOptions{
		Prefix: prefix,
		Owner:  popRandomClaimOwner,
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
		releaseErr := s.releaseClaimsBestEffort([]*EntryClaim{claim})
		if releaseErr != nil {
			return nil, errors.Join(err, releaseErr)
		}

		return nil, err
	}

	if !completed {
		completionErr := fmt.Errorf("%w: popRandom", ErrClaimCompletionFailed)

		releaseErr := s.releaseClaimsBestEffort([]*EntryClaim{claim})
		if releaseErr != nil {
			return nil, errors.Join(completionErr, releaseErr)
		}

		return nil, completionErr
	}

	return &Entry{
		Key:   claim.Entry.Key,
		Value: decoded,
	}, nil
}

// PopRandomMany claims up to count random free matching entries, deserializes all
// values, then completes each claim with DeleteKey=true.
//
// Completed deletes are not rolled back if a later completion fails.
func (s *SerializedStore) PopRandomMany(prefix string, count int64) ([]*Entry, error) {
	claims, err := s.ClaimRandomMany(&ClaimManyOptions{
		Prefix: prefix,
		Count:  count,
		Owner:  popRandomClaimOwner,
		TTLMs:  DefaultClaimTTLMs,
	})
	if err != nil {
		return nil, err
	}

	entries := make([]*Entry, 0, len(claims))

	for i, claim := range claims {
		completed, err := s.store.CompleteClaim(claim.Ref(), &CompleteClaimOptions{DeleteKey: true})
		if err != nil {
			releaseErr := s.releaseClaimsBestEffort(claims[i:])
			if releaseErr != nil {
				return nil, errors.Join(err, releaseErr)
			}

			return nil, err
		}

		if !completed {
			completionErr := fmt.Errorf("%w: popRandomMany", ErrClaimCompletionFailed)

			releaseErr := s.releaseClaimsBestEffort(claims[i:])
			if releaseErr != nil {
				return nil, errors.Join(completionErr, releaseErr)
			}

			return nil, completionErr
		}

		entries = append(entries, &Entry{
			Key:   claim.Entry.Key,
			Value: claim.Entry.Value,
		})
	}

	return entries, nil
}

// ClaimRandom leases one random free matching entry and deserializes its value.
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

// ClaimKey leases one specific free key and deserializes its value.
func (s *SerializedStore) ClaimKey(key string, opts *ClaimOptions) (*EntryClaim, error) {
	claim, err := s.store.ClaimKey(key, opts)
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
			return nil, fmt.Errorf("%w: claimKey release failed after decode error", err)
		}

		return nil, err
	}

	claim.Entry.Value = decoded

	return claim, nil
}

// ClaimRandomMany leases up to Count random free entries from one store call and
// deserializes values.
func (s *SerializedStore) ClaimRandomMany(opts *ClaimManyOptions) ([]*EntryClaim, error) {
	claims, err := s.store.ClaimRandomMany(opts)
	if err != nil || len(claims) == 0 {
		return claims, err
	}

	for _, claim := range claims {
		decoded, decodeErr := s.deserializeValue(claim.Entry.Value)
		if decodeErr != nil {
			return nil, errors.Join(decodeErr, s.releaseClaimsBestEffort(claims))
		}

		claim.Entry.Value = decoded
	}

	return claims, nil
}

// releaseClaimsBestEffort releases claims best effort.
func (s *SerializedStore) releaseClaimsBestEffort(claims []*EntryClaim) error {
	var joined error

	for _, claim := range claims {
		if claim == nil {
			continue
		}

		released, err := s.store.ReleaseClaim(claim.Ref())
		if err != nil {
			joined = errors.Join(joined, err)
			continue
		}

		if !released {
			joined = errors.Join(joined, fmt.Errorf("%w: releaseClaim", ErrClaimCompletionFailed))
		}
	}

	return joined
}

// ReleaseClaim delegates claim release to the underlying store.
func (s *SerializedStore) ReleaseClaim(ref *ClaimRef) (bool, error) {
	return s.store.ReleaseClaim(ref)
}

// CompleteClaim delegates claim completion to the underlying store.
func (s *SerializedStore) CompleteClaim(ref *ClaimRef, opts *CompleteClaimOptions) (bool, error) {
	return s.store.CompleteClaim(ref, opts)
}

// RenewClaim delegates claim renewal to the underlying store.
func (s *SerializedStore) RenewClaim(ref *ClaimRef, opts *RenewClaimOptions) (bool, error) {
	return s.store.RenewClaim(ref, opts)
}
