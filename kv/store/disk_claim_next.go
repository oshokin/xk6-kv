package store

import (
	"bytes"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

// ClaimNext leases the lexicographically smallest currently free matching key.
func (s *DiskStore) ClaimNext(opts *ClaimOptions) (*EntryClaim, error) {
	if s.trackedClaimsEnabled() {
		return s.claimNextTracked(opts)
	}

	return s.claimNextBolt(opts)
}

// claimNextTracked leases the lexicographically smallest free key from tracked indexes.
func (s *DiskStore) claimNextTracked(opts *ClaimOptions) (*EntryClaim, error) {
	release, err := s.beginOperation()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	if err := s.ensureWritable(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreWriteFailed, err)
	}

	normalized := normalizeClaimOptions(opts)
	if err := validateClaimOptions(normalized); err != nil {
		return nil, err
	}

	now := time.Now().UnixMilli()

	s.keysLock.Lock()
	defer s.keysLock.Unlock()

	s.reapExpiredTrackedClaimsLocked(now)

	return s.claimNextTrackedLocked(normalized, now)
}

func (s *DiskStore) claimNextTrackedLocked(
	normalized *ClaimOptions,
	now int64,
) (*EntryClaim, error) {
	for {
		if s.ost == nil || s.ost.SelectableLen() == 0 {
			return nil, nil //nolint:nilnil // no free key is a normal allocation result.
		}

		left, right := s.ost.SelectableRangeBounds(normalized.Prefix)
		if right <= left {
			return nil, nil //nolint:nilnil // no free matching key.
		}

		key, ok := s.ost.KthSelectable(left)
		if !ok {
			return nil, nil //nolint:nilnil // tracked index changed while selecting.
		}

		value, exists, err := s.loadTrackedDiskValueLocked(key)
		if err != nil {
			return nil, err
		}

		if !exists {
			// Defensive repair in case process-local tracking observed a stale key.
			s.removeKeyIndexLocked(key)
			continue
		}

		claim := s.applyTrackedClaimLocked(key, value, normalized.Owner, now+normalized.TTLMs)
		if claim != nil {
			return claim, nil
		}

		// Defensive retry: key metadata changed between selection and apply.
	}
}

// claimNextBolt leases the lexicographically smallest free key using one bbolt transaction.
func (s *DiskStore) claimNextBolt(opts *ClaimOptions) (*EntryClaim, error) {
	release, err := s.beginOperation()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	if err := s.ensureWritable(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreWriteFailed, err)
	}

	normalized := normalizeClaimOptions(opts)
	if err := validateClaimOptions(normalized); err != nil {
		return nil, err
	}

	now := time.Now().UnixMilli()

	var claim *EntryClaim

	err = s.handle.Update(func(tx *bolt.Tx) error {
		nextClaim, txErr := s.claimNextBoltTx(tx, normalized, now)
		if txErr != nil {
			return txErr
		}

		claim = nextClaim

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreWriteFailed, err)
	}

	return claim, nil
}

func (s *DiskStore) claimNextBoltTx(
	tx *bolt.Tx,
	normalized *ClaimOptions,
	now int64,
) (*EntryClaim, error) {
	bucket := tx.Bucket(s.bucket)
	if bucket == nil {
		return nil, fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
	}

	claimsBucket, err := s.ensureClaimsBucket(tx)
	if err != nil {
		return nil, err
	}

	if err := s.cleanupExpiredClaimsInBucketIfDue(claimsBucket, now); err != nil {
		return nil, err
	}

	prefixBytes := []byte(normalized.Prefix)
	cursor := bucket.Cursor()

	for keyBytes, valueBytes := cursor.Seek(prefixBytes); keyBytes != nil; keyBytes, valueBytes = cursor.Next() {
		if normalized.Prefix != "" && !bytes.HasPrefix(keyBytes, prefixBytes) {
			break
		}

		key := string(keyBytes)

		liveClaim, err := s.keyHasLiveClaimTx(claimsBucket, key, now)
		if err != nil {
			return nil, err
		}

		if liveClaim {
			continue
		}

		claim := newEntryClaimFromBytes(s, key, valueBytes, normalized, now)
		if err := s.putDiskClaimTx(claimsBucket, claim); err != nil {
			return nil, err
		}

		return claim, nil
	}

	return nil, nil //nolint:nilnil // no free matching key.
}
