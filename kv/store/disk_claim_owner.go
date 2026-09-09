package store

import (
	"fmt"
	"slices"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

// ClaimForOwner returns one sticky live claim for an exact (prefix, owner)
// pair or allocates a new random free claim.
func (s *DiskStore) ClaimForOwner(opts *ClaimOptions) (*EntryClaim, error) {
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
		s.lookupDiskOwnerBoundClaim,
		s.ClaimRandom,
	)
}

func (s *DiskStore) lookupDiskOwnerBoundClaim(
	ref *ClaimRef,
	opts *ClaimOptions,
) (*EntryClaim, bool, error) {
	if s.trackedClaimsEnabled() {
		return s.lookupTrackedOwnerBoundClaim(ref, opts)
	}

	return s.lookupBoltOwnerBoundClaim(ref, opts)
}

func (s *DiskStore) lookupTrackedOwnerBoundClaim(
	ref *ClaimRef,
	opts *ClaimOptions,
) (*EntryClaim, bool, error) {
	if !isValidClaimRef(ref) {
		return nil, false, nil
	}

	release, err := s.beginOperation()
	if err != nil {
		return nil, false, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	// ClaimForOwner is a claim operation. Read-only stores must reject it.
	if err := s.ensureWritable(); err != nil {
		return nil, false, fmt.Errorf("%w: %w", ErrDiskStoreWriteFailed, err)
	}

	if opts.Prefix != "" && !strings.HasPrefix(ref.Key, opts.Prefix) {
		return nil, false, nil
	}

	now := time.Now().UnixMilli()

	s.keysLock.RLock()
	defer s.keysLock.RUnlock()

	if s.ost == nil {
		return nil, false, nil
	}

	record, ok := s.ost.Meta(ref.Key)
	if !ok || record == nil {
		return nil, false, nil
	}

	if record.ExpiresAt <= now {
		return nil, false, nil
	}

	if record.ID != ref.ID ||
		record.Key != ref.Key ||
		record.Token != ref.Token ||
		record.Owner != opts.Owner {
		return nil, false, nil
	}

	value, exists, err := s.loadTrackedDiskValueLocked(ref.Key)
	if err != nil {
		return nil, false, err
	}

	if !exists {
		return nil, false, nil
	}

	return &EntryClaim{
		ID:  record.ID,
		Key: record.Key,
		Entry: Entry{
			Key:   record.Key,
			Value: value,
		},
		Owner:     record.Owner,
		Token:     record.Token,
		ExpiresAt: record.ExpiresAt,
	}, true, nil
}

func (s *DiskStore) lookupBoltOwnerBoundClaim(
	ref *ClaimRef,
	opts *ClaimOptions,
) (*EntryClaim, bool, error) {
	if !isValidClaimRef(ref) {
		return nil, false, nil
	}

	release, err := s.beginOperation()
	if err != nil {
		return nil, false, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	if err := s.ensureWritable(); err != nil {
		return nil, false, fmt.Errorf("%w: %w", ErrDiskStoreWriteFailed, err)
	}

	if opts.Prefix != "" && !strings.HasPrefix(ref.Key, opts.Prefix) {
		return nil, false, nil
	}

	now := time.Now().UnixMilli()

	claim, err := s.lookupBoltOwnerBoundClaimView(ref, opts.Owner, now)
	if err != nil {
		return nil, false, fmt.Errorf("%w: %w", ErrDiskStoreReadFailed, err)
	}

	return claim, claim != nil, nil
}

func (s *DiskStore) lookupBoltOwnerBoundClaimView(
	ref *ClaimRef,
	owner string,
	now int64,
) (*EntryClaim, error) {
	var claim *EntryClaim

	err := s.handle.View(func(tx *bolt.Tx) error {
		nextClaim, found, txErr := s.lookupBoltOwnerBoundClaimTx(tx, ref, owner, now)
		if txErr != nil {
			return txErr
		}

		if !found {
			return nil
		}

		claim = nextClaim

		return nil
	})
	if err != nil {
		return nil, err
	}

	return claim, nil
}

func (s *DiskStore) lookupBoltOwnerBoundClaimTx(
	tx *bolt.Tx,
	ref *ClaimRef,
	owner string,
	now int64,
) (*EntryClaim, bool, error) {
	bucket := tx.Bucket(s.bucket)
	if bucket == nil {
		return nil, false, fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
	}

	// Claims bucket may not exist on pristine databases in read paths.
	claimsBucket := tx.Bucket(diskClaimsBucket)
	if claimsBucket == nil {
		return nil, false, nil
	}

	record, exists, err := s.getDiskClaimByIDTx(claimsBucket, ref.ID)
	if err != nil {
		return nil, false, err
	}

	if !exists || record == nil {
		return nil, false, nil
	}

	if record.ExpiresAt <= now {
		return nil, false, nil
	}

	if record.ID != ref.ID ||
		record.Key != ref.Key ||
		record.Token != ref.Token ||
		record.Owner != owner {
		return nil, false, nil
	}

	// Verify the per-key claim index still points to this exact claim.
	indexedClaimID := claimsBucket.Get(s.claimKeyIndexKey(ref.Key))
	if indexedClaimID == nil || string(indexedClaimID) != ref.ID {
		return nil, false, nil
	}

	value := bucket.Get([]byte(ref.Key))
	if value == nil {
		return nil, false, nil
	}

	return &EntryClaim{
		ID:  record.ID,
		Key: record.Key,
		Entry: Entry{
			Key:   record.Key,
			Value: slices.Clone(value),
		},
		Owner:     record.Owner,
		Token:     record.Token,
		ExpiresAt: record.ExpiresAt,
	}, true, nil
}
