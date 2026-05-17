package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"time"

	bolt "go.etcd.io/bbolt"
	boltErrors "go.etcd.io/bbolt/errors"
)

// diskClaimRecord is the internal representation of a claim record.
type diskClaimRecord struct {
	// ID is the unique claim identifier.
	ID string `json:"id"`
	// Key is the claimed user key.
	Key string `json:"key"`
	// Owner is an optional logical owner identifier for diagnostics.
	Owner string `json:"owner,omitempty"`
	// Token is a monotonically increasing fence token for stale-holder protection.
	Token int64 `json:"token"`
	// ExpiresAt is Unix epoch milliseconds when the lease expires.
	ExpiresAt int64 `json:"expires_at"`
}

const (
	// diskClaimsBucketName is the name of the claims bucket.
	diskClaimsBucketName = "__xk6_kv_claims"
	// diskClaimsCleanupIntervalMs throttles full expired-claim scans in allocation paths.
	diskClaimsCleanupIntervalMs int64 = 5_000
)

//nolint:gochecknoglobals // immutable internal bbolt key bytes reused across claim operations.
var (
	// diskClaimsBucket is the internal bbolt bucket key for claims.
	diskClaimsBucket = []byte(diskClaimsBucketName)
	// claimIDPrefix is the internal bbolt prefix for claim IDs.
	claimIDPrefix = []byte("id:")
	// claimKeyPrefix is the internal bbolt prefix for claim keys.
	claimKeyPrefix = []byte("key:")
)

// PopRandom claims one random free matching entry, then completes it with DeleteKey=true.
// If there are no free matching entries, it returns nil, nil.
func (s *DiskStore) PopRandom(prefix string) (*Entry, error) {
	if s.trackedClaimsEnabled() {
		return s.popRandomTracked(prefix)
	}

	return s.popRandomBolt(prefix)
}

// PopRandomMany claims up to count random free matching entries, then completes
// each claim with DeleteKey=true.
//
// Completed deletes are not rolled back if a later completion fails.
func (s *DiskStore) PopRandomMany(prefix string, count int64) ([]*Entry, error) {
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
		completed, err := s.CompleteClaim(claim.Ref(), &CompleteClaimOptions{DeleteKey: true})
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

// releaseClaimsBestEffort releases the claims best effort.
// It returns an error if the release fails.
func (s *DiskStore) releaseClaimsBestEffort(claims []*EntryClaim) error {
	var joined error

	for _, claim := range claims {
		if claim == nil {
			continue
		}

		released, err := s.ReleaseClaim(claim.Ref())
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

// releaseTrackedClaimBestEffort releases a tracked claim best effort.
// It returns an error if the release fails.
func (s *DiskStore) releaseTrackedClaimBestEffort(ref *ClaimRef) error {
	released, err := s.releaseTrackedClaim(ref)
	if err != nil {
		return err
	}

	if !released {
		return fmt.Errorf("%w: releaseClaim", ErrClaimCompletionFailed)
	}

	return nil
}

// popRandomTracked pops a random tracked claim.
func (s *DiskStore) popRandomTracked(prefix string) (*Entry, error) {
	claim, err := s.claimRandomTracked(&ClaimOptions{
		Prefix: prefix,
		TTLMs:  DefaultClaimTTLMs,
	})
	if err != nil || claim == nil {
		return nil, err
	}

	if err := s.completeTrackedPopClaim(claim); err != nil {
		return nil, err
	}

	return &Entry{
		Key:   claim.Entry.Key,
		Value: claim.Entry.Value,
	}, nil
}

// completeTrackedPopClaim completes a tracked pop claim.
// It returns an error if the completion fails.
func (s *DiskStore) completeTrackedPopClaim(claim *EntryClaim) error {
	completed, err := s.completeTrackedClaim(claim.Ref(), &CompleteClaimOptions{DeleteKey: true})
	if err != nil {
		releaseErr := s.releaseTrackedClaimBestEffort(claim.Ref())
		if releaseErr != nil {
			return errors.Join(err, releaseErr)
		}

		return err
	}

	if !completed {
		completionErr := fmt.Errorf("%w: popRandom", ErrClaimCompletionFailed)

		releaseErr := s.releaseTrackedClaimBestEffort(claim.Ref())
		if releaseErr != nil {
			return errors.Join(completionErr, releaseErr)
		}

		return completionErr
	}

	return nil
}

// popRandomBolt pops a random bolt claim.
//
//nolint:gocognit,funlen // transactional random-retry plus fallback scan keeps lease checks explicit and correct.
func (s *DiskStore) popRandomBolt(prefix string) (*Entry, error) {
	release, err := s.beginOperation()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	if err := s.ensureWritable(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreDeleteFailed, err)
	}

	if s.trackKeys {
		s.keysLock.Lock()
		defer s.keysLock.Unlock()
	}

	var (
		now    = time.Now().UnixMilli()
		popped *Entry
	)

	err = s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		claimsBucket, err := s.ensureClaimsBucket(tx)
		if err != nil {
			return err
		}

		if err := s.cleanupExpiredClaimsInBucketIfDue(claimsBucket, now); err != nil {
			return err
		}

		matchCount := s.countKeysInBucket(bucket, prefix)
		if matchCount == 0 {
			return nil
		}

		for range randomKeyMaxAttempts {
			target := rand.Int64N(matchCount) //nolint:gosec // math/rand/v2 is enough for non-crypto sampling.

			key, found := s.keyByIndexInBucket(bucket, prefix, target)
			if !found {
				continue
			}

			liveClaim, err := s.keyHasLiveClaimTx(claimsBucket, key, now)
			if err != nil {
				return err
			}

			if liveClaim {
				continue
			}

			value := bucket.Get([]byte(key))
			if value == nil {
				continue
			}

			entryValue := slices.Clone(value)

			if err := bucket.Delete([]byte(key)); err != nil {
				return err
			}

			if err := s.deleteClaimForKeyTx(claimsBucket, key); err != nil {
				return err
			}

			popped = &Entry{
				Key:   key,
				Value: entryValue,
			}

			return nil
		}

		prefixBytes := []byte(prefix)

		cursor := bucket.Cursor()
		for k, v := cursor.Seek(prefixBytes); k != nil; k, v = cursor.Next() {
			if prefix != "" && !bytes.HasPrefix(k, prefixBytes) {
				break
			}

			key := string(k)

			liveClaim, err := s.keyHasLiveClaimTx(claimsBucket, key, now)
			if err != nil {
				return err
			}

			if liveClaim {
				continue
			}

			entryValue := slices.Clone(v)

			if err := bucket.Delete(k); err != nil {
				return err
			}

			if err := s.deleteClaimForKeyTx(claimsBucket, key); err != nil {
				return err
			}

			popped = &Entry{
				Key:   key,
				Value: entryValue,
			}

			return nil
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreDeleteFailed, err)
	}

	if popped != nil && s.trackKeys {
		s.removeKeyIndexLocked(popped.Key)
	}

	return popped, nil
}

// ClaimRandom leases one random free matching entry.
// If no free (unclaimed/expired) matching entry exists, it returns nil, nil.
func (s *DiskStore) ClaimRandom(opts *ClaimOptions) (*EntryClaim, error) {
	if s.trackedClaimsEnabled() {
		return s.claimRandomTracked(opts)
	}

	return s.claimRandomBolt(opts)
}

// claimRandomBolt claims a random bolt claim.
//
//nolint:gocognit,funlen // transactional claim path intentionally handles random retries, expiry cleanup, and fallback scan.
func (s *DiskStore) claimRandomBolt(opts *ClaimOptions) (*EntryClaim, error) {
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
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		claimsBucket, err := s.ensureClaimsBucket(tx)
		if err != nil {
			return err
		}

		if err := s.cleanupExpiredClaimsInBucketIfDue(claimsBucket, now); err != nil {
			return err
		}

		matchCount := s.countKeysInBucket(bucket, normalized.Prefix)
		if matchCount == 0 {
			return nil
		}

		for range randomKeyMaxAttempts {
			target := rand.Int64N(matchCount) //nolint:gosec // non-crypto sampling is intended.

			key, found := s.keyByIndexInBucket(bucket, normalized.Prefix, target)
			if !found {
				continue
			}

			liveClaim, err := s.keyHasLiveClaimTx(claimsBucket, key, now)
			if err != nil {
				return err
			}

			if liveClaim {
				continue
			}

			value := bucket.Get([]byte(key))
			if value == nil {
				continue
			}

			claim = newEntryClaimFromBytes(s, key, value, normalized, now)
			if err := s.putDiskClaimTx(claimsBucket, claim); err != nil {
				return err
			}

			return nil
		}

		prefixBytes := []byte(normalized.Prefix)

		cursor := bucket.Cursor()
		for k, v := cursor.Seek(prefixBytes); k != nil; k, v = cursor.Next() {
			if normalized.Prefix != "" && !bytes.HasPrefix(k, prefixBytes) {
				break
			}

			key := string(k)

			liveClaim, err := s.keyHasLiveClaimTx(claimsBucket, key, now)
			if err != nil {
				return err
			}

			if liveClaim {
				continue
			}

			claim = newEntryClaimFromBytes(s, key, v, normalized, now)
			if err := s.putDiskClaimTx(claimsBucket, claim); err != nil {
				return err
			}

			return nil
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreWriteFailed, err)
	}

	return claim, nil
}

// ClaimKey leases one specific free key.
// If the key is missing or currently live-claimed, it returns nil, nil.
func (s *DiskStore) ClaimKey(key string, opts *ClaimOptions) (*EntryClaim, error) {
	if s.trackedClaimsEnabled() {
		return s.claimKeyTracked(key, opts)
	}

	return s.claimKeyBolt(key, opts)
}

// claimKeyBolt claims a key in the bolt store.
func (s *DiskStore) claimKeyBolt(key string, opts *ClaimOptions) (*EntryClaim, error) {
	release, err := s.beginOperation()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	if err := s.ensureWritable(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreWriteFailed, err)
	}

	if err := validateNonEmptyKey(key); err != nil {
		return nil, err
	}

	normalized := normalizeClaimOptions(opts)
	if err := validateClaimOptions(normalized); err != nil {
		return nil, err
	}

	now := time.Now().UnixMilli()

	var claim *EntryClaim

	err = s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		claimsBucket, err := s.ensureClaimsBucket(tx)
		if err != nil {
			return err
		}

		if err := s.cleanupExpiredClaimsInBucketIfDue(claimsBucket, now); err != nil {
			return err
		}

		value := bucket.Get([]byte(key))
		if value == nil {
			return nil
		}

		liveClaim, err := s.keyHasLiveClaimTx(claimsBucket, key, now)
		if err != nil {
			return err
		}

		if liveClaim {
			return nil
		}

		claim = newEntryClaimFromBytes(s, key, value, normalized, now)

		return s.putDiskClaimTx(claimsBucket, claim)
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreWriteFailed, err)
	}

	return claim, nil
}

// ClaimRandomMany leases up to Count unique random free matching entries.
func (s *DiskStore) ClaimRandomMany(opts *ClaimManyOptions) ([]*EntryClaim, error) {
	if s.trackedClaimsEnabled() {
		return s.claimRandomManyTracked(opts)
	}

	return s.claimRandomManyBolt(opts)
}

// claimRandomManyBolt claims many bolt claims.
func (s *DiskStore) claimRandomManyBolt(opts *ClaimManyOptions) ([]*EntryClaim, error) {
	release, err := s.beginOperation()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	if err := s.ensureWritable(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreWriteFailed, err)
	}

	normalized := normalizeClaimManyOptions(opts)
	if err := validateClaimManyOptions(normalized); err != nil {
		return nil, err
	}

	now := time.Now().UnixMilli()
	claims := make([]*EntryClaim, 0, normalized.Count)

	err = s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		claimsBucket, err := s.ensureClaimsBucket(tx)
		if err != nil {
			return err
		}

		if err := s.cleanupExpiredClaimsInBucketIfDue(claimsBucket, now); err != nil {
			return err
		}

		candidates, err := s.collectClaimRandomManyCandidatesTx(bucket, claimsBucket, normalized.Prefix, now)
		if err != nil {
			return err
		}

		if len(candidates) == 0 {
			return nil
		}

		rand.Shuffle(len(candidates), func(i, j int) {
			candidates[i], candidates[j] = candidates[j], candidates[i]
		})

		limit := min(int(normalized.Count), len(candidates))

		claimOpts := &ClaimOptions{
			Prefix: normalized.Prefix,
			Owner:  normalized.Owner,
			TTLMs:  normalized.TTLMs,
		}

		return s.persistClaimRandomManyCandidatesTx(
			bucket,
			claimsBucket,
			candidates[:limit],
			claimOpts,
			now,
			&claims,
		)
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreWriteFailed, err)
	}

	return claims, nil
}

// collectClaimRandomManyCandidatesTx collects the candidate keys for random many claims.
// It returns the candidate keys and an error if the collection fails.
func (s *DiskStore) collectClaimRandomManyCandidatesTx(
	bucket *bolt.Bucket,
	claimsBucket *bolt.Bucket,
	prefix string,
	now int64,
) ([]string, error) {
	candidates := make([]string, 0)
	prefixBytes := []byte(prefix)

	cursor := bucket.Cursor()
	for k, _ := cursor.Seek(prefixBytes); k != nil; k, _ = cursor.Next() {
		if prefix != "" && !bytes.HasPrefix(k, prefixBytes) {
			break
		}

		key := string(k)

		liveClaim, err := s.keyHasLiveClaimTx(claimsBucket, key, now)
		if err != nil {
			return nil, err
		}

		if liveClaim {
			continue
		}

		candidates = append(candidates, key)
	}

	return candidates, nil
}

// persistClaimRandomManyCandidatesTx persists the candidate claims for random many claims.
// It returns an error if the persistence fails.
func (s *DiskStore) persistClaimRandomManyCandidatesTx(
	bucket *bolt.Bucket,
	claimsBucket *bolt.Bucket,
	candidateKeys []string,
	claimOpts *ClaimOptions,
	now int64,
	claims *[]*EntryClaim,
) error {
	for _, key := range candidateKeys {
		value := bucket.Get([]byte(key))
		if value == nil {
			continue
		}

		claim := newEntryClaimFromBytes(s, key, value, claimOpts, now)
		if err := s.putDiskClaimTx(claimsBucket, claim); err != nil {
			return err
		}

		*claims = append(*claims, claim)
	}

	return nil
}

// ReleaseClaim releases a live claim and makes the key claimable again.
func (s *DiskStore) ReleaseClaim(ref *ClaimRef) (bool, error) {
	if s.trackedClaimsEnabled() {
		return s.releaseTrackedClaim(ref)
	}

	return s.releaseClaimBolt(ref)
}

// releaseClaimBolt releases a bolt claim.
func (s *DiskStore) releaseClaimBolt(ref *ClaimRef) (bool, error) {
	release, err := s.beginOperation()
	if err != nil {
		return false, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	if !isValidClaimRef(ref) {
		return false, nil
	}

	if err := s.ensureWritable(); err != nil {
		return false, fmt.Errorf("%w: %w", ErrDiskStoreWriteFailed, err)
	}

	now := time.Now().UnixMilli()

	var released bool

	err = s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		claimsBucket, err := s.ensureClaimsBucket(tx)
		if err != nil {
			return err
		}

		record, found, err := s.getDiskClaimByIDTx(claimsBucket, ref.ID)
		if err != nil {
			return err
		}

		if !found {
			return nil
		}

		if record.ExpiresAt <= now {
			return s.deleteDiskClaimRecordTx(claimsBucket, record.ID, record.Key)
		}

		if record.Key != ref.Key || record.Token != ref.Token {
			return nil
		}

		if bucket.Get([]byte(record.Key)) == nil {
			return s.deleteDiskClaimRecordTx(claimsBucket, record.ID, record.Key)
		}

		if err := s.deleteDiskClaimRecordTx(claimsBucket, record.ID, record.Key); err != nil {
			return err
		}

		released = true

		return nil
	})
	if err != nil {
		return false, fmt.Errorf("%w: %w", ErrDiskStoreWriteFailed, err)
	}

	return released, nil
}

// CompleteClaim completes a live claim and optionally deletes the underlying key.
func (s *DiskStore) CompleteClaim(ref *ClaimRef, opts *CompleteClaimOptions) (bool, error) {
	if s.trackedClaimsEnabled() {
		return s.completeTrackedClaim(ref, opts)
	}

	return s.completeClaimBolt(ref, opts)
}

// completeClaimBolt completes a bolt claim.
func (s *DiskStore) completeClaimBolt(ref *ClaimRef, opts *CompleteClaimOptions) (bool, error) {
	release, err := s.beginOperation()
	if err != nil {
		return false, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	if !isValidClaimRef(ref) {
		return false, nil
	}

	if err := s.ensureWritable(); err != nil {
		return false, fmt.Errorf("%w: %w", ErrDiskStoreWriteFailed, err)
	}

	normalized := normalizeCompleteClaimOptions(opts)
	now := time.Now().UnixMilli()

	if normalized.DeleteKey && s.trackKeys {
		s.keysLock.Lock()
		defer s.keysLock.Unlock()
	}

	var completed bool

	err = s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		claimsBucket, err := s.ensureClaimsBucket(tx)
		if err != nil {
			return err
		}

		record, found, err := s.getDiskClaimByIDTx(claimsBucket, ref.ID)
		if err != nil {
			return err
		}

		if !found {
			return nil
		}

		if record.ExpiresAt <= now {
			return s.deleteDiskClaimRecordTx(claimsBucket, record.ID, record.Key)
		}

		if record.Key != ref.Key || record.Token != ref.Token {
			return nil
		}

		if bucket.Get([]byte(record.Key)) == nil {
			return s.deleteDiskClaimRecordTx(claimsBucket, record.ID, record.Key)
		}

		if err := s.deleteDiskClaimRecordTx(claimsBucket, record.ID, record.Key); err != nil {
			return err
		}

		if normalized.DeleteKey {
			if err := bucket.Delete([]byte(record.Key)); err != nil {
				return err
			}
		}

		completed = true

		return nil
	})
	if err != nil {
		return false, fmt.Errorf("%w: %w", ErrDiskStoreWriteFailed, err)
	}

	if completed && normalized.DeleteKey && s.trackKeys {
		s.removeKeyIndexLocked(ref.Key)
	}

	return completed, nil
}

// RenewClaim extends a live claim lease without changing token.
func (s *DiskStore) RenewClaim(ref *ClaimRef, opts *RenewClaimOptions) (bool, error) {
	if s.trackedClaimsEnabled() {
		return s.renewClaimTracked(ref, opts)
	}

	return s.renewClaimBolt(ref, opts)
}

// renewClaimBolt renews a bolt claim.
func (s *DiskStore) renewClaimBolt(ref *ClaimRef, opts *RenewClaimOptions) (bool, error) {
	release, err := s.beginOperation()
	if err != nil {
		return false, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	if !isValidClaimRef(ref) {
		return false, nil
	}

	if err := s.ensureWritable(); err != nil {
		return false, fmt.Errorf("%w: %w", ErrDiskStoreWriteFailed, err)
	}

	if err := validateRenewClaimOptions(opts); err != nil {
		return false, err
	}

	now := time.Now().UnixMilli()

	var renewed bool

	err = s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		claimsBucket, err := s.ensureClaimsBucket(tx)
		if err != nil {
			return err
		}

		record, found, err := s.getDiskClaimByIDTx(claimsBucket, ref.ID)
		if err != nil {
			return err
		}

		if !found {
			return nil
		}

		if record.ExpiresAt <= now {
			return s.deleteDiskClaimRecordTx(claimsBucket, record.ID, record.Key)
		}

		if record.Key != ref.Key || record.Token != ref.Token {
			return nil
		}

		if bucket.Get([]byte(record.Key)) == nil {
			return s.deleteDiskClaimRecordTx(claimsBucket, record.ID, record.Key)
		}

		record.ExpiresAt = now + opts.TTLMs

		payload, err := s.encodeDiskClaim(record)
		if err != nil {
			return err
		}

		if err := claimsBucket.Put(s.claimIDKey(record.ID), payload); err != nil {
			return err
		}

		if err := claimsBucket.Put(s.claimKeyIndexKey(record.Key), []byte(record.ID)); err != nil {
			return err
		}

		renewed = true

		return nil
	})
	if err != nil {
		return false, fmt.Errorf("%w: %w", ErrDiskStoreWriteFailed, err)
	}

	return renewed, nil
}

// ensureClaimsBucket ensures the claims bucket exists.
func (s *DiskStore) ensureClaimsBucket(tx *bolt.Tx) (*bolt.Bucket, error) {
	bucket, err := tx.CreateBucketIfNotExists(diskClaimsBucket)
	if err != nil {
		return nil, fmt.Errorf("%w: claims bucket: %w", ErrDiskStoreWriteFailed, err)
	}

	return bucket, nil
}

// clearDiskClaims clears the claims bucket.
func (s *DiskStore) clearDiskClaims() error {
	if err := s.handle.Update(s.clearClaimsBucket); err != nil {
		return fmt.Errorf("%w: %w", ErrDiskStoreWriteFailed, err)
	}

	return nil
}

// clearClaimsBucket clears the claims bucket.
func (s *DiskStore) clearClaimsBucket(tx *bolt.Tx) error {
	err := tx.DeleteBucket(diskClaimsBucket)
	if err != nil && !errors.Is(err, boltErrors.ErrBucketNotFound) {
		return fmt.Errorf("%w: claims bucket: %w", ErrDiskStoreDeleteFailed, err)
	}

	_, err = tx.CreateBucketIfNotExists(diskClaimsBucket)
	if err != nil {
		return fmt.Errorf("%w: claims bucket: %w", ErrDiskStoreWriteFailed, err)
	}

	return nil
}

// deleteClaimForKeyTx deletes a claim for a key.
func (s *DiskStore) deleteClaimForKeyTx(claims *bolt.Bucket, key string) error {
	claimID := claims.Get(s.claimKeyIndexKey(key))
	if claimID != nil {
		if err := claims.Delete(s.claimIDKey(string(claimID))); err != nil {
			return err
		}
	}

	if err := claims.Delete(s.claimKeyIndexKey(key)); err != nil {
		return err
	}

	return nil
}

// putDiskClaimTx puts a claim for a key.
func (s *DiskStore) putDiskClaimTx(claims *bolt.Bucket, claim *EntryClaim) error {
	record := &diskClaimRecord{
		ID:        claim.ID,
		Key:       claim.Key,
		Owner:     claim.Owner,
		Token:     claim.Token,
		ExpiresAt: claim.ExpiresAt,
	}

	payload, err := s.encodeDiskClaim(record)
	if err != nil {
		return err
	}

	if err := claims.Put(s.claimIDKey(record.ID), payload); err != nil {
		return err
	}

	return claims.Put(s.claimKeyIndexKey(record.Key), []byte(record.ID))
}

// getDiskClaimByIDTx gets a claim by ID.
func (s *DiskStore) getDiskClaimByIDTx(claims *bolt.Bucket, claimID string) (*diskClaimRecord, bool, error) {
	raw := claims.Get(s.claimIDKey(claimID))
	if raw == nil {
		return nil, false, nil
	}

	record, err := s.decodeDiskClaim(raw)
	if err != nil {
		return nil, false, err
	}

	return record, true, nil
}

// keyHasLiveClaimTx checks if a key has a live claim.
func (s *DiskStore) keyHasLiveClaimTx(claims *bolt.Bucket, key string, now int64) (bool, error) {
	claimID := claims.Get(s.claimKeyIndexKey(key))
	if claimID == nil {
		return false, nil
	}

	claimIDStr := string(claimID)

	recordRaw := claims.Get(s.claimIDKey(claimIDStr))
	if recordRaw == nil {
		if err := claims.Delete(s.claimKeyIndexKey(key)); err != nil {
			return false, err
		}

		return false, nil
	}

	record, err := s.decodeDiskClaim(recordRaw)
	if err != nil {
		return false, err
	}

	if record.Key != key || record.ID != claimIDStr || record.ExpiresAt <= now {
		if err := s.deleteDiskClaimRecordTx(claims, claimIDStr, key); err != nil {
			return false, err
		}

		return false, nil
	}

	return true, nil
}

// cleanupExpiredClaimsInBucketIfDue cleans up expired claims in the bucket if due.
func (s *DiskStore) cleanupExpiredClaimsInBucketIfDue(claims *bolt.Bucket, now int64) error {
	if !s.claimsCleanupDue(now) {
		return nil
	}

	s.claimsCleanupMu.Lock()
	defer s.claimsCleanupMu.Unlock()

	if !s.claimsCleanupDue(now) {
		return nil
	}

	if err := s.cleanupExpiredClaimsInBucket(claims, now); err != nil {
		return err
	}

	s.lastClaimsCleanupUnixMilli.Store(now)

	return nil
}

// claimsCleanupDue checks if the claims cleanup is due.
func (s *DiskStore) claimsCleanupDue(now int64) bool {
	last := s.lastClaimsCleanupUnixMilli.Load()

	return last == 0 || now-last >= diskClaimsCleanupIntervalMs
}

// cleanupExpiredClaimsInBucket cleans up expired claims in the bucket.
func (s *DiskStore) cleanupExpiredClaimsInBucket(claims *bolt.Bucket, now int64) error {
	type expiredClaim struct {
		id  string
		key string
	}

	expired := make([]expiredClaim, 0)
	cursor := claims.Cursor()

	for k, v := cursor.Seek(claimIDPrefix); k != nil; k, v = cursor.Next() {
		if !bytes.HasPrefix(k, claimIDPrefix) {
			break
		}

		record, err := s.decodeDiskClaim(v)
		if err != nil {
			return err
		}

		if record.ExpiresAt <= now {
			expired = append(expired, expiredClaim{
				id:  record.ID,
				key: record.Key,
			})
		}
	}

	for _, claim := range expired {
		if err := s.deleteDiskClaimRecordTx(claims, claim.id, claim.key); err != nil {
			return err
		}
	}

	return nil
}

// deleteDiskClaimRecordTx deletes a claim record.
func (s *DiskStore) deleteDiskClaimRecordTx(claims *bolt.Bucket, claimID, key string) error {
	if err := claims.Delete(s.claimIDKey(claimID)); err != nil {
		return err
	}

	if err := claims.Delete(s.claimKeyIndexKey(key)); err != nil {
		return err
	}

	return nil
}

// claimIDKey creates a claim ID key.
func (s *DiskStore) claimIDKey(id string) []byte {
	return append(slices.Clone(claimIDPrefix), id...)
}

// claimKeyIndexKey creates a claim key index key.
func (s *DiskStore) claimKeyIndexKey(key string) []byte {
	return append(slices.Clone(claimKeyPrefix), key...)
}

// encodeDiskClaim encodes a disk claim record.
func (s *DiskStore) encodeDiskClaim(claim *diskClaimRecord) ([]byte, error) {
	payload, err := json.Marshal(claim)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreWriteFailed, err)
	}

	return payload, nil
}

// decodeDiskClaim decodes a disk claim record.
func (s *DiskStore) decodeDiskClaim(payload []byte) (*diskClaimRecord, error) {
	record := new(diskClaimRecord)
	if err := json.Unmarshal(payload, record); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreReadFailed, err)
	}

	return record, nil
}
