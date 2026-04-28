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
)

//nolint:gochecknoglobals // Immutable internal bbolt key bytes reused across claim operations.
var (
	// diskClaimsBucket is the internal bbolt bucket key for claims.
	diskClaimsBucket = []byte(diskClaimsBucketName)
	// claimIDPrefix is the internal bbolt prefix for claim IDs.
	claimIDPrefix = []byte("id:")
	// claimKeyPrefix is the internal bbolt prefix for claim keys.
	claimKeyPrefix = []byte("key:")
)

// PopRandom atomically selects and removes a random free matching entry.
// If there are no free matching entries, it returns nil, nil.
//
//nolint:gocognit,funlen // Transactional random-retry plus fallback scan keeps lease checks explicit and correct.
func (s *DiskStore) PopRandom(prefix string) (*Entry, error) {
	release, err := s.beginOperation()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

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

		claimsBucket, err := ensureClaimsBucket(tx)
		if err != nil {
			return err
		}

		if err := cleanupExpiredClaimsInBucket(claimsBucket, now); err != nil {
			return err
		}

		matchCount := countKeysInBucket(bucket, prefix)
		if matchCount == 0 {
			return nil
		}

		for range randomKeyMaxAttempts {
			target := rand.Int64N(matchCount) //nolint:gosec // math/rand/v2 is enough for non-crypto sampling.

			key, found := keyByIndexInBucket(bucket, prefix, target)
			if !found {
				continue
			}

			liveClaim, err := keyHasLiveClaimTx(claimsBucket, key, now)
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

			if err := deleteClaimForKeyTx(claimsBucket, key); err != nil {
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

			liveClaim, err := keyHasLiveClaimTx(claimsBucket, key, now)
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

			if err := deleteClaimForKeyTx(claimsBucket, key); err != nil {
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

// ClaimRandom atomically leases a random matching entry.
// If no free (unclaimed/expired) matching entry exists, it returns nil, nil.
//
//nolint:gocognit,funlen // transactional claim path intentionally handles random retries, expiry cleanup, and fallback scan.
func (s *DiskStore) ClaimRandom(opts *ClaimOptions) (*EntryClaim, error) {
	release, err := s.beginOperation()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	normalized := normalizeClaimOptions(opts)
	now := time.Now().UnixMilli()

	var claim *EntryClaim

	err = s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		claimsBucket, err := ensureClaimsBucket(tx)
		if err != nil {
			return err
		}

		if err := cleanupExpiredClaimsInBucket(claimsBucket, now); err != nil {
			return err
		}

		matchCount := countKeysInBucket(bucket, normalized.Prefix)
		if matchCount == 0 {
			return nil
		}

		for range randomKeyMaxAttempts {
			target := rand.Int64N(matchCount) //nolint:gosec // non-crypto sampling is intended.

			key, found := keyByIndexInBucket(bucket, normalized.Prefix, target)
			if !found {
				continue
			}

			liveClaim, err := keyHasLiveClaimTx(claimsBucket, key, now)
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
			if err := putDiskClaimTx(claimsBucket, claim); err != nil {
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

			liveClaim, err := keyHasLiveClaimTx(claimsBucket, key, now)
			if err != nil {
				return err
			}

			if liveClaim {
				continue
			}

			claim = newEntryClaimFromBytes(s, key, v, normalized, now)
			if err := putDiskClaimTx(claimsBucket, claim); err != nil {
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

// ReleaseClaim releases a live claim and makes the key claimable again.
func (s *DiskStore) ReleaseClaim(ref *ClaimRef) (bool, error) {
	release, err := s.beginOperation()
	if err != nil {
		return false, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	if !isValidClaimRef(ref) {
		return false, nil
	}

	now := time.Now().UnixMilli()

	var released bool

	err = s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		claimsBucket, err := ensureClaimsBucket(tx)
		if err != nil {
			return err
		}

		record, found, err := getDiskClaimByIDTx(claimsBucket, ref.ID)
		if err != nil {
			return err
		}

		if !found {
			return nil
		}

		if record.ExpiresAt <= now {
			return deleteDiskClaimRecordTx(claimsBucket, record.ID, record.Key)
		}

		if record.Key != ref.Key || record.Token != ref.Token {
			return nil
		}

		if bucket.Get([]byte(record.Key)) == nil {
			return deleteDiskClaimRecordTx(claimsBucket, record.ID, record.Key)
		}

		if err := deleteDiskClaimRecordTx(claimsBucket, record.ID, record.Key); err != nil {
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
	release, err := s.beginOperation()
	if err != nil {
		return false, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	if !isValidClaimRef(ref) {
		return false, nil
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

		claimsBucket, err := ensureClaimsBucket(tx)
		if err != nil {
			return err
		}

		record, found, err := getDiskClaimByIDTx(claimsBucket, ref.ID)
		if err != nil {
			return err
		}

		if !found {
			return nil
		}

		if record.ExpiresAt <= now {
			return deleteDiskClaimRecordTx(claimsBucket, record.ID, record.Key)
		}

		if record.Key != ref.Key || record.Token != ref.Token {
			return nil
		}

		if bucket.Get([]byte(record.Key)) == nil {
			return deleteDiskClaimRecordTx(claimsBucket, record.ID, record.Key)
		}

		if err := deleteDiskClaimRecordTx(claimsBucket, record.ID, record.Key); err != nil {
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

func newEntryClaimFromBytes(
	store *DiskStore,
	key string,
	value []byte,
	opts *ClaimOptions,
	now int64,
) *EntryClaim {
	token := store.claimToken.Add(1)

	return &EntryClaim{
		ID:  claimIDFromToken(token),
		Key: key,
		Entry: Entry{
			Key:   key,
			Value: slices.Clone(value),
		},
		Owner:     opts.Owner,
		Token:     token,
		ExpiresAt: now + opts.TTLMs,
	}
}

func ensureClaimsBucket(tx *bolt.Tx) (*bolt.Bucket, error) {
	bucket, err := tx.CreateBucketIfNotExists(diskClaimsBucket)
	if err != nil {
		return nil, fmt.Errorf("%w: claims bucket: %w", ErrDiskStoreWriteFailed, err)
	}

	return bucket, nil
}

func (s *DiskStore) clearDiskClaims() error {
	if err := s.handle.Update(clearClaimsBucket); err != nil {
		return fmt.Errorf("%w: %w", ErrDiskStoreWriteFailed, err)
	}

	return nil
}

func clearClaimsBucket(tx *bolt.Tx) error {
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

func deleteClaimForKeyTx(claims *bolt.Bucket, key string) error {
	claimID := claims.Get(claimKeyIndexKey(key))
	if claimID != nil {
		if err := claims.Delete(claimIDKey(string(claimID))); err != nil {
			return err
		}
	}

	if err := claims.Delete(claimKeyIndexKey(key)); err != nil {
		return err
	}

	return nil
}

func putDiskClaimTx(claims *bolt.Bucket, claim *EntryClaim) error {
	record := &diskClaimRecord{
		ID:        claim.ID,
		Key:       claim.Key,
		Owner:     claim.Owner,
		Token:     claim.Token,
		ExpiresAt: claim.ExpiresAt,
	}

	payload, err := encodeDiskClaim(record)
	if err != nil {
		return err
	}

	if err := claims.Put(claimIDKey(record.ID), payload); err != nil {
		return err
	}

	return claims.Put(claimKeyIndexKey(record.Key), []byte(record.ID))
}

func getDiskClaimByIDTx(claims *bolt.Bucket, claimID string) (*diskClaimRecord, bool, error) {
	raw := claims.Get(claimIDKey(claimID))
	if raw == nil {
		return nil, false, nil
	}

	record, err := decodeDiskClaim(raw)
	if err != nil {
		return nil, false, err
	}

	return record, true, nil
}

func keyHasLiveClaimTx(claims *bolt.Bucket, key string, now int64) (bool, error) {
	claimID := claims.Get(claimKeyIndexKey(key))
	if claimID == nil {
		return false, nil
	}

	claimIDStr := string(claimID)

	recordRaw := claims.Get(claimIDKey(claimIDStr))
	if recordRaw == nil {
		if err := claims.Delete(claimKeyIndexKey(key)); err != nil {
			return false, err
		}

		return false, nil
	}

	record, err := decodeDiskClaim(recordRaw)
	if err != nil {
		return false, err
	}

	if record.Key != key || record.ID != claimIDStr || record.ExpiresAt <= now {
		if err := deleteDiskClaimRecordTx(claims, claimIDStr, key); err != nil {
			return false, err
		}

		return false, nil
	}

	return true, nil
}

func cleanupExpiredClaimsInBucket(claims *bolt.Bucket, now int64) error {
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

		record, err := decodeDiskClaim(v)
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
		if err := deleteDiskClaimRecordTx(claims, claim.id, claim.key); err != nil {
			return err
		}
	}

	return nil
}

func deleteDiskClaimRecordTx(claims *bolt.Bucket, claimID, key string) error {
	if err := claims.Delete(claimIDKey(claimID)); err != nil {
		return err
	}

	if err := claims.Delete(claimKeyIndexKey(key)); err != nil {
		return err
	}

	return nil
}

func claimIDKey(id string) []byte {
	return append(slices.Clone(claimIDPrefix), id...)
}

func claimKeyIndexKey(key string) []byte {
	return append(slices.Clone(claimKeyPrefix), key...)
}

func encodeDiskClaim(claim *diskClaimRecord) ([]byte, error) {
	payload, err := json.Marshal(claim)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreWriteFailed, err)
	}

	return payload, nil
}

func decodeDiskClaim(payload []byte) (*diskClaimRecord, error) {
	record := new(diskClaimRecord)
	if err := json.Unmarshal(payload, record); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreReadFailed, err)
	}

	return record, nil
}
