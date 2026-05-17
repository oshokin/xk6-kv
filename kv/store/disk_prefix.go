package store

import (
	"bytes"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// DeleteByPrefix deletes up to limit keys with the given non-empty prefix.
func (s *DiskStore) DeleteByPrefix(prefix string, limit int64) (*DeleteByPrefixResult, error) {
	release, err := s.beginOperation()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	if err := s.validateDeleteByPrefixOptions(prefix, limit); err != nil {
		return nil, err
	}

	if err := s.ensureWritable(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreDeleteFailed, err)
	}

	if s.trackKeys {
		// Keep bbolt mutation and index update as one logical operation.
		s.keysLock.Lock()
		defer s.keysLock.Unlock()
	}

	result := &DeleteByPrefixResult{}
	deletedKeys := make([]string, 0)

	err = s.handle.Update(func(tx *bolt.Tx) error {
		var txErr error

		deletedKeys, txErr = s.deleteByPrefixTx(tx, prefix, limit, result)

		return txErr
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreDeleteFailed, err)
	}

	if s.trackKeys {
		for _, key := range deletedKeys {
			s.removeKeyIndexLocked(key)
		}
	}

	return result, nil
}

// validateDeleteByPrefixOptions validates the options for DeleteByPrefix.
// It returns an error if the prefix is empty or the limit is not positive
// or greater than MaxDeleteByPrefixLimit.
func (s *DiskStore) validateDeleteByPrefixOptions(prefix string, limit int64) error {
	if prefix == "" {
		return fmt.Errorf("%w: prefix must be non-empty", ErrKVOptionsInvalid)
	}

	if limit <= 0 {
		return fmt.Errorf("%w: limit must be positive: %d", ErrKVOptionsInvalid, limit)
	}

	if limit > MaxDeleteByPrefixLimit {
		return fmt.Errorf(
			"%w: limit must be less than or equal to %d: %d",
			ErrKVOptionsInvalid,
			MaxDeleteByPrefixLimit,
			limit,
		)
	}

	return nil
}

// deleteByPrefixTx deletes the keys with the given prefix
// and limit in the given transaction.
// It returns the deleted keys and an error if the bucket is not found
// or the claims bucket cannot be obtained.
func (s *DiskStore) deleteByPrefixTx(
	tx *bolt.Tx,
	prefix string,
	limit int64,
	result *DeleteByPrefixResult,
) ([]string, error) {
	bucket := tx.Bucket(s.bucket)
	if bucket == nil {
		return nil, fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
	}

	claimsBucket, err := s.claimsBucketForMutatorTx(tx)
	if err != nil {
		return nil, err
	}

	keysToDelete := s.selectDiskKeysByPrefixTx(bucket, prefix, limit)
	deletedKeys := make([]string, 0, len(keysToDelete))

	for _, key := range keysToDelete {
		deleted, deleteErr := s.deleteKeyAndClaimInTx(bucket, claimsBucket, key)
		if deleteErr != nil {
			return nil, deleteErr
		}

		if !deleted {
			continue
		}

		deletedKeys = append(deletedKeys, key)
		result.Deleted++
	}

	result.Done = !s.diskPrefixExistsTx(bucket, prefix)

	return deletedKeys, nil
}

// selectDiskKeysByPrefixTx selects the keys with the given prefix
// and limit in the given bucket.
// It returns the selected keys.
func (s *DiskStore) selectDiskKeysByPrefixTx(bucket *bolt.Bucket, prefix string, limit int64) []string {
	keys := make([]string, 0)

	if limit <= 0 {
		return keys
	}

	cursor := bucket.Cursor()
	prefixBytes := []byte(prefix)

	for key, _ := cursor.Seek(prefixBytes); key != nil && bytes.HasPrefix(key, prefixBytes); key, _ = cursor.Next() {
		keys = append(keys, string(key))
		if int64(len(keys)) >= limit {
			break
		}
	}

	return keys
}

// diskPrefixExistsTx checks if the prefix exists in the given bucket.
// It returns true if the prefix exists, false otherwise.
func (s *DiskStore) diskPrefixExistsTx(bucket *bolt.Bucket, prefix string) bool {
	cursor := bucket.Cursor()
	prefixBytes := []byte(prefix)

	key, _ := cursor.Seek(prefixBytes)

	return key != nil && bytes.HasPrefix(key, prefixBytes)
}
