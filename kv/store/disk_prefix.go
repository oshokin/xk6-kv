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

	if prefix == "" {
		return nil, fmt.Errorf("%w: prefix must be non-empty", ErrKVOptionsInvalid)
	}

	if limit <= 0 {
		return nil, fmt.Errorf("%w: limit must be positive: %d", ErrKVOptionsInvalid, limit)
	}

	if s.trackKeys {
		// Keep bbolt mutation and index update as one logical operation.
		s.keysLock.Lock()
		defer s.keysLock.Unlock()
	}

	result := &DeleteByPrefixResult{}
	deletedKeys := make([]string, 0)

	err = s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		claimsBucket, err := ensureClaimsBucket(tx)
		if err != nil {
			return err
		}

		keysToDelete := selectDiskKeysByPrefixTx(bucket, prefix, limit)
		for _, key := range keysToDelete {
			if deleteErr := bucket.Delete([]byte(key)); deleteErr != nil {
				return deleteErr
			}

			if claimErr := deleteClaimForKeyTx(claimsBucket, key); claimErr != nil {
				return claimErr
			}

			deletedKeys = append(deletedKeys, key)
			result.Deleted++
		}

		result.Done = !diskPrefixExistsTx(bucket, prefix)

		return nil
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

func selectDiskKeysByPrefixTx(bucket *bolt.Bucket, prefix string, limit int64) []string {
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

func diskPrefixExistsTx(bucket *bolt.Bucket, prefix string) bool {
	cursor := bucket.Cursor()
	prefixBytes := []byte(prefix)

	key, _ := cursor.Seek(prefixBytes)

	return key != nil && bytes.HasPrefix(key, prefixBytes)
}
