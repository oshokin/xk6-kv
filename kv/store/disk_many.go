package store

import (
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// SetMany stores all entries inside a single writable bbolt transaction.
//
// Values are normalized up-front, so type errors abort before any mutation.
func (s *DiskStore) SetMany(entries []Entry) (int64, error) {
	release, err := s.beginOperation()
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	if len(entries) == 0 {
		return 0, nil
	}

	normalizedKeys := make([]string, len(entries))

	normalizedValues := make([][]byte, len(entries))
	for i := range entries {
		valueBytes, normalizeErr := normalizeToBytes(entries[i].Value)
		if normalizeErr != nil {
			return 0, normalizeErr
		}

		normalizedKeys[i] = entries[i].Key
		normalizedValues[i] = valueBytes
	}

	if s.trackKeys {
		// Keep bbolt mutation and index update as one logical operation.
		s.keysLock.Lock()
		defer s.keysLock.Unlock()
	}

	newKeys := make([]string, 0, len(normalizedKeys))

	err = s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		for i := range normalizedKeys {
			keyBytes := []byte(normalizedKeys[i])
			existed := bucket.Get(keyBytes) != nil

			if putErr := bucket.Put(keyBytes, normalizedValues[i]); putErr != nil {
				return putErr
			}

			if !existed {
				newKeys = append(newKeys, normalizedKeys[i])
			}
		}

		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrDiskStoreWriteFailed, err)
	}

	if s.trackKeys {
		for _, key := range newKeys {
			s.addKeyIndexLocked(key)
		}
	}

	return int64(len(normalizedKeys)), nil
}
