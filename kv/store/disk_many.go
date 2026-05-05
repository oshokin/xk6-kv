package store

import (
	"bytes"
	"fmt"
	"slices"

	bolt "go.etcd.io/bbolt"
)

// GetMany returns entries in the same order as keys.
// Missing keys are represented as nil entries.
func (s *DiskStore) GetMany(keys []string) ([]*Entry, error) {
	release, err := s.beginOperation()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	result := make([]*Entry, len(keys))

	err = s.handle.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		for i, key := range keys {
			result[i] = getDiskEntryByKey(bucket, key)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreReadFailed, err)
	}

	return result, nil
}

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
		if entries[i].Key == "" {
			return 0, fmt.Errorf("%w: entries[%d]", ErrKeyEmpty, i)
		}

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

// DeleteMany deletes explicit non-empty keys and returns delete/missing counts.
//
// The disk backend applies all mutations in a single writable bbolt transaction.
func (s *DiskStore) DeleteMany(keys []string) (*DeleteManyResult, error) {
	release, err := s.beginOperation()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	for i, key := range keys {
		if key == "" {
			return nil, fmt.Errorf("%w: keys[%d]", ErrKeyEmpty, i)
		}
	}

	if len(keys) == 0 {
		return &DeleteManyResult{}, nil
	}

	if s.trackKeys {
		// Keep bbolt mutation and index update as one logical operation.
		s.keysLock.Lock()
		defer s.keysLock.Unlock()
	}

	result := &DeleteManyResult{}

	err = s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		claimsBucket, err := ensureClaimsBucket(tx)
		if err != nil {
			return err
		}

		for _, key := range keys {
			keyBytes := []byte(key)

			if !diskKeyExists(bucket, keyBytes) {
				// Keep behavior aligned with single-key delete paths: stale claims
				// should be cleaned even when the user key is already absent.
				if claimErr := deleteClaimForKeyTx(claimsBucket, key); claimErr != nil {
					return claimErr
				}

				result.Missing++

				continue
			}

			if deleteErr := bucket.Delete(keyBytes); deleteErr != nil {
				return deleteErr
			}

			if claimErr := deleteClaimForKeyTx(claimsBucket, key); claimErr != nil {
				return claimErr
			}

			result.Deleted++
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreDeleteFailed, err)
	}

	if s.trackKeys {
		// Defensive cleanup: remove all requested keys from the in-memory
		// index (idempotent) so stale-positive index entries are also cleared.
		for _, key := range keys {
			s.removeKeyIndexLocked(key)
		}
	}

	return result, nil
}

func diskKeyExists(bucket *bolt.Bucket, key []byte) bool {
	cursor := bucket.Cursor()
	foundKey, _ := cursor.Seek(key)

	return foundKey != nil && bytes.Equal(foundKey, key)
}

func getDiskEntryByKey(bucket *bolt.Bucket, key string) *Entry {
	keyBytes := []byte(key)

	cursor := bucket.Cursor()
	foundKey, value := cursor.Seek(keyBytes)

	if foundKey == nil || !bytes.Equal(foundKey, keyBytes) {
		return nil
	}

	return &Entry{
		Key:   key,
		Value: slices.Clone(value),
	}
}
