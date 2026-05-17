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
			result[i] = s.getDiskEntryByKey(bucket, key)
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

	if err := s.ensureWritable(); err != nil {
		return 0, fmt.Errorf("%w: %w", ErrDiskStoreWriteFailed, err)
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

	if err := validateDeleteManyKeys(keys); err != nil {
		return nil, err
	}

	if len(keys) == 0 {
		return &DeleteManyResult{}, nil
	}

	if err := s.ensureWritable(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreDeleteFailed, err)
	}

	if s.trackKeys {
		// Keep bbolt mutation and index update as one logical operation.
		s.keysLock.Lock()
		defer s.keysLock.Unlock()
	}

	result := &DeleteManyResult{}

	err = s.handle.Update(func(tx *bolt.Tx) error {
		return s.deleteManyTx(tx, keys, result)
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

func validateDeleteManyKeys(keys []string) error {
	for i, key := range keys {
		if key == "" {
			return fmt.Errorf("%w: keys[%d]", ErrKeyEmpty, i)
		}
	}

	return nil
}

func (s *DiskStore) deleteManyTx(tx *bolt.Tx, keys []string, result *DeleteManyResult) error {
	bucket := tx.Bucket(s.bucket)
	if bucket == nil {
		return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
	}

	claimsBucket, err := s.claimsBucketForMutatorTx(tx)
	if err != nil {
		return err
	}

	for _, key := range keys {
		deleted, deleteErr := s.deleteKeyAndClaimInTx(bucket, claimsBucket, key)
		if deleteErr != nil {
			return deleteErr
		}

		if deleted {
			result.Deleted++
			continue
		}

		result.Missing++
	}

	return nil
}

func (s *DiskStore) claimsBucketForMutatorTx(tx *bolt.Tx) (*bolt.Bucket, error) {
	if s.trackedClaimsEnabled() {
		return nil, nil //nolint:nilnil // tracked mode intentionally skips bbolt claims bucket access.
	}

	return s.ensureClaimsBucket(tx)
}

func (s *DiskStore) deleteKeyAndClaimInTx(bucket *bolt.Bucket, claimsBucket *bolt.Bucket, key string) (bool, error) {
	keyBytes := []byte(key)
	if !s.diskKeyExists(bucket, keyBytes) {
		if claimsBucket != nil {
			if claimErr := s.deleteClaimForKeyTx(claimsBucket, key); claimErr != nil {
				return false, claimErr
			}
		}

		return false, nil
	}

	if err := bucket.Delete(keyBytes); err != nil {
		return false, err
	}

	if claimsBucket != nil {
		if claimErr := s.deleteClaimForKeyTx(claimsBucket, key); claimErr != nil {
			return false, claimErr
		}
	}

	return true, nil
}

func (s *DiskStore) diskKeyExists(bucket *bolt.Bucket, key []byte) bool {
	cursor := bucket.Cursor()
	foundKey, _ := cursor.Seek(key)

	return foundKey != nil && bytes.Equal(foundKey, key)
}

func (s *DiskStore) getDiskEntryByKey(bucket *bolt.Bucket, key string) *Entry {
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
