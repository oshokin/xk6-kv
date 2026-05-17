package store

import (
	"bytes"
	"fmt"
	"slices"

	bolt "go.etcd.io/bbolt"
)

// Scan returns a page of key-value pairs, ordered lexicographically.
// If prefix is non-empty, only keys starting with prefix are considered.
// If afterKey is non-empty, scanning starts strictly after it; otherwise from the first key.
// If limit > 0, at most limit entries are returned; if limit <= 0, all matching entries are returned.
// Returns a ScanPage with Entries and NextKey (set to the last key when more results exist; empty when done).
func (s *DiskStore) Scan(prefix, afterKey string, limit int64) (*ScanPage, error) {
	release, err := s.beginOperation()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	page := &ScanPage{
		Entries: make([]Entry, 0),
	}

	err = s.handle.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		return s.fillDiskScanPage(page, bucket.Cursor(), prefix, afterKey, limit)
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreScanFailed, err)
	}

	return page, nil
}

// ScanKeys returns a page of key names, ordered lexicographically.
// If prefix is non-empty, only keys starting with prefix are considered.
// If afterKey is non-empty, scanning starts strictly after it; otherwise from the first key.
// If limit > 0, at most limit keys are returned; if limit <= 0, all matching keys are returned.
// Returns a KeyScanPage with Keys and NextKey (set to the last key when more results exist; empty when done).
func (s *DiskStore) ScanKeys(prefix, afterKey string, limit int64) (*KeyScanPage, error) {
	release, err := s.beginOperation()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	page := &KeyScanPage{
		Keys: make([]string, 0),
	}

	err = s.handle.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		return s.fillDiskScanKeysPage(page, bucket.Cursor(), prefix, afterKey, limit)
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreScanFailed, err)
	}

	return page, nil
}

// ListKeys returns matching keys ordered lexicographically.
func (s *DiskStore) ListKeys(prefix string, limit int64) ([]string, error) {
	page, err := s.ScanKeys(prefix, "", limit)
	if err != nil {
		return nil, err
	}

	return page.Keys, nil
}

// Count returns the number of keys that match prefix.
// Count("") is equivalent to Size().
func (s *DiskStore) Count(prefix string) (int64, error) {
	release, err := s.beginOperation()
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	if s.trackKeys {
		count, ok := s.countFromIndex(prefix)
		if ok {
			return count, nil
		}
	}

	total, err := s.countByCursor(prefix)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrDiskStoreCountFailed, err)
	}

	return total, nil
}

func (s *DiskStore) countFromIndex(prefix string) (int64, bool) {
	s.keysLock.RLock()
	defer s.keysLock.RUnlock()

	if s.ost == nil {
		return 0, false
	}

	if prefix == "" {
		return int64(len(s.keysList)), true
	}

	left, right := s.ost.RangeBounds(prefix)

	return int64(right - left), true
}

// fillDiskScanPage populates page with cursor results that match prefix/afterKey/limit constraints.
func (s *DiskStore) fillDiskScanPage(page *ScanPage, cursor *bolt.Cursor, prefix, afterKey string, limit int64) error {
	startKey := s.chooseDiskScanStart(prefix, afterKey)

	k, v := s.seekDiskCursor(cursor, startKey)
	if k == nil {
		return nil
	}

	var (
		prefixBytes = []byte(prefix)
		afterBytes  = []byte(afterKey)
		hasLimit    = limit > 0
	)

	for ; k != nil; k, v = cursor.Next() {
		// Skip keys <= afterKey to implement "strictly after" semantics.
		if len(afterBytes) > 0 && bytes.Compare(k, afterBytes) <= 0 {
			continue
		}

		// Once we've passed the prefix range, stop iterating.
		// bbolt cursor maintains lexicographic order, so this is safe.
		if len(prefixBytes) > 0 && !bytes.HasPrefix(k, prefixBytes) {
			break
		}

		lastKey := string(k)
		page.Entries = append(page.Entries, Entry{
			Key:   lastKey,
			Value: slices.Clone(v),
		})

		if hasLimit && int64(len(page.Entries)) >= limit {
			s.setDiskNextKey(page, cursor, prefixBytes, lastKey)

			break
		}
	}

	return nil
}

// fillDiskScanKeysPage populates page with cursor results that match prefix/afterKey/limit constraints.
func (s *DiskStore) fillDiskScanKeysPage(
	page *KeyScanPage,
	cursor *bolt.Cursor,
	prefix, afterKey string,
	limit int64,
) error {
	startKey := s.chooseDiskScanStart(prefix, afterKey)

	k, _ := s.seekDiskCursor(cursor, startKey)
	if k == nil {
		return nil
	}

	var (
		prefixBytes = []byte(prefix)
		afterBytes  = []byte(afterKey)
		hasLimit    = limit > 0
	)

	for ; k != nil; k, _ = cursor.Next() {
		// Skip keys <= afterKey to implement "strictly after" semantics.
		if len(afterBytes) > 0 && bytes.Compare(k, afterBytes) <= 0 {
			continue
		}

		// Once we've passed the prefix range, stop iterating.
		// bbolt cursor maintains lexicographic order, so this is safe.
		if len(prefixBytes) > 0 && !bytes.HasPrefix(k, prefixBytes) {
			break
		}

		lastKey := string(k)
		page.Keys = append(page.Keys, lastKey)

		if hasLimit && int64(len(page.Keys)) >= limit {
			s.setDiskKeyNextKey(page, cursor, prefixBytes, lastKey)

			break
		}
	}

	return nil
}

// chooseDiskScanStart returns the lexicographic starting point for a scan given prefix and afterKey.
// The start key is the maximum of prefix and afterKey to ensure we don't miss entries
// while respecting both constraints.
func (s *DiskStore) chooseDiskScanStart(prefix, afterKey string) string {
	if prefix == "" {
		return afterKey
	}

	// If afterKey is empty or lexicographically <= prefix, start from prefix.
	// Otherwise, afterKey is later, so start from there.
	if afterKey == "" || afterKey <= prefix {
		return prefix
	}

	return afterKey
}

// seekDiskCursor positions cursor at startKey (or the first key when empty).
func (s *DiskStore) seekDiskCursor(cursor *bolt.Cursor, startKey string) ([]byte, []byte) {
	if startKey == "" {
		return cursor.First()
	}

	return cursor.Seek([]byte(startKey))
}

// setDiskNextKey determines whether another key with the same prefix exists and records lastKey as NextKey.
// Peek at the next cursor entry without consuming it to check for more results.
func (s *DiskStore) setDiskNextKey(page *ScanPage, cursor *bolt.Cursor, prefix []byte, lastKey string) {
	if len(page.Entries) == 0 {
		return
	}

	// Position the cursor at lastKey and advance once, because pagination is exclusive.
	nextKey, _ := cursor.Next()
	if nextKey == nil {
		return
	}

	// If next key matches prefix (or no prefix filter), there are more results.
	if len(prefix) == 0 || bytes.HasPrefix(nextKey, prefix) {
		page.NextKey = lastKey
	}
}

// setDiskKeyNextKey determines whether another key with the same prefix exists and records lastKey as NextKey.
// Peek at the next cursor entry without consuming it to check for more results.
func (s *DiskStore) setDiskKeyNextKey(page *KeyScanPage, cursor *bolt.Cursor, prefix []byte, lastKey string) {
	if len(page.Keys) == 0 {
		return
	}

	// Position the cursor at lastKey and advance once, because pagination is exclusive.
	nextKey, _ := cursor.Next()
	if nextKey == nil {
		return
	}

	// If next key matches prefix (or no prefix filter), there are more results.
	if len(prefix) == 0 || bytes.HasPrefix(nextKey, prefix) {
		page.NextKey = lastKey
	}
}

// countByCursor counts prefix matches from bbolt cursor state.
func (s *DiskStore) countByCursor(prefix string) (int64, error) {
	var total int64

	err := s.handle.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		total = s.countKeysInBucket(bucket, prefix)

		return nil
	})
	if err != nil {
		return 0, err
	}

	return total, nil
}
