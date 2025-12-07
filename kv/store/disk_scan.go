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
	// Ensure the store is open.
	if err := s.ensureOpen(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}

	page := &ScanPage{
		Entries: make([]Entry, 0),
	}

	err := s.handle.View(func(tx *bolt.Tx) error {
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

	// Peek ahead: cursor is already positioned after lastKey from the loop.
	nextKey, _ := cursor.Next()
	if nextKey == nil {
		return
	}

	// If next key matches prefix (or no prefix filter), there are more results.
	if len(prefix) == 0 || bytes.HasPrefix(nextKey, prefix) {
		page.NextKey = lastKey
	}
}
