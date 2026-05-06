package store

import (
	"bytes"
	"fmt"
	"math/rand/v2"

	bolt "go.etcd.io/bbolt"
)

// RandomKey returns a random key, optionally filtered by prefix.
// Empty store or no matching prefix => "", nil.
// Paths:
//   - trackKeys = true:
//     prefix==""  -> O(1) from keysList.
//     prefix!=""  -> O(log n) via OSTree.
//   - trackKeys = false -> two-pass scan in a single bbolt snapshot.
func (s *DiskStore) RandomKey(prefix string) (string, error) {
	release, err := s.beginOperation()
	if err != nil {
		return "", err
	}
	defer release()

	if s.trackKeys {
		return s.randomKeyWithTracking(prefix)
	}

	return s.randomKeyWithoutTracking(prefix)
}

// RandomKeys returns random key names matching prefix.
func (s *DiskStore) RandomKeys(prefix string, count int64, unique bool) ([]string, error) {
	if count <= 0 {
		return []string{}, nil
	}

	release, err := s.beginOperation()
	if err != nil {
		return nil, err
	}
	defer release()

	if s.trackKeys {
		keys, usedTracking := s.randomKeysWithTracking(prefix, count, unique)
		if usedTracking {
			return keys, nil
		}
	}

	return s.randomKeysWithoutTracking(prefix, count, unique)
}

func (s *DiskStore) randomKeysWithTracking(prefix string, count int64, unique bool) ([]string, bool) {
	s.keysLock.RLock()
	defer s.keysLock.RUnlock()

	if s.ost == nil || s.ost.Len() == 0 {
		return []string{}, true
	}

	left, right := 0, s.ost.Len()
	if prefix != "" {
		left, right = s.ost.RangeBounds(prefix)
	}

	total := right - left
	if total <= 0 {
		return []string{}, true
	}

	// For near-full unique sampling, linear cursor scan + in-memory sampling
	// is usually faster than repeated Kth(rank) lookups.
	if shouldFallbackToCursorRandomKeys(unique, count, total) {
		return nil, false
	}

	if unique {
		return s.randomUniqueKeysFromTrackingRange(left, total, count), true
	}

	return s.randomKeysWithReplacementFromTrackingRange(left, total, count), true
}

// shouldFallbackToCursorRandomKeys is intentionally benchmark-driven.
// For small unique samples (K << M), indexed rank selection via Kth() is faster
// because it avoids scanning the full matching range.
// For near-full unique samples (K ~ M), returning most of the range is
// inherently linear and a cursor scan is usually faster than many Kth() lookups.
// Tune thresholds only with stable benchmark evidence.
func shouldFallbackToCursorRandomKeys(unique bool, count int64, total int) bool {
	if !unique || total <= 0 {
		return false
	}

	// For small prefix ranges, keeping everything in the indexed path is simpler
	// and avoids extra bbolt cursor scans.
	const minRangeForFallback = 512
	if total < minRangeForFallback {
		return false
	}

	// Near-full unique sample (K ~ M): prefer cursor path.
	half := int64(total) / 2
	if int64(total)%2 != 0 {
		half++
	}

	return count >= half
}

func (s *DiskStore) randomUniqueKeysFromTrackingRange(left, total int, count int64) []string {
	if count >= int64(total) {
		keys := make([]string, 0, total)

		for i := range total {
			key, ok := s.ost.Kth(left + i)
			if ok {
				keys = append(keys, key)
			}
		}

		return shuffleKeys(keys)
	}

	offsets := sampleUniqueOffsets(total, int(count))
	keys := make([]string, 0, len(offsets))

	for _, offset := range offsets {
		key, ok := s.ost.Kth(left + offset)
		if ok {
			keys = append(keys, key)
		}
	}

	return keys
}

func (s *DiskStore) randomKeysWithReplacementFromTrackingRange(left, total int, count int64) []string {
	keys := make([]string, 0, count)

	for range count {
		//nolint:gosec // math/rand/v2 is enough for non-crypto sampling.
		offset := rand.IntN(total)

		key, ok := s.ost.Kth(left + offset)
		if ok {
			keys = append(keys, key)
		}
	}

	return keys
}

func (s *DiskStore) randomKeysWithoutTracking(prefix string, count int64, unique bool) ([]string, error) {
	page := &KeyScanPage{
		Keys: make([]string, 0),
	}

	err := s.handle.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		return s.fillDiskScanKeysPage(page, bucket.Cursor(), prefix, "", 0)
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreScanFailed, err)
	}

	return sampleKeys(page.Keys, count, unique), nil
}

// addKeyIndexLocked inserts key into in-memory indexes (O(1)).
// Precondition: caller holds keysLock.
func (s *DiskStore) addKeyIndexLocked(key string) {
	// Check if key exists.
	if _, exists := s.keysMap[key]; exists {
		return
	}

	// New key: append to keysList and record its index in keysMap.
	s.keysMap[key] = len(s.keysList)
	s.keysList = append(s.keysList, key)

	// Update prefix index.
	if s.ost != nil {
		s.ost.Insert(key)
	}
}

// removeKeyIndexLocked removes key from in-memory indexes using swap-delete.
// Precondition: caller holds keysLock.
func (s *DiskStore) removeKeyIndexLocked(key string) {
	// Check if key exists.
	idx, exists := s.keysMap[key]
	if !exists {
		return
	}

	// Defensive guard against accidental index corruption.
	// Even if keysMap/keysList diverge, delete path must not panic.
	if len(s.keysList) == 0 || idx < 0 || idx >= len(s.keysList) {
		delete(s.keysMap, key)

		if s.ost != nil {
			s.ost.Delete(key)
		}

		return
	}

	// Swap-with-last deletion to avoid shifting array elements (O(1) instead of O(n)).
	lastIndex := len(s.keysList) - 1
	if idx != lastIndex {
		// Swap target with last element.
		moved := s.keysList[lastIndex]

		s.keysList[idx] = moved
		s.keysMap[moved] = idx
	}

	// Truncate list and remove from map.
	s.keysList = s.keysList[:lastIndex]
	delete(s.keysMap, key)

	// Update OST for prefix-based operations.
	if s.ost != nil {
		s.ost.Delete(key)
	}
}

// rebuildKeyListLocked scans bbolt and rebuilds keysList/keysMap and the OSTree.
// Caller must hold keysLock.
func (s *DiskStore) rebuildKeyListLocked() error {
	newKeys := []string{}
	newMap := make(map[string]int)

	err := s.handle.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		return bucket.ForEach(func(k, _ []byte) error {
			keyStr := string(k)
			newMap[keyStr] = len(newKeys)
			newKeys = append(newKeys, keyStr)

			return nil
		})
	})
	if err != nil {
		return err
	}

	s.keysList = newKeys
	s.keysMap = newMap

	if s.ost != nil {
		s.ost = NewOSTree()

		for _, k := range newKeys {
			s.ost.Insert(k)
		}
	}

	return nil
}

// randomKeyWithTracking picks a random key using in-memory structures.
//   - No prefix: O(1) from keysList.
//   - With prefix: O(log n) via OSTree range + Kth selection.
func (s *DiskStore) randomKeyWithTracking(prefix string) (string, error) {
	s.keysLock.RLock()
	defer s.keysLock.RUnlock()

	// No prefix: uniform from the whole set.
	if prefix == "" {
		if len(s.keysList) == 0 {
			return "", nil
		}

		return s.keysList[rand.IntN(len(s.keysList))], nil //nolint:gosec // math/rand/v2 is safe.
	}

	// Prefix form: consult OSTree index.
	if s.ost == nil || s.ost.Len() == 0 {
		return "", nil
	}

	// OSTree provides O(log n) range bounds for prefix.
	l, r := s.ost.RangeBounds(prefix)
	if r <= l {
		return "", nil
	}

	// Pick random index in range and retrieve Kth element.
	idx := l + rand.IntN(r-l) //nolint:gosec // math/rand/v2 is safe.
	if key, ok := s.ost.Kth(idx); ok {
		return key, nil
	}

	// Kth failed - extremely unlikely unless concurrent delete raced us.
	return "", nil
}

// randomKeyWithoutTracking performs a two-pass prefix scan inside one bbolt
// View transaction:
// 1) count matching keys;
// 2) pick the r-th and iterate again to select it.
// Running both passes in one View keeps count and selection on the same snapshot.
func (s *DiskStore) randomKeyWithoutTracking(prefix string) (string, error) {
	var selected string

	err := s.handle.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		// Pass 1: count matches in this snapshot.
		count := countKeysInBucket(bucket, prefix)
		if count == 0 {
			return nil
		}

		// Pass 2: pick and return the r-th key from the same snapshot.
		target := rand.Int64N(count) //nolint:gosec // math/rand/v2 is safe

		key, found := keyByIndexInBucket(bucket, prefix, target)
		if found {
			selected = key
		}

		return nil
	})
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrDiskStoreRandomAccessFailed, err)
	}

	return selected, nil
}

// countKeysInBucket counts how many keys match prefix using the provided bucket.
// When prefix == "", it returns KeyN from bucket stats.
func countKeysInBucket(bucket *bolt.Bucket, prefix string) int64 {
	if prefix == "" {
		return int64(bucket.Stats().KeyN)
	}

	var (
		count       int64
		cursor      = bucket.Cursor()
		prefixBytes = []byte(prefix)
	)

	// Iterate from prefix start until we leave the prefix range.
	// Cursor maintains lexicographic order, so we can break once prefix no longer matches.
	for k, _ := cursor.Seek(prefixBytes); k != nil; k, _ = cursor.Next() {
		if !bytes.HasPrefix(k, prefixBytes) {
			break
		}

		count++
	}

	return count
}

// keyByIndexInBucket returns the key at the zero-based index among keys that
// match prefix in the provided bucket snapshot.
func keyByIndexInBucket(bucket *bolt.Bucket, prefix string, index int64) (string, bool) {
	var (
		current     int64
		cursor      = bucket.Cursor()
		prefixBytes = []byte(prefix)
	)

	// Iterate through matching keys until we reach the target index.
	// Cursor iteration yields both key and value; we only need keys.
	for k, _ := cursor.Seek(prefixBytes); k != nil; k, _ = cursor.Next() {
		if prefix != "" && !bytes.HasPrefix(k, prefixBytes) {
			break
		}

		// Found the key at target index: return it.
		if current == index {
			return string(k), true
		}

		current++
	}

	return "", false
}
