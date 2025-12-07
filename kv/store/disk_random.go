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
//   - trackKeys = false -> two-pass scan over bbolt cursor.
func (s *DiskStore) RandomKey(prefix string) (string, error) {
	err := s.ensureOpen()
	if err != nil {
		return "", err
	}

	if s.trackKeys {
		return s.randomKeyWithTracking(prefix)
	}

	return s.randomKeyWithoutTracking(prefix)
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

// randomKeyWithoutTracking performs a two-pass prefix scan over bbolt:
// 1) count matching keys;
// 2) pick the r-th and iterate again to select it.
func (s *DiskStore) randomKeyWithoutTracking(prefix string) (string, error) {
	// Pass 1: count.
	count, err := s.countKeys(prefix)
	if err != nil || count == 0 {
		return "", err
	}

	// Pass 2: pick and return the r-th.
	target := rand.Int64N(count) //nolint:gosec // math/rand/v2 is safe

	return s.getKeyByIndex(prefix, target)
}

// countKeys counts how many keys match a given prefix using a bbolt cursor.
// When prefix == "", we can return KeyN directly from stats.
func (s *DiskStore) countKeys(prefix string) (int64, error) {
	var count int64

	err := s.handle.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket)
		if b == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		// Fast path: no prefix means count all keys, use bbolt stats (O(1)).
		if prefix == "" {
			count = int64(b.Stats().KeyN)

			return nil
		}

		c := b.Cursor()
		p := []byte(prefix)

		// Iterate from prefix start until we leave the prefix range.
		// Cursor maintains lexicographic order, so we can break once prefix no longer matches.
		// Cursor.Next/Seek return both key and value; this matcher only needs keys,
		// so we intentionally ignore the value part of the tuple.
		for k, _ := c.Seek([]byte(prefix)); k != nil; k, _ = c.Next() {
			if !bytes.HasPrefix(k, p) {
				break
			}

			count++
		}

		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrDiskStoreCountFailed, err)
	}

	return count, nil
}

// getKeyByIndex returns the key at the given zero-based position among
// keys that match prefix. If the index is out of range due to races,
// it returns "" and nil to preserve the "no error when empty" contract.
func (s *DiskStore) getKeyByIndex(prefix string, index int64) (string, error) {
	var (
		key     string
		found   bool
		current int64
	)

	err := s.handle.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket)
		if b == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		c := b.Cursor()
		p := []byte(prefix)

		// Iterate through matching keys until we reach the target index.
		// Cursor iteration yields both key and value; the random selector only
		// cares about keys, so the value is deliberately ignored.
		for k, _ := c.Seek([]byte(prefix)); k != nil; k, _ = c.Next() {
			if prefix != "" && !bytes.HasPrefix(k, p) {
				break
			}

			// Found the key at target index: return it.
			if current == index {
				key = string(k)
				found = true

				return nil
			}

			current++
		}

		return nil
	})
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrDiskStoreRandomAccessFailed, err)
	}

	if !found {
		return "", nil
	}

	return key, nil
}
