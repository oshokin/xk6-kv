package store

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	bolt "go.etcd.io/bbolt"
)

// DiskStore is a persistent key-value store backed by BoltDB. It optionally
// maintains in-memory key indexes for efficient random sampling.
//
// Concurrency:
//   - All exported methods are safe for concurrent use.
//   - Disk operations use a Bolt write transaction to guarantee atomicity.
//   - When trackKeys is enabled, we maintain keysList/keysMap and an OSTree
//     to keep RandomKey() fast and consistent under concurrent edits.
type DiskStore struct {
	path     string
	handle   *bolt.DB
	bucket   []byte
	opened   atomic.Bool
	refCount atomic.Int64
	lock     sync.Mutex // Serializes open/close transitions

	// In-memory key tracking (enabled when trackKeys == true).
	// This allows:
	//   - O(1) random key when no prefix.
	//   - O(log n) random key with a prefix via the OSTree index.
	trackKeys bool           // Whether in-memory key tracking is enabled
	keysList  []string       // Slice of all keys for O(1) random access by index
	keysMap   map[string]int // Maps key to its index in keysList for O(1) deletion
	keysLock  sync.RWMutex   // Mutex to protect concurrent access to keysList/keysMap/ost
	ost       *OSTree        // Order-statistics index (lexicographic; used for prefix random)
}

const (
	// DefaultDiskStorePath is the default filesystem path to the BoltDB file.
	DefaultDiskStorePath = ".k6.kv"

	// DefaultKvBucket is the default BoltDB bucket name we use inside the file.
	DefaultKvBucket = "k6"
)

// NewDiskStore constructs a DiskStore using the provided filesystem path and DefaultKvBucket.
// When path is empty, DefaultDiskStorePath is used to preserve backwards compatibility.
// If trackKeys is true, an in-memory index is initialized to accelerate RandomKey().
func NewDiskStore(trackKeys bool, path string) *DiskStore {
	var idx *OSTree
	if trackKeys {
		idx = NewOSTree()
	}

	if path == "" {
		path = DefaultDiskStorePath
	}

	diskPath := resolveDiskPath(path)

	return &DiskStore{
		path:      diskPath,
		handle:    new(bolt.DB),
		opened:    atomic.Bool{},
		refCount:  atomic.Int64{},
		lock:      sync.Mutex{},
		trackKeys: trackKeys,
		keysMap:   make(map[string]int),
		keysList:  []string{},
		keysLock:  sync.RWMutex{},
		ost:       idx,
	}
}

// resolveDiskPath normalizes user-provided paths and applies fast-fail defaults.
// Empty strings or directory-like inputs revert to the default DB file path.
func resolveDiskPath(candidate string) string {
	trimmed := strings.TrimSpace(candidate)
	if trimmed == "" {
		return DefaultDiskStorePath
	}

	cleaned := filepath.Clean(trimmed)

	// Treat directory-like inputs as requests to use the default database file.
	if strings.HasSuffix(trimmed, "/") || strings.HasSuffix(trimmed, "\\") {
		return DefaultDiskStorePath
	}

	return cleaned
}

// open ensures the DB is open and increments the reference counter once.
// It is safe to call multiple times; we only open once, but we increment the
// internal refcount for each open() to match Close() calls.
//
// Non-obvious: the fast-path returns with a bumped refCount so that Close()
// can properly balance concurrent users.
func (s *DiskStore) open() error {
	// Fast path: if already open, just bump the refcount.
	if s.opened.Load() {
		s.refCount.Add(1)
		return nil
	}

	// Only one goroutine performs the first open/initialization.
	s.lock.Lock()
	defer s.lock.Unlock()

	// Another goroutine may have opened it while we were waiting.
	if s.opened.Load() {
		s.refCount.Add(1)

		return nil
	}

	// Open the BoltDB file. If the configured path fails, fall back to the default file.
	handler, err := bolt.Open(s.path, 0o600, nil)
	if err != nil {
		if s.path != DefaultDiskStorePath {
			handler, err = bolt.Open(DefaultDiskStorePath, 0o600, nil)
			if err != nil {
				return err
			}

			s.path = DefaultDiskStorePath
		} else {
			return err
		}
	}

	// Ensure the application bucket exists.
	err = handler.Update(func(tx *bolt.Tx) error {
		_, bucketErr := tx.CreateBucketIfNotExists([]byte(DefaultKvBucket))
		if bucketErr != nil {
			return fmt.Errorf("failed to create internal bucket %q: %w", DefaultKvBucket, bucketErr)
		}

		return nil
	})
	if err != nil {
		_ = handler.Close()

		return err
	}

	s.handle = handler
	s.bucket = []byte(DefaultKvBucket)
	s.opened.Store(true)
	s.refCount.Add(1)

	// If tracking is enabled, hydrate keys slice/map and the OSTree.
	if s.trackKeys {
		s.keysLock.Lock()
		err := s.rebuildKeyListLocked()
		s.keysLock.Unlock()

		if err != nil {
			// If we fail to build the key list, close DB and return error.
			_ = s.handle.Close()
			s.opened.Store(false)

			return fmt.Errorf("failed to initialize key list: %w", err)
		}
	}

	return nil
}

// Get retrieves the raw []byte value from the disk store.
//
// With tracking enabled, we first check the in-memory index (O(1)) to avoid
// an unnecessary Bolt transaction when the key is definitely missing.
func (s *DiskStore) Get(key string) (any, error) {
	// Ensure the store is open.
	if err := s.open(); err != nil {
		return nil, fmt.Errorf("failed to open disk store: %w", err)
	}

	// Fast negative check via in-memory index (if enabled).
	if s.trackKeys {
		s.keysLock.RLock()
		_, exists := s.keysMap[key]
		s.keysLock.RUnlock()

		if !exists {
			return nil, fmt.Errorf("key %q not found", key)
		}
	}

	var value []byte

	// Get the value from the database within a BoltDB transaction.
	err := s.handle.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", s.bucket)
		}

		value = bucket.Get([]byte(key))

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("unable to get value from disk store: %w", err)
	}

	if value == nil {
		return nil, fmt.Errorf("key %q not found", key)
	}

	// Return the raw bytes - serialization will be handled by the SerializedStore wrapper.
	return value, nil
}

// Set inserts or updates the value for a given key.
// If this is a new key and tracking is enabled, we update indexes.
func (s *DiskStore) Set(key string, value any) error {
	// Ensure the store is open.
	if err := s.open(); err != nil {
		return fmt.Errorf("failed to open disk store: %w", err)
	}

	// Convert value to bytes if it's not already.
	valueBytes, err := normalizeToBytes(value)
	if err != nil {
		return err
	}

	var existed bool

	if s.trackKeys {
		// Lightweight existence check to decide whether to update indexes later.
		s.keysLock.RLock()
		_, existed = s.keysMap[key]
		s.keysLock.RUnlock()
	}

	// Update the value in the database within a BoltDB transaction.
	err = s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return errors.New("bucket not found")
		}

		return bucket.Put([]byte(key), valueBytes)
	})
	if err != nil {
		return fmt.Errorf("unable to insert value into disk store: %w", err)
	}

	// Update indexes only if the key was new.
	if s.trackKeys && !existed {
		s.keysLock.Lock()
		s.addKeyIndexLocked(key)
		s.keysLock.Unlock()
	}

	return nil
}

// IncrementBy atomically adds delta to the integer value stored at key.
// Absent keys are treated as 0. Values must be decimal ASCII int64.
// Returns the new value as int64.
func (s *DiskStore) IncrementBy(key string, delta int64) (int64, error) {
	// Ensure the store is open.
	if err := s.open(); err != nil {
		return 0, fmt.Errorf("failed to open disk store: %w", err)
	}

	var (
		newVal int64
		wasNew bool
	)

	// Update the value in the database within a BoltDB transaction.
	err := s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return errors.New("bucket not found")
		}

		// Get current value.
		current := bucket.Get([]byte(key))

		// Parse current value or start from 0.
		var currentInt int64

		if current != nil {
			var err error

			currentInt, err = strconv.ParseInt(string(current), 10, 64)
			if err != nil {
				return fmt.Errorf("value at %q is not a valid integer: %w", key, err)
			}
		} else {
			wasNew = true
		}

		// Calculate new value.
		currentInt += delta
		newVal = currentInt

		return bucket.Put([]byte(key), []byte(strconv.FormatInt(currentInt, 10)))
	})
	if err != nil {
		return 0, fmt.Errorf("unable to increment value in disk store: %w", err)
	}

	// Update tracking if this was a new key.
	if s.trackKeys && wasNew {
		s.keysLock.Lock()
		s.addKeyIndexLocked(key)
		s.keysLock.Unlock()
	}

	return newVal, nil
}

// GetOrSet returns the existing value (loaded=true) if key is present,
// otherwise stores "value" and returns it (loaded=false).
func (s *DiskStore) GetOrSet(key string, value any) (actual any, loaded bool, err error) {
	// Ensure the store is open.
	if err := s.open(); err != nil {
		return nil, false, fmt.Errorf("failed to open disk store: %w", err)
	}

	// Convert value to bytes if it's not already.
	valueBytes, err := normalizeToBytes(value)
	if err != nil {
		return nil, false, err
	}

	var (
		result []byte
		exists bool
	)

	err = s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return errors.New("bucket not found")
		}

		// Check if key exists.
		if got := bucket.Get([]byte(key)); got != nil {
			exists = true

			result = append([]byte(nil), got...)

			return nil
		}

		// Key doesn't exist, set it.
		return bucket.Put([]byte(key), valueBytes)
	})
	if err != nil {
		return nil, false, fmt.Errorf("unable to get or set value in disk store: %w", err)
	}

	if exists {
		return result, true, nil
	}

	// Update tracking if enabled.
	if s.trackKeys {
		s.keysLock.Lock()
		s.addKeyIndexLocked(key)
		s.keysLock.Unlock()
	}

	return valueBytes, false, nil
}

// Swap replaces the value and returns the previous value (if existed) and whether it existed.
func (s *DiskStore) Swap(key string, value any) (previous any, loaded bool, err error) {
	// Ensure the store is open.
	if err := s.open(); err != nil {
		return nil, false, fmt.Errorf("failed to open disk store: %w", err)
	}

	// Convert value to bytes if it's not already.
	valueBytes, err := normalizeToBytes(value)
	if err != nil {
		return nil, false, err
	}

	var (
		prev    []byte
		existed bool
	)

	err = s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return errors.New("bucket not found")
		}

		// Get previous value (copy for safety).
		if current := bucket.Get([]byte(key)); current != nil {
			existed = true

			prev = append([]byte(nil), current...)
		}

		return bucket.Put([]byte(key), valueBytes)
	})
	if err != nil {
		return nil, false, fmt.Errorf("unable to swap value in disk store: %w", err)
	}

	// Update tracking if this is a new key.
	if s.trackKeys && !existed {
		s.keysLock.Lock()
		s.addKeyIndexLocked(key)
		s.keysLock.Unlock()
	}

	// Return a real nil interface when key was not present.
	if !existed {
		return nil, false, nil
	}

	return prev, true, nil
}

// CompareAndSwap replaces value only if current equals 'old'. Returns true if swapped.
func (s *DiskStore) CompareAndSwap(key string, oldValue any, newValue any) (bool, error) {
	// Ensure the store is open.
	if err := s.open(); err != nil {
		return false, fmt.Errorf("failed to open disk store: %w", err)
	}

	// Convert old value to bytes if it's not already.
	oldBytes, err := normalizeToBytes(oldValue)
	if err != nil {
		return false, err
	}

	// Convert new value to bytes if it's not already.
	newBytes, err := normalizeToBytes(newValue)
	if err != nil {
		return false, err
	}

	var swapped bool

	err = s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return errors.New("bucket not found")
		}

		// Get current value.
		current := bucket.Get([]byte(key))

		// Compare current with old.
		if (current == nil && oldValue != nil) ||
			(current != nil && oldValue == nil) ||
			(current != nil && oldValue != nil && !bytes.Equal(current, oldBytes)) {
			return nil
		}

		// Values match, perform swap.
		swapped = true

		return bucket.Put([]byte(key), newBytes)
	})
	if err != nil {
		return false, fmt.Errorf("unable to compare and swap in disk store: %w", err)
	}

	// Indexes unchanged (key existed before and after).
	return swapped, nil
}

// Delete removes a key and its value from the store.
// If tracking is enabled, removes from keysList/keysMap in O(1)
// (swap-with-last trick) and updates the OSTree.
func (s *DiskStore) Delete(key string) error {
	// Ensure the store is open.
	if err := s.open(); err != nil {
		return fmt.Errorf("failed to open disk store: %w", err)
	}

	// Remove from BoltDB.
	err := s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", s.bucket)
		}

		return bucket.Delete([]byte(key))
	})
	if err != nil {
		return fmt.Errorf("unable to delete value from disk store: %w", err)
	}

	// If tracking is enabled, remove the key from the in-memory structures.
	if s.trackKeys {
		s.keysLock.Lock()
		s.removeKeyIndexLocked(key)
		s.keysLock.Unlock()
	}

	return nil
}

// Exists checks if a given key exists.
// With tracking: in-memory O(1). Without: single Bolt read.
func (s *DiskStore) Exists(key string) (bool, error) {
	// Ensure the store is open.
	if err := s.open(); err != nil {
		return false, fmt.Errorf("failed to open disk store: %w", err)
	}

	if s.trackKeys {
		s.keysLock.RLock()
		_, exists := s.keysMap[key]
		s.keysLock.RUnlock()

		return exists, nil
	}

	var exists bool

	err := s.handle.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", s.bucket)
		}

		exists = bucket.Get([]byte(key)) != nil

		return nil
	})
	if err != nil {
		return exists, fmt.Errorf("unable to check if key exists in disk store: %w", err)
	}

	return exists, nil
}

// DeleteIfExists deletes key if it exists. Returns true if deleted.
func (s *DiskStore) DeleteIfExists(key string) (bool, error) {
	// Ensure the store is open.
	if err := s.open(); err != nil {
		return false, fmt.Errorf("failed to open disk store: %w", err)
	}

	var deleted bool

	err := s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return errors.New("bucket not found")
		}

		// Check if key exists.
		if bucket.Get([]byte(key)) == nil {
			return nil
		}

		// Delete the key.
		err := bucket.Delete([]byte(key))
		if err != nil {
			return err
		}

		deleted = true

		return nil
	})
	if err != nil {
		return false, fmt.Errorf("unable to delete if exists in disk store: %w", err)
	}

	// Update tracking structures if deleted.
	if deleted && s.trackKeys {
		s.keysLock.Lock()
		s.removeKeyIndexLocked(key)
		s.keysLock.Unlock()
	}

	return deleted, nil
}

// CompareAndDelete deletes key only if current equals "oldValue".
// Returns true if deleted.
func (s *DiskStore) CompareAndDelete(key string, oldValue any) (bool, error) {
	// Ensure the store is open.
	if err := s.open(); err != nil {
		return false, fmt.Errorf("failed to open disk store: %w", err)
	}

	oldBytes, err := normalizeToBytes(oldValue)
	if err != nil {
		return false, err
	}

	var deleted bool

	err = s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return errors.New("bucket not found")
		}

		// Get current value.
		current := bucket.Get([]byte(key))

		// Compare current with old.
		if (current == nil && oldValue != nil) ||
			(current != nil && oldValue == nil) ||
			(current != nil && oldValue != nil && !bytes.Equal(current, oldBytes)) {
			return nil
		}

		// Values match, perform deletion.
		err := bucket.Delete([]byte(key))
		if err != nil {
			return err
		}

		deleted = true

		return nil
	})
	if err != nil {
		return false, fmt.Errorf("unable to compare and delete in disk store: %w", err)
	}

	// Update tracking structures if deleted.
	if deleted && s.trackKeys {
		s.keysLock.Lock()
		s.removeKeyIndexLocked(key)
		s.keysLock.Unlock()
	}

	return deleted, nil
}

// Clear wipes all keys and values from the store.
// We drop and recreate the bucket (cheap), then reset in-memory indexes.
func (s *DiskStore) Clear() error {
	// Ensure the store is open.
	if err := s.open(); err != nil {
		return fmt.Errorf("failed to open disk store: %w", err)
	}

	err := s.handle.Update(func(tx *bolt.Tx) error {
		// Drop whole bucket.
		err := tx.DeleteBucket(s.bucket)
		if err != nil {
			return fmt.Errorf("failed to delete bucket %s: %w", s.bucket, err)
		}

		// Recreate it empty.
		_, err = tx.CreateBucket(s.bucket)
		if err != nil {
			return fmt.Errorf("failed to create bucket %s: %w", s.bucket, err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("unable to clear disk store: %w", err)
	}

	// Reset the in-memory key tracking structures.
	if s.trackKeys {
		s.keysLock.Lock()

		s.keysList = []string{}
		s.keysMap = make(map[string]int)

		if s.ost != nil {
			s.ost = NewOSTree()
		}

		s.keysLock.Unlock()
	}

	return nil
}

// Size returns the number of keys in the store (O(1) from Bolt stats).
func (s *DiskStore) Size() (int64, error) {
	// Ensure the store is open.
	if err := s.open(); err != nil {
		return 0, fmt.Errorf("failed to open disk store: %w", err)
	}

	var size int64

	err := s.handle.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", s.bucket)
		}

		size = int64(bucket.Stats().KeyN)

		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("unable to get size of disk store: %w", err)
	}

	return size, nil
}

// List returns key-value pairs filtered by prefix and limited by count.
// Keys are returned in lexicographical order (Bolt cursor order).
// If prefix == "", Seek("") positions at the first key.
func (s *DiskStore) List(prefix string, limit int64) ([]Entry, error) {
	// Ensure the store is open.
	if err := s.open(); err != nil {
		return nil, fmt.Errorf("failed to open disk store: %w", err)
	}

	var entries []Entry

	err := s.handle.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", s.bucket)
		}

		var (
			count    int64
			hasLimit = limit > 0
			c        = bucket.Cursor()
			p        = []byte(prefix)
		)

		for k, v := c.Seek([]byte(prefix)); k != nil; k, v = c.Next() {
			// When a prefix is provided, stop as soon as we exit its range.
			if prefix != "" && !bytes.HasPrefix(k, p) {
				break
			}

			if hasLimit && count >= limit {
				break
			}

			entries = append(entries, Entry{
				Key:   string(k),
				Value: v,
			})
			count++
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("unable to list entries from disk store: %w", err)
	}

	return entries, nil
}

// RandomKey returns a random key, optionally filtered by prefix.
// Empty store or no matching prefix => "", nil.
//
// Paths:
//   - trackKeys = true:
//     prefix==""  -> O(1) from keysList.
//     prefix!=""  -> O(log n) via OSTree.
//   - trackKeys = false -> two-pass scan over BoltDB cursor.
func (s *DiskStore) RandomKey(prefix string) (string, error) {
	if err := s.open(); err != nil {
		return "", err
	}

	if s.trackKeys {
		return s.randomKeyWithTracking(prefix)
	}

	return s.randomKeyWithoutTracking(prefix)
}

// RebuildKeyList re-scans all keys from BoltDB to rebuild the in-memory key index.
// Useful after crashes or manual intervention. No-op if tracking is disabled.
func (s *DiskStore) RebuildKeyList() error {
	if !s.trackKeys {
		return nil
	}

	if err := s.open(); err != nil {
		return fmt.Errorf("failed to open disk store: %w", err)
	}

	s.keysLock.Lock()
	defer s.keysLock.Unlock()

	if err := s.rebuildKeyListLocked(); err != nil {
		return fmt.Errorf("unable to rebuild keys from disk: %w", err)
	}

	return nil
}

// Close decrements an internal reference count and closes the DB when it
// reaches zero.
// Subsequent operations can re-open the DB on demand.
func (s *DiskStore) Close() error {
	// Only one goroutine actually closes the DB.
	s.lock.Lock()
	defer s.lock.Unlock()

	// If it's already closed, do nothing.
	if !s.opened.Load() {
		return nil
	}

	// Decrement the reference count exactly once per Close call on an open DB.
	newCount := s.refCount.Add(-1)

	// If there are still users, keep the DB open.
	if newCount > 0 {
		return nil
	}

	// We are the last closer: actually close the handle.
	if newCount < 0 {
		s.refCount.Store(0)
	}

	if err := s.handle.Close(); err != nil {
		return err
	}

	s.opened.Store(false)

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

		return s.keysList[rand.IntN(len(s.keysList))], nil //nolint:gosec // math/rand/v2 is safe
	}

	// Prefix form: consult OSTree index.
	if s.ost == nil || s.ost.Len() == 0 {
		return "", nil
	}

	l, r := s.ost.RangeBounds(prefix)
	if r <= l {
		return "", nil
	}

	idx := l + rand.IntN(r-l) //nolint:gosec // math/rand/v2 is safe
	if key, ok := s.ost.Kth(idx); ok {
		return key, nil
	}

	// Extremely unlikely unless a concurrent delete races us; be user-friendly.
	return "", nil
}

// randomKeyWithoutTracking performs a two-pass prefix scan over Bolt:
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

// countKeys counts how many keys match a given prefix using a BoltDB cursor.
// When prefix == "", we can return KeyN directly from stats.
func (s *DiskStore) countKeys(prefix string) (int64, error) {
	var count int64

	err := s.handle.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket)
		if b == nil {
			return fmt.Errorf("bucket %s not found", s.bucket)
		}

		if prefix == "" {
			count = int64(b.Stats().KeyN)

			return nil
		}

		c := b.Cursor()
		p := []byte(prefix)

		for k, _ := c.Seek([]byte(prefix)); k != nil; k, _ = c.Next() {
			if !bytes.HasPrefix(k, p) {
				break
			}

			count++
		}

		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("failed to count keys: %w", err)
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
			return fmt.Errorf("bucket %s not found", s.bucket)
		}

		c := b.Cursor()
		p := []byte(prefix)

		for k, _ := c.Seek([]byte(prefix)); k != nil; k, _ = c.Next() {
			if prefix != "" && !bytes.HasPrefix(k, p) {
				break
			}

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
		return "", fmt.Errorf("failed to get key by index: %w", err)
	}

	if !found {
		return "", nil
	}

	return key, nil
}

// rebuildKeyListLocked scans Bolt and rebuilds keysList/keysMap and the OSTree.
// Caller must hold keysLock.
func (s *DiskStore) rebuildKeyListLocked() error {
	newKeys := []string{}
	newMap := make(map[string]int)

	err := s.handle.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", s.bucket)
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

// addKeyIndexLocked inserts key into in-memory indexes (O(1)).
//
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
//
// Precondition: caller holds keysLock.
func (s *DiskStore) removeKeyIndexLocked(key string) {
	// Check if key exists.
	idx, exists := s.keysMap[key]
	if !exists {
		return
	}

	// Swap with last element for O(1) deletion.
	lastIndex := len(s.keysList) - 1
	if idx != lastIndex {
		moved := s.keysList[lastIndex]

		s.keysList[idx] = moved
		s.keysMap[moved] = idx
	}

	// Remove last element.
	s.keysList = s.keysList[:lastIndex]
	delete(s.keysMap, key)

	// Update OST for prefix-based operations.
	if s.ost != nil {
		s.ost.Delete(key)
	}
}
