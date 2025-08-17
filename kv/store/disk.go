package store

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"

	bolt "go.etcd.io/bbolt"
)

// DiskStore is a persistent key-value store using BoltDB with optional
// in-memory key tracking for efficient random key access.
type DiskStore struct {
	path     string
	handle   *bolt.DB
	bucket   []byte
	opened   atomic.Bool
	refCount atomic.Int64
	lock     sync.Mutex

	// In-memory key tracking (enabled when trackKeys == true).
	// This allows:
	//   - O(1) random key when no prefix
	//   - O(log n) random key with a prefix via the OSTree index
	trackKeys bool           // Whether in-memory key tracking is enabled
	keysList  []string       // Slice of all keys for O(1) random access by index
	keysMap   map[string]int // Maps key to its index in keysList for O(1) deletion
	keysLock  sync.RWMutex   // Mutex to protect concurrent access to keysList/keysMap/ost
	ost       *OSTree        // Order-statistics index (lexicographic; used for prefix random)
}

const (
	// DefaultDiskStorePath is the default path to the BoltDB database file.
	DefaultDiskStorePath = ".k6.kv"

	// DefaultKvBucket is the default bucket name for the KV store
	DefaultKvBucket = "k6"
)

// NewDiskStore creates a new DiskStore instance.
//
// When trackKeys is true we initialize an empty OSTree and the internal
// in-memory structures; otherwise, all random-key operations use a
// prefix-aware two-pass scan over Bolt cursors.
func NewDiskStore(trackKeys bool) *DiskStore {
	var idx *OSTree
	if trackKeys {
		idx = NewOSTree()
	}

	return &DiskStore{
		path:      DefaultDiskStorePath,
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

// open opens the database if it is not already open.
//
// It is safe to call this method multiple times.
// The database will only be opened once.
func (s *DiskStore) open() error {
	// Fast path: if already open, just bump the refcount.
	if s.opened.Load() {
		s.refCount.Add(1)
		return nil
	}

	// Only one goroutine performs the first open.
	s.lock.Lock()
	defer s.lock.Unlock()

	// Another goroutine may have opened it while we were waiting.
	if s.opened.Load() {
		return nil
	}

	// Open the BoltDB file.
	handler, err := bolt.Open(s.path, 0o600, nil)
	if err != nil {
		return err
	}

	// Ensure our bucket exists.
	err = handler.Update(func(tx *bolt.Tx) error {
		_, bucketErr := tx.CreateBucketIfNotExists([]byte(DefaultDiskStorePath))
		if bucketErr != nil {
			return fmt.Errorf("failed to create internal bucket: %w", bucketErr)
		}

		return nil
	})
	if err != nil {
		return err
	}

	s.handle = handler
	s.bucket = []byte(DefaultDiskStorePath)
	s.opened.Store(true)
	s.refCount.Add(1)

	// If tracking is enabled, hydrate keys slice/map and the OSTree.
	if s.trackKeys {
		if err := s.rebuildKeyListUnlocked(); err != nil {
			// If we fail to build the key list, close DB and return error.
			_ = s.handle.Close()
			s.opened.Store(false)

			return fmt.Errorf("failed to initialize key list: %w", err)
		}
	}

	return nil
}

// Get retrieves a value from the disk store.
// With tracking enabled, we first check the in-memory index (O(1)) to avoid
// an unnecessary Bolt transaction when the key is definitely missing.
func (s *DiskStore) Get(key string) (any, error) {
	// Ensure the store is open
	if err := s.open(); err != nil {
		return nil, fmt.Errorf("failed to open disk store: %w", err)
	}

	// Fast negative check via in-memory index (if enabled).
	if s.trackKeys {
		s.keysLock.RLock()
		_, exists := s.keysMap[key]
		s.keysLock.RUnlock()

		if !exists {
			return nil, fmt.Errorf("key %s not found", key)
		}
	}

	var value []byte

	// Get the value from the database within a BoltDB transaction
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
		return nil, fmt.Errorf("key %s not found", key)
	}

	// Return the raw bytes - serialization will be handled by the SerializedStore wrapper
	return value, nil
}

// Set inserts or updates the value for a given key.
// If this is a new key and tracking is enabled, we update keysList/keysMap and the OST.
func (s *DiskStore) Set(key string, value any) error {
	// Ensure the store is open
	if err := s.open(); err != nil {
		return fmt.Errorf("failed to open disk store: %w", err)
	}

	// Convert value to bytes if it's not already
	var valueBytes []byte
	switch v := value.(type) {
	case []byte:
		valueBytes = v
	case string:
		valueBytes = []byte(v)
	default:
		return fmt.Errorf("unsupported value type for disk store: %T", value)
	}

	// Update the value in the database within a BoltDB transaction
	err := s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("bucket not found")
		}

		return bucket.Put([]byte(key), valueBytes)
	})
	if err != nil {
		return fmt.Errorf("unable to insert value into disk store: %w", err)
	}

	// Update in-memory structures only if the key didn't exist before.
	if s.trackKeys {
		s.keysLock.Lock()
		if _, exists := s.keysMap[key]; !exists {
			// New key: append to keysList and record its index in keysMap
			s.keysMap[key] = len(s.keysList)
			s.keysList = append(s.keysList, key)

			// Update prefix index
			if s.ost != nil {
				s.ost.Insert(key)
			}
		}

		s.keysLock.Unlock()
	}

	return nil
}

// Delete removes a key and its value from the store.
// If tracking is enabled, removes from keysList/keysMap
// in O(1) (swap-with-last trick) and updates the OSTree.
func (s *DiskStore) Delete(key string) error {
	// Ensure the store is open
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

	// If tracking is enabled, remove the key from the in-memory structures
	if !s.trackKeys {
		return nil
	}

	s.keysLock.Lock()
	if idx, exists := s.keysMap[key]; exists {
		lastIndex := len(s.keysList) - 1
		lastKey := s.keysList[lastIndex]
		// Swap the element to delete with the last element, to enable O(1) removal
		if idx != lastIndex {
			s.keysList[idx] = lastKey
			s.keysMap[lastKey] = idx
		}

		// Remove the last element (which is now the target key)
		s.keysList = s.keysList[:lastIndex]

		delete(s.keysMap, key)

		if s.ost != nil {
			s.ost.Delete(key)
		}
	}

	s.keysLock.Unlock()

	return nil
}

// Exists checks if a given key exists.
// With tracking: in-memory O(1). Without: single Bolt read.
func (s *DiskStore) Exists(key string) (bool, error) {
	// Ensure the store is open
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

// Clear wipes all keys and values from the store.
// We drop and recreate the bucket (cheap), then reset in-memory indexes.
func (s *DiskStore) Clear() error {
	// Ensure the store is open
	if err := s.open(); err != nil {
		return fmt.Errorf("failed to open disk store: %w", err)
	}

	err := s.handle.Update(func(tx *bolt.Tx) error {
		// Drop whole bucket
		err := tx.DeleteBucket(s.bucket)
		if err != nil {
			return fmt.Errorf("failed to delete bucket %s: %w", s.bucket, err)
		}

		// Recreate it empty
		_, err = tx.CreateBucket(s.bucket)
		if err != nil {
			return fmt.Errorf("failed to create bucket %s: %w", s.bucket, err)
		}

		return err
	})
	if err != nil {
		return fmt.Errorf("unable to clear disk store: %w", err)
	}

	// Reset the in-memory key tracking structures
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

// Size returns the size of the store.
// Uses Bolt's bucket stats (O(1)).
func (s *DiskStore) Size() (int64, error) {
	// Ensure the store is open
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

// List returns all key-value pairs in the store, optionally filtered by prefix and limited to a maximum count.
// Keys are returned in lexicographical order (BoltDB cursor order).
// If prefix == "", Seek("") positions at the first key.
func (s *DiskStore) List(prefix string, limit int64) ([]Entry, error) {
	// Ensure the store is open
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
			key := string(k)

			// When a prefix is provided, stop as soon as we exit its range
			if prefix != "" && !bytes.HasPrefix(k, p) {
				break
			}

			if hasLimit && count >= limit {
				break
			}

			entries = append(entries, Entry{
				Key:   key,
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

// randomKeyWithTracking picks a random key using in-memory structures.
//   - No prefix: O(1) from keysList.
//   - With prefix: O(log n) via OSTree range + Kth selection.
func (s *DiskStore) randomKeyWithTracking(prefix string) (string, error) {
	s.keysLock.RLock()
	defer s.keysLock.RUnlock()

	// No prefix: uniform from the whole set
	if prefix == "" {
		if len(s.keysList) == 0 {
			return "", nil
		}

		return s.keysList[rand.IntN(len(s.keysList))], nil //nolint:gosec
	}

	// Prefix form: consult OSTree index
	if s.ost == nil || s.ost.Len() == 0 {
		return "", nil
	}

	l, r := s.ost.RangeBounds(prefix)
	if r <= l {
		return "", nil
	}

	idx := l + rand.IntN(r-l) //nolint:gosec
	if key, ok := s.ost.Kth(idx); ok {
		return key, nil
	}

	// Extremely unlikely unless a concurrent delete races us; be user-friendly
	return "", nil
}

// randomKeyWithoutTracking performs a two-pass prefix scan over Bolt:
// 1) count matching keys;
// 2) pick r-th and iterate again to select it.
func (s *DiskStore) randomKeyWithoutTracking(prefix string) (string, error) {
	// Pass 1: count
	count, err := s.countKeys(prefix)
	if err != nil || count == 0 {
		return "", err
	}

	// Pass 2: pick and return the r-th
	target := rand.Int64N(count) //nolint:gosec

	return s.getKeyByIndex(prefix, target)
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

// countKeys counts how many keys match a given prefix using a BoltDB cursor.
// When prefix == "", we can return KeyN directly.
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
// keys that match prefix.
// If the index is out of range due to races, it returns ""
// and nil to preserve the "no error when empty" contract.
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

// RebuildKeyList re-scans all keys from BoltDB to rebuild the in-memory key index.
// This can be used to recover from any inconsistency between the in-memory list
// and the actual data on disk (for example, after a corruption or manual intervention).
func (s *DiskStore) RebuildKeyList() error {
	if !s.trackKeys {
		return nil
	}

	if err := s.open(); err != nil {
		return fmt.Errorf("failed to open disk store: %w", err)
	}

	s.keysLock.Lock()
	defer s.keysLock.Unlock()

	if err := s.rebuildKeyListUnlocked(); err != nil {
		return fmt.Errorf("unable to rebuild keys from disk: %w", err)
	}

	return nil
}

// rebuildKeyListUnlocked (caller holds keysLock if needed) scans Bolt and
// fills keysList/keysMap, then rebuilds the OSTree (if enabled).
func (s *DiskStore) rebuildKeyListUnlocked() error {
	// We don't lock keysLock here because this is called during initialization
	// when no other operations are in progress.
	// Alternatively, we could lock it to be safe.
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

// Close closes the disk store.
func (s *DiskStore) Close() error {
	if !s.opened.Load() {
		return nil
	}

	// Only one goroutine actually closes the DB.
	s.lock.Lock()
	defer s.lock.Unlock()

	// Decrement the reference count
	newCount := s.refCount.Add(-1)
	if newCount > 0 {
		// Still in use by other instances
		return nil
	}

	// Close the database
	err := s.handle.Close()
	if err != nil {
		return err
	}

	s.opened.Store(false)

	return nil
}
