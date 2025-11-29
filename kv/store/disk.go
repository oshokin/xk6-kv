package store

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
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
	// path is the filesystem path to the BoltDB file.
	path string
	// handle is the underlying BoltDB handle.
	handle *bolt.DB
	// bucket is the internal BoltDB bucket name.
	bucket []byte
	// trackKeys is a flag to track whether in-memory key tracking is enabled.
	trackKeys bool
	// keysList is a slice of all keys for O(1) random access by index.
	keysList []string
	// keysMap is a map from key to its index in keysList for O(1) deletion.
	keysMap map[string]int
	// keysLock is a mutex to protect concurrent access to keysList/keysMap/ost.
	keysLock sync.RWMutex
	// ost is an order-statistics index (lexicographic; used for prefix random).
	ost *OSTree
	// opened is a flag to track whether the store is open.
	// Uses atomic to allow lock-free reads in ensureOpen().
	opened atomic.Bool
	// refCount is a counter to track the number of openers.
	// Incremented on each Open(), decremented on Close(). DB closes when it reaches 0.
	refCount atomic.Int64
	// lock is a mutex to serialize open/close transitions.
	// Prevents race conditions during state changes.
	lock sync.Mutex
}

const (
	// DefaultDiskStorePath is the default filesystem path to the BoltDB file.
	DefaultDiskStorePath = ".k6.kv"

	// DefaultBoltDBBucket is the default BoltDB bucket name we use inside the file.
	DefaultBoltDBBucket = "k6"
)

// defaultBoltDBBucketBytes is the default bucket name for the BoltDB file.
//
//nolint:gochecknoglobals // readonly constant is used for default bucket name.
var defaultBoltDBBucketBytes = []byte(DefaultBoltDBBucket)

// NewDiskStore constructs a DiskStore using the provided filesystem path and DefaultBoltDBBucket.
// When path is empty, DefaultDiskStorePath is used to preserve backwards compatibility.
// If trackKeys is true, an in-memory index is initialized to accelerate RandomKey().
func NewDiskStore(trackKeys bool, path string) (*DiskStore, error) {
	var idx *OSTree
	if trackKeys {
		idx = NewOSTree()
	}

	diskPath, err := ResolveDiskPath(path)
	if err != nil {
		return nil, err
	}

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
	}, nil
}

// Open initializes the underlying BoltDB handle when needed and increments the
// reference counter for each caller. It is safe for concurrent use.
func (s *DiskStore) Open() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.opened.Load() {
		// Already open: just increment reference counter for this caller.
		// Multiple Open() calls are allowed and tracked via refCount.
		s.refCount.Add(1)

		return nil
	}

	// Ensure the parent directory exists in case it was removed between configuration
	// time and the actual open call.
	dirPath := filepath.Dir(s.path)
	if err := os.MkdirAll(dirPath, 0o750); err != nil {
		return fmt.Errorf("%w: %q: %w", ErrDiskDirectoryCreateFailed, dirPath, err)
	}

	// Open the database file.
	handler, err := bolt.Open(s.path, 0o600, nil)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}

	// Create the internal bucket if it doesn't exist.
	err = handler.Update(func(tx *bolt.Tx) error {
		_, bucketErr := tx.CreateBucketIfNotExists(defaultBoltDBBucketBytes)
		if bucketErr != nil {
			return fmt.Errorf("%w: internal bucket %q: %w", ErrBoltDBBucketCreateFailed, DefaultBoltDBBucket, bucketErr)
		}

		return nil
	})
	if err != nil {
		_ = handler.Close()

		return err
	}

	// Set the handle and bucket.
	s.handle = handler
	s.bucket = defaultBoltDBBucketBytes

	// Rebuild the key list if tracking is enabled.
	if s.trackKeys {
		s.keysLock.Lock()
		rebuildErr := s.rebuildKeyListLocked()
		s.keysLock.Unlock()

		if rebuildErr != nil {
			_ = handler.Close()

			return fmt.Errorf("%w: %w", ErrDiskStoreRebuildKeysFailed, rebuildErr)
		}
	}

	// Mark as opened and initialize refCount to 1 (this Open() call).
	// Subsequent Open() calls will just increment refCount without reopening.
	s.opened.Store(true)
	s.refCount.Store(1)

	return nil
}

// Get retrieves the raw []byte value from the disk store.
func (s *DiskStore) Get(key string) (any, error) {
	// Ensure the store is open.
	if err := s.ensureOpen(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}

	var value []byte

	// Get the value from the database within a BoltDB transaction.
	err := s.handle.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		value = bucket.Get([]byte(key))
		if value != nil {
			value = slices.Clone(value)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreReadFailed, err)
	}

	if value == nil {
		return nil, fmt.Errorf("%w: %q", ErrKeyNotFound, key)
	}

	// Return the raw bytes - serialization will be handled by the SerializedStore wrapper.
	return value, nil
}

// Set inserts or updates the value for a given key.
// If this is a new key and tracking is enabled, we update indexes.
func (s *DiskStore) Set(key string, value any) error {
	// Ensure the store is open.
	if err := s.ensureOpen(); err != nil {
		return fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}

	// Convert value to bytes if it's not already.
	valueBytes, err := normalizeToBytes(value)
	if err != nil {
		return err
	}

	var existed bool

	if s.trackKeys {
		// Lightweight existence check to decide whether to update indexes later.
		// We check before the transaction to avoid holding keysLock during disk I/O.
		// This optimization reduces lock contention by minimizing lock duration.
		s.keysLock.RLock()
		_, existed = s.keysMap[key]
		s.keysLock.RUnlock()
	}

	// Update the value in the database within a BoltDB transaction.
	err = s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		return bucket.Put([]byte(key), valueBytes)
	})
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDiskStoreWriteFailed, err)
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
	if err := s.ensureOpen(); err != nil {
		return 0, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}

	var (
		newValue int64
		wasNew   bool
	)

	// Update the value in the database within a BoltDB transaction.
	err := s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		// Get currentValue value.
		currentValue := bucket.Get([]byte(key))

		// Parse current value or start from 0.
		// Absent keys are treated as having value 0 for increment operations.
		var parsedCurrentValue int64

		if currentValue != nil {
			var err error

			parsedCurrentValue, err = strconv.ParseInt(string(currentValue), 10, 64)
			if err != nil {
				return fmt.Errorf("%w: key %q: %w", ErrValueParseFailed, key, err)
			}
		} else {
			// Key doesn't exist: will create it with value delta.
			wasNew = true
		}

		// Calculate new value.
		parsedCurrentValue += delta
		newValue = parsedCurrentValue

		return bucket.Put([]byte(key), []byte(strconv.FormatInt(parsedCurrentValue, 10)))
	})
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrDiskStoreIncrementFailed, err)
	}

	// Update tracking if this was a new key.
	if s.trackKeys && wasNew {
		s.keysLock.Lock()
		s.addKeyIndexLocked(key)
		s.keysLock.Unlock()
	}

	return newValue, nil
}

// GetOrSet returns the existing value (loaded=true) if key is present,
// otherwise stores "value" and returns it (loaded=false).
func (s *DiskStore) GetOrSet(key string, value any) (actual any, loaded bool, err error) {
	// Ensure the store is open.
	if err := s.ensureOpen(); err != nil {
		return nil, false, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
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
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		// Check if key exists.
		if got := bucket.Get([]byte(key)); got != nil {
			exists = true
			// Clone to avoid aliasing BoltDB's internal memory buffers.
			result = slices.Clone(got)

			return nil
		}

		// Key doesn't exist, set it.
		// This happens atomically within the Update transaction.
		return bucket.Put([]byte(key), valueBytes)
	})
	if err != nil {
		return nil, false, fmt.Errorf("%w: %w", ErrDiskStoreGetOrSetFailed, err)
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
	if err := s.ensureOpen(); err != nil {
		return nil, false, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
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
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		// Get previous value (copy for safety).
		if current := bucket.Get([]byte(key)); current != nil {
			existed = true

			prev = slices.Clone(current)
		}

		return bucket.Put([]byte(key), valueBytes)
	})
	if err != nil {
		return nil, false, fmt.Errorf("%w: %w", ErrDiskStoreSwapFailed, err)
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
	if err := s.ensureOpen(); err != nil {
		return false, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}

	expectAbsent := oldValue == nil

	var (
		oldBytes []byte
		err      error
	)

	if !expectAbsent {
		oldBytes, err = normalizeToBytes(oldValue)
		if err != nil {
			return false, err
		}
	}

	// Convert new value to bytes if it's not already.
	newBytes, err := normalizeToBytes(newValue)
	if err != nil {
		return false, err
	}

	var (
		swapped  bool
		inserted bool
	)

	err = s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		// Get current value.
		current := bucket.Get([]byte(key))

		switch {
		case expectAbsent:
			// CAS with nil oldValue: only succeed if key doesn't exist.
			if current != nil {
				return nil
			}

			inserted = true
		default:
			// CAS with non-nil oldValue: compare byte-for-byte.
			if current == nil || !bytes.Equal(current, oldBytes) {
				return nil
			}
		}

		// Values match (or key absent as expected), perform swap.
		swapped = true

		return bucket.Put([]byte(key), newBytes)
	})
	if err != nil {
		return false, fmt.Errorf("%w: %w", ErrDiskStoreCompareSwapFailed, err)
	}

	// Indexes only change when a new key is inserted via expectAbsent semantics.
	if swapped && inserted && s.trackKeys {
		s.keysLock.Lock()
		s.addKeyIndexLocked(key)
		s.keysLock.Unlock()
	}

	return swapped, nil
}

// Delete removes a key and its value from the store.
// If tracking is enabled, removes from keysList/keysMap in O(1)
// (swap-with-last trick) and updates the OSTree.
func (s *DiskStore) Delete(key string) error {
	// Ensure the store is open.
	if err := s.ensureOpen(); err != nil {
		return fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}

	// Remove from BoltDB.
	err := s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		return bucket.Delete([]byte(key))
	})
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDiskStoreDeleteFailed, err)
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
// With tracking enabled, we trust positive hits from the in-memory index but
// fall back to BoltDB for negative results to avoid stale reads.
func (s *DiskStore) Exists(key string) (bool, error) {
	// Ensure the store is open.
	if err := s.ensureOpen(); err != nil {
		return false, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}

	// When tracking is enabled, check in-memory index first for fast-path.
	// This avoids disk I/O for most Exists() calls when keys are present.
	if s.trackKeys {
		s.keysLock.RLock()
		_, exists := s.keysMap[key]
		s.keysLock.RUnlock()

		// Trust positive hits from index (key definitely exists).
		// For negative results, we must check disk to handle potential index drift
		// (e.g., if RebuildKeyList hasn't been called after manual DB edits).
		if exists {
			return true, nil
		}
		// Fall through to disk check for negative results to handle index drift.
	}

	var exists bool

	err := s.handle.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		exists = bucket.Get([]byte(key)) != nil

		return nil
	})
	if err != nil {
		return exists, fmt.Errorf("%w: %w", ErrDiskStoreExistsFailed, err)
	}

	return exists, nil
}

// DeleteIfExists deletes key if it exists. Returns true if deleted.
func (s *DiskStore) DeleteIfExists(key string) (bool, error) {
	// Ensure the store is open.
	if err := s.ensureOpen(); err != nil {
		return false, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}

	var deleted bool

	err := s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
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
		return false, fmt.Errorf("%w: %w", ErrDiskStoreDeleteIfExistsFailed, err)
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
	if err := s.ensureOpen(); err != nil {
		return false, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}

	oldBytes, err := normalizeToBytes(oldValue)
	if err != nil {
		return false, err
	}

	var deleted bool

	err = s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		// Get current value.
		current := bucket.Get([]byte(key))

		// Compare current with old: must match exactly (both nil or both equal bytes).
		// Three cases: both nil (match), one nil (mismatch), both non-nil (compare bytes).
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
		return false, fmt.Errorf("%w: %w", ErrDiskStoreCompareDeleteFailed, err)
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
	if err := s.ensureOpen(); err != nil {
		return fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}

	err := s.handle.Update(func(tx *bolt.Tx) error {
		// Drop whole bucket: faster than deleting keys one-by-one.
		// BoltDB optimizes bucket deletion as a single operation.
		err := tx.DeleteBucket(s.bucket)
		if err != nil {
			return fmt.Errorf("%w: bucket %s: %w", ErrDiskStoreDeleteFailed, s.bucket, err)
		}

		// Recreate it empty: ensures bucket exists for subsequent operations.
		_, err = tx.CreateBucket(s.bucket)
		if err != nil {
			return fmt.Errorf("%w: bucket %s: %w", ErrDiskStoreWriteFailed, s.bucket, err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDiskStoreClearFailed, err)
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
	if err := s.ensureOpen(); err != nil {
		return 0, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}

	var size int64

	err := s.handle.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		size = int64(bucket.Stats().KeyN)

		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrDiskStoreSizeFailed, err)
	}

	return size, nil
}

// List returns key-value pairs filtered by prefix and limited by count.
// Keys are returned in lexicographical order (Bolt cursor order).
// If prefix == "", Seek("") positions at the first key.
func (s *DiskStore) List(prefix string, limit int64) ([]Entry, error) {
	page, err := s.Scan(prefix, "", limit)
	if err != nil {
		return nil, err
	}

	return page.Entries, nil
}

// RebuildKeyList re-scans all keys from BoltDB to rebuild the in-memory key index.
// Useful after crashes or manual intervention. No-op if tracking is disabled.
func (s *DiskStore) RebuildKeyList() error {
	if !s.trackKeys {
		return nil
	}

	if err := s.ensureOpen(); err != nil {
		return fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}

	s.keysLock.Lock()
	defer s.keysLock.Unlock()

	if err := s.rebuildKeyListLocked(); err != nil {
		return fmt.Errorf("%w: %w", ErrDiskStoreRebuildKeysFailed, err)
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

	// If there are still references to the store, do nothing.
	remaining := s.refCount.Add(-1)
	if remaining > 0 {
		return nil
	}

	// Close the DB.
	if err := s.handle.Close(); err != nil {
		// If we fail to close the DB, increment the reference counter to match the open.
		s.refCount.Add(1)
		return err
	}

	// Reset the store to its initial state.
	s.opened.Store(false)
	s.refCount.Store(0)

	return nil
}

// ensureOpen checks whether the store is already opened. It is invoked by all
// operations that require an active Bolt handle.
func (s *DiskStore) ensureOpen() error {
	if s.opened.Load() {
		return nil
	}

	return ErrDiskStoreClosed
}
