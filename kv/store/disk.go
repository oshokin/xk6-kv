package store

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"

	bolt "go.etcd.io/bbolt"
)

// DiskStore is a persistent key-value store backed by bbolt. It optionally
// maintains in-memory key indexes for efficient random sampling.
//
// Concurrency:
//   - All exported methods are safe for concurrent use.
//   - Disk operations use a bbolt write transaction to guarantee atomicity.
//   - When trackKeys is enabled, we maintain keysList/keysMap and an OSTree
//     to keep RandomKey() fast and consistent under concurrent edits.
type DiskStore struct {
	// path is the filesystem path to the bbolt file.
	path string
	// handle is the underlying bbolt handle.
	handle *bolt.DB
	// boltOptions holds user-provided bbolt options (nil when defaults are used).
	boltOptions *bolt.Options
	// bucket is the internal bbolt bucket name.
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
	// Checked under lifecycleMu by beginOperation().
	opened atomic.Bool
	// refCount is a counter to track the number of openers.
	// Incremented on each Open(), decremented on Close(). DB closes when it reaches 0.
	refCount atomic.Int64
	// lock is a mutex to serialize open/close transitions.
	// Prevents race conditions during state changes.
	lock sync.Mutex
	// lifecycleMu prevents close-vs-operation races.
	// Operations hold RLock for the full ensureOpen+tx window;
	// Open/Close take Lock while transitioning lifecycle state.
	lifecycleMu sync.RWMutex
	// testRestoreHook is a test-only synchronization hook invoked in Restore()
	// after any restore lock is acquired and before snapshot I/O begins.
	testRestoreHook func()
	// claimToken is a process-local monotonically increasing token for claims.
	claimToken atomic.Int64
}

const (
	// DefaultDiskStorePath is the default filesystem path to the bbolt file.
	DefaultDiskStorePath = ".k6.kv"

	// DefaultBBoltBucket is the default bbolt bucket name we use inside the file.
	DefaultBBoltBucket = "k6"
)

// defaultBBoltBucketBytes is the default bucket name for the bbolt file.
//
//nolint:gochecknoglobals // readonly constant is used for default bucket name.
var defaultBBoltBucketBytes = []byte(DefaultBBoltBucket)

// NewDiskStore constructs a DiskStore using the provided filesystem path and DefaultBBoltBucket.
// When path is empty, DefaultDiskStorePath is used to preserve backwards compatibility.
// If trackKeys is true, an in-memory index is initialized to accelerate RandomKey().
func NewDiskStore(trackKeys bool, path string, cfg *DiskConfig) (*DiskStore, error) {
	var idx *OSTree
	if trackKeys {
		idx = NewOSTree()
	}

	diskPath, err := ResolveDiskPath(path)
	if err != nil {
		return nil, err
	}

	boltOpts, err := buildBBoltOptions(cfg)
	if err != nil {
		return nil, err
	}

	return &DiskStore{
		path:        diskPath,
		handle:      new(bolt.DB),
		boltOptions: boltOpts,
		opened:      atomic.Bool{},
		refCount:    atomic.Int64{},
		lock:        sync.Mutex{},
		trackKeys:   trackKeys,
		keysMap:     make(map[string]int),
		keysList:    []string{},
		keysLock:    sync.RWMutex{},
		ost:         idx,
	}, nil
}

// Open initializes the underlying bbolt handle when needed and increments the
// reference counter for each caller. It is safe for concurrent use.
func (s *DiskStore) Open() error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()

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
	handler, err := bolt.Open(s.path, 0o600, s.boltOptions)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}

	// Read-only handles cannot execute write transactions. When the caller
	// explicitly requested read-only mode, verify that the bucket already
	// exists instead of trying to create it.
	isReadOnly := s.boltOptions != nil && s.boltOptions.ReadOnly
	if isReadOnly {
		err = handler.View(func(tx *bolt.Tx) error {
			if tx.Bucket(defaultBBoltBucketBytes) == nil {
				return fmt.Errorf("%w: %s", ErrBucketNotFound, DefaultBBoltBucket)
			}

			return nil
		})
	} else {
		// Create the internal bucket if it doesn't exist.
		err = handler.Update(func(tx *bolt.Tx) error {
			_, bucketErr := tx.CreateBucketIfNotExists(defaultBBoltBucketBytes)
			if bucketErr != nil {
				return fmt.Errorf(
					"%w: internal bucket %q: %w",
					ErrBBoltBucketCreateFailed,
					DefaultBBoltBucket,
					bucketErr,
				)
			}

			// Claims are process-local leases: drop stale persisted claim metadata
			// whenever a writable process opens the store.
			if clearErr := clearClaimsBucket(tx); clearErr != nil {
				return clearErr
			}

			return nil
		})
	}

	if err != nil {
		// Close failure here only adds context-creation already failed-so treat it as best-effort.
		_ = handler.Close()

		return err
	}

	// Set the handle and bucket.
	s.handle = handler
	s.bucket = defaultBBoltBucketBytes

	// Rebuild the key list if tracking is enabled.
	if s.trackKeys {
		s.keysLock.Lock()
		rebuildErr := s.rebuildKeyListLocked()
		s.keysLock.Unlock()

		if rebuildErr != nil {
			// Same as above: closing best-effort since the handler never became visible.
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
	release, err := s.beginOperation()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	var value []byte

	// Get the value from the database within a bbolt transaction.
	err = s.handle.View(func(tx *bolt.Tx) error {
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
	if key == "" {
		return ErrKeyEmpty
	}

	release, err := s.beginOperation()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	// Convert value to bytes if it's not already.
	valueBytes, err := normalizeToBytes(value)
	if err != nil {
		return err
	}

	if s.trackKeys {
		// Keep bbolt mutation and index update as one logical operation.
		s.keysLock.Lock()
		defer s.keysLock.Unlock()
	}

	var existed bool

	// Update the value in the database within a bbolt transaction.
	err = s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		existed = bucket.Get([]byte(key)) != nil

		return bucket.Put([]byte(key), valueBytes)
	})
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDiskStoreWriteFailed, err)
	}

	// Update indexes only if the key was new.
	if s.trackKeys && !existed {
		s.addKeyIndexLocked(key)
	}

	return nil
}

// IncrementBy atomically adds delta to the integer value stored at key.
// Absent keys are treated as 0. Values must be decimal ASCII int64.
// Returns the new value as int64.
func (s *DiskStore) IncrementBy(key string, delta int64) (int64, error) {
	release, err := s.beginOperation()
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	if s.trackKeys {
		// Keep bbolt mutation and index update as one logical operation.
		s.keysLock.Lock()
		defer s.keysLock.Unlock()
	}

	var (
		newValue int64
		wasNew   bool
	)

	// Update the value in the database within a bbolt transaction.
	err = s.handle.Update(func(tx *bolt.Tx) error {
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

			parsedCurrentValue, err = parseCounterValue(currentValue)
			if err != nil {
				return fmt.Errorf("%w: key %q: %w", ErrValueParseFailed, key, err)
			}
		} else {
			// Key doesn't exist: will create it with value delta.
			wasNew = true
		}

		// Calculate new value with overflow guard.
		if (delta > 0 && parsedCurrentValue > math.MaxInt64-delta) ||
			(delta < 0 && parsedCurrentValue < math.MinInt64-delta) {
			return fmt.Errorf("%w: key %q: integer overflow", ErrValueParseFailed, key)
		}

		parsedCurrentValue += delta
		newValue = parsedCurrentValue

		return bucket.Put([]byte(key), []byte(strconv.FormatInt(parsedCurrentValue, 10)))
	})
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrDiskStoreIncrementFailed, err)
	}

	// Update tracking if this was a new key.
	if s.trackKeys && wasNew {
		s.addKeyIndexLocked(key)
	}

	return newValue, nil
}

// GetOrSet returns the existing value (loaded=true) if key is present,
// otherwise stores "value" and returns it (loaded=false).
func (s *DiskStore) GetOrSet(key string, value any) (actual any, loaded bool, err error) {
	release, err := s.beginOperation()
	if err != nil {
		return nil, false, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	// Convert value to bytes if it's not already.
	valueBytes, err := normalizeToBytes(value)
	if err != nil {
		return nil, false, err
	}

	if s.trackKeys {
		// Keep bbolt mutation and index update as one logical operation.
		s.keysLock.Lock()
		defer s.keysLock.Unlock()
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
			// Clone to avoid aliasing bbolt's internal memory buffers.
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
		s.addKeyIndexLocked(key)
	}

	return valueBytes, false, nil
}

// Swap replaces the value and returns the previous value (if existed) and whether it existed.
func (s *DiskStore) Swap(key string, value any) (previous any, loaded bool, err error) {
	release, err := s.beginOperation()
	if err != nil {
		return nil, false, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	// Convert value to bytes if it's not already.
	valueBytes, err := normalizeToBytes(value)
	if err != nil {
		return nil, false, err
	}

	if s.trackKeys {
		// Keep bbolt mutation and index update as one logical operation.
		s.keysLock.Lock()
		defer s.keysLock.Unlock()
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
		s.addKeyIndexLocked(key)
	}

	// Return a real nil interface when key was not present.
	if !existed {
		return nil, false, nil
	}

	return prev, true, nil
}

// Delete removes a key and its value from the store.
// If tracking is enabled, removes from keysList/keysMap in O(1)
// (swap-with-last trick) and updates the OSTree.
func (s *DiskStore) Delete(key string) error {
	release, err := s.beginOperation()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	if s.trackKeys {
		// Keep bbolt mutation and index update as one logical operation.
		s.keysLock.Lock()
		defer s.keysLock.Unlock()
	}

	// Remove from bbolt.
	err = s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		if err := bucket.Delete([]byte(key)); err != nil {
			return err
		}

		claimsBucket, err := ensureClaimsBucket(tx)
		if err != nil {
			return err
		}

		return deleteClaimForKeyTx(claimsBucket, key)
	})
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDiskStoreDeleteFailed, err)
	}

	// If tracking is enabled, remove the key from the in-memory structures.
	if s.trackKeys {
		s.removeKeyIndexLocked(key)
	}

	return nil
}

// Exists checks if a given key exists.
// We always validate against bbolt to avoid stale-positive answers from
// in-memory indexes.
func (s *DiskStore) Exists(key string) (bool, error) {
	release, err := s.beginOperation()
	if err != nil {
		return false, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	var exists bool

	err = s.handle.View(func(tx *bolt.Tx) error {
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
	release, err := s.beginOperation()
	if err != nil {
		return false, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	if s.trackKeys {
		// Keep bbolt mutation and index update as one logical operation.
		s.keysLock.Lock()
		defer s.keysLock.Unlock()
	}

	var deleted bool

	err = s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		claimsBucket, err := ensureClaimsBucket(tx)
		if err != nil {
			return err
		}

		// Check if key exists.
		if bucket.Get([]byte(key)) == nil {
			// Defensive cleanup: remove stale claim metadata if present.
			if err := deleteClaimForKeyTx(claimsBucket, key); err != nil {
				return err
			}

			return nil
		}

		// Delete the key.
		if err := bucket.Delete([]byte(key)); err != nil {
			return err
		}

		if err := deleteClaimForKeyTx(claimsBucket, key); err != nil {
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
		s.removeKeyIndexLocked(key)
	}

	return deleted, nil
}

// Clear wipes all keys and values from the store.
// We drop and recreate the bucket (cheap), then reset in-memory indexes.
func (s *DiskStore) Clear() error {
	release, err := s.beginOperation()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	if s.trackKeys {
		// Keep bbolt mutation and index reset as one logical operation.
		s.keysLock.Lock()
		defer s.keysLock.Unlock()
	}

	err = s.handle.Update(func(tx *bolt.Tx) error {
		// Drop whole bucket: faster than deleting keys one-by-one.
		// bbolt optimizes bucket deletion as a single operation.
		err := tx.DeleteBucket(s.bucket)
		if err != nil {
			return fmt.Errorf("%w: bucket %s: %w", ErrDiskStoreDeleteFailed, s.bucket, err)
		}

		// Recreate it empty: ensures bucket exists for subsequent operations.
		_, err = tx.CreateBucket(s.bucket)
		if err != nil {
			return fmt.Errorf("%w: bucket %s: %w", ErrDiskStoreWriteFailed, s.bucket, err)
		}

		if clearErr := clearClaimsBucket(tx); clearErr != nil {
			return clearErr
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDiskStoreClearFailed, err)
	}

	// Reset the in-memory key tracking structures.
	if s.trackKeys {
		s.keysList = []string{}
		s.keysMap = make(map[string]int)

		if s.ost != nil {
			s.ost = NewOSTree()
		}
	}

	return nil
}

// Size returns the number of keys in the store.
// Implementation reads KeyN via bbolt bucket stats.
func (s *DiskStore) Size() (int64, error) {
	release, err := s.beginOperation()
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	var size int64

	err = s.handle.View(func(tx *bolt.Tx) error {
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
// Keys are returned in lexicographical order (bbolt cursor order).
// If prefix == "", Seek("") positions at the first key.
func (s *DiskStore) List(prefix string, limit int64) ([]Entry, error) {
	page, err := s.Scan(prefix, "", limit)
	if err != nil {
		return nil, err
	}

	return page.Entries, nil
}

// RebuildKeyList re-scans all keys from bbolt to rebuild the in-memory key index.
// Useful after crashes or manual intervention. No-op if tracking is disabled.
func (s *DiskStore) RebuildKeyList() error {
	if !s.trackKeys {
		return nil
	}

	release, err := s.beginOperation()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	s.keysLock.Lock()
	defer s.keysLock.Unlock()

	if err := s.rebuildKeyListLocked(); err != nil {
		return fmt.Errorf("%w: %w", ErrDiskStoreRebuildKeysFailed, err)
	}

	return nil
}

// Close decrements an internal reference count and closes the DB when it
// reaches zero.
// Subsequent operations fail until Open() is called explicitly.
func (s *DiskStore) Close() error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()

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

// ensureOpen checks whether the store is already opened.
// It is invoked by all operations that require an active bbolt handle.
func (s *DiskStore) ensureOpen() error {
	if s.opened.Load() {
		return nil
	}

	return ErrDiskStoreClosed
}

// beginOperation acquires the lifecycle read-lock and verifies the store is open.
// The returned release function must be called exactly once.
func (s *DiskStore) beginOperation() (func(), error) {
	s.lifecycleMu.RLock()

	if err := s.ensureOpen(); err != nil {
		s.lifecycleMu.RUnlock()
		return nil, err
	}

	return s.lifecycleMu.RUnlock, nil
}
