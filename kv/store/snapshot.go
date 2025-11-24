package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	bolt "go.etcd.io/bbolt"
	boltErrors "go.etcd.io/bbolt/errors"
)

type (
	// BackupOptions configure how Backup writes a BoltDB-based snapshot.
	BackupOptions struct {
		// FileName is the destination path for the snapshot. Required.
		FileName string

		// AllowConcurrentWrites controls whether writers are allowed to progress
		// while the snapshot is being persisted.
		// When true, writers are unblocked immediately after the key list is captured
		// and the snapshot becomes best-effort:
		//   - inserts that arrive after the key snapshot are excluded entirely,
		//   - deletes committed after the snapshot may reappear, and
		//   - updates to existing keys that land before a chunk is cloned may be
		//     captured even though they happened after the snapshot began.
		// In short: AllowConcurrentWrites does not provide a point-in-time view.
		// When false, writers are blocked for the entire backup to provide
		// a strictly consistent view.
		AllowConcurrentWrites bool
	}

	// BackupSummary reports metrics about a backup operation.
	BackupSummary struct {
		// TotalEntries is the number of key/value pairs captured in the snapshot.
		TotalEntries int

		// BytesWritten is the final size of the snapshot file on disk.
		BytesWritten int64

		// BestEffort is true when the snapshot was taken with AllowConcurrentWrites.
		BestEffort bool

		// Warning contains additional context about the snapshot mode.
		// When BestEffort is true, Warning repeats allowConcurrentWritesWarning so callers
		// can surface it to operators.
		Warning string
	}

	// RestoreOptions configure Restore behavior.
	RestoreOptions struct {
		// FileName is the path to the Bolt snapshot file. Required.
		FileName string

		// MaxEntries limits how many entries may be loaded.
		// Zero disables the cap.
		MaxEntries int

		// MaxBytes limits the aggregate key/value payload hydrated during restore.
		// Zero disables the cap.
		MaxBytes int64
	}

	// RestoreSummary reports the outcome of a restore operation.
	RestoreSummary struct {
		// TotalEntries is the number of key/value pairs hydrated from the snapshot.
		TotalEntries int
	}

	// boltDBBatchWriter buffers entries and flushes them in bounded BoltDB transactions.
	boltDBBatchWriter struct {
		// db is the underlying BoltDB database.
		db *bolt.DB

		// pending is a slice of key/value pairs staged for writing to the BoltDB database.
		pending []*snapshotPair

		// pendingBytes is the total size of the pending key/value pairs.
		pendingBytes int
	}

	// snapshotPair represents a key/value pair staged for BoltDB writes.
	snapshotPair struct {
		// key is the key of the key/value pair.
		key []byte

		// value is the value of the key/value pair.
		value []byte
	}

	// restoreBudget tracks safety limits during restore operations.
	// Prevents OOM and excessive resource consumption during snapshot hydration.
	restoreBudget struct {
		// maxEntries is the maximum number of entries that can be restored.
		maxEntries int

		// maxBytes is the maximum number of bytes that can be restored.
		maxBytes int64

		// entries is the number of entries that have been restored.
		entries int

		// bytesUsed is the number of bytes that have been restored.
		bytesUsed int64
	}
)

const (
	// defaultMapPreallocationCapacity is the default map capacity for restores when
	// no better hint is available.
	defaultMapPreallocationCapacity = 1024

	// maxMapPreallocationCapacity caps map preallocation to keep memory bounded.
	maxMapPreallocationCapacity = 10_000_000

	// boltDBSnapshotMaxBatchEntries caps how many entries we write per BoltDB transaction.
	boltDBSnapshotMaxBatchEntries = 50_000

	// boltDBSnapshotMaxBatchBytes caps the total key/value payload per BoltDB transaction (~64MB).
	boltDBSnapshotMaxBatchBytes = 64 << 20

	// allowConcurrentWritesWarning is a warning message about the best-effort
	// nature of concurrent writes during backup.
	allowConcurrentWritesWarning = "AllowConcurrentWrites is best-effort: " +
		"inserts after the key snapshot are skipped, deletes may reappear, and " +
		"later updates may leak into the backup."
)

// streamSnapshotChunkObserver is a test hook that captures chunk sizes streamed
// during AllowConcurrentWrites backups. Production code leaves this nil.
//
//nolint:gochecknoglobals // this is a test hook.
var streamSnapshotChunkObserver func(chunkLen int)

// normalize applies defaults and validates backup options.
// When FileName is empty or whitespace-only, defaults to the standard disk path.
// Always cleans the path to normalize separators and remove redundant elements.
func (m *BackupOptions) normalize() error {
	if strings.TrimSpace(m.FileName) == "" {
		defaultPath, err := defaultMemorySnapshotPath()
		if err != nil {
			return err
		}

		m.FileName = defaultPath
	}

	m.FileName = filepath.Clean(m.FileName)

	return nil
}

// normalize validates and applies defaults to restore options.
// When FileName is empty or whitespace-only, defaults to the standard disk path.
// Always cleans the path to normalize separators and remove redundant elements.
func (m *RestoreOptions) normalize() error {
	if strings.TrimSpace(m.FileName) == "" {
		defaultPath, err := defaultMemorySnapshotPath()
		if err != nil {
			return err
		}

		m.FileName = defaultPath
	}

	m.FileName = filepath.Clean(m.FileName)

	return nil
}

// Backup writes the on-disk Bolt database to a standalone snapshot file.
// Unlike MemoryStore, DiskStore already persists data, so Backup simply copies
// the existing Bolt file using a consistent View transaction.
func (s *DiskStore) Backup(opts *BackupOptions) (*BackupSummary, error) {
	if opts == nil {
		return nil, ErrBackupOptionsNil
	}

	if err := opts.normalize(); err != nil {
		return nil, err
	}

	if err := s.ensureOpen(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}

	if s.isSelfSnapshot(opts.FileName) {
		return s.diskSelfSnapshotSummary()
	}

	return s.writeDiskSnapshot(opts.FileName)
}

// isSelfSnapshot checks if the snapshot path is the same as the disk store path.
func (s *DiskStore) isSelfSnapshot(path string) bool {
	absSnapshotPath, err := filepath.Abs(path)
	if err != nil {
		return false
	}

	return absSnapshotPath == s.path
}

// diskSelfSnapshotSummary returns a summary for a self-snapshot operation.
func (s *DiskStore) diskSelfSnapshotSummary() (*BackupSummary, error) {
	totalEntries, err := s.diskKeyCount()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreCountFailed, err)
	}

	info, err := os.Stat(s.path)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreStatFailed, err)
	}

	return &BackupSummary{
		TotalEntries: totalEntries,
		BytesWritten: info.Size(),
		BestEffort:   false,
	}, nil
}

// writeDiskSnapshot writes a snapshot to a file.
func (s *DiskStore) writeDiskSnapshot(destination string) (*BackupSummary, error) {
	targetDir := filepath.Dir(destination)
	targetBase := filepath.Base(destination)

	if targetDir != "" {
		if err := os.MkdirAll(targetDir, 0o750); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrBackupDirectoryFailed, err)
		}
	}

	//nolint:forbidigo // file I/O is required for snapshot backup.
	tempHandle, err := os.CreateTemp(targetDir, targetBase+".*.tmp")
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBackupTempFileFailed, err)
	}

	tempFile := tempHandle.Name()
	// Track export error for defer cleanup: only remove temp file if operation failed.
	// On success, temp file is renamed to final destination, so cleanup would be wrong.
	var exportErr error

	defer func() {
		// Only clean up temp file if an error occurred. On success, Rename() moves
		// the temp file to the final destination, so removing it would delete the backup.
		if exportErr != nil {
			_ = tempHandle.Close()
			//nolint:forbidigo // clean up temporary file on error.
			_ = os.Remove(tempFile)
		}
	}()

	var totalEntries int

	// Use a read-only View transaction to atomically copy the entire database.
	// tx.WriteTo streams the database file format directly, preserving all buckets,
	// transactions, and metadata. This is more efficient than iterating entries.
	if err := s.handle.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		// Capture entry count before WriteTo (Stats() is only valid during transaction).
		totalEntries = bucket.Stats().KeyN

		// WriteTo streams the entire BoltDB file format to the writer.
		// This includes all buckets, pages, and metadata in a consistent snapshot.
		if _, err := tx.WriteTo(tempHandle); err != nil {
			return fmt.Errorf("%w: %w", ErrBackupCopyFailed, err)
		}

		return nil
	}); err != nil {
		exportErr = fmt.Errorf("%w: %w", ErrSnapshotExportFailed, err)
		return nil, exportErr
	}

	// Sync file system buffers to ensure all data is physically written to disk
	// before renaming. This prevents data loss if the process crashes between
	// WriteTo and Rename.
	if err := tempHandle.Sync(); err != nil {
		exportErr = fmt.Errorf("%w: %w", ErrSnapshotExportFailed, err)
		return nil, exportErr
	}

	if err := tempHandle.Close(); err != nil {
		exportErr = fmt.Errorf("%w: %w", ErrBackupTempFileFailed, err)
		return nil, exportErr
	}

	info, err := os.Stat(tempFile)
	if err != nil {
		exportErr = fmt.Errorf("%w: %w", ErrBoltDBSnapshotStatFailed, err)
		return nil, exportErr
	}

	//nolint:forbidigo // rename required for atomic replacement.
	if err := os.Rename(tempFile, destination); err != nil {
		exportErr = fmt.Errorf("%w: %w", ErrBackupFinalizeFailed, err)
		return nil, exportErr
	}

	return &BackupSummary{
		TotalEntries: totalEntries,
		BytesWritten: info.Size(),
		BestEffort:   false,
	}, nil
}

// Restore replaces the disk store contents with entries from a snapshot file.
// The restore is performed inside a single Bolt write transaction for atomicity.
func (s *DiskStore) Restore(opts *RestoreOptions) (*RestoreSummary, error) {
	if opts == nil {
		return nil, ErrRestoreOptionsNil
	}

	if err := opts.normalize(); err != nil {
		return nil, err
	}

	if err := s.ensureOpen(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}

	// Fast path: if restoring from the same file (self-reference), return metadata
	// without copying. This avoids unnecessary work when snapshot path matches store path.
	if absSnapshotPath, err := filepath.Abs(opts.FileName); err == nil && absSnapshotPath == s.path {
		totalEntries, err := s.diskKeyCount()
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrDiskStoreCountFailed, err)
		}

		return &RestoreSummary{TotalEntries: totalEntries}, nil
	}

	snapshotDB, err := bolt.Open(opts.FileName, 0o600, &bolt.Options{ReadOnly: true})
	if err != nil {
		return nil, wrapSnapshotOpenError(err, opts.FileName)
	}

	defer snapshotDB.Close()

	budget := &restoreBudget{
		maxEntries: opts.MaxEntries,
		maxBytes:   opts.MaxBytes,
	}

	var totalEntries int

	// Perform restore in a single write transaction for atomicity.
	// If restore fails partway through, the entire operation rolls back.
	if err := s.handle.Update(func(tx *bolt.Tx) error {
		// Drop existing bucket to start fresh. ErrBucketNotFound is acceptable
		// if the bucket doesn't exist yet (empty store).
		if err := tx.DeleteBucket(s.bucket); err != nil && !errors.Is(err, boltErrors.ErrBucketNotFound) {
			return fmt.Errorf("%w: %w", ErrDiskStoreDeleteFailed, err)
		}

		// Create fresh bucket for restored data.
		bucket, err := tx.CreateBucket(s.bucket)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrDiskStoreWriteFailed, err)
		}

		// Nested transaction pattern: read from snapshot DB (read-only View)
		// while writing to destination DB (write Update). This is safe because
		// BoltDB allows concurrent read transactions from different DB handles.
		return snapshotDB.View(func(snapshotTx *bolt.Tx) error {
			srcBucket := snapshotTx.Bucket(defaultBoltDBBucketBytes)
			if srcBucket == nil {
				return fmt.Errorf("%w: %q", ErrBucketNotFound, defaultBoltDBBucketBytes)
			}

			// Copy all entries from snapshot bucket to destination bucket.
			return srcBucket.ForEach(func(k, v []byte) error {
				// Check budget limits before accumulating this entry.
				if err := budget.track(len(k), len(v)); err != nil {
					return err
				}

				if err := bucket.Put(k, v); err != nil {
					return fmt.Errorf("%w: %w", ErrDiskStoreWriteFailed, err)
				}

				totalEntries++

				return nil
			})
		})
	}); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrSnapshotReadFailed, err)
	}

	if s.trackKeys {
		if err := s.RebuildKeyList(); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrKeyListRebuildFailed, err)
		}
	}

	return &RestoreSummary{
		TotalEntries: totalEntries,
	}, nil
}

// diskKeyCount returns the number of keys in the disk store's bucket.
// Uses BoltDB's Stats().KeyN for O(1) counting without iterating entries.
func (s *DiskStore) diskKeyCount() (int, error) {
	var count int

	err := s.handle.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		count = bucket.Stats().KeyN

		return nil
	})
	if err != nil {
		return 0, err
	}

	return count, nil
}

// Backup writes the entire memory store into a Bolt snapshot file.
// When AllowConcurrentWrites is enabled the snapshot is best-effort: new writes that
// arrive after the key snapshot are excluded and deletes committed after the snapshot
// may reappear in the backed-up data.
func (s *MemoryStore) Backup(opts *BackupOptions) (*BackupSummary, error) {
	if opts == nil {
		return nil, ErrBackupOptionsNil
	}

	if err := opts.normalize(); err != nil {
		return nil, err
	}

	// Ensure destination directory exists so temp+final paths are valid.
	targetDir := filepath.Dir(opts.FileName)
	targetBase := filepath.Base(opts.FileName)

	if targetDir != "" {
		if err := os.MkdirAll(targetDir, 0o750); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrBackupDirectoryFailed, err)
		}
	}

	// Create a unique temporary file alongside the final destination
	// so we can atomically swap on success.
	//nolint:forbidigo // file I/O is required for snapshot backup.
	tempHandle, err := os.CreateTemp(targetDir, targetBase+".*.tmp")
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBackupTempFileFailed, err)
	}

	tempFile := tempHandle.Name()

	if err := tempHandle.Close(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBackupTempFileFailed, err)
	}

	summary, backupErr := s.backupToBolt(tempFile, opts)
	if backupErr != nil {
		//nolint:forbidigo // clean up temporary file on error.
		_ = os.Remove(tempFile)

		return nil, backupErr
	}

	//nolint:forbidigo // rename required for atomic replacement.
	if err := os.Rename(tempFile, opts.FileName); err != nil {
		//nolint:forbidigo // clean up temporary file on error.
		_ = os.Remove(tempFile)

		return nil, fmt.Errorf("%w: %w", ErrBackupFinalizeFailed, err)
	}

	return summary, nil
}

// backupToBolt backs up a snapshot to a BoltDB file using one of two strategies:
//  1. Concurrent mode (AllowConcurrentWrites): blocks writers only during key capture,
//     then streams values while writers proceed. Best-effort consistency.
//  2. Blocking mode: holds mutation lock for entire backup. Strict consistency.
func (s *MemoryStore) backupToBolt(tempFile string, opts *BackupOptions) (*BackupSummary, error) {
	if opts.AllowConcurrentWrites {
		return s.backupConcurrent(tempFile)
	}

	return s.backupBlocking(tempFile)
}

// backupConcurrent backs up a snapshot with brief writer blocking for key capture only.
// Writers are blocked only during the initial key snapshot, then immediately released
// while values are streamed with cloning. This provides best-effort consistency.
func (s *MemoryStore) backupConcurrent(tempFile string) (*BackupSummary, error) {
	if err := s.blockMutations(ErrBackupInProgress); err != nil {
		return nil, err
	}

	// Track whether we've already unblocked mutations to avoid double-unlock.
	var blockReleased bool

	defer func() {
		if !blockReleased {
			s.unblockMutations()
		}
	}()

	// Capture the key list while holding the mutation lock.
	keys := s.snapshotKeys()

	// Immediately unblock writers - they can now proceed while we stream values.
	// Any mutation that lands after this point may leak into the snapshot,
	// which is why the returned summary always advertises best-effort mode.
	s.unblockMutations()

	blockReleased = true

	return s.writeBoltSnapshot(tempFile, true, func(yield func(string, []byte) error) error {
		return s.streamKeysWithClonedValues(keys, yield)
	})
}

// backupBlocking backs up a snapshot while blocking all writers for the entire duration.
// Provides strict point-in-time consistency: no keys can be added/removed/modified
// during the backup. Values are streamed without cloning since mutation is impossible.
func (s *MemoryStore) backupBlocking(tempFile string) (*BackupSummary, error) {
	if err := s.blockMutations(ErrBackupInProgress); err != nil {
		return nil, err
	}

	defer s.unblockMutations()

	// Capture keys while holding the mutation block.
	keys := s.snapshotKeys()

	return s.writeBoltSnapshot(tempFile, false, func(yield func(string, []byte) error) error {
		for _, key := range keys {
			// Brief read lock per key - mutation block ensures container stability.
			s.mu.RLock()
			value, exists := s.container[key]
			s.mu.RUnlock()

			if !exists {
				// Should never happen in blocking mode - indicates a bug.
				return fmt.Errorf("key %q missing during snapshot", key)
			}

			// Writers are blocked for the duration of this backup, so we can stream the
			// existing value slice directly without cloning. This keeps the blocking
			// path low-overhead compared to the concurrent snapshot mode.
			if err := yield(key, value); err != nil {
				return err
			}
		}

		return nil
	})
}

// writeBoltSnapshot writes a snapshot to a BoltDB file using batched transactions.
// The iterate callback provides key/value pairs via a yield function, which are
// buffered and written in bounded batches to prevent unbounded transaction sizes.
// Returns a summary with entry count, file size, and consistency guarantees.
func (s *MemoryStore) writeBoltSnapshot(
	tempFile string,
	bestEffort bool,
	iterate func(yield func(string, []byte) error) error,
) (*BackupSummary, error) {
	// Open BoltDB file with restrictive permissions (owner read/write only).
	db, err := bolt.Open(tempFile, 0o600, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBoltDBSnapshotOpenFailed, err)
	}

	// Track whether we successfully closed to avoid double-close in defer.
	var closed bool

	defer func() {
		if !closed {
			_ = db.Close()
		}
	}()

	// Batch writer handles transaction size limits automatically.
	writer := newBoltBatchWriter(db)

	var written int

	// Stream entries from iterator into batched BoltDB writes.
	if err := iterate(func(key string, value []byte) error {
		written++

		return writer.append(key, value)
	}); err != nil {
		return nil, err
	}

	// Flush any remaining buffered entries.
	if err := writer.flush(); err != nil {
		return nil, err
	}

	// Explicit close to detect write errors before returning success.
	if err := db.Close(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBoltDBSnapshotCloseFailed, err)
	}

	closed = true

	// Stat file to report final on-disk size.
	info, err := os.Stat(tempFile)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBoltDBSnapshotStatFailed, err)
	}

	summary := &BackupSummary{
		TotalEntries: written,
		BytesWritten: info.Size(),
		BestEffort:   bestEffort,
	}

	if bestEffort {
		summary.Warning = allowConcurrentWritesWarning
	}

	return summary, nil
}

// Restore replaces the store contents with entries from a Bolt snapshot.
// All existing data is atomically replaced with the snapshot contents.
func (s *MemoryStore) Restore(opts *RestoreOptions) (*RestoreSummary, error) {
	if opts == nil {
		return nil, ErrRestoreOptionsNil
	}

	if err := opts.normalize(); err != nil {
		return nil, err
	}

	if err := s.blockMutations(ErrRestoreInProgress); err != nil {
		return nil, err
	}

	// Track whether we've already unblocked mutations to ensure cleanup on error.
	var blockReleased bool

	defer func() {
		if !blockReleased {
			s.unblockMutations()
		}
	}()

	// Open snapshot file in read-only mode.
	db, err := bolt.Open(opts.FileName, 0o600, &bolt.Options{ReadOnly: true})
	if err != nil {
		return nil, wrapSnapshotOpenError(err, opts.FileName)
	}

	defer db.Close()

	// Preallocate map based on expected size to reduce allocations during restore.
	// We use MaxEntries as a hint if provided, otherwise fall back to default capacity.
	// Preallocation reduces map rehashing overhead when loading large snapshots.
	builder := make(map[string][]byte, s.getPreallocationCapacity(0, opts.MaxEntries))

	// Budget tracking prevents OOM by enforcing limits before accumulating entries.
	budget := &restoreBudget{
		maxEntries: opts.MaxEntries,
		maxBytes:   opts.MaxBytes,
	}

	if err := db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(defaultBoltDBBucketBytes)
		if bucket == nil {
			return fmt.Errorf("%w: %q", ErrBucketNotFound, defaultBoltDBBucketBytes)
		}

		return bucket.ForEach(func(k, v []byte) error {
			// Check budget limits before accumulating this entry.
			if err := budget.track(len(k), len(v)); err != nil {
				return err
			}

			// Clone value bytes to avoid aliasing BoltDB's internal memory.
			builder[string(k)] = slices.Clone(v)

			return nil
		})
	}); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrSnapshotReadFailed, err)
	}

	// Atomically replace entire store contents. This happens while mutations are blocked,
	// ensuring no concurrent writes can interfere with the restore operation.
	s.applySnapshot(builder)

	summary := &RestoreSummary{TotalEntries: len(builder)}

	// Unblock mutations after successful restore. Set flag to prevent double-unlock in defer.
	s.unblockMutations()

	blockReleased = true

	return summary, nil
}

// applySnapshot atomically replaces the store's entire contents with the restored container.
// Rebuilds key tracking structures (keysList, keysMap, OSTree) if key tracking is enabled.
// This is the critical operation where the old state is discarded and new state takes over.
func (s *MemoryStore) applySnapshot(container map[string][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.container = container

	if !s.trackKeys {
		// No tracking: explicitly nil out structures to release memory and ensure
		// they're not accidentally used later.
		s.keysList = nil
		s.keysMap = nil
		s.ost = nil

		return
	}

	// Rebuild tracking structures from scratch with preallocated capacity
	// to minimize allocations during the rebuild.
	s.keysList = make([]string, 0, len(container))
	s.keysMap = make(map[string]int, len(container))

	s.ost = NewOSTree()

	// Populate tracking structures by iterating restored container.
	// addKeyLocked handles duplicate detection internally (idempotent).
	for key := range container {
		s.addKeyLocked(key)
	}
}

// getPreallocationCapacity calculates the optimal map capacity for restore preallocation.
// Balances between memory efficiency and avoiding repeated reallocations.
func (s *MemoryStore) getPreallocationCapacity(totalEntries, maxEntries int) int {
	switch {
	case maxEntries > 0:
		return clamp(maxEntries, defaultMapPreallocationCapacity, maxMapPreallocationCapacity)
	case totalEntries > 0:
		return clamp(totalEntries, defaultMapPreallocationCapacity, maxMapPreallocationCapacity)
	default:
		return defaultMapPreallocationCapacity
	}
}

// snapshotKeys returns a defensive copy of all keys in the store at this instant.
// The copy prevents concurrent modifications from affecting the snapshot consistency.
// Uses tracking index when available (O(n) copy), otherwise scans container (O(n) iteration).
func (s *MemoryStore) snapshotKeys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.cloneKeysLocked()
}

// cloneKeysLocked returns a defensive copy of keys from the store for snapshotting.
// Caller must hold mu.RLock().
func (s *MemoryStore) cloneKeysLocked() []string {
	if s.trackKeys {
		return slices.Clone(s.keysList)
	}

	keys := make([]string, 0, len(s.container))

	for key := range s.container {
		keys = append(keys, key)
	}

	return keys
}

// streamKeysWithClonedValues streams keys with their cloned values in batches.
// For each key, acquires a brief read lock to clone the value, then yields it.
// Keys deleted after the initial snapshot are silently skipped (best-effort mode).
// Batching metrics are reported via streamSnapshotChunkObserver for test observability.
func (s *MemoryStore) streamKeysWithClonedValues(keys []string, yield func(string, []byte) error) error {
	if len(keys) == 0 {
		return nil
	}

	var (
		chunkEntries int
		chunkBytes   int
	)

	flushChunk := func() {
		if chunkEntries == 0 {
			return
		}

		if streamSnapshotChunkObserver != nil {
			streamSnapshotChunkObserver(chunkEntries)
		}

		chunkEntries = 0
		chunkBytes = 0
	}

	for _, key := range keys {
		// Brief read lock to fetch value - concurrent writes may proceed.
		s.mu.RLock()
		value, exists := s.container[key]
		s.mu.RUnlock()

		// Key was deleted after snapshot - skip it (best-effort semantics).
		if !exists {
			continue
		}

		// Clone to avoid aliasing live store memory. Because we yield immediately
		// afterward, this keeps peak memory bounded to a single entry.
		cloned := slices.Clone(value)

		if err := yield(key, cloned); err != nil {
			return err
		}

		chunkEntries++
		chunkBytes += len(key) + len(cloned)

		// Notify the test hook whenever we reach the same thresholds used by the
		// BoltDB batch writer so tests can assert chunking behavior without peeking
		// inside the writer itself.
		if chunkEntries >= boltDBSnapshotMaxBatchEntries ||
			chunkBytes >= boltDBSnapshotMaxBatchBytes {
			flushChunk()
		}
	}

	// Final notification for any remaining entries in the last partial chunk.
	flushChunk()

	return nil
}

// newBoltBatchWriter creates a new boltDBBatchWriter that buffers writes
// into bounded transactions to prevent memory exhaustion and transaction timeouts.
// Preallocates buffer capacity to reduce allocations during streaming.
func newBoltBatchWriter(db *bolt.DB) *boltDBBatchWriter {
	return &boltDBBatchWriter{
		db:      db,
		pending: make([]*snapshotPair, 0, boltDBSnapshotMaxBatchEntries),
	}
}

// append adds a key/value pair to the pending batch buffer.
// Automatically flushes when batch size limits are reached (entry count or byte size).
// This prevents unbounded transaction growth and memory usage during large backups.
func (b *boltDBBatchWriter) append(key string, value []byte) error {
	kv := &snapshotPair{
		key:   []byte(key),
		value: value,
	}

	b.pending = append(b.pending, kv)
	b.pendingBytes += len(kv.key) + len(kv.value)

	// Auto-flush when either limit is reached to keep transactions bounded.
	if len(b.pending) >= boltDBSnapshotMaxBatchEntries || b.pendingBytes >= boltDBSnapshotMaxBatchBytes {
		return b.flush()
	}

	return nil
}

// flush writes all buffered entries to BoltDB in a single transaction.
// After successful write, resets the buffer for the next batch.
// The slice aliasing (batch = b.pending) is safe because the backup pipeline
// is single-threaded - no concurrent appends can occur during the transaction.
func (b *boltDBBatchWriter) flush() error {
	if len(b.pending) == 0 {
		return nil
	}

	// Capture pending slice and reset for next batch.
	// This aliases the underlying array, which is safe because the pipeline
	// is single-threaded: no new appends occur until this transaction completes.
	batch := b.pending
	b.pending = b.pending[:0]
	b.pendingBytes = 0

	// Write entire batch in single transaction for atomicity and performance.
	if err := b.db.Update(func(tx *bolt.Tx) error {
		// Create bucket if first write, or get existing bucket.
		bucket, err := tx.CreateBucketIfNotExists(defaultBoltDBBucketBytes)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrBoltDBBucketCreateFailed, err)
		}

		// Write all buffered entries within this transaction.
		for _, entry := range batch {
			if err := bucket.Put(entry.key, entry.value); err != nil {
				return fmt.Errorf("%w: %w", ErrBoltDBWriteFailed, err)
			}
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}

// track updates the restore budget with the size of a key/value pair.
// Returns an error if adding this entry would exceed MaxEntries or MaxBytes limits.
func (b *restoreBudget) track(keyLen, valueLen int) error {
	if b == nil {
		return nil
	}

	// Check entry count limit first (cheaper check).
	if b.maxEntries > 0 {
		if b.entries >= b.maxEntries {
			return fmt.Errorf("%w: cap=%d", ErrRestoreBudgetEntriesExceeded, b.maxEntries)
		}
	}

	entryBytes := int64(keyLen + valueLen)

	// Check byte limit after computing entry size. We check BEFORE incrementing
	// to ensure we never exceed the limit, even by one entry's worth of bytes.
	if b.maxBytes > 0 && b.bytesUsed+entryBytes > b.maxBytes {
		return fmt.Errorf("%w: cap=%d", ErrRestoreBudgetBytesExceeded, b.maxBytes)
	}

	// Only increment counters after all checks pass to maintain consistency.
	b.entries++
	b.bytesUsed += entryBytes

	return nil
}

// defaultMemorySnapshotPath returns the default path for memory snapshot files.
// Uses the same default location as the disk backend (.k6.kv in working directory)
// to provide consistent behavior and avoid surprising users with file placement.
func defaultMemorySnapshotPath() (string, error) {
	path, err := ResolveDiskPath("")
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrSnapshotPathResolveFailed, err)
	}

	return path, nil
}

// clamp constrains a value to lie within [low, high] bounds.
// Used to cap preallocation sizes to prevent OOM while avoiding tiny allocations.
func clamp(value, low, high int) int {
	return max(low, min(value, high))
}

// wrapSnapshotOpenError wraps an error with a snapshot file path.
func wrapSnapshotOpenError(err error, path string) error {
	switch {
	case errors.Is(err, os.ErrNotExist):
		return fmt.Errorf("%w: %s", ErrSnapshotNotFound, path)
	case errors.Is(err, os.ErrPermission): //nolint:forbidigo // os.ErrPermission is necessary here.
		return fmt.Errorf("%w: %s", ErrSnapshotPermissionDenied, path)
	default:
		return fmt.Errorf("%w: %s: %w", ErrSnapshotOpenFailed, path, err)
	}
}
