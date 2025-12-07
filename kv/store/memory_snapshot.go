package store

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"

	bolt "go.etcd.io/bbolt"
)

type (

	// bboltBatchWriter buffers entries and flushes them in bounded bbolt transactions.
	bboltBatchWriter struct {
		// db is the underlying bbolt database.
		db *bolt.DB

		// pending is a slice of key/value pairs staged for writing to the bbolt database.
		pending []*snapshotPair

		// pendingBytes is the total size of the pending key/value pairs.
		pendingBytes int
	}

	// snapshotPair represents a key/value pair staged for bbolt writes.
	snapshotPair struct {
		// key is the key of the key/value pair.
		key []byte

		// value is the value of the key/value pair.
		value []byte
	}
)

const (
	// defaultMapPreallocationCapacity is the default map capacity for restores when
	// no better hint is available.
	defaultMapPreallocationCapacity = 1024

	// maxMapPreallocationCapacity caps map preallocation to keep memory bounded.
	maxMapPreallocationCapacity = 10_000_000

	// boltDBSnapshotMaxBatchEntries caps how many entries we write per bbolt transaction.
	boltDBSnapshotMaxBatchEntries = 50_000

	// boltDBSnapshotMaxBatchBytes caps the total key/value payload per bbolt transaction (~64MB).
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

// Backup writes the entire memory store into a bbolt snapshot file.
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

	summary, backupErr := s.backupToBBolt(tempFile, opts)
	if backupErr != nil {
		//nolint:forbidigo // file I/O is required for removing the temporary file on error.
		// Cleanup is best-effort-the caller already has backupErr to act on.
		_ = os.Remove(tempFile)

		return nil, backupErr
	}

	//nolint:forbidigo // file I/O is required for renaming the temporary file to the destination file.
	if err := os.Rename(tempFile, opts.FileName); err != nil {
		//nolint:forbidigo // file I/O is required for removing the temporary file on error.
		// Same logic: if the rename failed, removing the temporary file is a courtesy.
		_ = os.Remove(tempFile)

		return nil, fmt.Errorf("%w: %w", ErrBackupFinalizeFailed, err)
	}

	return summary, nil
}

// backupToBBolt backs up a snapshot to a bbolt file using one of two strategies:
//  1. Concurrent mode (AllowConcurrentWrites): blocks writers only during key capture,
//     then streams values while writers proceed. Best-effort consistency.
//  2. Blocking mode: holds mutation lock for entire backup. Strict consistency.
func (s *MemoryStore) backupToBBolt(tempFile string, opts *BackupOptions) (*BackupSummary, error) {
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
	// Set to true after successful unblock to prevent defer from unlocking again.
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
	// This trade-off allows backups to proceed without blocking all writes.
	s.unblockMutations()

	blockReleased = true

	return s.writeBBoltSnapshot(tempFile, true, func(yield func(string, []byte) error) error {
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

	return s.writeBBoltSnapshot(tempFile, false, func(yield func(string, []byte) error) error {
		for _, key := range keys {
			shard := s.getShardByKey(key)
			shard.mu.RLock()
			value, exists := shard.container[key]
			shard.mu.RUnlock()

			if !exists {
				// Should never happen in blocking mode - indicates a bug.
				return fmt.Errorf("%w: %q", ErrSnapshotKeyMissing, key)
			}

			// Writers are blocked for the duration of this backup, so we can stream the
			// existing value slice directly without cloning. This keeps the blocking
			// path low-overhead compared to the concurrent snapshot mode.
			// No risk of concurrent modification since mutations are blocked.
			if err := yield(key, value); err != nil {
				return err
			}
		}

		return nil
	})
}

// writeBBoltSnapshot writes a snapshot to a bbolt file using batched transactions.
// The iterate callback provides key/value pairs via a yield function, which are
// buffered and written in bounded batches to prevent unbounded transaction sizes.
// Returns a summary with entry count, file size, and consistency guarantees.
func (s *MemoryStore) writeBBoltSnapshot(
	tempFile string,
	bestEffort bool,
	iterate func(yield func(string, []byte) error) error,
) (*BackupSummary, error) {
	// Open bbolt file with restrictive permissions (owner read/write only).
	db, err := bolt.Open(tempFile, 0o600, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBBoltSnapshotOpenFailed, err)
	}

	// Track whether we successfully closed to avoid double-close in defer.
	var closed bool

	defer func() {
		if !closed {
			// The restore failed before we returned the DB to callers, so closing
			// it is purely best-effort - the original error is what users need.
			_ = db.Close()
		}
	}()

	// Batch writer handles transaction size limits automatically.
	writer := newBBoltBatchWriter(db)

	var written int64

	// Stream entries from iterator into batched bbolt writes.
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
		return nil, fmt.Errorf("%w: %w", ErrBBoltSnapshotCloseFailed, err)
	}

	closed = true

	// Stat file to report final on-disk size.
	info, err := os.Stat(tempFile)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBBoltSnapshotStatFailed, err)
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

// Restore replaces the store contents with entries from a bbolt snapshot.
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
		bucket := tx.Bucket(defaultBBoltBucketBytes)
		if bucket == nil {
			return fmt.Errorf("%w: %q", ErrBucketNotFound, defaultBBoltBucketBytes)
		}

		return bucket.ForEach(func(k, v []byte) error {
			// Check budget limits before accumulating this entry.
			if err := budget.track(len(k), len(v)); err != nil {
				return err
			}

			// Clone value bytes to avoid aliasing bbolt's internal memory.
			builder[string(k)] = slices.Clone(v)

			return nil
		})
	}); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrSnapshotReadFailed, err)
	}

	// Atomically replace entire store contents. This happens while mutations are blocked,
	// ensuring no concurrent writes can interfere with the restore operation.
	s.applySnapshot(builder)

	summary := &RestoreSummary{
		TotalEntries: int64(len(builder)),
	}

	// Unblock mutations after successful restore. Set flag to prevent double-unlock in defer.
	s.unblockMutations()

	blockReleased = true

	return summary, nil
}

// applySnapshot atomically replaces the store's entire contents with the restored container.
// Rebuilds key tracking structures (keysList, keysMap, OSTree) if key tracking is enabled.
// This is the critical operation where the old state is discarded and new state takes over.
// Locks all shards upfront to ensure atomic replacement across all shards.
func (s *MemoryStore) applySnapshot(container map[string][]byte) {
	// Acquire all shard locks before modifying any shard to prevent partial state.
	for _, shard := range s.shards {
		shard.mu.Lock()
	}

	for _, shard := range s.shards {
		shard.container = make(map[string][]byte)

		if s.trackKeys {
			shard.keysList = []string{}
			shard.keysMap = make(map[string]int)
			shard.ost = NewOSTree()
		} else {
			shard.keysList = nil
			shard.keysMap = nil
			shard.ost = nil
		}
	}

	for key, value := range container {
		shard := s.getShardByKey(key)
		shard.container[key] = value
		shard.addKeyTrackingLocked(key)
	}

	// Unlock in reverse order (best practice for nested locks, though not strictly required here).
	for i := len(s.shards) - 1; i >= 0; i-- {
		s.shards[i].mu.Unlock()
	}
}

// getPreallocationCapacity calculates the optimal map capacity for restore preallocation.
// Balances between memory efficiency and avoiding repeated reallocations.
func (s *MemoryStore) getPreallocationCapacity(totalEntries, maxEntries int64) int64 {
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
	s.lockAllShardReaders()
	defer s.unlockAllShardReaders()

	if s.trackKeys {
		var total int
		for _, shard := range s.shards {
			total += len(shard.keysList)
		}

		keys := make([]string, 0, total)
		for _, shard := range s.shards {
			keys = append(keys, shard.keysList...)
		}

		return keys
	}

	var total int
	for _, shard := range s.shards {
		total += len(shard.container)
	}

	keys := make([]string, 0, total)

	for _, shard := range s.shards {
		for key := range shard.container {
			keys = append(keys, key)
		}
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
		shard := s.getShardByKey(key)
		shard.mu.RLock()
		value, exists := shard.container[key]
		shard.mu.RUnlock()

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
		// bbolt batch writer so tests can assert chunking behavior without peeking
		// inside the writer itself. This allows tests to verify that streaming
		// produces expected batch boundaries.
		if chunkEntries >= boltDBSnapshotMaxBatchEntries ||
			chunkBytes >= boltDBSnapshotMaxBatchBytes {
			flushChunk()
		}
	}

	// Final notification for any remaining entries in the last partial chunk.
	flushChunk()

	return nil
}

// newBBoltBatchWriter creates a new bboltBatchWriter that buffers writes
// into bounded transactions to prevent memory exhaustion and transaction timeouts.
// Preallocates buffer capacity to reduce allocations during streaming.
func newBBoltBatchWriter(db *bolt.DB) *bboltBatchWriter {
	return &bboltBatchWriter{
		db:      db,
		pending: make([]*snapshotPair, 0, boltDBSnapshotMaxBatchEntries),
	}
}

// append adds a key/value pair to the pending batch buffer.
// Automatically flushes when batch size limits are reached (entry count or byte size).
// This prevents unbounded transaction growth and memory usage during large backups.
func (b *bboltBatchWriter) append(key string, value []byte) error {
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

// flush writes all buffered entries to bbolt in a single transaction.
// After successful write, resets the buffer for the next batch.
// The slice aliasing (batch = b.pending) is safe because the backup pipeline
// is single-threaded - no concurrent appends can occur during the transaction.
func (b *bboltBatchWriter) flush() error {
	if len(b.pending) == 0 {
		return nil
	}

	// Capture pending slice and reset for next batch.
	// This aliases the underlying array, which is safe because the pipeline
	// is single-threaded: no new appends occur until this transaction completes.
	// The slice header is copied, but the underlying array is shared (safe here).
	batch := b.pending
	b.pending = b.pending[:0]
	b.pendingBytes = 0

	// Write entire batch in single transaction for atomicity and performance.
	if err := b.db.Update(func(tx *bolt.Tx) error {
		// Create bucket if first write, or get existing bucket.
		bucket, err := tx.CreateBucketIfNotExists(defaultBBoltBucketBytes)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrBBoltBucketCreateFailed, err)
		}

		// Write all buffered entries within this transaction.
		for _, entry := range batch {
			if err := bucket.Put(entry.key, entry.value); err != nil {
				return fmt.Errorf("%w: %w", ErrBBoltWriteFailed, err)
			}
		}

		return nil
	}); err != nil {
		return err
	}

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
