package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	bolt "go.etcd.io/bbolt"
	boltErrors "go.etcd.io/bbolt/errors"

	"github.com/oshokin/xk6-kv/internal/fileutil"
)

type (
	// diskRestoreFromSnapshotDBParams carries inputs for restoring from a snapshot bbolt file.
	diskRestoreFromSnapshotDBParams struct {
		// ctx cancels long-running restore work.
		ctx context.Context
		// snapshotDB is the opened snapshot database handle.
		snapshotDB *bolt.DB
		// maxEntries caps how many entries may be loaded.
		maxEntries int64
		// maxBytes caps total loaded payload size.
		maxBytes int64
	}

	// diskTryFinalizeSelfRestoreParams carries inputs for finalizing an in-place self-restore.
	diskTryFinalizeSelfRestoreParams struct {
		// ctx cancels long-running restore work.
		ctx context.Context
		// snapshotPath is the on-disk path of the snapshot being applied.
		snapshotPath string
	}

	// diskSelfRestoreResult reports whether self-restore completed and its summary.
	diskSelfRestoreResult struct {
		// summary describes restore outcome when done is true.
		summary *RestoreSummary
		// done reports whether self-restore finished successfully.
		done bool
	}
)

// Backup writes the on-disk bbolt database to a standalone snapshot file.
// Unlike MemoryStore, DiskStore already persists data, so Backup simply copies
// the existing bbolt file using a consistent View transaction.
func (s *DiskStore) Backup(opts *BackupOptions) (*BackupSummary, error) {
	if opts == nil {
		return nil, ErrBackupOptionsNil
	}

	ctx := operationContextOrBackground(opts.Context)
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if err := opts.normalize(); err != nil {
		return nil, err
	}

	release, err := s.beginOperation()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	if s.isSelfSnapshot(opts.FileName) {
		return s.diskSelfSnapshotSummary()
	}

	return s.writeDiskSnapshot(ctx, opts.FileName)
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
//
//nolint:funlen // this is a complex function.
func (s *DiskStore) writeDiskSnapshot(ctx context.Context, destination string) (*BackupSummary, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

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
	// Track export error for defer cleanup: only remove temporary file if operation failed.
	// On success, temporary file is renamed to final destination, so cleanup would be wrong.
	// Using a named return variable allows defer to inspect the final error state.
	var exportErr error

	defer func() {
		// Only clean up temporary file if an error occurred. On success, Rename() moves
		// the temporary file to the final destination, so removing it would delete the backup.
		// On Unix-like filesystems this gives atomic replacement semantics. On
		// platforms where os.Rename is not guaranteed atomic, this still avoids
		// writing partial data directly into the destination path.
		if exportErr != nil {
			// Best-effort close for the temp handle; failure here just adds context
			// to the original export error.
			_ = tempHandle.Close()
			//nolint:forbidigo // file I/O is required for removing the temporary file on error.
			// Cleanup is likewise best-effort; even if removal fails we still return
			// the root exportErr to tell the caller the backup didn't finish.
			_ = os.Remove(tempFile)
		}
	}()

	var totalEntries int64

	// Use a read-only View transaction to atomically copy the entire database.
	// tx.WriteTo streams the database file format directly, preserving all buckets,
	// transactions, and metadata. This is more efficient than iterating entries.
	if err := s.handle.View(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		// Capture entry count before WriteTo (Stats() is only valid during transaction).
		// Must read stats while transaction is active, before WriteTo consumes it.
		totalEntries = int64(bucket.Stats().KeyN)

		// WriteTo streams the entire bbolt file format to the writer.
		// This includes all buckets, pages, and metadata in a consistent snapshot.
		// More efficient than iterating entries: copies raw database pages directly.
		if _, err := tx.WriteTo(tempHandle); err != nil {
			return fmt.Errorf("%w: %w", ErrBackupCopyFailed, err)
		}

		return nil
	}); err != nil {
		exportErr = fmt.Errorf("%w: %w", ErrSnapshotExportFailed, err)

		return nil, exportErr
	}

	if err := ctx.Err(); err != nil {
		exportErr = err
		return nil, exportErr
	}

	// Sync file system buffers to ensure all data is physically written to disk
	// before renaming. This prevents data loss if the process crashes between
	// WriteTo and Rename. Without sync, buffered writes might be lost on crash.
	if err := tempHandle.Sync(); err != nil {
		exportErr = fmt.Errorf("%w: %w", ErrSnapshotExportFailed, err)

		return nil, exportErr
	}

	if err := tempHandle.Close(); err != nil {
		exportErr = fmt.Errorf("%w: %w", ErrBackupTempFileFailed, err)

		return nil, exportErr
	}

	if err := ctx.Err(); err != nil {
		exportErr = err
		return nil, exportErr
	}

	info, err := os.Stat(tempFile)
	if err != nil {
		exportErr = fmt.Errorf("%w: %w", ErrBBoltSnapshotStatFailed, err)

		return nil, exportErr
	}

	if err := fileutil.ReplaceFile(tempFile, destination); err != nil {
		exportErr = fmt.Errorf("%w: %w", ErrBackupFinalizeFailed, err)

		return nil, exportErr
	}

	if err := fileutil.SyncParentDir(destination); err != nil {
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
// The restore is performed inside a single bbolt write transaction for atomicity.
func (s *DiskStore) Restore(opts *RestoreOptions) (*RestoreSummary, error) {
	if opts == nil {
		return nil, ErrRestoreOptionsNil
	}

	ctx := operationContextOrBackground(opts.Context)
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if err := opts.normalize(); err != nil {
		return nil, err
	}

	release, err := s.beginOperation()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	if err := s.ensureWritable(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrSnapshotReadFailed, err)
	}

	if s.trackKeys {
		// Keep restore mutation and index rebuild as one logical operation.
		s.keysLock.Lock()
		defer s.keysLock.Unlock()
	}

	if s.testRestoreHook != nil {
		s.testRestoreHook()
	}

	// Fast path: if restoring from the same file (self-reference), return metadata
	// without copying. This avoids unnecessary work when snapshot path matches store path.
	selfRestoreResult, err := s.tryFinalizeSelfRestore(&diskTryFinalizeSelfRestoreParams{
		ctx:          ctx,
		snapshotPath: opts.FileName,
	})
	if err != nil {
		return nil, err
	}

	if selfRestoreResult.done {
		return selfRestoreResult.summary, nil
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	snapshotDB, err := bolt.Open(opts.FileName, 0o600, &bolt.Options{ReadOnly: true})
	if err != nil {
		return nil, wrapSnapshotOpenError(err, opts.FileName)
	}

	defer snapshotDB.Close()

	totalEntries, err := s.restoreFromSnapshotDB(&diskRestoreFromSnapshotDBParams{
		ctx:        ctx,
		snapshotDB: snapshotDB,
		maxEntries: opts.MaxEntries,
		maxBytes:   opts.MaxBytes,
	})
	if err != nil {
		return nil, err
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if s.trackKeys {
		if err := s.rebuildKeyListLocked(); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrKeyListRebuildFailed, err)
		}

		s.resetTrackedClaimsLocked()
	}

	return &RestoreSummary{
		TotalEntries: totalEntries,
	}, nil
}

// tryDiskSelfRestore checks if the snapshot path matches the store path.
// Returns done=true with summary when self-restore is detected.
func (s *DiskStore) tryDiskSelfRestore(snapshotPath string) *diskSelfRestoreResult {
	absSnapshotPath, err := filepath.Abs(snapshotPath)
	if err != nil || absSnapshotPath != s.path {
		return &diskSelfRestoreResult{}
	}

	totalEntries, err := s.diskKeyCount()
	if err != nil {
		return &diskSelfRestoreResult{}
	}

	return &diskSelfRestoreResult{
		summary: &RestoreSummary{
			TotalEntries: totalEntries,
		},
		done: true,
	}
}

// tryFinalizeSelfRestore completes an in-place restore and clears persisted claim leases.
func (s *DiskStore) tryFinalizeSelfRestore(
	params *diskTryFinalizeSelfRestoreParams,
) (*diskSelfRestoreResult, error) {
	selfRestoreResult := s.tryDiskSelfRestore(params.snapshotPath)
	if !selfRestoreResult.done {
		return selfRestoreResult, nil
	}

	if err := params.ctx.Err(); err != nil {
		return nil, err
	}

	// Self-restore still starts a "new world"; drop all persisted claim leases.
	if err := s.clearDiskClaims(); err != nil {
		return nil, err
	}

	if s.trackKeys {
		s.resetTrackedClaimsLocked()
	}

	return selfRestoreResult, nil
}

// restoreFromSnapshotDB performs the actual restore from an opened snapshot database.
func (s *DiskStore) restoreFromSnapshotDB(params *diskRestoreFromSnapshotDBParams) (int64, error) {
	if err := params.ctx.Err(); err != nil {
		return 0, err
	}

	budget := &restoreBudget{
		maxEntries: params.maxEntries,
		maxBytes:   params.maxBytes,
	}

	var totalEntries int64

	// Perform restore in a single write transaction for atomicity.
	// If restore fails partway through, the entire operation rolls back.
	if err := s.handle.Update(func(tx *bolt.Tx) error {
		if err := params.ctx.Err(); err != nil {
			return err
		}

		// Drop existing bucket to start fresh. ErrBucketNotFound is acceptable
		// if the bucket doesn't exist yet (empty store).
		if err := tx.DeleteBucket(s.bucket); err != nil && !errors.Is(err, boltErrors.ErrBucketNotFound) {
			return fmt.Errorf("%w: %w", ErrDiskStoreDeleteFailed, err)
		}

		// Claims are process-local leases, restore always starts a new lease world.
		if err := s.clearClaimsBucket(tx); err != nil {
			return err
		}

		// Create fresh bucket for restored data.
		bucket, err := tx.CreateBucket(s.bucket)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrDiskStoreWriteFailed, err)
		}

		// Nested transaction pattern: read from snapshot DB (read-only View)
		// while writing to destination DB (write Update). This is safe because
		// bbolt allows concurrent read transactions from different DB handles.
		// The snapshot DB is opened read-only, so no write conflicts can occur.
		return params.snapshotDB.View(func(snapshotTx *bolt.Tx) error {
			if err := params.ctx.Err(); err != nil {
				return err
			}

			srcBucket := snapshotTx.Bucket(defaultBBoltBucketBytes)
			if srcBucket == nil {
				return fmt.Errorf("%w: %q", ErrBucketNotFound, defaultBBoltBucketBytes)
			}

			// Copy all entries from snapshot bucket to destination bucket.
			return srcBucket.ForEach(func(k, v []byte) error {
				if err := params.ctx.Err(); err != nil {
					return err
				}

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
		return 0, fmt.Errorf("%w: %w", ErrSnapshotReadFailed, err)
	}

	if err := params.ctx.Err(); err != nil {
		return 0, err
	}

	return totalEntries, nil
}

// diskKeyCount returns the number of keys in the disk store's bucket.
// Uses bbolt's Stats().KeyN for O(1) counting without iterating entries.
func (s *DiskStore) diskKeyCount() (int64, error) {
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

	return int64(count), nil
}
