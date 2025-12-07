package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type (
	// BackupOptions configure how Backup writes a bbolt-based snapshot.
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
	// Fields are tagged for JavaScript camelCase convention when exposed via Sobek.
	BackupSummary struct {
		// TotalEntries is the number of key/value pairs captured in the snapshot.
		TotalEntries int64 `js:"totalEntries"`

		// BytesWritten is the final size of the snapshot file on disk.
		BytesWritten int64 `js:"bytesWritten"`

		// BestEffort is true when the snapshot was taken with AllowConcurrentWrites.
		BestEffort bool `js:"bestEffort"`

		// Warning contains additional context about the snapshot mode.
		// When BestEffort is true, Warning repeats allowConcurrentWritesWarning so callers
		// can surface it to operators.
		Warning string `js:"warning"`
	}

	// RestoreOptions configure Restore behavior.
	RestoreOptions struct {
		// FileName is the path to the bbolt snapshot file. Required.
		FileName string

		// MaxEntries limits how many entries may be loaded.
		// Zero disables the cap.
		MaxEntries int64

		// MaxBytes limits the aggregate key/value payload hydrated during restore.
		// Zero disables the cap.
		MaxBytes int64
	}

	// RestoreSummary reports the outcome of a restore operation.
	// Fields are tagged for JavaScript camelCase convention when exposed via Sobek.
	RestoreSummary struct {
		// TotalEntries is the number of key/value pairs hydrated from the snapshot.
		TotalEntries int64 `js:"totalEntries"`
	}

	// restoreBudget tracks safety limits during restore operations.
	// Prevents OOM and excessive resource consumption during snapshot hydration.
	restoreBudget struct {
		// maxEntries is the maximum number of entries that can be restored.
		maxEntries int64

		// maxBytes is the maximum number of bytes that can be restored.
		maxBytes int64

		// entries is the number of entries that have been restored.
		entries int64

		// bytesUsed is the number of bytes that have been restored.
		bytesUsed int64
	}
)

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
	// This prevents partial entry accumulation that would violate the budget.
	if b.maxBytes > 0 && b.bytesUsed+entryBytes > b.maxBytes {
		return fmt.Errorf("%w: cap=%d", ErrRestoreBudgetBytesExceeded, b.maxBytes)
	}

	// Only increment counters after all checks pass to maintain consistency.
	// If any check fails, budget state remains unchanged (atomic-like behavior).
	b.entries++
	b.bytesUsed += entryBytes

	return nil
}

// wrapSnapshotOpenError wraps an error with a snapshot file path.
func wrapSnapshotOpenError(err error, path string) error {
	switch {
	case errors.Is(err, os.ErrNotExist):
		return fmt.Errorf("%w: %s", ErrSnapshotNotFound, path)
	case errors.Is(err, os.ErrPermission): //nolint:forbidigo // file I/O is required for checking the permission.
		return fmt.Errorf("%w: %s", ErrSnapshotPermissionDenied, path)
	default:
		return fmt.Errorf("%w: %s: %w", ErrSnapshotOpenFailed, path, err)
	}
}
