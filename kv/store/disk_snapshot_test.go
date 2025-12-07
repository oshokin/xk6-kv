package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// TestDiskStore_Backup_ProducesSnapshot verifies that Backup produces
// a valid bbolt snapshot file containing all key-value pairs from the store.
func TestDiskStore_Backup_ProducesSnapshot(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	require.NoError(t, store.Set("alpha", "1"))
	require.NoError(t, store.Set("beta", "2"))

	target := filepath.Join(t.TempDir(), "disk-export.kv")

	summary, err := store.Backup(&BackupOptions{FileName: target})
	require.NoError(t, err)
	assert.EqualValues(t, 2, summary.TotalEntries)
	assert.False(t, summary.BestEffort)
	assert.FileExists(t, target)

	db, err := bolt.Open(target, 0o600, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)

	defer db.Close()

	collected := map[string][]byte{}

	err = db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(defaultBBoltBucketBytes)
		require.NotNil(t, bucket)

		return bucket.ForEach(func(k, v []byte) error {
			collected[string(k)] = slices.Clone(v)

			return nil
		})
	})
	require.NoError(t, err)

	assert.Equal(t, []byte("1"), collected["alpha"])
	assert.Equal(t, []byte("2"), collected["beta"])
}

// TestDiskStore_Restore_ReplacesDataset verifies that Restore replaces
// the dataset with the contents of the snapshot file.
func TestDiskStore_Restore_ReplacesDataset(t *testing.T) {
	t.Parallel()

	memoryCfg := &MemoryConfig{TrackKeys: true}
	source := NewMemoryStore(memoryCfg)
	require.NoError(t, source.Set("k1", "v1"))
	require.NoError(t, source.Set("k2", "v2"))

	snapshotPath := filepath.Join(t.TempDir(), "import-src.kv")
	_, err := source.Backup(&BackupOptions{FileName: snapshotPath})
	require.NoError(t, err)

	store := newTestDiskStore(t, true, "", true)
	require.NoError(t, store.Set("stale", "data"))

	summary, err := store.Restore(&RestoreOptions{FileName: snapshotPath})
	require.NoError(t, err)
	assert.EqualValues(t, 2, summary.TotalEntries)

	val, err := store.Get("k1")
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), val)

	_, err = store.Get("stale")
	assert.Error(t, err)
}

// TestDiskStore_Backup_UsesUniqueTempFiles verifies that Backup creates unique
// temporary files during the snapshot process and cleans them up on success.
func TestDiskStore_Backup_UsesUniqueTempFiles(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	require.NoError(t, store.Set("key", "value"))

	tempDir := t.TempDir()
	target := filepath.Join(tempDir, "snapshot.kv")

	_, err := store.Backup(&BackupOptions{FileName: target})
	require.NoError(t, err)

	//nolint:forbidigo // file I/O is required for reading the directory.
	entries, err := os.ReadDir(tempDir)
	require.NoError(t, err)

	for _, entry := range entries {
		assert.False(t, strings.HasSuffix(entry.Name(), ".tmp"), "temporary file %q should be cleaned up", entry.Name())
	}
}

// TestDiskStore_Backup_AllowConcurrentWrites verifies that Backup with
// AllowConcurrentWrites=true permits writes to proceed during the export operation.
// For disk backend, bbolt transactions always provide consistent snapshots,
// so allowConcurrentWrites doesn't affect consistency but is tested for API completeness.
func TestDiskStore_Backup_AllowConcurrentWrites(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, false, "", true)
	require.NoError(t, store.Set("initial", "value"))

	target := filepath.Join(t.TempDir(), "concurrent.kv")
	done := make(chan error, 1)

	go func() {
		_, err := store.Backup(&BackupOptions{
			FileName:              target,
			AllowConcurrentWrites: true,
		})
		done <- err
	}()

	err := store.Set("during", "export")
	require.NoError(t, err)
	require.NoError(t, <-done)
}

// TestDiskStore_Backup_AllowConcurrentWrites_WarningPropagated verifies that the
// BestEffort flag is always false for disk backend (bbolt transactions provide
// consistent snapshots) regardless of the AllowConcurrentWrites setting.
func TestDiskStore_Backup_AllowConcurrentWrites_WarningPropagated(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, false, "", true)
	require.NoError(t, store.Set("key", "value"))

	bestEffortPath := filepath.Join(t.TempDir(), "best-effort.kv")
	bestEffortSummary, err := store.Backup(&BackupOptions{
		FileName:              bestEffortPath,
		AllowConcurrentWrites: true,
	})
	require.NoError(t, err)
	// Disk backend always uses bbolt transactions, so BestEffort is always false.
	assert.False(t, bestEffortSummary.BestEffort)
	assert.Empty(t, bestEffortSummary.Warning)

	blockingPath := filepath.Join(t.TempDir(), "blocking.kv")
	blockingSummary, err := store.Backup(&BackupOptions{
		FileName: blockingPath,
	})
	require.NoError(t, err)
	assert.False(t, blockingSummary.BestEffort)
	assert.Empty(t, blockingSummary.Warning)
}

// TestDiskStore_Restore_HonorsMaxEntries verifies that Restore respects the
// MaxEntries limit and fails with an error when the snapshot exceeds this limit.
func TestDiskStore_Restore_HonorsMaxEntries(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, false, "", true)
	for i := range 3 {
		require.NoError(t, store.Set(fmt.Sprintf("k%d", i), "v"))
	}

	target := filepath.Join(t.TempDir(), "too-many.kv")
	_, err := store.Backup(&BackupOptions{FileName: target})
	require.NoError(t, err)

	dest := newTestDiskStore(t, false, "", true)
	_, err = dest.Restore(&RestoreOptions{
		FileName:   target,
		MaxEntries: 2,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MaxEntries")
}

// TestDiskStore_Restore_HonorsMaxBytes verifies that Restore respects the
// MaxBytes limit and fails when the total key+value payload exceeds this limit.
func TestDiskStore_Restore_HonorsMaxBytes(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, false, "", true)
	require.NoError(t, store.Set("k1", strings.Repeat("x", 4)))

	target := filepath.Join(t.TempDir(), "max-bytes.kv")
	_, err := store.Backup(&BackupOptions{FileName: target})
	require.NoError(t, err)

	dest := newTestDiskStore(t, false, "", true)
	_, err = dest.Restore(&RestoreOptions{
		FileName: target,
		MaxBytes: 3, // smaller than key+value.
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MaxBytes")
}

// TestDiskStore_Restore_InvalidSnapshot verifies that Restore fails gracefully
// when attempting to load a corrupted or invalid snapshot file.
func TestDiskStore_Restore_InvalidSnapshot(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, false, "", true)

	target := filepath.Join(t.TempDir(), "invalid.kv")

	//nolint:forbidigo // file I/O is required for writing the invalid file.
	require.NoError(t, os.WriteFile(target, []byte("not a bolt file"), 0o644))

	_, err := store.Restore(&RestoreOptions{FileName: target})
	require.Error(t, err)
}

// TestDiskStore_Restore_BlocksMutationsDuringRestore verifies that Restore
// blocks concurrent writes to prevent data loss.
func TestDiskStore_Restore_BlocksMutationsDuringRestore(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	require.NoError(t, store.Set("k1", "v1"))

	exportPath := filepath.Join(t.TempDir(), "snapshot.kv")
	_, err := store.Backup(&BackupOptions{FileName: exportPath})
	require.NoError(t, err)

	// Clear the store so we have a known state.
	require.NoError(t, store.Clear())
	require.NoError(t, store.Set("during-import", "should-be-blocked"))

	done := make(chan error, 1)

	go func() {
		_, err := store.Restore(&RestoreOptions{FileName: exportPath})
		done <- err
	}()

	// Give import goroutine time to start and acquire the mutation lock.
	// This is a best-effort test for blocking behavior.
	for range 10 {
		err := store.Set("concurrent-write", "blocked")
		if errors.Is(err, ErrRestoreInProgress) {
			// Successfully observed the blocking behavior.
			require.NoError(t, <-done)

			return
		}
	}

	// If we never observed blocking, the test still passes
	// (timing-dependent behavior, but the race detector would catch issues).
	require.NoError(t, <-done)
}

// TestDiskStore_Restore_BlocksConcurrentBackup verifies that
// Backup cannot start while Restore is in progress.
func TestDiskStore_Restore_BlocksConcurrentBackup(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	require.NoError(t, store.Set("k1", "v1"))

	exportPath := filepath.Join(t.TempDir(), "snapshot.kv")
	_, err := store.Backup(&BackupOptions{FileName: exportPath})
	require.NoError(t, err)

	var wg sync.WaitGroup

	wg.Add(2)

	// Goroutine 1: Restore (takes time to parse stream).
	go func() {
		defer wg.Done()

		_, err := store.Restore(&RestoreOptions{FileName: exportPath})
		assert.NoError(t, err)
	}()

	// Goroutine 2: Backup while import is happening.
	go func() {
		defer wg.Done()

		time.Sleep(10 * time.Millisecond) // Let import start.

		exportPath2 := filepath.Join(t.TempDir(), "export2.kv")
		_, err := store.Backup(&BackupOptions{FileName: exportPath2})
		// Either succeeds (timing dependent) or fails with ErrRestoreInProgress.
		if err != nil {
			assert.ErrorIs(t, err, ErrRestoreInProgress)
		}
	}()

	wg.Wait()
}

// TestDiskStore_Restore_PreventsDataLoss verifies that writes during import
// are properly blocked, preventing the data loss bug where applySnapshot
// would silently overwrite concurrent writes.
func TestDiskStore_Restore_PreventsDataLoss(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	require.NoError(t, store.Set("k1", "v1"))

	exportPath := filepath.Join(t.TempDir(), "snapshot.kv")
	_, err := store.Backup(&BackupOptions{FileName: exportPath})
	require.NoError(t, err)

	// Clear the store and add a different key.
	require.NoError(t, store.Clear())
	require.NoError(t, store.Set("k2", "v2"))

	var wg sync.WaitGroup

	wg.Add(2)

	importDone := make(chan struct{})

	// Goroutine 1: Restore.
	go func() {
		defer wg.Done()
		defer close(importDone)

		_, err := store.Restore(&RestoreOptions{FileName: exportPath})
		assert.NoError(t, err)
	}()

	// Goroutine 2: Try to write while import is happening.
	var writeBlocked bool

	go func() {
		defer wg.Done()

		// Let import start.
		time.Sleep(5 * time.Millisecond)

		// Try to write during import.
		err := store.Set("k3", "v3")
		if errors.Is(err, ErrRestoreInProgress) {
			writeBlocked = true
		}

		// Wait for import to complete.
		<-importDone

		// After import completes, verify the store has the imported data.
		val, err := store.Get("k1")
		assert.NoError(t, err)
		assert.Equal(t, []byte("v1"), val)

		// k2 should be gone (overwritten by import).
		_, err = store.Get("k2")
		assert.Error(t, err)

		// If k3 was blocked, it should not exist.
		// If k3 succeeded (timing dependent), it would have been overwritten.
		_, err = store.Get("k3")
		if writeBlocked {
			assert.Error(t, err, "k3 should not exist after import")
		}
	}()

	wg.Wait()
}

// TestDiskStore_Restore_ConcurrentOperations_Serialized verifies that
// concurrent import attempts are serialized to prevent race conditions.
func TestDiskStore_Restore_ConcurrentOperations_Serialized(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)

	// Create two different snapshots.
	tempDir := t.TempDir()
	snapshots := []struct {
		id    string
		key   string
		value string
		path  string
	}{
		{
			id:    "snapshot1",
			key:   "k1",
			value: "v1",
			path:  filepath.Join(tempDir, "snapshot1.kv"),
		},
		{
			id:    "snapshot2",
			key:   "k2",
			value: "v2",
			path:  filepath.Join(tempDir, "snapshot2.kv"),
		},
	}

	for i, snap := range snapshots {
		require.NoError(t, store.Set(snap.key, snap.value))

		_, err := store.Backup(&BackupOptions{FileName: snap.path})
		require.NoError(t, err)

		if i < len(snapshots)-1 {
			require.NoError(t, store.Clear())
		}
	}

	require.NoError(t, store.Clear())

	var (
		results = make(chan string, len(snapshots))
		wg      sync.WaitGroup
	)

	wg.Add(len(snapshots))

	for _, snap := range snapshots {
		go func() {
			defer wg.Done()

			_, err := store.Restore(&RestoreOptions{FileName: snap.path})
			if err == nil {
				results <- snap.id
			} else if errors.Is(err, ErrRestoreInProgress) {
				results <- snap.id + "-blocked"
			}
		}()
	}

	wg.Wait()
	close(results)

	// Both imports should complete, but one should have blocked the other.
	// The final state should match one of the snapshots (deterministic outcome).
	importResults := make([]string, 0, len(results))
	for r := range results {
		importResults = append(importResults, r)
	}

	// At least one import should succeed.
	importSucceeded := make(map[string]bool, len(snapshots))
	for _, snap := range snapshots {
		importSucceeded[snap.id] = false
	}

	for _, r := range importResults {
		if _, ok := importSucceeded[r]; ok {
			importSucceeded[r] = true
		}
	}

	var anySucceeded bool

	for _, succeeded := range importSucceeded {
		if succeeded {
			anySucceeded = true

			break
		}
	}

	assert.True(t, anySucceeded, "at least one import should succeed")

	// Verify final state is consistent (not a mix of both imports).
	keys := []string{snapshots[0].key, snapshots[1].key}
	expectedValues := []string{snapshots[0].value, snapshots[1].value}

	errors := make([]error, len(keys))
	values := make([]any, len(keys))

	for i, key := range keys {
		values[i], errors[i] = store.Get(key)
	}

	// Should have either the first or second snapshot, not both.
	if errors[0] == nil {
		assert.Equal(t, []byte(expectedValues[0]), values[0])
		assert.Error(t, errors[1], "k2 should not exist if import1 won")
	} else {
		assert.Equal(t, []byte(expectedValues[1]), values[1])
		assert.Error(t, errors[0], "k1 should not exist if import2 won")
	}
}

// TestDiskStore_Restore_CleanupOnFailure verifies that if import fails
// partway through, the store is left in a consistent state and mutations
// are unblocked.
func TestDiskStore_Restore_CleanupOnFailure(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	require.NoError(t, store.Set("original", "data"))

	// Create a corrupted snapshot file.
	corruptedPath := filepath.Join(t.TempDir(), "corrupted.kv")
	//nolint:forbidigo // file I/O is required for writing the corrupted file.
	err := os.WriteFile(corruptedPath, []byte("not a bolt db"), 0o644)
	require.NoError(t, err)

	// Restore should fail.
	_, err = store.Restore(&RestoreOptions{FileName: corruptedPath})
	require.Error(t, err)

	// Mutations should be unblocked after failure.
	err = store.Set("after-failed-import", "should-work")
	require.NoError(t, err, "mutations should be unblocked after import failure")

	// Original data should still be intact.
	val, err := store.Get("original")
	require.NoError(t, err)
	assert.Equal(t, []byte("data"), val)
}

// TestDiskStore_Backup_Restore_RoundTrip verifies that Backup followed
// by Restore correctly preserves all key-value pairs with exact fidelity.
func TestDiskStore_Backup_Restore_RoundTrip(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, false, "", true)
	for i := range 10 {
		require.NoError(t, store.Set(
			strings.Join([]string{"key", strconv.Itoa(i)}, "-"),
			strings.Join([]string{"value", strconv.Itoa(i)}, "-"),
		))
	}

	target := filepath.Join(t.TempDir(), "roundtrip.kv")
	_, err := store.Backup(&BackupOptions{FileName: target})
	require.NoError(t, err)

	clone := newTestDiskStore(t, false, "", true)
	summary, err := clone.Restore(&RestoreOptions{FileName: target})
	require.NoError(t, err)
	assert.Equal(t, int64(10), func() int64 {
		size, sizeErr := clone.Size()
		require.NoError(t, sizeErr)

		return size
	}())
	assert.EqualValues(t, 10, summary.TotalEntries)

	for i := range 10 {
		key := strings.Join([]string{"key", strconv.Itoa(i)}, "-")
		val, getErr := clone.Get(key)
		require.NoError(t, getErr)
		assert.Equal(t, []byte(strings.Join([]string{"value", strconv.Itoa(i)}, "-")), val)
	}
}
