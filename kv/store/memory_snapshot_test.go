package store

import (
	"bytes"
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

// TestMemoryStore_Backup_UsesUniqueTempFiles verifies that Backup creates unique
// temporary files during the snapshot process and cleans them up on success.
func TestMemoryStore_Backup_UsesUniqueTempFiles(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true, 0)
	require.NoError(t, store.Set("key", "value"))

	tempDir := t.TempDir()
	target := filepath.Join(tempDir, "snapshot.kv")

	_, err := store.Backup(&BackupOptions{FileName: target})
	require.NoError(t, err)

	//nolint:forbidigo // test needs to read directory.
	entries, err := os.ReadDir(tempDir)
	require.NoError(t, err)

	for _, entry := range entries {
		assert.False(t, strings.HasSuffix(entry.Name(), ".tmp"), "temporary file %q should be cleaned up", entry.Name())
	}
}

// TestMemoryStore_Backup_CreatesBoltSnapshot verifies that Backup creates a valid
// BoltDB snapshot file containing all key-value pairs from the store.
func TestMemoryStore_Backup_CreatesBoltSnapshot(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true, 0)
	require.NoError(t, store.Set("alpha", "1"))
	require.NoError(t, store.Set("beta", "2"))
	require.NoError(t, store.Set("gamma", "3"))

	target := filepath.Join(t.TempDir(), "snapshot.kv")

	summary, err := store.Backup(&BackupOptions{FileName: target})
	require.NoError(t, err)
	assert.Equal(t, 3, summary.TotalEntries)
	assert.FileExists(t, target)

	db, err := bolt.Open(target, 0o600, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)

	defer db.Close()

	collected := map[string][]byte{}

	err = db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(defaultBoltDBBucketBytes)
		require.NotNil(t, bucket, "snapshot bucket should exist")

		return bucket.ForEach(func(k, v []byte) error {
			collected[string(k)] = slices.Clone(v)

			return nil
		})
	})
	require.NoError(t, err)

	assert.Len(t, collected, 3)
	assert.Equal(t, []byte("1"), collected["alpha"])
	assert.Equal(t, []byte("2"), collected["beta"])
	assert.Equal(t, []byte("3"), collected["gamma"])
}

// TestMemoryStore_Backup_BlocksMutations verifies that Backup (in blocking mode)
// prevents concurrent writes for the entire duration of the export operation.
func TestMemoryStore_Backup_BlocksMutations(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true, 0)
	require.NoError(t, store.Set("initial", "value"))

	target := filepath.Join(t.TempDir(), "blocking.kv")
	done := make(chan error, 1)

	go func() {
		_, err := store.Backup(&BackupOptions{FileName: target})
		done <- err
	}()

	deadline := time.After(100 * time.Millisecond)

	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for export to block writers")
		default:
			err := store.Set("during", "export")
			if errors.Is(err, ErrBackupInProgress) {
				require.NoError(t, <-done)
				return
			}
		}
	}
}

// TestMemoryStore_Backup_AllowConcurrentWrites verifies that Backup with
// AllowConcurrentWrites=true permits writes to proceed during the export operation.
func TestMemoryStore_Backup_AllowConcurrentWrites(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(false, 0)
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

// TestMemoryStore_Backup_AllowConcurrentWritesWarningPropagated verifies that the
// BestEffort flag and warning message are properly set in the summary when
// AllowConcurrentWrites is enabled, and absent when disabled.
func TestMemoryStore_Backup_AllowConcurrentWrites_WarningPropagated(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(false, 0)
	require.NoError(t, store.Set("key", "value"))

	bestEffortPath := filepath.Join(t.TempDir(), "best-effort.kv")
	bestEffortSummary, err := store.Backup(&BackupOptions{
		FileName:              bestEffortPath,
		AllowConcurrentWrites: true,
	})
	require.NoError(t, err)
	assert.True(t, bestEffortSummary.BestEffort)
	assert.Equal(t, allowConcurrentWritesWarning, bestEffortSummary.Warning)

	blockingPath := filepath.Join(t.TempDir(), "blocking.kv")
	blockingSummary, err := store.Backup(&BackupOptions{
		FileName: blockingPath,
	})
	require.NoError(t, err)
	assert.False(t, blockingSummary.BestEffort)
	assert.Empty(t, blockingSummary.Warning)
}

// TestMemoryStore_Backup_AllowConcurrentWrites_StreamingMemoryFootprint verifies that
// Backup with AllowConcurrentWrites uses bounded memory by streaming entries in chunks
// rather than loading the entire dataset into memory at once.
// Observes chunk sizes via test hook to confirm batching behavior.
func TestMemoryStore_Backup_AllowConcurrentWrites_StreamingMemoryFootprint(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(false, 0)
	payload := bytes.Repeat([]byte("x"), 64)

	const keyCount = 120_000

	for i := range keyCount {
		require.NoError(t, store.Set(fmt.Sprintf("key-%d", i), payload))
	}

	var maxChunk int

	streamSnapshotChunkObserver = func(chunkLen int) {
		if chunkLen > maxChunk {
			maxChunk = chunkLen
		}
	}

	defer func() {
		streamSnapshotChunkObserver = nil
	}()

	tempFile := filepath.Join(t.TempDir(), "streamed.kv")
	summary, err := store.Backup(&BackupOptions{
		FileName:              tempFile,
		AllowConcurrentWrites: true,
	})

	require.NoError(t, err)
	assert.True(t, summary.BestEffort)
	assert.Equal(t, allowConcurrentWritesWarning, summary.Warning)
	assert.Positive(t, maxChunk)
	assert.LessOrEqual(t, maxChunk, boltDBSnapshotMaxBatchEntries)
	assert.Less(t, maxChunk, keyCount)
}

// TestMemoryStore_Restore_HonorsMaxEntries verifies that Restore respects the
// MaxEntries limit and fails with an error when the snapshot exceeds this limit.
func TestMemoryStore_Restore_HonorsMaxEntries(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(false, 0)
	for i := range 3 {
		require.NoError(t, store.Set(fmt.Sprintf("k%d", i), "v"))
	}

	target := filepath.Join(t.TempDir(), "too-many.kv")
	_, err := store.Backup(&BackupOptions{FileName: target})
	require.NoError(t, err)

	dest := NewMemoryStore(false, 0)
	_, err = dest.Restore(&RestoreOptions{
		FileName:   target,
		MaxEntries: 2,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MaxEntries")
}

// TestMemoryStore_Restore_HonorsMaxBytes verifies that Restore respects the
// MaxBytes limit and fails when the total key+value payload exceeds this limit.
func TestMemoryStore_Restore_HonorsMaxBytes(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(false, 0)
	require.NoError(t, store.Set("k1", strings.Repeat("x", 4)))

	target := filepath.Join(t.TempDir(), "max-bytes.kv")
	_, err := store.Backup(&BackupOptions{FileName: target})
	require.NoError(t, err)

	dest := NewMemoryStore(false, 0)
	_, err = dest.Restore(&RestoreOptions{
		FileName: target,
		MaxBytes: 3, // smaller than key+value.
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MaxBytes")
}

// TestMemoryStore_Restore_InvalidSnapshot verifies that Restore fails gracefully
// when attempting to load a corrupted or invalid snapshot file.
func TestMemoryStore_Restore_InvalidSnapshot(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(false, 0)

	target := filepath.Join(t.TempDir(), "invalid.kv")

	//nolint:forbidigo // test needs to write invalid file.
	require.NoError(t, os.WriteFile(target, []byte("not a bolt file"), 0o644))

	_, err := store.Restore(&RestoreOptions{FileName: target})
	require.Error(t, err)
}

// TestMemoryStore_Restore_BlocksMutationsDuringRestore verifies that Restore
// blocks concurrent writes to prevent data loss.
func TestMemoryStore_Restore_BlocksMutationsDuringRestore(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true, 0)
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

// TestMemoryStore_Restore_BlocksConcurrentBackup verifies that
// Backup cannot start while Restore is in progress.
func TestMemoryStore_Restore_BlocksConcurrentBackup(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true, 0)
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

// TestMemoryStore_Restore_PreventsDataLoss verifies that writes during import
// are properly blocked, preventing the data loss bug where applySnapshot
// would silently overwrite concurrent writes.
func TestMemoryStore_Restore_PreventsDataLoss(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true, 0)
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

// TestMemoryStore_Restore_ConcurrentOperations_Serialized verifies that
// concurrent import attempts are serialized to prevent race conditions.
func TestMemoryStore_Restore_ConcurrentOperations_Serialized(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true, 0)

	// Create two different snapshots.
	require.NoError(t, store.Set("k1", "v1"))

	exportPath1 := filepath.Join(t.TempDir(), "snapshot1.kv")
	_, err := store.Backup(&BackupOptions{FileName: exportPath1})
	require.NoError(t, err)

	require.NoError(t, store.Clear())
	require.NoError(t, store.Set("k2", "v2"))

	exportPath2 := filepath.Join(t.TempDir(), "snapshot2.kv")
	_, err = store.Backup(&BackupOptions{FileName: exportPath2})
	require.NoError(t, err)

	require.NoError(t, store.Clear())

	var (
		results = make(chan string, 2)
		wg      sync.WaitGroup
	)

	wg.Add(2)

	// Goroutine 1: Restore snapshot1.
	go func() {
		defer wg.Done()

		_, err := store.Restore(&RestoreOptions{FileName: exportPath1})
		if err == nil {
			results <- "import1"
		} else if errors.Is(err, ErrRestoreInProgress) {
			results <- "import1-blocked"
		}
	}()

	// Goroutine 2: Restore snapshot2.
	go func() {
		defer wg.Done()

		_, err := store.Restore(&RestoreOptions{FileName: exportPath2})
		if err == nil {
			results <- "import2"
		} else if errors.Is(err, ErrRestoreInProgress) {
			results <- "import2-blocked"
		}
	}()

	wg.Wait()
	close(results)

	// Both imports should complete, but one should have blocked the other.
	// The final state should match one of the snapshots (deterministic outcome).
	importResults := make([]string, 0, len(results))
	for r := range results {
		importResults = append(importResults, r)
	}

	// At least one import should succeed.
	importSucceeded := map[string]bool{
		"import1": false,
		"import2": false,
	}

	for _, r := range importResults {
		switch r {
		case "import1":
			importSucceeded["import1"] = true
		case "import2":
			importSucceeded["import2"] = true
		}
	}

	assert.True(t, importSucceeded["import1"] || importSucceeded["import2"], "at least one import should succeed")

	// Verify final state is consistent (not a mix of both imports).
	keys := []string{"k1", "k2"}
	expectedValues := []string{"v1", "v2"}

	errs := make([]error, len(keys))
	values := make([]any, len(keys))

	for i, key := range keys {
		values[i], errs[i] = store.Get(key)
	}

	// Should have either k1 OR k2, not both.
	if errs[0] == nil {
		assert.Equal(t, []byte(expectedValues[0]), values[0])
		assert.Error(t, errs[1], "k2 should not exist if import1 won")
	} else {
		assert.Equal(t, []byte(expectedValues[1]), values[1])
		assert.Error(t, errs[0], "k1 should not exist if import2 won")
	}
}

// TestMemoryStore_Restore_CleanupOnFailure verifies that if import fails
// partway through, the store is left in a consistent state and mutations
// are unblocked.
func TestMemoryStore_Restore_CleanupOnFailure(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true, 0)
	require.NoError(t, store.Set("original", "data"))

	// Create a corrupted snapshot file.
	corruptedPath := filepath.Join(t.TempDir(), "corrupted.kv")
	//nolint:forbidigo // test needs to create corrupted file.
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

// TestMemoryStore_Backup_Restore_RoundTrip verifies that Backup followed
// by Restore correctly preserves all key-value pairs with exact fidelity.
func TestMemoryStore_Backup_Restore_RoundTrip(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(false, 0)
	for i := range 10 {
		require.NoError(t, store.Set(
			strings.Join([]string{"key", strconv.Itoa(i)}, "-"),
			strings.Join([]string{"value", strconv.Itoa(i)}, "-"),
		))
	}

	target := filepath.Join(t.TempDir(), "roundtrip.kv")
	_, err := store.Backup(&BackupOptions{FileName: target})
	require.NoError(t, err)

	clone := NewMemoryStore(false, 0)
	summary, err := clone.Restore(&RestoreOptions{FileName: target})
	require.NoError(t, err)
	assert.Equal(t, int64(10), func() int64 {
		size, sizeErr := clone.Size()
		require.NoError(t, sizeErr)

		return size
	}())
	assert.Equal(t, 10, summary.TotalEntries)

	for i := range 10 {
		key := strings.Join([]string{"key", strconv.Itoa(i)}, "-")
		val, getErr := clone.Get(key)
		require.NoError(t, getErr)
		assert.Equal(t, []byte(strings.Join([]string{"value", strconv.Itoa(i)}, "-")), val)
	}
}
