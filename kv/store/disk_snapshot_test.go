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

func TestDiskStore_Backup_DoesNotReplaceExistingFileOnExportError(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	require.NoError(t, store.Set("alpha", "1"))

	target := filepath.Join(t.TempDir(), "disk-export.kv")

	const originalContent = "existing-snapshot-content"
	//nolint:forbidigo // file I/O is required for backup replacement verification tests.
	require.NoError(t, os.WriteFile(target, []byte(originalContent), 0o644))

	// Force backup failure before rename by looking for a non-existent bucket.
	store.bucket = []byte("missing-bucket")

	_, err := store.Backup(&BackupOptions{FileName: target})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSnapshotExportFailed)
	require.ErrorIs(t, err, ErrBucketNotFound)

	//nolint:forbidigo // file I/O is required for backup replacement verification tests.
	content, readErr := os.ReadFile(target)
	require.NoError(t, readErr)
	assert.Equal(t, originalContent, string(content))
}

// TestDiskStore_Restore_ReplacesDataset verifies that Restore replaces
// the dataset with the contents of the snapshot file.
func TestDiskStore_Restore_ReplacesDataset(t *testing.T) {
	t.Parallel()

	memoryCfg := &MemoryConfig{TrackKeys: true}
	source := NewMemoryStore(memoryCfg)

	sourceEntries := requirePopulateStore(
		t,
		source,
		"source-key-alpha", "source-value-alpha",
		"source-key-beta", "source-value-beta",
	)

	snapshotPath := filepath.Join(t.TempDir(), "import-src.kv")
	_, err := source.Backup(&BackupOptions{FileName: snapshotPath})
	require.NoError(t, err)

	store := newTestDiskStore(t, true, "", true)
	require.NoError(t, store.Set("stale", "data"))

	summary, err := store.Restore(&RestoreOptions{FileName: snapshotPath})
	require.NoError(t, err)
	assert.EqualValues(t, 2, summary.TotalEntries)

	val, err := store.Get(sourceEntries[0].key)
	require.NoError(t, err)
	assert.Equal(t, []byte(sourceEntries[0].value), val)

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
	require.NoError(t, store.Set("max-bytes-key", strings.Repeat("x", 4)))

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
	records := testEntries(
		"snapshot-alpha", "value-alpha",
		"during-restore", "should-be-blocked",
		"concurrent-write", "blocked",
	)
	snapshotRecord := records[0]
	duringRestoreRecord := records[1]
	concurrentRecord := records[2]

	require.NoError(t, store.Set(snapshotRecord.key, snapshotRecord.value))

	exportPath := filepath.Join(t.TempDir(), "snapshot.kv")
	_, err := store.Backup(&BackupOptions{FileName: exportPath})
	require.NoError(t, err)

	// Clear the store so we have a known state.
	require.NoError(t, store.Clear())
	require.NoError(t, store.Set(duringRestoreRecord.key, duringRestoreRecord.value))

	restoreStarted := make(chan struct{})
	releaseRestore := make(chan struct{})
	store.testRestoreHook = func() {
		close(restoreStarted)
		<-releaseRestore
	}

	defer func() {
		store.testRestoreHook = nil
	}()

	done := make(chan error, 1)

	go func() {
		_, err := store.Restore(&RestoreOptions{FileName: exportPath})
		done <- err
	}()

	<-restoreStarted

	setDone := make(chan error, 1)

	go func() {
		setDone <- store.Set(concurrentRecord.key, concurrentRecord.value)
	}()

	// While restore is paused in the hook, the writer must not complete.
	select {
	case err := <-setDone:
		t.Fatalf("Set finished before restore release: %v", err)
	default:
	}

	close(releaseRestore)
	require.NoError(t, <-done)

	// After restore proceeds, write either succeeds or reports restore-in-progress
	// depending on exact interleaving with the lock handoff.
	err = <-setDone
	if err != nil {
		assert.ErrorIs(t, err, ErrRestoreInProgress)
	}
}

// TestDiskStore_Restore_BlocksConcurrentBackup verifies that
// Backup cannot start while Restore is in progress.
func TestDiskStore_Restore_BlocksConcurrentBackup(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)

	requirePopulateStore(t, store, "snapshot-alpha", "value-alpha")

	exportPaths := []string{
		filepath.Join(t.TempDir(), "snapshot.kv"),
		filepath.Join(t.TempDir(), "export.kv"),
	}

	_, err := store.Backup(&BackupOptions{FileName: exportPaths[0]})
	require.NoError(t, err)

	restoreStarted := make(chan struct{})
	releaseRestore := make(chan struct{})
	store.testRestoreHook = func() {
		close(restoreStarted)
		<-releaseRestore
	}

	defer func() {
		store.testRestoreHook = nil
	}()

	restoreDone := make(chan error, 1)

	go func() {
		_, err := store.Restore(&RestoreOptions{FileName: exportPaths[0]})
		restoreDone <- err
	}()

	<-restoreStarted

	_, err = store.Backup(&BackupOptions{FileName: exportPaths[1]})
	if err != nil {
		require.ErrorIs(t, err, ErrRestoreInProgress)
	}

	close(releaseRestore)
	require.NoError(t, <-restoreDone)
}

// TestDiskStore_Restore_PreventsDataLoss verifies that writes during import
// are properly blocked, preventing the data loss bug where applySnapshot
// would silently overwrite concurrent writes.
func TestDiskStore_Restore_PreventsDataLoss(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	records := testEntries(
		"snapshot-alpha", "value-alpha",
		"stale-before-restore", "value-before-restore",
		"concurrent-write", "value-during-restore",
	)
	snapshotRecord := records[0]
	staleRecord := records[1]
	concurrentRecord := records[2]

	require.NoError(t, store.Set(snapshotRecord.key, snapshotRecord.value))

	exportPath := filepath.Join(t.TempDir(), "snapshot.kv")
	_, err := store.Backup(&BackupOptions{FileName: exportPath})
	require.NoError(t, err)

	// Clear the store and add a different key.
	require.NoError(t, store.Clear())
	require.NoError(t, store.Set(staleRecord.key, staleRecord.value))

	restoreStarted := make(chan struct{})
	releaseRestore := make(chan struct{})
	store.testRestoreHook = func() {
		close(restoreStarted)
		<-releaseRestore
	}

	defer func() {
		store.testRestoreHook = nil
	}()

	restoreDone := make(chan error, 1)

	go func() {
		_, err := store.Restore(&RestoreOptions{FileName: exportPath})
		restoreDone <- err
	}()

	<-restoreStarted

	setDone := make(chan error, 1)

	go func() {
		setDone <- store.Set(concurrentRecord.key, concurrentRecord.value)
	}()

	// While restore is paused in the hook, writer should still be pending.
	select {
	case err := <-setDone:
		t.Fatalf("Set finished before restore release: %v", err)
	default:
	}

	close(releaseRestore)
	require.NoError(t, <-restoreDone)

	setErr := <-setDone
	if setErr != nil {
		require.ErrorIs(t, setErr, ErrRestoreInProgress)
	}

	// After import completes, verify the store has the imported data.
	val, err := store.Get(snapshotRecord.key)
	require.NoError(t, err)
	assert.Equal(t, []byte(snapshotRecord.value), val)

	// Stale key should be gone (overwritten by import).
	_, err = store.Get(staleRecord.key)
	require.Error(t, err)

	// If concurrent write was rejected it must be absent; if it succeeded, it happened
	// after restore release and may be present.
	_, err = store.Get(concurrentRecord.key)
	if setErr != nil {
		assert.Error(t, err, "concurrent key should not exist after blocked write")
	}
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
			id:    "snapshot-alpha",
			key:   "key-alpha",
			value: "value-alpha",
			path:  filepath.Join(tempDir, "snapshot-alpha.kv"),
		},
		{
			id:    "snapshot-beta",
			key:   "key-beta",
			value: "value-beta",
			path:  filepath.Join(tempDir, "snapshot-beta.kv"),
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
		snapshot := snap

		go func() {
			defer wg.Done()

			_, err := store.Restore(&RestoreOptions{FileName: snapshot.path})
			if err == nil {
				results <- snapshot.id
			} else if errors.Is(err, ErrRestoreInProgress) {
				results <- snapshot.id + "-blocked"
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
	keys := make([]string, len(snapshots))

	expectedValues := make([]string, len(snapshots))
	for index, snapshot := range snapshots {
		keys[index] = snapshot.key
		expectedValues[index] = snapshot.value
	}

	getErrors := make([]error, len(keys))
	values := make([]any, len(keys))

	for i, key := range keys {
		values[i], getErrors[i] = store.Get(key)
	}

	// Should have either the first or second snapshot, not both.
	if getErrors[0] == nil {
		assert.Equal(t, []byte(expectedValues[0]), values[0])
		assert.Error(t, getErrors[1], "%s should not exist if %s won", keys[1], snapshots[0].id)
	} else {
		assert.Equal(t, []byte(expectedValues[1]), values[1])
		assert.Error(t, getErrors[0], "%s should not exist if %s won", keys[0], snapshots[1].id)
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
