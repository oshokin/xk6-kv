package store

import (
	"fmt"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestDiskStore_SetMany_Empty(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
			written, err := store.SetMany([]Entry{})
			require.NoError(t, err)
			assert.EqualValues(t, 0, written)

			size, err := store.Size()
			require.NoError(t, err)
			assert.EqualValues(t, 0, size)

			if trackKeys {
				requireDiskTrackingMatchesStore(t, store)
			}
		})
	}
}

func TestDiskStore_SetMany_WritesAndOverwrites(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)

			written, err := store.SetMany([]Entry{
				{Key: "user:1", Value: "one"},
				{Key: "user:2", Value: "two"},
			})
			require.NoError(t, err)
			assert.EqualValues(t, 2, written)

			written, err = store.SetMany([]Entry{
				{Key: "user:2", Value: "two-updated"},
				{Key: "user:3", Value: "three"},
			})
			require.NoError(t, err)
			assert.EqualValues(t, 2, written)

			size, err := store.Size()
			require.NoError(t, err)
			assert.EqualValues(t, 3, size)

			valueAny, err := store.Get("user:2")
			require.NoError(t, err)
			assert.Equal(t, []byte("two-updated"), valueAny.([]byte))

			if trackKeys {
				requireDiskTrackingMatchesStore(t, store)
			}
		})
	}
}

func TestDiskStore_SetMany_UnsupportedValueTypeDoesNotWrite(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	written, err := store.SetMany([]Entry{
		{Key: "ok", Value: "value"},
		{Key: "bad", Value: 123},
	})
	require.ErrorIs(t, err, ErrUnsupportedValueType)
	assert.EqualValues(t, 0, written)

	exists, existsErr := store.Exists("ok")
	require.NoError(t, existsErr)
	assert.False(t, exists, "batch must be all-or-nothing")
}

func TestDiskStore_SetMany_EmptyKeyDoesNotWrite(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	written, err := store.SetMany([]Entry{
		{Key: "ok", Value: "value"},
		{Key: "", Value: "empty"},
	})
	require.ErrorIs(t, err, ErrKeyEmpty)
	assert.EqualValues(t, 0, written)

	exists, existsErr := store.Exists("ok")
	require.NoError(t, existsErr)
	assert.False(t, exists, "batch must be all-or-nothing")
}

func TestDiskStore_SetMany_DoesNotClearLiveClaim(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	require.NoError(t, store.Set("user:1", []byte("old")))

	claim, err := store.ClaimRandom(&ClaimOptions{
		Prefix: "user:",
		TTLMs:  60_000,
	})
	require.NoError(t, err)
	require.NotNil(t, claim)

	written, err := store.SetMany([]Entry{
		{Key: "user:1", Value: []byte("new")},
	})
	require.NoError(t, err)
	assert.EqualValues(t, 1, written)

	secondClaim, err := store.ClaimRandom(&ClaimOptions{
		Prefix: "user:",
		TTLMs:  60_000,
	})
	require.NoError(t, err)
	assert.Nil(t, secondClaim, "overwriting value must not release live claim")
}

func TestDiskStore_SetMany_Concurrent_TrackKeysConsistency(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)

			writers := scaledStressCount(12, 4)
			batchesPerWriter := scaledStressCount(20, 6)

			const entriesPerBatch = 8

			startBarrier := make(chan struct{})
			errorCh := make(chan error, writers)

			var wg sync.WaitGroup
			wg.Add(writers)

			for writerID := range writers {
				go func(id int) {
					defer wg.Done()

					<-startBarrier

					for batchID := range batchesPerWriter {
						entries := make([]Entry, 0, entriesPerBatch)
						for entryID := range entriesPerBatch {
							entries = append(entries, Entry{
								Key: fmt.Sprintf("setmany:w:%03d:b:%03d:e:%02d", id, batchID, entryID),
								Value: fmt.Appendf(nil,
									"value-%03d-%03d-%02d",
									id,
									batchID,
									entryID,
								),
							})
						}

						if _, err := store.SetMany(entries); err != nil {
							errorCh <- fmt.Errorf("writer %d batch %d failed: %w", id, batchID, err)
							return
						}
					}
				}(writerID)
			}

			close(startBarrier)
			wg.Wait()
			close(errorCh)

			for err := range errorCh {
				require.NoError(t, err)
			}

			expectedTotal := int64(writers * batchesPerWriter * entriesPerBatch)

			size, err := store.Size()
			require.NoError(t, err)
			assert.Equal(t, expectedTotal, size)

			count := requireCountMatchesScan(t, store, "setmany:")
			assert.Equal(t, expectedTotal, count)

			if trackKeys {
				requireDiskTrackingMatchesStore(t, store)
			}
		})
	}
}

func TestDiskStore_GetMany_EmptyKeys(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
			entries, err := store.GetMany([]string{})
			require.NoError(t, err)
			assert.Empty(t, entries)
		})
	}
}

func TestDiskStore_GetMany_PreservesOrderAndMissing(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
			require.NoError(t, store.Set("user:1", []byte("one")))
			require.NoError(t, store.Set("user:2", []byte("two")))

			entries, err := store.GetMany([]string{"user:2", "missing", "user:1"})
			require.NoError(t, err)
			require.Len(t, entries, 3)

			require.NotNil(t, entries[0])
			assert.Equal(t, "user:2", entries[0].Key)
			assert.Equal(t, []byte("two"), entries[0].Value)

			assert.Nil(t, entries[1])

			require.NotNil(t, entries[2])
			assert.Equal(t, "user:1", entries[2].Key)
			assert.Equal(t, []byte("one"), entries[2].Value)
		})
	}
}

func TestDiskStore_GetMany_DuplicateKeys(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	require.NoError(t, store.Set("user:1", []byte("one")))

	entries, err := store.GetMany([]string{"user:1", "user:1"})
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.NotNil(t, entries[0])
	require.NotNil(t, entries[1])
	assert.Equal(t, []byte("one"), entries[0].Value)
	assert.Equal(t, []byte("one"), entries[1].Value)
}

func TestDiskStore_GetMany_ReturnsDefensiveCopies(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	require.NoError(t, store.Set("user:1", []byte("one")))

	entries, err := store.GetMany([]string{"user:1"})
	require.NoError(t, err)
	require.NotNil(t, entries[0])

	value := entries[0].Value.([]byte)
	value[0] = 'X'

	actual, err := store.Get("user:1")
	require.NoError(t, err)
	assert.Equal(t, []byte("one"), actual)
}

func TestDiskStore_GetMany_EmptyKeyReturnsMissingEntry(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)

	entries, err := store.GetMany([]string{""})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Nil(t, entries[0])
}

func TestDiskStore_GetMany_ConcurrentWithSetMany(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)

	const (
		writers    = 4
		readers    = 8
		iterations = 30
	)

	start := make(chan struct{})
	errCh := make(chan error, writers+readers)

	var wg sync.WaitGroup

	for writerID := range writers {
		wg.Go(func() {
			<-start

			for iter := range iterations {
				entries := make([]Entry, 0, 10)
				for i := range 10 {
					entries = append(entries, Entry{
						Key:   fmt.Sprintf("writer:%d:iter:%d:key:%d", writerID, iter, i),
						Value: []byte("value"),
					})
				}

				if _, err := store.SetMany(entries); err != nil {
					errCh <- err
					return
				}
			}
		})
	}

	for range readers {
		wg.Go(func() {
			<-start

			for range iterations {
				keys := []string{
					"writer:0:iter:0:key:0",
					"writer:1:iter:1:key:1",
					"missing",
				}

				entries, err := store.GetMany(keys)
				if err != nil {
					errCh <- err
					return
				}

				if len(entries) != len(keys) {
					errCh <- fmt.Errorf("unexpected result length: got %d want %d", len(entries), len(keys))
					return
				}
			}
		})
	}

	close(start)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}
}

func TestDiskStore_DeleteMany_EmptyKeys(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)

			result, err := store.DeleteMany([]string{})
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.EqualValues(t, 0, result.Deleted)
			assert.EqualValues(t, 0, result.Missing)
		})
	}
}

func TestDiskStore_DeleteMany_EmptyKeyRejectsWithoutPartialDelete(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	require.NoError(t, store.Set("ok", []byte("value")))

	result, err := store.DeleteMany([]string{"ok", ""})
	require.ErrorIs(t, err, ErrKeyEmpty)
	assert.Nil(t, result)

	exists, existsErr := store.Exists("ok")
	require.NoError(t, existsErr)
	assert.True(t, exists, "DeleteMany must validate first and avoid partial delete")
}

func TestDiskStore_DeleteMany_DeletesExistingAndCountsMissing(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
			require.NoError(t, store.Set("user:1", []byte("one")))
			require.NoError(t, store.Set("user:2", []byte("two")))

			result, err := store.DeleteMany([]string{"user:1", "missing", "user:2"})
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.EqualValues(t, 2, result.Deleted)
			assert.EqualValues(t, 1, result.Missing)

			items, getErr := store.GetMany([]string{"user:1", "user:2"})
			require.NoError(t, getErr)
			require.Len(t, items, 2)
			assert.Nil(t, items[0])
			assert.Nil(t, items[1])
		})
	}
}

func TestDiskStore_DeleteMany_DuplicateKeys(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	require.NoError(t, store.Set("user:1", []byte("one")))

	result, err := store.DeleteMany([]string{"user:1", "user:1"})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 1, result.Deleted)
	assert.EqualValues(t, 1, result.Missing)
}

func TestDiskStore_DeleteMany_UpdatesCountAndTracking(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	_, err := store.SetMany([]Entry{
		{Key: "user:1", Value: []byte("one")},
		{Key: "user:2", Value: []byte("two")},
		{Key: "order:1", Value: []byte("order")},
	})
	require.NoError(t, err)

	result, err := store.DeleteMany([]string{"user:1", "user:2"})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 2, result.Deleted)
	assert.EqualValues(t, 0, result.Missing)

	count, countErr := store.Count("user:")
	require.NoError(t, countErr)
	assert.EqualValues(t, 0, count)

	count, countErr = store.Count("order:")
	require.NoError(t, countErr)
	assert.EqualValues(t, 1, count)

	requireDiskTrackingMatchesStore(t, store)
}

func TestDiskStore_DeleteMany_CleansClaims(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	require.NoError(t, store.Set("user:1", []byte("one")))

	claim, err := store.ClaimRandom(&ClaimOptions{
		Prefix: "user:",
		TTLMs:  60_000,
	})
	require.NoError(t, err)
	require.NotNil(t, claim)

	result, err := store.DeleteMany([]string{"user:1"})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 1, result.Deleted)
	assert.EqualValues(t, 0, result.Missing)

	released, releaseErr := store.ReleaseClaim(&ClaimRef{
		ID:    claim.ID,
		Key:   claim.Key,
		Token: claim.Token,
	})
	require.NoError(t, releaseErr)
	assert.False(t, released, "claim metadata for deleted key must be removed")
}

func TestDiskStore_DeleteMany_CleansClaimsForMissingKey(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	require.NoError(t, store.Set("user:stale", []byte("value")))

	claim, err := store.ClaimRandom(&ClaimOptions{
		Prefix: "user:",
		TTLMs:  60_000,
	})
	require.NoError(t, err)
	require.NotNil(t, claim)

	err = store.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(store.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, store.bucket)
		}

		return bucket.Delete([]byte(claim.Key))
	})
	require.NoError(t, err)

	var staleClaimID []byte

	err = store.handle.View(func(tx *bolt.Tx) error {
		claimsBucket := tx.Bucket(diskClaimsBucket)
		require.NotNil(t, claimsBucket)

		staleClaimID = slices.Clone(claimsBucket.Get(claimKeyIndexKey(claim.Key)))
		require.NotNil(t, staleClaimID, "test setup requires stale claim metadata")

		return nil
	})
	require.NoError(t, err)

	result, err := store.DeleteMany([]string{claim.Key})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 0, result.Deleted)
	assert.EqualValues(t, 1, result.Missing)

	err = store.handle.View(func(tx *bolt.Tx) error {
		claimsBucket := tx.Bucket(diskClaimsBucket)
		require.NotNil(t, claimsBucket)
		assert.Nil(t, claimsBucket.Get(claimKeyIndexKey(claim.Key)))
		assert.Nil(t, claimsBucket.Get(claimIDKey(string(staleClaimID))))

		return nil
	})
	require.NoError(t, err)
}

func TestDiskStore_DeleteMany_CleansStalePositiveIndexForMissingKey(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)

	const key = "user:stale-index"

	require.NoError(t, store.Set(key, []byte("value")))

	// Simulate stale-positive index by deleting only from bbolt.
	err := store.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(store.bucket)
		require.NotNil(t, bucket)

		return bucket.Delete([]byte(key))
	})
	require.NoError(t, err)

	store.keysLock.RLock()
	_, indexed := store.keysMap[key]
	store.keysLock.RUnlock()
	require.True(t, indexed, "test setup requires stale-positive index entry")

	result, err := store.DeleteMany([]string{key})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 0, result.Deleted)
	assert.EqualValues(t, 1, result.Missing)

	store.keysLock.RLock()
	_, indexed = store.keysMap[key]
	store.keysLock.RUnlock()
	assert.False(t, indexed, "DeleteMany must clean stale-positive index entries")
}

func TestDiskStore_DeleteMany_ConcurrentWithSetAndGet(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)

	const (
		writers    = 3
		deleters   = 2
		readers    = 4
		iterations = 25
	)

	start := make(chan struct{})
	errCh := make(chan error, writers+deleters+readers)

	var wg sync.WaitGroup

	for writerID := range writers {
		wg.Go(func() {
			<-start

			for iter := range iterations {
				entries := make([]Entry, 0, 4)
				for i := range 4 {
					entries = append(entries, Entry{
						Key:   fmt.Sprintf("dm:w:%d:i:%d:k:%d", writerID, iter, i),
						Value: []byte("value"),
					})
				}

				if _, err := store.SetMany(entries); err != nil {
					errCh <- err
					return
				}
			}
		})
	}

	for deleterID := range deleters {
		wg.Go(func() {
			<-start

			for iter := range iterations {
				keys := []string{
					fmt.Sprintf("dm:w:%d:i:%d:k:%d", deleterID%writers, iter, 0),
					fmt.Sprintf("dm:w:%d:i:%d:k:%d", (deleterID+1)%writers, iter, 1),
					fmt.Sprintf("dm:missing:%d:%d", deleterID, iter),
				}

				if _, err := store.DeleteMany(keys); err != nil {
					errCh <- err
					return
				}
			}
		})
	}

	for range readers {
		wg.Go(func() {
			<-start

			for range iterations {
				keys := []string{
					"dm:w:0:i:0:k:0",
					"dm:w:1:i:1:k:1",
					"dm:missing",
				}

				entries, err := store.GetMany(keys)
				if err != nil {
					errCh <- err
					return
				}

				if len(entries) != len(keys) {
					errCh <- fmt.Errorf("unexpected result length: got %d want %d", len(entries), len(keys))
					return
				}
			}
		})
	}

	close(start)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}

	requireDiskTrackingMatchesStore(t, store)
}
