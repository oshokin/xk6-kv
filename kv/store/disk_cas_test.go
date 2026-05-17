package store

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
	boltErrors "go.etcd.io/bbolt/errors"
)

// TestDiskStore_CompareAndSwap_Basic verifies CAS fails on wrong old value
// and succeeds on correct one.
func TestDiskStore_CompareAndSwap_Basic(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	require.NoError(t, store.Set("k", "old"))

	ok, err := store.CompareAndSwap("k", "BAD", "new")
	require.NoError(t, err)
	assert.False(t, ok, "CAS must fail with wrong old")

	got, err := store.Get("k")
	require.NoError(t, err)
	assert.Equal(t, []byte("old"), got.([]byte), "value must remain unchanged on failed CAS")

	ok, err = store.CompareAndSwap("k", "old", "new")
	require.NoError(t, err)
	assert.True(t, ok, "CAS must succeed on correct old")

	got, err = store.Get("k")
	require.NoError(t, err)
	assert.Equal(t, []byte("new"), got.([]byte), "value must be updated")
}

// TestDiskStore_CompareAndSwap_InsertWhenAbsent tests that CompareAndSwap inserts
// a new value when the key is absent.
func TestDiskStore_CompareAndSwap_InsertWhenAbsent(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)

	ok, err := store.CompareAndSwap("lock", nil, "holder")
	require.NoError(t, err)
	assert.True(t, ok, "CAS should insert when expecting absence")

	got, err := store.Get("lock")
	require.NoError(t, err)
	assert.Equal(t, []byte("holder"), got.([]byte))

	ok, err = store.CompareAndSwap("lock", nil, "other")
	require.NoError(t, err)
	assert.False(t, ok, "subsequent CAS with nil expectation should fail")
}

// TestDiskStore_CompareAndSwap_ConcurrentSingleWinner ensures
// exactly one CAS succeeds under contention.
func TestDiskStore_CompareAndSwap_ConcurrentSingleWinner(t *testing.T) {
	t.Parallel()

	const concurrencyLevel = 200

	var (
		store = newTestDiskStore(t, true, "", true)
		okCh  = make(chan bool, concurrencyLevel)
		wg    sync.WaitGroup
	)

	require.NoError(t, store.Set("k", "state-initial"))

	wg.Add(concurrencyLevel)

	for range concurrencyLevel {
		go func() {
			defer wg.Done()

			ok, err := store.CompareAndSwap("k", "state-initial", "state-updated")
			assert.NoError(t, err)

			okCh <- ok
		}()
	}

	wg.Wait()
	close(okCh)

	var successCount int

	for ok := range okCh {
		if ok {
			successCount++
		}
	}

	assert.Equal(t, 1, successCount, "exactly one CAS must succeed")

	got, err := store.Get("k")
	require.NoError(t, err)
	assert.Equal(t, []byte("state-updated"), got.([]byte))
}

// TestDiskStore_CompareAndSwapDetailed_Metadata verifies reason/existed/current
// fields for mismatch and success outcomes, including includeCurrentOnMismatch behavior.
func TestDiskStore_CompareAndSwapDetailed_Metadata(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	require.NoError(t, store.Set("k", "old"))

	wantMismatchOmitCurrent := &CompareAndSwapDetailedResult{
		Reason:  CompareReasonMismatch,
		Existed: true,
	}
	wantMismatchIncludeCurrent := &CompareAndSwapDetailedResult{
		Reason:     CompareReasonMismatch,
		Existed:    true,
		Current:    []byte("old"),
		HasCurrent: true,
	}
	wantMissingKey := &CompareAndSwapDetailedResult{
		Reason: CompareReasonMismatch,
	}
	wantSwapMatch := &CompareAndSwapDetailedResult{
		Swapped: true,
		Reason:  CompareAndSwapReasonSwapped,
		Existed: true,
	}
	wantNilOldInsert := &CompareAndSwapDetailedResult{
		Swapped: true,
		Reason:  CompareAndSwapReasonSwapped,
	}

	result, err := store.CompareAndSwapDetailed("k", "BAD", "new", false)
	require.NoError(t, err)
	assert.Equal(t, wantMismatchOmitCurrent, result)

	result, err = store.CompareAndSwapDetailed("k", "BAD", "new", true)
	require.NoError(t, err)
	assert.Equal(t, wantMismatchIncludeCurrent, result)

	result, err = store.CompareAndSwapDetailed("missing", "expected", "new", true)
	require.NoError(t, err)
	assert.Equal(t, wantMissingKey, result)

	result, err = store.CompareAndSwapDetailed("k", "old", "new", true)
	require.NoError(t, err)
	assert.Equal(t, wantSwapMatch, result)

	result, err = store.CompareAndSwapDetailed("lock", nil, "holder", true)
	require.NoError(t, err)
	assert.Equal(t, wantNilOldInsert, result)
}

// TestDiskStore_CompareAndDelete_Basic checks that CompareAndDelete only removes
// the key when the expected value matches.
func TestDiskStore_CompareAndDelete_Basic(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	require.NoError(t, store.Set("k", "state-current"))

	ok, err := store.CompareAndDelete("k", "BAD")
	require.NoError(t, err)
	assert.False(t, ok, "wrong value must not delete")

	exists, _ := store.Exists("k")
	assert.True(t, exists, "key should still exist")

	ok, err = store.CompareAndDelete("k", "state-current")
	require.NoError(t, err)
	assert.True(t, ok, "correct value must delete")

	exists, _ = store.Exists("k")
	assert.False(t, exists, "key must be removed")
}

func TestDiskStore_CompareAndDelete_TrackedClaimsMode_DoesNotTouchClaimsBucket(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	require.NoError(t, store.Set("k", "v"))

	require.NoError(t, store.handle.Update(func(tx *bolt.Tx) error {
		err := tx.DeleteBucket(diskClaimsBucket)
		if err != nil && !errors.Is(err, boltErrors.ErrBucketNotFound) {
			return err
		}

		return nil
	}))

	ok, err := store.CompareAndDelete("k", "v")
	require.NoError(t, err)
	require.True(t, ok)

	require.NoError(t, store.handle.View(func(tx *bolt.Tx) error {
		assert.Nil(t, tx.Bucket(diskClaimsBucket))

		return nil
	}))
}

func TestDiskStore_CompareAndDelete_BoltClaimsMode_CreatesClaimsBucket(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, false, "", true)
	require.NoError(t, store.Set("k", "v"))

	require.NoError(t, store.handle.Update(func(tx *bolt.Tx) error {
		err := tx.DeleteBucket(diskClaimsBucket)
		if err != nil && !errors.Is(err, boltErrors.ErrBucketNotFound) {
			return err
		}

		return nil
	}))

	ok, err := store.CompareAndDelete("k", "v")
	require.NoError(t, err)
	require.True(t, ok)

	require.NoError(t, store.handle.View(func(tx *bolt.Tx) error {
		assert.NotNil(t, tx.Bucket(diskClaimsBucket))

		return nil
	}))
}

// TestDiskStore_CompareAndDelete_ConcurrentSingleWinner ensures exactly
// one CompareAndDelete wins under contention.
func TestDiskStore_CompareAndDelete_ConcurrentSingleWinner(t *testing.T) {
	t.Parallel()

	const concurrencyLevel = 120

	var (
		store        = newTestDiskStore(t, true, "", true)
		successCount int
		mu           sync.Mutex
		wg           sync.WaitGroup
	)

	require.NoError(t, store.Set("k", "secret"))

	wg.Add(concurrencyLevel)

	for range concurrencyLevel {
		go func() {
			defer wg.Done()

			ok, err := store.CompareAndDelete("k", "secret")
			assert.NoError(t, err)

			if ok {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	assert.Equal(t, 1, successCount, "exactly one CompareAndDelete must succeed")

	exists, _ := store.Exists("k")
	assert.False(t, exists, "key must be deleted")
}

// TestDiskStore_CompareAndDeleteDetailed_Metadata verifies reason/existed/current
// fields for mismatch and success outcomes.
func TestDiskStore_CompareAndDeleteDetailed_Metadata(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	require.NoError(t, store.Set("k", "state-current"))

	wantMismatchOmitCurrent := &CompareAndDeleteDetailedResult{
		Reason:  CompareReasonMismatch,
		Existed: true,
	}
	wantMismatchIncludeCurrent := &CompareAndDeleteDetailedResult{
		Reason:     CompareReasonMismatch,
		Existed:    true,
		Current:    []byte("state-current"),
		HasCurrent: true,
	}
	wantMissingKey := &CompareAndDeleteDetailedResult{
		Reason: CompareReasonMismatch,
	}
	wantDeleteMatch := &CompareAndDeleteDetailedResult{
		Deleted: true,
		Reason:  CompareAndDeleteReasonDeleted,
		Existed: true,
	}

	result, err := store.CompareAndDeleteDetailed("k", "BAD", false)
	require.NoError(t, err)
	assert.Equal(t, wantMismatchOmitCurrent, result)

	result, err = store.CompareAndDeleteDetailed("k", "BAD", true)
	require.NoError(t, err)
	assert.Equal(t, wantMismatchIncludeCurrent, result)

	result, err = store.CompareAndDeleteDetailed("missing", "state-current", true)
	require.NoError(t, err)
	assert.Equal(t, wantMissingKey, result)

	result, err = store.CompareAndDeleteDetailed("k", "state-current", true)
	require.NoError(t, err)
	assert.Equal(t, wantDeleteMatch, result)
}

// TestDiskStore_CompareAndDelete_NilOldValueUnsupported verifies legacy
// store-level behavior where nil oldValue is not a valid raw compare input.
func TestDiskStore_CompareAndDelete_NilOldValueUnsupported(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	require.NoError(t, store.Set("k", "state-current"))

	ok, err := store.CompareAndDelete("k", nil)
	assert.False(t, ok)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnsupportedValueType)
}
