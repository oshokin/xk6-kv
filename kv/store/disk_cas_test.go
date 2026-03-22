package store

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	require.NoError(t, store.Set("k", "v0"))

	wg.Add(concurrencyLevel)

	for range concurrencyLevel {
		go func() {
			defer wg.Done()

			ok, err := store.CompareAndSwap("k", "v0", "v1")
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
	assert.Equal(t, []byte("v1"), got.([]byte))
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
	require.NoError(t, store.Set("k", "v1"))

	ok, err := store.CompareAndDelete("k", "BAD")
	require.NoError(t, err)
	assert.False(t, ok, "wrong value must not delete")

	exists, _ := store.Exists("k")
	assert.True(t, exists, "key should still exist")

	ok, err = store.CompareAndDelete("k", "v1")
	require.NoError(t, err)
	assert.True(t, ok, "correct value must delete")

	exists, _ = store.Exists("k")
	assert.False(t, exists, "key must be removed")
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
	require.NoError(t, store.Set("k", "v1"))

	wantMismatchOmitCurrent := &CompareAndDeleteDetailedResult{
		Reason:  CompareReasonMismatch,
		Existed: true,
	}
	wantMismatchIncludeCurrent := &CompareAndDeleteDetailedResult{
		Reason:     CompareReasonMismatch,
		Existed:    true,
		Current:    []byte("v1"),
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

	result, err = store.CompareAndDeleteDetailed("missing", "v1", true)
	require.NoError(t, err)
	assert.Equal(t, wantMissingKey, result)

	result, err = store.CompareAndDeleteDetailed("k", "v1", true)
	require.NoError(t, err)
	assert.Equal(t, wantDeleteMatch, result)
}

// TestDiskStore_CompareAndDelete_NilOldValueUnsupported verifies legacy
// store-level behavior where nil oldValue is not a valid raw compare input.
func TestDiskStore_CompareAndDelete_NilOldValueUnsupported(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	require.NoError(t, store.Set("k", "v1"))

	ok, err := store.CompareAndDelete("k", nil)
	assert.False(t, ok)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnsupportedValueType)
}
