package store

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMemoryStore_CompareAndSwap_Basic verifies CompareAndSwap fails when the
// old value doesn't match, and succeeds only when it matches exactly.
func TestMemoryStore_CompareAndSwap_Basic(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(&MemoryConfig{TrackKeys: true})
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

// TestMemoryStore_CompareAndSwap_InsertWhenAbsent verifies that CompareAndSwap
// with oldValue=nil creates the key when absent, and fails when the key exists.
func TestMemoryStore_CompareAndSwap_InsertWhenAbsent(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(&MemoryConfig{TrackKeys: true})

	ok, err := store.CompareAndSwap("lock", nil, "holder")
	require.NoError(t, err)
	assert.True(t, ok, "CAS should create the key when oldValue is nil")

	got, err := store.Get("lock")
	require.NoError(t, err)
	assert.Equal(t, []byte("holder"), got.([]byte), "value must match inserted payload")

	ok, err = store.CompareAndSwap("lock", nil, "other")
	require.NoError(t, err)
	assert.False(t, ok, "second CAS must fail because key now exists")
}

// TestMemoryStore_CompareAndSwap_ConcurrentSingleWinner verifies that under concurrent
// contention on the same key, exactly one CompareAndSwap operation succeeds.
func TestMemoryStore_CompareAndSwap_ConcurrentSingleWinner(t *testing.T) {
	t.Parallel()

	const concurrencyLevel = 200

	var (
		store = NewMemoryStore(&MemoryConfig{TrackKeys: true})
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

// TestMemoryStore_CompareAndSwapDetailed_Metadata verifies reason/existed/current fields
// for mismatch and success outcomes, including includeCurrentOnMismatch behavior.
func TestMemoryStore_CompareAndSwapDetailed_Metadata(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(&MemoryConfig{TrackKeys: true})
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

// TestMemoryStore_CompareAndDelete_Basic verifies CompareAndDelete only removes
// the key when the expected value matches exactly, and fails otherwise.
func TestMemoryStore_CompareAndDelete_Basic(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(&MemoryConfig{TrackKeys: true})
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

// TestMemoryStore_CompareAndDelete_ConcurrentSingleWinner verifies that under
// concurrent contention, exactly one CompareAndDelete operation succeeds.
func TestMemoryStore_CompareAndDelete_ConcurrentSingleWinner(t *testing.T) {
	t.Parallel()

	const concurrencyLevel = 120

	var (
		store        = NewMemoryStore(&MemoryConfig{TrackKeys: true})
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

// TestMemoryStore_CompareAndDeleteDetailed_Metadata verifies reason/existed/current
// fields for mismatch and success outcomes.
func TestMemoryStore_CompareAndDeleteDetailed_Metadata(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(&MemoryConfig{TrackKeys: true})
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

// TestMemoryStore_CompareAndDelete_NilOldValueUnsupported verifies legacy
// store-level behavior where nil oldValue is not a valid raw compare input.
func TestMemoryStore_CompareAndDelete_NilOldValueUnsupported(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(&MemoryConfig{TrackKeys: true})
	require.NoError(t, store.Set("k", "state-current"))

	ok, err := store.CompareAndDelete("k", nil)
	assert.False(t, ok)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnsupportedValueType)
}
