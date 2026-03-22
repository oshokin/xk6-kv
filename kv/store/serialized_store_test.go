package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestSerializedMemoryStore(t *testing.T) *SerializedStore {
	t.Helper()

	base := NewMemoryStore(&MemoryConfig{TrackKeys: true})
	require.NoError(t, base.Open())

	return NewSerializedStore(base, NewJSONSerializer())
}

// TestSerializedStore_CompareAndDeleteDetailed_NullSemantics verifies JS-facing
// null semantics after serialization: null is a regular expected value compare.
func TestSerializedStore_CompareAndDeleteDetailed_NullSemantics(t *testing.T) {
	t.Parallel()

	store := newTestSerializedMemoryStore(t)

	wantAbsentKey := &CompareAndDeleteDetailedResult{
		Reason: CompareReasonMismatch,
	}
	wantStringValueNilCompare := &CompareAndDeleteDetailedResult{
		Reason:     CompareReasonMismatch,
		Existed:    true,
		Current:    "v1",
		HasCurrent: true,
	}
	wantMatchedJSONNullDeleted := &CompareAndDeleteDetailedResult{
		Deleted: true,
		Reason:  CompareAndDeleteReasonDeleted,
		Existed: true,
	}

	result, err := store.CompareAndDeleteDetailed("k", nil, true)
	require.NoError(t, err)
	assert.Equal(t, wantAbsentKey, result)

	require.NoError(t, store.Set("k", "v1"))

	result, err = store.CompareAndDeleteDetailed("k", nil, true)
	require.NoError(t, err)
	assert.Equal(t, wantStringValueNilCompare, result)

	require.NoError(t, store.Set("k", nil))

	result, err = store.CompareAndDeleteDetailed("k", nil, true)
	require.NoError(t, err)
	assert.Equal(t, wantMatchedJSONNullDeleted, result)
}

// TestSerializedStore_CompareAndSwapDetailed_CurrentVisibility verifies current
// visibility/shape rules and deserialization behavior in mismatch paths.
func TestSerializedStore_CompareAndSwapDetailed_CurrentVisibility(t *testing.T) {
	t.Parallel()

	store := newTestSerializedMemoryStore(t)

	wantMismatchOmitCurrent := &CompareAndSwapDetailedResult{
		Reason:  CompareReasonMismatch,
		Existed: true,
	}
	wantMismatchIncludeCurrent := &CompareAndSwapDetailedResult{
		Reason:     CompareReasonMismatch,
		Existed:    true,
		Current:    map[string]any{"flag": true},
		HasCurrent: true,
	}
	wantSwapSuccess := &CompareAndSwapDetailedResult{
		Swapped: true,
		Reason:  CompareAndSwapReasonSwapped,
		Existed: true,
	}

	require.NoError(t, store.Set("k", map[string]any{"flag": true}))

	result, err := store.CompareAndSwapDetailed("k", map[string]any{"flag": false}, "next", false)
	require.NoError(t, err)
	assert.Equal(t, wantMismatchOmitCurrent, result)

	result, err = store.CompareAndSwapDetailed("k", map[string]any{"flag": false}, "next", true)
	require.NoError(t, err)
	assert.Equal(t, wantMismatchIncludeCurrent, result)

	result, err = store.CompareAndSwapDetailed("k", map[string]any{"flag": true}, "next", true)
	require.NoError(t, err)
	assert.Equal(t, wantSwapSuccess, result)
}

// TestSerializedStore_CompareAndDelete_BoolNilBehavior verifies legacy boolean
// API behavior stays compatible with serialized null comparison.
func TestSerializedStore_CompareAndDelete_BoolNilBehavior(t *testing.T) {
	t.Parallel()

	store := newTestSerializedMemoryStore(t)

	require.NoError(t, store.Set("k", nil))

	ok, err := store.CompareAndDelete("k", nil)
	require.NoError(t, err)
	assert.True(t, ok)
}

// TestSerializedStore_CompareAndSwap_NilSentinel verifies CAS absent-key
// semantics are preserved through serialization wrappers.
func TestSerializedStore_CompareAndSwap_NilSentinel(t *testing.T) {
	t.Parallel()

	store := newTestSerializedMemoryStore(t)

	first, err := store.CompareAndSwap("k", nil, "owner-1")
	require.NoError(t, err)
	assert.True(t, first)

	second, err := store.CompareAndSwap("k", nil, "owner-2")
	require.NoError(t, err)
	assert.False(t, second)
}
