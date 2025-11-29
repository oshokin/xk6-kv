package store

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDiskStore_RandomKey_WithTracking validates that with tracking enabled, RandomKey on an empty
// store returns "", and over repeated draws it eventually returns all present keys (sanity check).
func TestDiskStore_RandomKey_WithTracking(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)

	key, err := store.RandomKey("")
	require.NoError(t, err)
	assert.Empty(t, key, "empty store must return empty key")

	keys := []string{"key1", "key2", "key3"}
	for _, k := range keys {
		require.NoError(t, store.Set(k, "value"))
	}

	found := make(map[string]bool)

	for range 50 {
		key, err := store.RandomKey("")
		require.NoError(t, err)

		found[key] = true
	}

	for _, k := range keys {
		assert.Truef(t, found[k], "key not observed in random selections: %s", k)
	}
}

// TestDiskStore_RandomKey_WithoutTracking_Smoke validates that with tracking disabled we still get
// an empty string on empty store and a non-empty key after seeding. (Kept minimal; prefix cases are
// covered comprehensively in the next tests.)
func TestDiskStore_RandomKey_WithoutTracking_Smoke(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, false, "", true)

	key, err := store.RandomKey("")
	require.NoError(t, err)
	assert.Empty(t, key, "empty store must return empty key")

	for _, k := range []string{"key1", "key2", "key3"} {
		require.NoError(t, store.Set(k, "value"))
	}

	key, err = store.RandomKey("")
	require.NoError(t, err)
	assert.NotEmpty(t, key, "non-empty store must return some key")
}

// TestDiskStore_RandomKey_ConcurrentOperations performs a mixed workload (Set/Get/Exists/RandomKey/Delete)
// concurrently to smoke-test synchronization with randomKey in the mix.
func TestDiskStore_RandomKey_ConcurrentOperations(t *testing.T) {
	t.Parallel()

	var (
		store = newTestDiskStore(t, true, "", true)
		wg    sync.WaitGroup
	)

	for i := range 1000 {
		wg.Add(1)

		go func(opIndex int) {
			defer wg.Done()

			key := fmt.Sprintf("key%d", opIndex)
			_ = store.Set(key, "value")
			_, _ = store.Get(key)
			_, _ = store.Exists(key)
			_, _ = store.RandomKey("")
			_ = store.Delete(key)
		}(i)
	}

	wg.Wait()

	size, err := store.Size()
	require.NoError(t, err)
	assert.EqualValues(t, 0, size, "store must end up empty")
}

// TestDiskStore_RandomKey_WithPrefix_TrackingEnabled covers prefix filtering when tracking is enabled,
// including empty store, no-prefix, matching prefix, no-match prefix, and correctness after deletions.
func TestDiskStore_RandomKey_WithPrefix_TrackingEnabled(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)

	key, err := store.RandomKey("a:")
	require.NoError(t, err)
	assert.Empty(t, key, "empty store must return empty key for any prefix")

	require.NoError(t, store.Set("a:1", "v1"))
	require.NoError(t, store.Set("a:2", "v2"))
	require.NoError(t, store.Set("b:1", "v3"))

	key, err = store.RandomKey("")
	require.NoError(t, err)
	assert.NotEmpty(t, key, "no-prefix should return some key")

	key, err = store.RandomKey("a:")
	require.NoError(t, err)
	require.NotEmpty(t, key)
	assert.True(t, strings.HasPrefix(key, "a:"), "must return a key with prefix a:")

	key, err = store.RandomKey("z:")
	require.NoError(t, err)
	assert.Empty(t, key, "no-match prefix must return empty key")

	require.NoError(t, store.Delete("a:1"))

	key, err = store.RandomKey("a:")
	require.NoError(t, err)
	assert.Equal(t, "a:2", key, "after delete, the only remaining prefixed key must be returned")
}

// TestDiskStore_RandomKey_WithPrefix_TrackingDisabled covers prefix filtering with tracking disabled.
func TestDiskStore_RandomKey_WithPrefix_TrackingDisabled(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, false, "", true)

	key, err := store.RandomKey("a:")
	require.NoError(t, err)
	assert.Empty(t, key, "empty store must return empty key")

	require.NoError(t, store.Set("a:1", "v1"))
	require.NoError(t, store.Set("a:2", "v2"))
	require.NoError(t, store.Set("b:1", "v3"))

	key, err = store.RandomKey("a:")
	require.NoError(t, err)
	require.NotEmpty(t, key)
	assert.True(t, strings.HasPrefix(key, "a:"), "must return key with prefix a:")

	key, err = store.RandomKey("z:")
	require.NoError(t, err)
	assert.Empty(t, key, "no-match prefix must return empty key")
}
