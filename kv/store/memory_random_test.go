package store

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMemoryStore_RandomKey_Distribution_WithTracking verifies RandomKey returns
// empty string for empty stores, and over repeated calls returns all keys uniformly.
func TestMemoryStore_RandomKey_Distribution_WithTracking(t *testing.T) {
	t.Parallel()

	memoryCfg := &MemoryConfig{TrackKeys: true}
	store := NewMemoryStore(memoryCfg)

	key, err := store.RandomKey("")
	require.NoError(t, err)
	assert.Empty(t, key, "empty store must return empty key")

	keys := []string{"alpha", "beta", "gamma"}
	for _, k := range keys {
		require.NoError(t, store.Set(k, "some-value"))
	}

	found := make(map[string]bool)

	for range 1000 {
		k, err := store.RandomKey("")
		require.NoError(t, err)
		require.NotEmpty(t, k, "non-empty store must return some key")

		found[k] = true
	}

	for _, k := range keys {
		assert.Truef(t, found[k], "key not observed in random selections: %s", k)
	}
}

// TestMemoryStore_RandomKey_Distribution_NoTracking ensures RandomKey covers all
// keys when the store operates without auxiliary tracking structures.
func TestMemoryStore_RandomKey_Distribution_NoTracking(t *testing.T) {
	t.Parallel()

	memoryCfg := &MemoryConfig{TrackKeys: false}
	store := NewMemoryStore(memoryCfg)

	key, err := store.RandomKey("")
	require.NoError(t, err)
	assert.Empty(t, key, "empty store must return empty key")

	keys := []string{"alpha", "beta", "gamma"}
	for _, k := range keys {
		require.NoError(t, store.Set(k, "value"))
	}

	found := make(map[string]bool)

	for range 1000 {
		k, err := store.RandomKey("")
		require.NoError(t, err)
		require.NotEmpty(t, k, "non-empty store must return some key")

		found[k] = true
	}

	for _, k := range keys {
		assert.Truef(t, found[k], "key not observed in random selections: %s", k)
	}
}

// TestMemoryStore_RandomKey_WithPrefix_TrackingEnabled verifies RandomKey with
// prefix filtering works correctly when trackKeys=true, including edge cases like
// empty stores, non-matching prefixes, and behavior after deletions.
func TestMemoryStore_RandomKey_WithPrefix_TrackingEnabled(t *testing.T) {
	t.Parallel()

	memoryCfg := &MemoryConfig{TrackKeys: true}
	store := NewMemoryStore(memoryCfg)

	key, err := store.RandomKey("a:")
	require.NoError(t, err)
	assert.Empty(t, key, "empty store must return empty key for any prefix")

	mustSetInMemoryStore(t, store, "a:1", "v1")
	mustSetInMemoryStore(t, store, "a:2", "v2")
	mustSetInMemoryStore(t, store, "b:1", "v3")

	key, err = store.RandomKey("")
	require.NoError(t, err)
	assert.NotEmpty(t, key, "no-prefix should return some key")
	assert.True(t, key == "a:1" || key == "a:2" || key == "b:1", "must be one of seeded keys")

	key, err = store.RandomKey("a:")
	require.NoError(t, err)
	require.NotEmpty(t, key)
	assert.True(t, strings.HasPrefix(key, "a:"), "must return a key with prefix a:")

	key, err = store.RandomKey("z:")
	require.NoError(t, err)
	assert.Empty(t, key, "no-match prefix must return empty key")

	mustDeleteFromStore(t, store, "a:1")
	key, err = store.RandomKey("a:")
	require.NoError(t, err)
	assert.Equal(t, "a:2", key, "after delete, the only remaining prefixed key must be returned")

	mustClearStore(t, store)
	key, err = store.RandomKey("a:")
	require.NoError(t, err)
	assert.Empty(t, key, "after clear, prefix must return empty key")
}

// TestMemoryStore_RandomKey_WithPrefix_TrackingDisabled verifies RandomKey with
// prefix filtering works correctly when trackKeys=false (fallback two-pass scan mode).
func TestMemoryStore_RandomKey_WithPrefix_TrackingDisabled(t *testing.T) {
	t.Parallel()

	memoryCfg := &MemoryConfig{TrackKeys: false}
	store := NewMemoryStore(memoryCfg)

	key, err := store.RandomKey("a:")
	require.NoError(t, err)
	assert.Empty(t, key, "empty store must return empty key")

	mustSetInMemoryStore(t, store, "a:1", "v1")
	mustSetInMemoryStore(t, store, "a:2", "v2")
	mustSetInMemoryStore(t, store, "b:1", "v3")

	key, err = store.RandomKey("a:")
	require.NoError(t, err)
	require.NotEmpty(t, key)
	assert.True(t, strings.HasPrefix(key, "a:"), "must return key with prefix a:")

	key, err = store.RandomKey("z:")
	require.NoError(t, err)
	assert.Empty(t, key, "no-match prefix must return empty key")
}
