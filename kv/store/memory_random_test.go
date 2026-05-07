package store

import (
	"fmt"
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

	entries := requirePopulateStore(
		t,
		store,
		"alpha", "some-value",
		"beta", "some-value",
		"gamma", "some-value",
	)

	found := make(map[string]bool)

	for range 1000 {
		k, err := store.RandomKey("")
		require.NoError(t, err)
		require.NotEmpty(t, k, "non-empty store must return some key")

		found[k] = true
	}

	for _, entry := range entries {
		assert.Truef(t, found[entry.key], "key not observed in random selections: %s", entry.key)
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

	entries := requirePopulateStore(
		t,
		store,
		"alpha", "value",
		"beta", "value",
		"gamma", "value",
	)

	found := make(map[string]bool)

	for range 1000 {
		k, err := store.RandomKey("")
		require.NoError(t, err)
		require.NotEmpty(t, k, "non-empty store must return some key")

		found[k] = true
	}

	for _, entry := range entries {
		assert.Truef(t, found[entry.key], "key not observed in random selections: %s", entry.key)
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

	entries := requirePopulateStore(
		t,
		store,
		"a:alpha", "value-alpha",
		"a:beta", "value-beta",
		"b:alpha", "value-gamma",
	)

	key, err = store.RandomKey("")
	require.NoError(t, err)
	assert.NotEmpty(t, key, "no-prefix should return some key")
	allowed := map[string]bool{
		entries[0].key: true,
		entries[1].key: true,
		entries[2].key: true,
	}
	assert.True(t, allowed[key], "must be one of seeded keys")

	key, err = store.RandomKey("a:")
	require.NoError(t, err)
	require.NotEmpty(t, key)
	assert.True(t, strings.HasPrefix(key, "a:"), "must return a key with prefix a:")

	key, err = store.RandomKey("z:")
	require.NoError(t, err)
	assert.Empty(t, key, "no-match prefix must return empty key")

	mustDeleteFromStore(t, store, entries[0].key)
	key, err = store.RandomKey("a:")
	require.NoError(t, err)
	assert.Equal(t, entries[1].key, key, "after delete, the only remaining prefixed key must be returned")

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

	requirePopulateStore(
		t,
		store,
		"a:alpha", "value-alpha",
		"a:beta", "value-beta",
		"b:alpha", "value-gamma",
	)

	key, err = store.RandomKey("a:")
	require.NoError(t, err)
	require.NotEmpty(t, key)
	assert.True(t, strings.HasPrefix(key, "a:"), "must return key with prefix a:")

	key, err = store.RandomKey("z:")
	require.NoError(t, err)
	assert.Empty(t, key, "no-match prefix must return empty key")
}

func TestMemoryStore_RandomKeys_EmptyStore_ReturnsEmptySlice(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		name := "trackKeys=false"
		if trackKeys {
			name = "trackKeys=true"
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store := NewMemoryStore(&MemoryConfig{TrackKeys: trackKeys})

			keys, err := store.RandomKeys("user:", 10, true)
			require.NoError(t, err)
			assert.Empty(t, keys)

			keys, err = store.RandomKeys("user:", 0, true)
			require.NoError(t, err)
			assert.Empty(t, keys)
		})
	}
}

func TestMemoryStore_RandomKeys_CountAboveMaxReturnsError(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		name := "trackKeys=false"
		if trackKeys {
			name = "trackKeys=true"
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store := NewMemoryStore(&MemoryConfig{TrackKeys: trackKeys})

			_, err := store.RandomKeys("user:", MaxRandomKeysCount+1, false)
			require.Error(t, err)
			require.ErrorIs(t, err, ErrKVOptionsInvalid)
		})
	}
}

func TestMemoryStore_RandomKeys_PrefixFilter(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		name := "trackKeys=false"
		if trackKeys {
			name = "trackKeys=true"
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store := NewMemoryStore(&MemoryConfig{TrackKeys: trackKeys})
			requirePopulateStore(
				t,
				store,
				"user:1", "v1",
				"user:2", "v2",
				"order:1", "v3",
			)

			keys, err := store.RandomKeys("user:", 2, true)
			require.NoError(t, err)
			require.Len(t, keys, 2)
			assert.ElementsMatch(t, []string{"user:1", "user:2"}, keys)
		})
	}
}

func TestMemoryStore_RandomKeys_UniqueNoDuplicates(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		name := "trackKeys=false"
		if trackKeys {
			name = "trackKeys=true"
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store := NewMemoryStore(&MemoryConfig{TrackKeys: trackKeys})
			requirePopulateStore(
				t,
				store,
				"user:1", "v1",
				"user:2", "v2",
				"user:3", "v3",
			)

			keys, err := store.RandomKeys("user:", 2, true)
			require.NoError(t, err)
			require.Len(t, keys, 2)
			assert.NotEqual(t, keys[0], keys[1])
		})
	}
}

func TestMemoryStore_RandomKeys_UniqueCountLargerThanAvailableReturnsAll(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		name := "trackKeys=false"
		if trackKeys {
			name = "trackKeys=true"
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store := NewMemoryStore(&MemoryConfig{TrackKeys: trackKeys})
			requirePopulateStore(
				t,
				store,
				"user:1", "v1",
				"user:2", "v2",
			)

			keys, err := store.RandomKeys("user:", 10, true)
			require.NoError(t, err)
			require.Len(t, keys, 2)
			assert.ElementsMatch(t, []string{"user:1", "user:2"}, keys)
		})
	}
}

func TestMemoryStore_RandomKeys_NonUniqueSingleCandidateRepeats(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		name := "trackKeys=false"
		if trackKeys {
			name = "trackKeys=true"
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store := NewMemoryStore(&MemoryConfig{TrackKeys: trackKeys})
			requirePopulateStore(t, store, "user:1", "v1")

			keys, err := store.RandomKeys("user:", 5, false)
			require.NoError(t, err)
			require.Len(t, keys, 5)
			assert.Equal(t, []string{"user:1", "user:1", "user:1", "user:1", "user:1"}, keys)
		})
	}
}

func TestMemoryStore_RandomKeys_ResultSubsetOfListKeys(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		name := "trackKeys=false"
		if trackKeys {
			name = "trackKeys=true"
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store := NewMemoryStore(&MemoryConfig{TrackKeys: trackKeys})
			requirePopulateStore(
				t,
				store,
				"user:1", "v1",
				"user:2", "v2",
				"user:3", "v3",
				"user:4", "v4",
			)

			allKeys, err := store.ListKeys("user:", 0)
			require.NoError(t, err)
			require.NotEmpty(t, allKeys)

			allowed := make(map[string]struct{}, len(allKeys))
			for _, key := range allKeys {
				allowed[key] = struct{}{}
			}

			keys, err := store.RandomKeys("user:", 20, false)
			require.NoError(t, err)
			require.Len(t, keys, 20)

			for _, key := range keys {
				_, exists := allowed[key]
				assert.Truef(t, exists, "unexpected key: %s", key)
			}
		})
	}
}

func TestMemoryStore_RandomKeys_AfterDeleteManyDoesNotReturnDeletedKeys(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		name := "trackKeys=false"
		if trackKeys {
			name = "trackKeys=true"
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store := NewMemoryStore(&MemoryConfig{TrackKeys: trackKeys})
			requirePopulateStore(
				t,
				store,
				"user:keep:1", "v1",
				"user:keep:2", "v2",
				"user:remove:1", "v3",
			)

			_, err := store.DeleteMany([]string{"user:remove:1"})
			require.NoError(t, err)

			keys, err := store.RandomKeys("user:", 10, true)
			require.NoError(t, err)
			assert.NotContains(t, keys, "user:remove:1")
		})
	}
}

func TestMemoryStore_RandomKeys_AfterDeleteByPrefixDoesNotReturnDeletedKeys(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		name := "trackKeys=false"
		if trackKeys {
			name = "trackKeys=true"
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store := NewMemoryStore(&MemoryConfig{TrackKeys: trackKeys})
			requirePopulateStore(
				t,
				store,
				"user:keep:1", "v1",
				"user:keep:2", "v2",
				"user:remove:1", "v3",
				"user:remove:2", "v4",
			)

			_, err := store.DeleteByPrefix("user:remove:", 10)
			require.NoError(t, err)

			keys, err := store.RandomKeys("user:", 10, true)
			require.NoError(t, err)
			assert.NotContains(t, keys, "user:remove:1")
			assert.NotContains(t, keys, "user:remove:2")
		})
	}
}

func TestKeyFromShardRanges_CoversPrefixRange(t *testing.T) {
	t.Parallel()

	const (
		shardCount   = 256
		prefixedKeys = 2048
		otherKeys    = 256
	)

	store := NewMemoryStore(&MemoryConfig{
		TrackKeys:  true,
		ShardCount: shardCount,
	})

	for i := range prefixedKeys {
		key := fmt.Sprintf("user:%04d", i)
		require.NoError(t, store.Set(key, "value"))
	}

	for i := range otherKeys {
		key := fmt.Sprintf("order:%04d", i)
		require.NoError(t, store.Set(key, "value"))
	}

	expectedKeys, err := store.ListKeys("user:", 0)
	require.NoError(t, err)
	require.Len(t, expectedKeys, prefixedKeys)

	expectedSet := make(map[string]struct{}, len(expectedKeys))
	for _, key := range expectedKeys {
		expectedSet[key] = struct{}{}
	}

	store.lockAllShardReaders()
	defer store.unlockAllShardReaders()

	ranges, total := buildShardRandomRanges(store.shards, "user:")
	require.Equal(t, len(expectedKeys), total)

	seen := make(map[string]struct{}, total)
	for offset := range total {
		key, ok := keyFromShardRanges(ranges, offset)
		require.Truef(t, ok, "expected key for offset=%d", offset)
		assert.True(t, strings.HasPrefix(key, "user:"))

		_, exists := expectedSet[key]
		assert.Truef(t, exists, "unexpected key returned for offset=%d: %s", offset, key)
		seen[key] = struct{}{}
	}

	assert.Len(t, seen, total)

	outOfRangeKey, ok := keyFromShardRanges(ranges, total)
	assert.False(t, ok)
	assert.Empty(t, outOfRangeKey)

	negativeKey, ok := keyFromShardRanges(ranges, -1)
	assert.False(t, ok)
	assert.Empty(t, negativeKey)
}
