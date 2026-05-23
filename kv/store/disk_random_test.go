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

	entries := requirePopulateStore(
		t,
		store,
		"key-alpha", "value",
		"key-beta", "value",
		"key-gamma", "value",
	)

	found := make(map[string]bool)

	for range 50 {
		key, err := store.RandomKey("")
		require.NoError(t, err)

		found[key] = true
	}

	for _, entry := range entries {
		assert.Truef(t, found[entry.key], "key not observed in random selections: %s", entry.key)
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

	requirePopulateStore(
		t,
		store,
		"key-alpha", "value",
		"key-beta", "value",
		"key-gamma", "value",
	)

	key, err = store.RandomKey("")
	require.NoError(t, err)
	assert.NotEmpty(t, key, "non-empty store must return some key")
}

// TestDiskStore_RandomKey_ConcurrentOperations performs a mixed workload (Set/Get/Exists/RandomKey/Delete)
// concurrently to smoke-test synchronization with randomKey in the mix.
func TestDiskStore_RandomKey_ConcurrentOperations(t *testing.T) {
	t.Parallel()

	operationCount := scaledStressCount(1_000, 128)

	var (
		store = newTestDiskStore(t, true, "", true)
		wg    sync.WaitGroup
	)

	for i := range operationCount {
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

	key, err = store.RandomKey("a:")
	require.NoError(t, err)
	require.NotEmpty(t, key)
	assert.True(t, strings.HasPrefix(key, "a:"), "must return a key with prefix a:")

	key, err = store.RandomKey("z:")
	require.NoError(t, err)
	assert.Empty(t, key, "no-match prefix must return empty key")

	require.NoError(t, store.Delete(entries[0].key))

	key, err = store.RandomKey("a:")
	require.NoError(t, err)
	assert.Equal(t, entries[1].key, key, "after delete, the only remaining prefixed key must be returned")
}

// TestDiskStore_RandomKey_WithPrefix_TrackingDisabled covers prefix filtering with tracking disabled.
func TestDiskStore_RandomKey_WithPrefix_TrackingDisabled(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, false, "", true)

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

// TestDiskStore_RandomKeys_EmptyStore_ReturnsEmptySlice verifies that disk store random keys empty store returns empty slice.
func TestDiskStore_RandomKeys_EmptyStore_ReturnsEmptySlice(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		name := "trackKeys=false"
		if trackKeys {
			name = "trackKeys=true"
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)

			keys, err := store.RandomKeys("user:", 10, true)
			require.NoError(t, err)
			assert.Empty(t, keys)

			keys, err = store.RandomKeys("user:", 0, true)
			require.NoError(t, err)
			assert.Empty(t, keys)
		})
	}
}

// TestDiskStore_RandomKeys_CountAboveMaxReturnsError verifies that disk store random keys count above max returns error.
func TestDiskStore_RandomKeys_CountAboveMaxReturnsError(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		name := "trackKeys=false"
		if trackKeys {
			name = "trackKeys=true"
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)

			_, err := store.RandomKeys("user:", MaxRandomKeysCount+1, false)
			require.Error(t, err)
			require.ErrorIs(t, err, ErrKVOptionsInvalid)
		})
	}
}

// TestDiskStore_RandomKeys_PrefixFilter verifies that disk store random keys prefix filter.
func TestDiskStore_RandomKeys_PrefixFilter(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		name := "trackKeys=false"
		if trackKeys {
			name = "trackKeys=true"
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
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

// TestDiskStore_RandomKeys_UniqueNoDuplicates verifies that disk store random keys unique no duplicates.
func TestDiskStore_RandomKeys_UniqueNoDuplicates(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		name := "trackKeys=false"
		if trackKeys {
			name = "trackKeys=true"
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
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

// TestDiskStore_RandomKeys_UniqueCountLargerThanAvailableReturnsAll verifies that disk store random keys unique count larger than available returns all.
func TestDiskStore_RandomKeys_UniqueCountLargerThanAvailableReturnsAll(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		name := "trackKeys=false"
		if trackKeys {
			name = "trackKeys=true"
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
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

// TestDiskStore_RandomKeys_NonUniqueSingleCandidateRepeats verifies that disk store random keys non unique single candidate repeats.
func TestDiskStore_RandomKeys_NonUniqueSingleCandidateRepeats(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		name := "trackKeys=false"
		if trackKeys {
			name = "trackKeys=true"
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
			requirePopulateStore(t, store, "user:1", "v1")

			keys, err := store.RandomKeys("user:", 5, false)
			require.NoError(t, err)
			require.Len(t, keys, 5)
			assert.Equal(t, []string{"user:1", "user:1", "user:1", "user:1", "user:1"}, keys)
		})
	}
}

// TestDiskStore_RandomKeys_ResultSubsetOfListKeys verifies that disk store random keys result subset of list keys.
func TestDiskStore_RandomKeys_ResultSubsetOfListKeys(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		name := "trackKeys=false"
		if trackKeys {
			name = "trackKeys=true"
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
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

// TestDiskStore_RandomKeys_AfterDeleteManyDoesNotReturnDeletedKeys verifies that disk store random keys after delete many does not return deleted keys.
func TestDiskStore_RandomKeys_AfterDeleteManyDoesNotReturnDeletedKeys(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		name := "trackKeys=false"
		if trackKeys {
			name = "trackKeys=true"
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
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

// TestDiskStore_RandomKeys_AfterDeleteByPrefixDoesNotReturnDeletedKeys verifies that disk store random keys after delete by prefix does not return deleted keys.
func TestDiskStore_RandomKeys_AfterDeleteByPrefixDoesNotReturnDeletedKeys(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		name := "trackKeys=false"
		if trackKeys {
			name = "trackKeys=true"
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
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

// TestShouldFallbackToCursorRandomKeys verifies that should fallback to cursor random keys.
func TestShouldFallbackToCursorRandomKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		unique bool
		count  int64
		total  int
		want   bool
	}{
		{
			name:   "non_unique_never_fallbacks",
			unique: false,
			count:  1_000,
			total:  1_000,
			want:   false,
		},
		{
			name:   "empty_range_does_not_fallback",
			unique: true,
			count:  100,
			total:  0,
			want:   false,
		},
		{
			name:   "small_range_does_not_fallback",
			unique: true,
			count:  100,
			total:  200,
			want:   false,
		},
		{
			name:   "small_sample_does_not_fallback",
			unique: true,
			count:  10,
			total:  10_000,
			want:   false,
		},
		{
			name:   "near_full_unique_fallbacks",
			unique: true,
			count:  5_000,
			total:  10_000,
			want:   true,
		},
		{
			name:   "full_unique_fallbacks",
			unique: true,
			count:  10_000,
			total:  10_000,
			want:   true,
		},
		{
			name:   "over_full_unique_fallbacks",
			unique: true,
			count:  20_000,
			total:  10_000,
			want:   true,
		},
	}

	for testIndex := range tests {
		testCase := tests[testIndex]

		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			got := shouldFallbackToCursorRandomKeys(testCase.unique, testCase.count, testCase.total)
			assert.Equal(t, testCase.want, got)
		})
	}
}

// TestDiskStore_RandomKeys_TrackedNearFullUnique_ReturnsAvailableKeys verifies that disk store random keys tracked near full unique returns available keys.
func TestDiskStore_RandomKeys_TrackedNearFullUnique_ReturnsAvailableKeys(t *testing.T) {
	t.Parallel()

	const (
		userCount  = 512
		orderCount = 88
	)

	store := newTestDiskStore(t, true, "", true)

	entries := make([]testEntry, 0, userCount+orderCount)
	expectedUserKeys := make([]string, 0, userCount)

	for i := range userCount {
		key := fmt.Sprintf("user:%04d", i)
		entries = append(entries, testEntry{key: key, value: "value"})
		expectedUserKeys = append(expectedUserKeys, key)
	}

	for i := range orderCount {
		entries = append(entries, testEntry{
			key:   fmt.Sprintf("order:%04d", i),
			value: "value",
		})
	}

	requirePopulateStoreEntries(t, store, entries)

	keys, err := store.RandomKeys("user:", userCount, true)
	require.NoError(t, err)
	require.Len(t, keys, userCount)
	assert.ElementsMatch(t, expectedUserKeys, keys)

	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		assert.Truef(t, strings.HasPrefix(key, "user:"), "unexpected key: %s", key)
		seen[key] = struct{}{}
	}

	assert.Len(t, seen, len(keys))
}
