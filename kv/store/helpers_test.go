package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// Maximum int64 value as string.
	testMaxInt64String = "9223372036854775807"
	// Minimum int64 value as string.
	testMinInt64String = "-9223372036854775808"
	// Non-numeric string value.
	testNonNumeric = "not-a-number"
	// Maximum safe JavaScript integer.
	testMaxSafeJSInt = int64(9007199254740991)
)

type testEntry struct {
	key   string
	value string
}

// testEntries builds key/value fixtures from alternating arguments.
// Even positions are keys, odd positions are values.
func testEntries(v ...string) []testEntry {
	if len(v)%2 != 0 {
		panic("testEntries requires even number of arguments: key/value pairs")
	}

	entries := make([]testEntry, 0, len(v)/2)

	for i := 0; i < len(v); i += 2 {
		entries = append(entries, testEntry{
			key:   v[i],
			value: v[i+1],
		})
	}

	return entries
}

// requirePopulateStore builds key/value fixtures from alternating arguments and
// writes them into the store. It returns the parsed entries for later assertions.
func requirePopulateStore(t *testing.T, store Store, v ...string) []testEntry {
	t.Helper()

	entries := testEntries(v...)
	requirePopulateStoreEntries(t, store, entries)

	return entries
}

// requirePopulateStoreEntries writes prebuilt fixtures into the store.
func requirePopulateStoreEntries(t *testing.T, store Store, entries []testEntry) {
	t.Helper()

	for _, entry := range entries {
		require.NoErrorf(t, store.Set(entry.key, entry.value), "Set(%q) must succeed", entry.key)
	}
}

// requireStoredStringValue verifies that the value stored in the store for
// the given key is equal to the expected string.
func requireStoredStringValue(t *testing.T, store Store, key, expected string) {
	t.Helper()

	valueAny, err := store.Get(key)
	require.NoError(t, err)

	valueBytes, ok := valueAny.([]byte)
	require.Truef(t, ok, "expected []byte, got %T", valueAny)

	assert.Equal(t, []byte(expected), valueBytes)
}

// requireDiskTrackingMatchesStore verifies that the in-memory key tracking structures
// match the keys stored in the disk store.
func requireDiskTrackingMatchesStore(t *testing.T, store *DiskStore) {
	t.Helper()

	entries, err := store.List("", 0)
	require.NoError(t, err)

	dbKeys := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		dbKeys[entry.Key] = struct{}{}
	}

	store.keysLock.RLock()
	defer store.keysLock.RUnlock()

	require.Len(t, store.keysList, len(store.keysMap), "keysList and keysMap must have equal size")

	seen := make(map[string]struct{}, len(store.keysList))
	for index, key := range store.keysList {
		mapIndex, exists := store.keysMap[key]
		require.Truef(t, exists, "keysMap missing key from keysList: %s", key)
		assert.Equalf(t, index, mapIndex, "index mismatch for key %s", key)

		_, duplicate := seen[key]
		assert.Falsef(t, duplicate, "duplicate key in keysList: %s", key)
		seen[key] = struct{}{}

		_, existsInDB := dbKeys[key]
		assert.Truef(t, existsInDB, "indexed key missing from bbolt: %s", key)
	}

	for key := range dbKeys {
		_, indexed := store.keysMap[key]
		assert.Truef(t, indexed, "bbolt key missing from in-memory index: %s", key)
	}
}

// requireMemoryTrackingMatchesStore verifies that the in-memory key tracking structures
// match the keys stored in the memory store.
func requireMemoryTrackingMatchesStore(t *testing.T, store *MemoryStore) {
	t.Helper()

	for shardIndex, shard := range store.shards {
		shard.mu.RLock()

		require.Lenf(
			t,
			shard.keysList,
			len(shard.keysMap),
			"shard %d: keysList and keysMap must have equal size",
			shardIndex,
		)

		seen := make(map[string]struct{}, len(shard.keysList))
		for index, key := range shard.keysList {
			mapIndex, exists := shard.keysMap[key]
			require.Truef(t, exists, "shard %d: keysMap missing key %s", shardIndex, key)
			assert.Equalf(t, index, mapIndex, "shard %d: index mismatch for key %s", shardIndex, key)

			_, duplicate := seen[key]
			assert.Falsef(t, duplicate, "shard %d: duplicate key in keysList: %s", shardIndex, key)
			seen[key] = struct{}{}

			_, existsInContainer := shard.container[key]
			assert.Truef(t, existsInContainer, "shard %d: indexed key missing from container: %s", shardIndex, key)
		}

		for key := range shard.container {
			_, indexed := shard.keysMap[key]
			assert.Truef(t, indexed, "shard %d: container key missing from index: %s", shardIndex, key)
		}

		shard.mu.RUnlock()
	}
}
