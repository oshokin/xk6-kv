package store

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDiskStore_Scan_ReturnsDistinctBuffers ensures that Scan returns distinct buffers for each page.
func TestDiskStore_Scan_ReturnsDistinctBuffers(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	require.NoError(t, store.Set("scan-key", "v1"))

	firstPage, err := store.Scan("", "", 1)
	require.NoError(t, err)
	require.Len(t, firstPage.Entries, 1)

	secondPage, err := store.Scan("", "", 1)
	require.NoError(t, err)
	require.Len(t, secondPage.Entries, 1)

	first := firstPage.Entries[0].Value.([]byte)
	second := secondPage.Entries[0].Value.([]byte)

	require.Equal(t, []byte("v1"), first)
	require.Equal(t, []byte("v1"), second)

	if len(first) > 0 && len(second) > 0 {
		firstPtr := uintptr(unsafe.Pointer(&first[0]))
		secondPtr := uintptr(unsafe.Pointer(&second[0]))

		require.NotEqual(t, firstPtr, secondPtr, "Scan must return freshly cloned buffers")
	}
}

// TestDiskStore_Scan_ReturnsCopy tests that Scan returns a copy of the value.
func TestDiskStore_Scan_ReturnsCopy(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, false, "", true)
	require.NoError(t, store.Set("a", "value"))

	page, err := store.Scan("", "", 1)
	require.NoError(t, err)
	require.Len(t, page.Entries, 1)

	value := page.Entries[0].Value.([]byte)
	value[0] = 'X'

	again, err := store.Get("a")
	require.NoError(t, err)
	assert.Equal(t, []byte("value"), again.([]byte), "scan must return copies of values")
}

// TestDiskStore_Scan_PrefixPagination exercises Scan across multiple prefixes, pagination limits,
// empty prefixes, limit <= 0 semantics, and ensures results stay in sync with List() in both
// tracking modes.
func TestDiskStore_Scan_PrefixPagination(t *testing.T) {
	t.Parallel()

	prefixData := []struct {
		prefix string
		count  int
	}{
		{prefix: "alpha", count: 5},
		{prefix: "beta", count: 6},
		{prefix: "gamma", count: 7},
	}

	for _, trackKeys := range []bool{true, false} {
		t.Run(fmt.Sprintf("trackKeys=%t", trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
			prefixKeys := seedStorePrefixes(t, store, prefixData)

			// Collect full scan with pagination and compare to List results (keys and values).
			const pageSize = int64(3)

			var (
				allScanEntries []Entry
				after          string
			)

			for {
				page := mustScanStore(t, store, "", after, pageSize)

				if page.NextKey == "" {
					require.True(t, int64(len(page.Entries)) <= pageSize || pageSize <= 0, "page exceeded limit")
				} else {
					require.NotEmpty(t, page.Entries, "NextKey must only be set when entries exist")
					assert.Equal(t, page.Entries[len(page.Entries)-1].Key, page.NextKey, "NextKey must equal last key in page")
				}

				allScanEntries = append(allScanEntries, page.Entries...)

				if page.NextKey == "" {
					break
				}

				after = page.NextKey
			}

			listEntries := mustListStore(t, store)
			assert.Equal(
				t,
				keysFromEntries(t, listEntries),
				keysFromEntries(t, allScanEntries),
				"Scan keys must match List keys",
			)
			assert.Equal(
				t,
				valuesFromEntries(t, listEntries),
				valuesFromEntries(t, allScanEntries),
				"Scan values must match List values",
			)

			// Per-prefix scanning with pagination.
			for _, data := range prefixData {
				var (
					prefixCollected []string
					prefix          = data.prefix + ":"
					cursor          string
				)

				for {
					page := mustScanStore(t, store, prefix, cursor, 2)

					for _, entry := range page.Entries {
						assert.Truef(
							t,
							strings.HasPrefix(entry.Key, data.prefix+":"),
							"unexpected prefix entry: %s",
							entry.Key,
						)
						prefixCollected = append(prefixCollected, entry.Key)
					}

					if page.NextKey == "" {
						break
					}

					assert.Equal(t, page.Entries[len(page.Entries)-1].Key, page.NextKey)
					cursor = page.NextKey
				}

				assert.Equal(t, prefixKeys[data.prefix], prefixCollected, "prefix scan mismatch")
			}

			// limit <= 0 should return entire prefix range after the provided cursor.
			page := mustScanStore(t, store, "beta:", "beta:03", 0)
			assert.Equal(t, prefixKeys["beta"][3:], keysFromEntries(t, page.Entries))
			assert.Empty(t, page.NextKey)

			// afterKey outside prefix must result in an empty page.
			page = mustScanStore(t, store, "beta:", "gamma:99", 5)
			assert.Empty(t, page.Entries)
			assert.Empty(t, page.NextKey)

			// Non-existent prefix should yield empty result.
			page = mustScanStore(t, store, "does-not-exist:", "", 5)
			assert.Empty(t, page.Entries)
			assert.Empty(t, page.NextKey)

			// Ensure List and Scan stay in sync after deletions.
			require.NoError(t, store.Delete("alpha:02"))
			require.NoError(t, store.Delete("gamma:05"))

			scanAfterDelete := collectScanEntries(t, store, 4)
			listAfterDelete := mustListStore(t, store)

			assert.Equal(t,
				keysFromEntries(t, listAfterDelete),
				keysFromEntries(t, scanAfterDelete),
				"Scan/List keys must match after deletes",
			)
		})
	}
}

// TestDiskStore_Scan_ConcurrentMutations ensures Scan remains safe under concurrent Set/Delete
// workloads for both tracking modes and that NextKey always reflects the last entry key when set.
func TestDiskStore_Scan_ConcurrentMutations(t *testing.T) {
	t.Parallel()

	const (
		initialKeys  = 128
		pageSize     = int64(5)
		iterations   = 256
		prefix       = "conc"
		prefixFormat = "%s:%03d"
	)

	for _, trackKeys := range []bool{true, false} {
		t.Run(fmt.Sprintf("trackKeys=%t", trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)

			for i := range initialKeys {
				require.NoError(t, store.Set(fmt.Sprintf(prefixFormat, prefix, i), fmt.Sprintf("value-%d", i)))
			}

			var wg sync.WaitGroup
			wg.Add(3)

			// Global scanner.
			go func() {
				defer wg.Done()

				for range iterations {
					page, err := store.Scan("", "", pageSize)
					if err != nil {
						t.Errorf("global scan failed: %v", err)
						return
					}

					if len(page.Entries) == 0 {
						continue
					}

					if page.NextKey != "" {
						assert.Equal(t, page.Entries[len(page.Entries)-1].Key, page.NextKey)
					}

					for _, entry := range page.Entries {
						assert.NotEmpty(t, entry.Key)
					}
				}
			}()

			// Prefix scanner.
			go func() {
				defer wg.Done()

				for range iterations {
					page, err := store.Scan(prefix+":", "", pageSize)
					if err != nil {
						t.Errorf("prefix scan failed: %v", err)
						return
					}

					if page.NextKey != "" && len(page.Entries) > 0 {
						assert.Equal(t, page.Entries[len(page.Entries)-1].Key, page.NextKey)
					}
				}
			}()

			// Writer / deleter.
			go func() {
				defer wg.Done()

				for i := range iterations {
					key := fmt.Sprintf(prefixFormat, prefix, i%initialKeys)

					if i%2 == 0 {
						if err := store.Delete(key); err != nil {
							t.Errorf("delete failed: %v", err)
							return
						}
					} else {
						if err := store.Set(key, fmt.Sprintf("value-updated-%d", i)); err != nil {
							t.Errorf("set failed: %v", err)
							return
						}
					}
				}
			}()

			wg.Wait()
		})
	}
}

// TestDiskStore_Scan_MutationAfterPagination ensures inserting keys below the previously
// returned cursor does not cause duplicates in subsequent pages.
func TestDiskStore_Scan_MutationAfterPagination(t *testing.T) {
	t.Parallel()

	initialKeys := []string{"k1", "k2", "k3", "k4"}

	for _, trackKeys := range []bool{true, false} {
		t.Run(fmt.Sprintf("trackKeys=%t", trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)

			for _, key := range initialKeys {
				require.NoError(t, store.Set(key, key+"-value"))
			}

			firstPage := mustScanStore(t, store, "", "", 2)
			require.Equal(t, []string{"k1", "k2"}, keysFromEntries(t, firstPage.Entries))
			require.Equal(t, "k2", firstPage.NextKey)

			// Insert a key lexicographically before NextKey.
			require.NoError(t, store.Set("k1.5", "k1.5-value"))

			secondPage := mustScanStore(t, store, "", firstPage.NextKey, 2)
			require.Equal(t, []string{"k3", "k4"}, keysFromEntries(t, secondPage.Entries))
			assert.Empty(t, secondPage.NextKey, "final page must not expose NextKey")

			// Ensure the newly inserted key appears when scanning from scratch.
			fullScan := collectScanEntries(t, store, 0)
			assert.Equal(t, []string{"k1", "k1.5", "k2", "k3", "k4"}, keysFromEntries(t, fullScan))
		})
	}
}
