package store

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMemoryStore_Scan_ConcurrentMutations verifies Scan remains safe and consistent
// under concurrent Set/Delete operations, and NextKey always reflects the last entry when set.
func TestMemoryStore_Scan_ConcurrentMutations(t *testing.T) {
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

			memoryCfg := &MemoryConfig{TrackKeys: trackKeys}
			store := NewMemoryStore(memoryCfg)

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
						err := store.Delete(key)
						if err != nil {
							t.Errorf("delete failed: %v", err)

							return
						}
					} else {
						err := store.Set(key, fmt.Sprintf("value-updated-%d", i))
						if err != nil {
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

// TestMemoryStore_Scan_MutationAfterPagination verifies that inserting keys
// lexicographically before the NextKey cursor doesn't cause duplicates in subsequent pages.
func TestMemoryStore_Scan_MutationAfterPagination(t *testing.T) {
	t.Parallel()

	initialKeys := []string{"key-a", "key-b", "key-c", "key-d"}

	for _, trackKeys := range []bool{true, false} {
		t.Run(fmt.Sprintf("trackKeys=%t", trackKeys), func(t *testing.T) {
			t.Parallel()

			memoryCfg := &MemoryConfig{TrackKeys: trackKeys}
			store := NewMemoryStore(memoryCfg)

			for _, key := range initialKeys {
				require.NoError(t, store.Set(key, key+"-value"))
			}

			firstPage := mustScanStore(t, store, "", "", 2)
			require.Equal(t, []string{"key-a", "key-b"}, keysFromEntries(t, firstPage.Entries))
			require.Equal(t, "key-b", firstPage.NextKey)

			// Insert a key lexicographically before NextKey.
			require.NoError(t, store.Set("key-aa", "key-aa-value"))

			secondPage := mustScanStore(t, store, "", firstPage.NextKey, 2)
			require.Equal(t, []string{"key-c", "key-d"}, keysFromEntries(t, secondPage.Entries))
			assert.Empty(t, secondPage.NextKey, "final page must not expose NextKey")

			// Ensure the newly inserted key appears when scanning from scratch.
			fullScan := collectScanEntries(t, store, 0)
			assert.Equal(t, []string{"key-a", "key-aa", "key-b", "key-c", "key-d"}, keysFromEntries(t, fullScan))
		})
	}
}

// TestMemoryStore_Count verifies Count with/without prefix across tracking modes.
func TestMemoryStore_Count(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run(fmt.Sprintf("trackKeys=%t", trackKeys), func(t *testing.T) {
			t.Parallel()

			store := NewMemoryStore(&MemoryConfig{TrackKeys: trackKeys})

			count, err := store.Count("")
			require.NoError(t, err)
			assert.EqualValues(t, 0, count)

			requirePopulateStore(
				t,
				store,
				"user:1", "value-1",
				"user:2", "value-2",
				"session:1", "value-3",
			)

			count, err = store.Count("")
			require.NoError(t, err)
			assert.EqualValues(t, 3, count)

			count, err = store.Count("user:")
			require.NoError(t, err)
			assert.EqualValues(t, 2, count)

			count, err = store.Count("missing:")
			require.NoError(t, err)
			assert.EqualValues(t, 0, count)

			require.NoError(t, store.Delete("user:1"))

			count, err = store.Count("user:")
			require.NoError(t, err)
			assert.EqualValues(t, 1, count)
		})
	}
}

// mustScanStore scans the store and panics if it fails.
func mustScanStore(t *testing.T, store Store, prefix, after string, limit int64) *ScanPage {
	t.Helper()

	page, err := store.Scan(prefix, after, limit)
	require.NoError(t, err)

	return page
}
