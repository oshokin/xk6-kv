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

	initialKeys := []string{"k1", "k2", "k3", "k4"}

	for _, trackKeys := range []bool{true, false} {
		t.Run(fmt.Sprintf("trackKeys=%t", trackKeys), func(t *testing.T) {
			t.Parallel()

			memoryCfg := &MemoryConfig{TrackKeys: trackKeys}
			store := NewMemoryStore(memoryCfg)

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

// mustScanStore scans the store and panics if it fails.
func mustScanStore(t *testing.T, store Store, prefix, after string, limit int64) *ScanPage {
	t.Helper()

	page, err := store.Scan(prefix, after, limit)
	require.NoError(t, err)

	return page
}
