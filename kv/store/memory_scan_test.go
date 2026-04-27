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

// TestMemoryStore_Count verifies Count semantics on boundary-heavy prefix datasets
// and keeps Count aligned with Scan(prefix, "", 0) across tracking modes.
func TestMemoryStore_Count(t *testing.T) {
	t.Parallel()

	type (
		countCheck struct {
			prefix string
			want   int64
		}

		countCase struct {
			name    string
			records []string
			checks  []*countCheck
		}
	)

	countCases := []*countCase{
		{
			name: "mixed-boundaries-and-parity",
			records: []string{
				"user", "value-user",
				"user:", "value-user-colon",
				"user:1", "value-user-1",
				"user:10", "value-user-10",
				"userx", "value-userx",
				"abc", "value-abc",
				"abcd", "value-abcd",
				"abce", "value-abce",
				"abd", "value-abd",
				"\xff", "value-ff",
				"\xffa", "value-ffa",
				"пользователь:1", "value-user-ru-1",
				"пользователь:2", "value-user-ru-2",
				"заказ:1", "value-order-ru-1",
			},
			checks: []*countCheck{
				{prefix: "", want: 14},
				{prefix: "user:", want: 3},
				{prefix: "abc", want: 3},
				{prefix: "missing:", want: 0},
				{prefix: "\xff", want: 2},
				{prefix: "пользователь:", want: 2},
			},
		},
		{
			name: "exact-prefix-boundary",
			records: []string{
				"abc", "value-1",
				"abcd", "value-2",
				"abce", "value-3",
				"abd", "value-4",
			},
			checks: []*countCheck{
				{prefix: "abc", want: 3},
			},
		},
		{
			name: "delimiter-boundary",
			records: []string{
				"user", "value-user",
				"user:", "value-user-colon",
				"user:1", "value-user-1",
				"user:10", "value-user-10",
				"userx", "value-userx",
			},
			checks: []*countCheck{
				{prefix: "user:", want: 3},
			},
		},
		{
			name: "unicode-prefix",
			records: []string{
				"пользователь:1", "value-1",
				"пользователь:2", "value-2",
				"заказ:1", "value-3",
			},
			checks: []*countCheck{
				{prefix: "пользователь:", want: 2},
			},
		},
		{
			name: "ff-boundary",
			records: []string{
				"\xff", "value-1",
				"\xffa", "value-2",
				"a", "value-3",
			},
			checks: []*countCheck{
				{prefix: "\xff", want: 2},
			},
		},
	}

	for _, trackKeys := range []bool{true, false} {
		t.Run(fmt.Sprintf("trackKeys=%t", trackKeys), func(t *testing.T) {
			t.Parallel()

			store := NewMemoryStore(&MemoryConfig{TrackKeys: trackKeys})

			for _, countCase := range countCases {
				t.Run(countCase.name, func(t *testing.T) {
					require.NoError(t, store.Clear())
					requirePopulateStore(t, store, countCase.records...)

					for _, check := range countCase.checks {
						count := requireCountMatchesScan(t, store, check.prefix)
						assert.Equal(t, check.want, count)
					}
				})
			}
		})
	}
}

// TestMemoryStore_Count_MutationParity verifies Count remains consistent with Scan
// after common key lifecycle mutations, including creation-through-CAS and IncrementBy.
func TestMemoryStore_Count_MutationParity(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run(fmt.Sprintf("trackKeys=%t", trackKeys), func(t *testing.T) {
			t.Parallel()

			store := NewMemoryStore(&MemoryConfig{TrackKeys: trackKeys})
			assertCounts := func(userCount, counterCount, totalCount int64) {
				t.Helper()

				assert.Equal(t, userCount, requireCountMatchesScan(t, store, "user:"))
				assert.Equal(t, counterCount, requireCountMatchesScan(t, store, "counter:user:"))
				assert.Equal(t, totalCount, requireCountMatchesScan(t, store, ""))
			}

			require.NoError(t, store.Clear())
			assertCounts(0, 0, 0)

			require.NoError(t, store.Set("user:1", "value-1"))
			assertCounts(1, 0, 1)

			_, loaded, err := store.GetOrSet("user:2", "value-2")
			require.NoError(t, err)
			assert.False(t, loaded)
			assertCounts(2, 0, 2)

			_, loaded, err = store.Swap("user:3", "value-3")
			require.NoError(t, err)
			assert.False(t, loaded)
			assertCounts(3, 0, 3)

			swapped, err := store.CompareAndSwap("user:4", nil, "value-4")
			require.NoError(t, err)
			assert.True(t, swapped)
			assertCounts(4, 0, 4)

			counterValue, err := store.IncrementBy("counter:user:1", 1)
			require.NoError(t, err)
			assert.EqualValues(t, 1, counterValue)
			assertCounts(4, 1, 5)

			deleted, err := store.CompareAndDelete("user:2", "value-2")
			require.NoError(t, err)
			assert.True(t, deleted)
			assertCounts(3, 1, 4)

			deleted, err = store.DeleteIfExists("user:3")
			require.NoError(t, err)
			assert.True(t, deleted)
			assertCounts(2, 1, 3)

			require.NoError(t, store.Delete("user:1"))
			assertCounts(1, 1, 2)

			require.NoError(t, store.RebuildKeyList())
			assertCounts(1, 1, 2)

			require.NoError(t, store.Clear())
			assertCounts(0, 0, 0)
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
