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
	require.NoError(t, store.Set("scan-key", "scan-value"))

	firstPage, err := store.Scan("", "", 1)
	require.NoError(t, err)
	require.Len(t, firstPage.Entries, 1)

	secondPage, err := store.Scan("", "", 1)
	require.NoError(t, err)
	require.Len(t, secondPage.Entries, 1)

	first := firstPage.Entries[0].Value.([]byte)
	second := secondPage.Entries[0].Value.([]byte)

	require.Equal(t, []byte("scan-value"), first)
	require.Equal(t, []byte("scan-value"), second)

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
					assert.Equal(
						t,
						page.Entries[len(page.Entries)-1].Key,
						page.NextKey,
						"NextKey must equal last key in page",
					)
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
		prefix       = "conc"
		prefixFormat = "%s:%03d"
	)

	iterations := scaledStressCount(256, 48)

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

// TestDiskStore_Scan_MutationAfterPagination ensures inserting keys below the previously
// returned cursor does not cause duplicates in subsequent pages.
func TestDiskStore_Scan_MutationAfterPagination(t *testing.T) {
	t.Parallel()

	initialKeys := []string{"key-a", "key-b", "key-c", "key-d"}

	for _, trackKeys := range []bool{true, false} {
		t.Run(fmt.Sprintf("trackKeys=%t", trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)

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

// TestDiskStore_Count verifies Count semantics on boundary-heavy prefix datasets
// and keeps Count aligned with Scan(prefix, "", 0) across tracking modes.
func TestDiskStore_Count(t *testing.T) {
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

			store := newTestDiskStore(t, trackKeys, "", true)

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

// TestDiskStore_Count_MutationParity verifies Count remains consistent with Scan
// after common key lifecycle mutations, including creation-through-CAS and IncrementBy.
func TestDiskStore_Count_MutationParity(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run(fmt.Sprintf("trackKeys=%t", trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
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

func TestDiskStore_ListKeys_EmptyStore(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run(fmt.Sprintf("trackKeys=%t", trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)

			keys, err := store.ListKeys("", 0)
			require.NoError(t, err)
			assert.Empty(t, keys)
		})
	}
}

func TestDiskStore_ListKeys_AllKeysOrdered(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run(fmt.Sprintf("trackKeys=%t", trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
			_, err := store.SetMany([]Entry{
				{Key: "user:2", Value: []byte("two")},
				{Key: "order:1", Value: []byte("order")},
				{Key: "user:1", Value: []byte("one")},
			})
			require.NoError(t, err)

			keys, err := store.ListKeys("", 0)
			require.NoError(t, err)
			assert.Equal(t, []string{"order:1", "user:1", "user:2"}, keys)
		})
	}
}

func TestDiskStore_ListKeys_Prefix(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run(fmt.Sprintf("trackKeys=%t", trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
			_, err := store.SetMany([]Entry{
				{Key: "user:2", Value: []byte("two")},
				{Key: "order:1", Value: []byte("order")},
				{Key: "user:1", Value: []byte("one")},
			})
			require.NoError(t, err)

			keys, err := store.ListKeys("user:", 0)
			require.NoError(t, err)
			assert.Equal(t, []string{"user:1", "user:2"}, keys)
		})
	}
}

func TestDiskStore_ListKeys_Limit(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run(fmt.Sprintf("trackKeys=%t", trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
			_, err := store.SetMany([]Entry{
				{Key: "user:3", Value: []byte("three")},
				{Key: "user:1", Value: []byte("one")},
				{Key: "user:2", Value: []byte("two")},
			})
			require.NoError(t, err)

			keys, err := store.ListKeys("user:", 2)
			require.NoError(t, err)
			assert.Equal(t, []string{"user:1", "user:2"}, keys)
		})
	}
}

func TestDiskStore_ListKeys_AfterDeleteMany(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run(fmt.Sprintf("trackKeys=%t", trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
			_, err := store.SetMany([]Entry{
				{Key: "user:1", Value: []byte("one")},
				{Key: "user:2", Value: []byte("two")},
			})
			require.NoError(t, err)

			_, err = store.DeleteMany([]string{"user:1"})
			require.NoError(t, err)

			keys, err := store.ListKeys("user:", 0)
			require.NoError(t, err)
			assert.Equal(t, []string{"user:2"}, keys)
		})
	}
}

func TestDiskStore_ListKeys_AfterRebuildKeyList(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run(fmt.Sprintf("trackKeys=%t", trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
			_, err := store.SetMany([]Entry{
				{Key: "user:2", Value: []byte("two")},
				{Key: "user:1", Value: []byte("one")},
				{Key: "order:1", Value: []byte("order")},
			})
			require.NoError(t, err)

			require.NoError(t, store.RebuildKeyList())

			keys, err := store.ListKeys("user:", 0)
			require.NoError(t, err)
			assert.Equal(t, []string{"user:1", "user:2"}, keys)
		})
	}
}
