package store

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewMemoryStore verifies that NewMemoryStore returns a non-nil store whose internal
// container map is allocated and empty.
func TestNewMemoryStore(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true)

	require.NotNil(t, store, "NewMemoryStore() must not return nil")
	require.NotNil(t, store.container, "container map must be allocated")
	assert.Empty(t, store.container, "new store must be empty")
}

// TestMemoryStore_BlockMutationsAPI verifies that blockMutations correctly blocks all
// mutation operations with a custom error, and unblockMutations restores normal operation.
func TestMemoryStore_BlockMutationsAPI(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true)

	require.NoError(t, store.blockMutations(errors.New("custom block")))

	err := store.Set("k1", "v1")
	require.EqualError(t, err, "custom block")

	store.unblockMutations()
	require.NoError(t, store.Set("k1", "v1"))

	require.NoError(t, store.blockMutations(nil))

	err = store.Set("k2", "v2")
	require.ErrorIs(t, err, ErrMutationBlocked)

	store.unblockMutations()
	require.NoError(t, store.Set("k2", "v2"))
}

// TestMemoryStore_BlockMutations_WaitGroupRace stress-tests concurrent blocking
// and mutation operations to detect race conditions in the WaitGroup synchronization.
func TestMemoryStore_BlockMutations_WaitGroupRace(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(false)

	// Trigger many concurrent writes + blocks to hit the race.
	for range 100 {
		var wg sync.WaitGroup

		// Launch 50 concurrent writers.
		for i := range 50 {
			wg.Add(1)

			go func(id int) {
				defer wg.Done()

				for range 10 {
					_ = store.Set(fmt.Sprintf("k%d", id), "v")
				}
			}(i)
		}

		// Concurrently block mutations.
		for range 10 {
			wg.Go(func() {
				_ = store.blockMutations(errors.New("test"))
				store.unblockMutations()
			})
		}

		wg.Wait()
	}
}

// TestMemoryStore_GetSet_RoundtripAndTypes validates:
//  1. Get on a missing key returns an error;
//  2. string and []byte Set -> Get round-trip to the same bytes;
//  3. unsupported types cause an error on Set.
func TestMemoryStore_GetSet_RoundtripAndTypes(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true)

	// Missing key must error.
	_, err := store.Get("does-not-exist")
	require.Error(t, err, "Get must return an error for missing key")

	// String round-trip.
	require.NoError(t, store.Set("string-key", "string-value"))
	gotAny, err := store.Get("string-key")
	require.NoError(t, err)

	gotBytes, ok := gotAny.([]byte)
	require.Truef(t, ok, "expected []byte, got %T", gotAny)
	assert.Equal(t, []byte("string-value"), gotBytes)

	// []byte round-trip.
	byteValue := []byte("byte-value")
	require.NoError(t, store.Set("byte-key", byteValue))
	gotAny, err = store.Get("byte-key")
	require.NoError(t, err)

	gotBytes, ok = gotAny.([]byte)
	require.True(t, ok, "expected []byte from Get")
	assert.Equal(t, byteValue, gotBytes)

	// Unsupported type must error.
	require.Error(t, store.Set("invalid-key", 123), "Set of unsupported type must error")
}

// TestMemoryStore_Concurrency performs concurrent Set/Get loops to smoke-test
// synchronization. Passes if no deadlock or data race is detected (under -race).
func TestMemoryStore_Concurrency(t *testing.T) {
	t.Parallel()

	var (
		store = NewMemoryStore(true)
		wg    sync.WaitGroup
	)

	wg.Add(2)

	go func() {
		defer wg.Done()

		for range 100 {
			_ = store.Set("key", "value")
		}
	}()

	go func() {
		defer wg.Done()

		for range 100 {
			_, _ = store.Get("key")
		}
	}()

	wg.Wait()
}

// TestMemoryStore_IncrementBy_Basic verifies IncrementBy starts absent keys at 0,
// handles positive/negative increments correctly, and rejects non-integer values.
func TestMemoryStore_IncrementBy_Basic(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true)

	newValue, err := store.IncrementBy("ctr", 5)
	require.NoError(t, err)
	assert.EqualValues(t, 5, newValue)

	newValue, err = store.IncrementBy("ctr", -2)
	require.NoError(t, err)
	assert.EqualValues(t, 3, newValue)

	require.NoError(t, store.Set("bad", "not-an-int"))
	_, err = store.IncrementBy("bad", 1)
	require.Error(t, err, "non-integer value must cause IncrementBy error")
}

// TestMemoryStore_IncrementBy_Concurrent verifies that concurrent IncrementBy operations
// are atomic and produce the exact expected sum without data loss.
func TestMemoryStore_IncrementBy_Concurrent(t *testing.T) {
	t.Parallel()

	const (
		concurrencyLevel       = 1000
		delta            int64 = 1
	)

	var (
		store = NewMemoryStore(true)
		wg    sync.WaitGroup
	)

	wg.Add(concurrencyLevel)

	for range concurrencyLevel {
		go func() {
			defer wg.Done()

			_, err := store.IncrementBy("ctr", delta)
			assert.NoError(t, err)
		}()
	}

	wg.Wait()

	resultAny, err := store.Get("ctr")
	require.NoError(t, err)

	resultBytes := resultAny.([]byte)
	actual, parseErr := strconv.ParseInt(string(resultBytes), 10, 64)
	require.NoError(t, parseErr)
	assert.EqualValues(t, concurrencyLevel, actual, "counter mismatch")
}

// TestMemoryStore_GetOrSet_Basic verifies first-writer-wins semantics: the first call
// stores the value (loaded=false), subsequent calls return the existing value (loaded=true).
func TestMemoryStore_GetOrSet_Basic(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true)
	value, loaded, err := store.GetOrSet("k", "v1")

	require.NoError(t, err)
	require.False(t, loaded, "first insert must be loaded=false")
	assert.Equal(t, []byte("v1"), value.([]byte))

	value, loaded, err = store.GetOrSet("k", "v2")

	require.NoError(t, err)
	require.True(t, loaded, "existing key must return loaded=true")
	assert.Equal(t, []byte("v1"), value.([]byte), "existing value must be returned")
}

// TestMemoryStore_GetOrSet_Concurrent verifies that under concurrent contention,
// exactly one goroutine wins and stores its value, and all others observe that value.
func TestMemoryStore_GetOrSet_Concurrent(t *testing.T) {
	t.Parallel()

	const concurrencyLevel = 256

	type goroutineResult struct {
		actualBytes []byte
		loaded      bool
		err         error
	}

	var (
		store     = NewMemoryStore(true)
		resultsCh = make(chan goroutineResult, concurrencyLevel)
		wg        sync.WaitGroup
	)

	wg.Add(concurrencyLevel)

	for i := range concurrencyLevel {
		value := "v" + strconv.Itoa(i)

		go func(v string) {
			defer wg.Done()

			actual, loaded, err := store.GetOrSet("one", v)

			var actualBytes []byte
			if actual != nil {
				actualBytes = actual.([]byte)
			}

			resultsCh <- goroutineResult{
				actualBytes: actualBytes,
				loaded:      loaded,
				err:         err,
			}
		}(value)
	}

	wg.Wait()
	close(resultsCh)

	var (
		firstWriterCount int
		firstValue       string
		pendingReaders   [][]byte
	)

	for result := range resultsCh {
		require.NoError(t, result.err)

		if !result.loaded {
			firstWriterCount++
			firstValue = string(result.actualBytes)

			for _, reader := range pendingReaders {
				assert.Equal(t, firstValue, string(reader), "all readers must see the same stored value")
			}

			pendingReaders = nil

			continue
		}

		if firstWriterCount == 0 {
			pendingReaders = append(pendingReaders, result.actualBytes)

			continue
		}

		assert.Equal(t, firstValue, string(result.actualBytes), "all readers must see the same stored value")
	}

	assert.Equal(t, 1, firstWriterCount, "exactly one goroutine must create the value")
}

// TestMemoryStore_Swap_Basic verifies Swap returns (nil, false) for new keys,
// and (previous_value, true) when replacing existing keys.
func TestMemoryStore_Swap_Basic(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true)

	prev, loaded, err := store.Swap("k", "v1")
	require.NoError(t, err)
	assert.False(t, loaded, "first Swap must report loaded=false")
	assert.Nil(t, prev, "first Swap must return prev=nil")

	prev, loaded, err = store.Swap("k", "v2")
	require.NoError(t, err)
	assert.True(t, loaded, "second Swap must report loaded=true")
	assert.Equal(t, []byte("v1"), prev.([]byte))

	got, err := store.Get("k")
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), got.([]byte), "value must be replaced")
}

// TestMemoryStore_CompareAndSwap_Basic verifies CompareAndSwap fails when the
// old value doesn't match, and succeeds only when it matches exactly.
func TestMemoryStore_CompareAndSwap_Basic(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true)
	require.NoError(t, store.Set("k", "old"))

	ok, err := store.CompareAndSwap("k", "BAD", "new")
	require.NoError(t, err)
	assert.False(t, ok, "CAS must fail with wrong old")

	got, err := store.Get("k")
	require.NoError(t, err)
	assert.Equal(t, []byte("old"), got.([]byte), "value must remain unchanged on failed CAS")

	ok, err = store.CompareAndSwap("k", "old", "new")
	require.NoError(t, err)
	assert.True(t, ok, "CAS must succeed on correct old")

	got, err = store.Get("k")
	require.NoError(t, err)
	assert.Equal(t, []byte("new"), got.([]byte), "value must be updated")
}

// TestMemoryStore_CompareAndSwap_InsertWhenAbsent verifies that CompareAndSwap
// with oldValue=nil creates the key when absent, and fails when the key exists.
func TestMemoryStore_CompareAndSwap_InsertWhenAbsent(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true)

	ok, err := store.CompareAndSwap("lock", nil, "holder")
	require.NoError(t, err)
	assert.True(t, ok, "CAS should create the key when oldValue is nil")

	got, err := store.Get("lock")
	require.NoError(t, err)
	assert.Equal(t, []byte("holder"), got.([]byte), "value must match inserted payload")

	ok, err = store.CompareAndSwap("lock", nil, "other")
	require.NoError(t, err)
	assert.False(t, ok, "second CAS must fail because key now exists")
}

// TestMemoryStore_CompareAndSwap_ConcurrentSingleWinner verifies that under concurrent
// contention on the same key, exactly one CompareAndSwap operation succeeds.
func TestMemoryStore_CompareAndSwap_ConcurrentSingleWinner(t *testing.T) {
	t.Parallel()

	const concurrencyLevel = 200

	var (
		store = NewMemoryStore(true)
		okCh  = make(chan bool, concurrencyLevel)
		wg    sync.WaitGroup
	)

	require.NoError(t, store.Set("k", "v0"))

	wg.Add(concurrencyLevel)

	for range concurrencyLevel {
		go func() {
			defer wg.Done()

			ok, err := store.CompareAndSwap("k", "v0", "v1")
			assert.NoError(t, err)

			okCh <- ok
		}()
	}

	wg.Wait()
	close(okCh)

	var successCount int

	for ok := range okCh {
		if ok {
			successCount++
		}
	}

	assert.Equal(t, 1, successCount, "exactly one CAS must succeed")

	got, err := store.Get("k")
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), got.([]byte))
}

// TestMemoryStore_Delete verifies Delete removes existing keys from the store,
// and is a no-op (returns no error) for keys that don't exist.
func TestMemoryStore_Delete(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true)
	require.NoError(t, store.Set("test-key", "test-value"))

	require.NoError(t, store.Delete("test-key"))
	_, exists := store.container["test-key"]
	assert.False(t, exists, "key must be removed from container")

	require.NoError(t, store.Delete("non-existent"), "Delete on missing key must not error")
}

// TestMemoryStore_Exists verifies Exists correctly returns false for missing keys
// and true for keys that are present in the store.
func TestMemoryStore_Exists(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true)

	exists, err := store.Exists("non-existent")
	require.NoError(t, err)
	assert.False(t, exists)

	require.NoError(t, store.Set("test-key", "test-value"))

	exists, err = store.Exists("test-key")
	require.NoError(t, err)
	assert.True(t, exists)
}

// TestMemoryStore_DeleteIfExists_Basic verifies DeleteIfExists returns false for
// absent keys, returns true when successfully deleting, and removes the key.
func TestMemoryStore_DeleteIfExists_Basic(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true)

	ok, err := store.DeleteIfExists("k")
	require.NoError(t, err)
	assert.False(t, ok, "absent key must return false")

	require.NoError(t, store.Set("k", "v"))

	ok, err = store.DeleteIfExists("k")
	require.NoError(t, err)
	assert.True(t, ok, "present key must return true")

	exists, _ := store.Exists("k")
	assert.False(t, exists, "key must not exist after deletion")
}

// TestMemoryStore_DeleteIfExists_ConcurrentSingleWinner verifies that under
// concurrent contention, exactly one DeleteIfExists operation succeeds.
func TestMemoryStore_DeleteIfExists_ConcurrentSingleWinner(t *testing.T) {
	t.Parallel()

	const concurrencyLevel = 128

	var (
		store = NewMemoryStore(true)
		wins  int
		mu    sync.Mutex
		wg    sync.WaitGroup
	)

	require.NoError(t, store.Set("k", "v"))

	wg.Add(concurrencyLevel)

	for range concurrencyLevel {
		go func() {
			defer wg.Done()

			ok, err := store.DeleteIfExists("k")
			assert.NoError(t, err)

			if ok {
				mu.Lock()

				wins++

				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	assert.Equal(t, 1, wins, "exactly one deletion must succeed")

	exists, _ := store.Exists("k")
	assert.False(t, exists, "key must be removed")
}

// TestMemoryStore_CompareAndDelete_Basic verifies CompareAndDelete only removes
// the key when the expected value matches exactly, and fails otherwise.
func TestMemoryStore_CompareAndDelete_Basic(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true)
	require.NoError(t, store.Set("k", "v1"))

	ok, err := store.CompareAndDelete("k", "BAD")
	require.NoError(t, err)
	assert.False(t, ok, "wrong value must not delete")

	exists, _ := store.Exists("k")
	assert.True(t, exists, "key should still exist")

	ok, err = store.CompareAndDelete("k", "v1")
	require.NoError(t, err)
	assert.True(t, ok, "correct value must delete")

	exists, _ = store.Exists("k")
	assert.False(t, exists, "key must be removed")
}

// TestMemoryStore_CompareAndDelete_ConcurrentSingleWinner verifies that under
// concurrent contention, exactly one CompareAndDelete operation succeeds.
func TestMemoryStore_CompareAndDelete_ConcurrentSingleWinner(t *testing.T) {
	t.Parallel()

	const concurrencyLevel = 120

	var (
		store        = NewMemoryStore(true)
		successCount int
		mu           sync.Mutex
		wg           sync.WaitGroup
	)

	require.NoError(t, store.Set("k", "secret"))

	wg.Add(concurrencyLevel)

	for range concurrencyLevel {
		go func() {
			defer wg.Done()

			ok, err := store.CompareAndDelete("k", "secret")
			assert.NoError(t, err)

			if ok {
				mu.Lock()

				successCount++

				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	assert.Equal(t, 1, successCount, "exactly one CompareAndDelete must succeed")

	exists, _ := store.Exists("k")
	assert.False(t, exists, "key must be deleted")
}

// TestMemoryStore_AtomicOps_DoNotChangeBytesUnexpectedly verifies the store clones
// byte slices defensively: mutating the original slice after storing doesn't affect stored data.
func TestMemoryStore_AtomicOps_DoNotChangeBytesUnexpectedly(t *testing.T) {
	t.Parallel()

	var (
		store         = NewMemoryStore(true)
		originalBytes = []byte("payload")
	)

	_, _, err := store.GetOrSet("k", originalBytes)
	require.NoError(t, err)

	// Mutate the original slice; the stored copy must remain unchanged.
	originalBytes[0] = 'X'

	got, err := store.Get("k")
	require.NoError(t, err)

	assert.False(t,
		bytes.Equal(got.([]byte), originalBytes) && strings.HasPrefix(string(got.([]byte)), "X"),
		"store must not alias external byte slices",
	)
}

// TestMemoryStore_Clear verifies that Clear removes all entries from the store.
func TestMemoryStore_Clear(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true)

	entries := []struct{ key, value string }{
		{"key1", "value1"},
		{"key2", "value2"},
	}

	for _, e := range entries {
		require.NoError(t, store.Set(e.key, e.value))
	}

	require.NoError(t, store.Clear())
	assert.Empty(t, store.container, "store must be empty after Clear")
}

// TestMemoryStore_Size verifies Size returns 0 for empty stores and accurately
// reports the number of entries after insertions.
func TestMemoryStore_Size(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true)

	size, err := store.Size()
	require.NoError(t, err)
	assert.EqualValues(t, 0, size, "empty store must report size=0")

	entries := []struct{ key, value string }{
		{"key1", "value1"},
		{"key2", "value2"},
	}

	for _, e := range entries {
		require.NoError(t, store.Set(e.key, e.value))
	}

	size, err = store.Size()
	require.NoError(t, err)
	assert.EqualValues(t, len(entries), size, "size must equal number of entries")
}

// TestMemoryStore_Scan_PrefixPagination verifies Scan correctly handles prefix filtering,
// pagination with limits, limit<=0 semantics, and stays consistent with List() results
// across both tracking modes (trackKeys on/off).
func TestMemoryStore_Scan_PrefixPagination(t *testing.T) {
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

			store := NewMemoryStore(trackKeys)
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
				keysFromEntries(listEntries),
				keysFromEntries(allScanEntries),
				"Scan keys must match List keys",
			)
			assert.Equal(
				t,
				valuesFromEntries(listEntries),
				valuesFromEntries(allScanEntries),
				"Scan values must match List values",
			)

			// Per-prefix scanning with pagination.
			for _, data := range prefixData {
				var (
					prefixCollected []string
					afterPrefix     string
				)

				for {
					page := mustScanStore(t, store, data.prefix+":", afterPrefix, 2)

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
					afterPrefix = page.NextKey
				}

				assert.Equal(t, prefixKeys[data.prefix], prefixCollected, "prefix scan mismatch")
			}

			// limit <= 0 should return entire prefix range after the provided cursor.
			page := mustScanStore(t, store, "beta:", "beta:03", 0)
			assert.Equal(t, prefixKeys["beta"][3:], keysFromEntries(page.Entries))
			assert.Empty(t, page.NextKey, "NextKey must be empty when limit <= 0")

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
			assert.Equal(
				t,
				keysFromEntries(listAfterDelete),
				keysFromEntries(scanAfterDelete),
				"Scan/List keys must match after deletes",
			)
		})
	}
}

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

			store := NewMemoryStore(trackKeys)

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

// TestMemoryStore_Scan_MutationAfterPagination verifies that inserting keys
// lexicographically before the NextKey cursor doesn't cause duplicates in subsequent pages.
func TestMemoryStore_Scan_MutationAfterPagination(t *testing.T) {
	t.Parallel()

	initialKeys := []string{"k1", "k2", "k3", "k4"}

	for _, trackKeys := range []bool{true, false} {
		t.Run(fmt.Sprintf("trackKeys=%t", trackKeys), func(t *testing.T) {
			t.Parallel()

			store := NewMemoryStore(trackKeys)

			for _, key := range initialKeys {
				require.NoError(t, store.Set(key, key+"-value"))
			}

			firstPage := mustScanStore(t, store, "", "", 2)
			require.Equal(t, []string{"k1", "k2"}, keysFromEntries(firstPage.Entries))
			require.Equal(t, "k2", firstPage.NextKey)

			// Insert a key lexicographically before NextKey.
			require.NoError(t, store.Set("k1.5", "k1.5-value"))

			secondPage := mustScanStore(t, store, "", firstPage.NextKey, 2)
			require.Equal(t, []string{"k3", "k4"}, keysFromEntries(secondPage.Entries))
			assert.Empty(t, secondPage.NextKey, "final page must not expose NextKey")

			// Ensure the newly inserted key appears when scanning from scratch.
			fullScan := collectScanEntries(t, store, 0)
			assert.Equal(t, []string{"k1", "k1.5", "k2", "k3", "k4"}, keysFromEntries(fullScan))
		})
	}
}

// TestMemoryStore_List verifies List returns entries in lexicographic order,
// correctly applies prefix filters and limits, and handles empty stores.
func TestMemoryStore_List(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true)

	// Empty store.
	entries, err := store.List("", 0)
	require.NoError(t, err)
	assert.Empty(t, entries, "empty store must list zero entries")

	// Seed data.
	testData := map[string]string{
		"key1":      "value1",
		"key2":      "value2",
		"prefix1":   "value3",
		"prefix2":   "value4",
		"different": "value5",
	}

	for key, value := range testData {
		require.NoError(t, store.Set(key, value))
	}

	// All entries: length and sorted order.
	entries, err = store.List("", 0)
	require.NoError(t, err)
	require.Len(t, entries, len(testData))

	for i := 1; i < len(entries); i++ {
		assert.LessOrEqualf(t, entries[i-1].Key, entries[i].Key, "entries must be sorted by key")
	}

	// Prefix filter.
	entries, err = store.List("prefix", 0)
	require.NoError(t, err)
	assert.Len(t, entries, 2)

	for _, entry := range entries {
		assert.Truef(t, strings.HasPrefix(entry.Key, "prefix"), "unexpected key without prefix: %s", entry.Key)
	}

	// Limit only.
	entries, err = store.List("", 2)
	require.NoError(t, err)
	assert.Len(t, entries, 2)

	// Prefix + limit.
	entries, err = store.List("prefix", 1)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.True(t, strings.HasPrefix(entries[0].Key, "prefix"))
}

// TestMemoryStore_KeyTrackingConsistency verifies the internal key index
// (keysList/keysMap) remains consistent after Set/Delete/Clear when trackKeys is enabled.
func TestMemoryStore_KeyTrackingConsistency(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true)
	keys := []string{"key1", "key2", "key3"}

	for _, key := range keys {
		require.NoError(t, store.Set(key, "value"))
	}

	store.mu.RLock()

	assert.Len(t, store.keysList, len(keys))

	for _, key := range keys {
		_, exists := store.keysMap[key]
		assert.Truef(t, exists, "key missing from index: %s", key)
	}

	store.mu.RUnlock()

	require.NoError(t, store.Delete(keys[1]))

	store.mu.RLock()

	assert.Len(t, store.keysList, len(keys)-1)
	_, exists := store.keysMap[keys[1]]
	assert.False(t, exists, "deleted key must not remain in index")

	store.mu.RUnlock()

	require.NoError(t, store.Clear())

	store.mu.RLock()
	assert.Empty(t, store.keysList, "index must be empty after Clear")
	store.mu.RUnlock()
}

// TestMemoryStore_RandomKey_Distribution_WithTracking verifies RandomKey returns
// empty string for empty stores, and over repeated calls returns all keys uniformly.
func TestMemoryStore_RandomKey_Distribution_WithTracking(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true)

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

// TestMemoryStore_RandomKey_WithPrefix_TrackingEnabled verifies RandomKey with
// prefix filtering works correctly when trackKeys=true, including edge cases like
// empty stores, non-matching prefixes, and behavior after deletions.
func TestMemoryStore_RandomKey_WithPrefix_TrackingEnabled(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true)

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

	store := NewMemoryStore(false)

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

// TestMemoryStore_RebuildKeyList_RandomKeyPrefix verifies RebuildKeyList correctly
// reconstructs the internal index, restoring RandomKey functionality with prefixes.
func TestMemoryStore_RebuildKeyList_RandomKeyPrefix(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true)

	mustSetInMemoryStore(t, store, "p:1", "v1")
	mustSetInMemoryStore(t, store, "p:2", "v2")
	mustSetInMemoryStore(t, store, "q:1", "v3")

	// Simulate index loss: clear, then repopulate fresh, then rebuild index.
	require.NoError(t, store.Clear())
	mustSetInMemoryStore(t, store, "p:1", "v1")
	mustSetInMemoryStore(t, store, "p:2", "v2")
	mustSetInMemoryStore(t, store, "q:1", "v3")

	require.NoError(t, store.RebuildKeyList(), "RebuildKeyList must succeed")

	key, err := store.RandomKey("p:")
	require.NoError(t, err)
	require.NotEmpty(t, key)
	assert.True(t, strings.HasPrefix(key, "p:"), "must return key with prefix p:")
}

// TestMemoryStore_Close verifies Close is a no-op for MemoryStore and returns no error.
func TestMemoryStore_Close(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true)
	require.NoError(t, store.Close())
}

// TestMemory_GetOrSet_Delete_Interleave_NoPanic verifies that concurrent GetOrSet
// and Delete operations on the same key do not cause panics or slice bound errors.
func TestMemory_GetOrSet_Delete_Interleave_NoPanic(t *testing.T) {
	t.Parallel()

	const (
		testKey         = "order-new"
		iterationsCount = 5_000
		keysCount       = 100
	)

	var (
		store     = NewMemoryStore(true)
		testValue = []byte("processed")
	)

	// Seed a realistic number of keys so the store is not empty and
	// the index structures (if any) are exercised further than the trivial case.
	for i := range keysCount {
		require.NoError(t, store.Set(fmt.Sprintf("order-%d", i), testValue))
	}

	// Interleave GetOrSet and Delete in parallel - used to trigger [: -1].
	var wg sync.WaitGroup

	wg.Add(2)

	// Writer/creator: repeatedly tries to insert-or-read the same key.
	go func() {
		defer wg.Done()

		for range iterationsCount {
			_, _, _ = store.GetOrSet(testKey, testValue)
		}
	}()

	// Remover: repeatedly deletes the same key, racing with the writer above.
	go func() {
		defer wg.Done()

		for range iterationsCount {
			_ = store.Delete(testKey)
		}
	}()

	// If the implementation mishandles swap-delete or slice bounds on empty lists,
	// a panic would bubble up and fail this test.
	wg.Wait()
}

// mustSetInMemoryStore sets a key in the memory store and panics if it fails.
func mustSetInMemoryStore(t *testing.T, store Store, key string, value any) {
	t.Helper()

	require.NoErrorf(t, store.Set(key, value), "Set(%q) must succeed", key)
}

// mustDeleteFromStore deletes a key in the store and panics if it fails.
func mustDeleteFromStore(t *testing.T, store Store, key string) {
	t.Helper()

	require.NoErrorf(t, store.Delete(key), "Delete(%q) must succeed", key)
}

// mustClearStore clears the store and panics if it fails.
func mustClearStore(t *testing.T, store Store) {
	t.Helper()

	require.NoError(t, store.Clear(), "Clear() must succeed")
}

// seedStorePrefixes seeds the store with prefixes and panics if it fails.
func seedStorePrefixes(t *testing.T, store Store, data []struct {
	prefix string
	count  int
},
) map[string][]string {
	t.Helper()

	prefixKeys := make(map[string][]string, len(data))

	for _, cfg := range data {
		for i := 1; i <= cfg.count; i++ {
			key := fmt.Sprintf("%s:%02d", cfg.prefix, i)
			value := fmt.Sprintf("%s-value-%02d", cfg.prefix, i)
			require.NoError(t, store.Set(key, value))
			prefixKeys[cfg.prefix] = append(prefixKeys[cfg.prefix], key)
		}
	}

	for prefix := range prefixKeys {
		sort.Strings(prefixKeys[prefix])
	}

	return prefixKeys
}

// mustScanStore scans the store and panics if it fails.
func mustScanStore(t *testing.T, store Store, prefix, after string, limit int64) *ScanPage {
	t.Helper()

	page, err := store.Scan(prefix, after, limit)
	require.NoError(t, err)

	return page
}

// mustListStore lists the store and panics if it fails.
func mustListStore(t *testing.T, store Store) []Entry {
	t.Helper()

	entries, err := store.List("", 0)
	require.NoError(t, err)

	return entries
}

// keysFromEntries returns the keys from a list of entries.
func keysFromEntries(entries []Entry) []string {
	keys := make([]string, len(entries))

	for i, entry := range entries {
		keys[i] = entry.Key
	}

	return keys
}

// valuesFromEntries returns the values from a list of entries.
func valuesFromEntries(entries []Entry) map[string]string {
	values := make(map[string]string, len(entries))

	for _, entry := range entries {
		values[entry.Key] = entryValueAsString(entry.Value)
	}

	return values
}

// collectScanEntries collects the entries from a scan and panics if it fails.
func collectScanEntries(t *testing.T, store Store, limit int64) []Entry {
	t.Helper()

	var (
		results []Entry
		after   string
	)

	for {
		page := mustScanStore(t, store, "", after, limit)
		results = append(results, page.Entries...)

		if page.NextKey == "" {
			break
		}

		after = page.NextKey
	}

	return results
}

// entryValueAsString returns the value of an entry as a string.
func entryValueAsString(value any) string {
	switch v := value.(type) {
	case []byte:
		return string(v)
	case string:
		return v
	default:
		return fmt.Sprintf("%v", v)
	}
}
