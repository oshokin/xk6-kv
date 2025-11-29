package store

import (
	"bytes"
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

	store := NewMemoryStore(true, 0)

	require.NotNil(t, store, "NewMemoryStore() must not return nil")
	require.NotNil(t, store.shards, "shards slice must be allocated")
	require.Len(t, store.shards, store.shardCount, "shards slice must match shardCount")

	firstShard := store.shards[0]
	require.NotNil(t, firstShard.container, "shard container map must be allocated")
	assert.Empty(t, firstShard.container, "new store must be empty")
}

// TestMemoryStore_Get_ReturnsCopy ensures Get callers cannot mutate stored bytes.
func TestMemoryStore_Get_ReturnsCopy(t *testing.T) {
	t.Parallel()

	const key = "immutable"

	store := NewMemoryStore(false, 0)

	require.NoError(t, store.Set(key, []byte("original")))

	gotAny, err := store.Get(key)
	require.NoError(t, err)

	gotBytes := gotAny.([]byte)

	// Mutate returned slice, store should stay untouched.
	gotBytes[0] = 'X'

	nextAny, err := store.Get(key)
	require.NoError(t, err)

	assert.Equal(t, []byte("original"), nextAny.([]byte))
}

// TestMemoryStore_GetSet_RoundtripAndTypes validates:
//  1. Get on a missing key returns an error;
//  2. string and []byte Set -> Get round-trip to the same bytes;
//  3. unsupported types cause an error on Set.
func TestMemoryStore_GetSet_RoundtripAndTypes(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true, 0)

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

// TestMemoryStore_GetSet_Concurrency performs concurrent Set/Get loops to smoke-test
// synchronization. Passes if no deadlock or data race is detected (under -race).
func TestMemoryStore_GetSet_Concurrency(t *testing.T) {
	t.Parallel()

	var (
		store = NewMemoryStore(true, 0)
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

	store := NewMemoryStore(true, 0)

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
		store = NewMemoryStore(true, 0)
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

	store := NewMemoryStore(true, 0)
	value, loaded, err := store.GetOrSet("k", "v1")

	require.NoError(t, err)
	require.False(t, loaded, "first insert must be loaded=false")
	assert.Equal(t, []byte("v1"), value.([]byte))

	value, loaded, err = store.GetOrSet("k", "v2")

	require.NoError(t, err)
	require.True(t, loaded, "existing key must return loaded=true")
	assert.Equal(t, []byte("v1"), value.([]byte), "existing value must be returned")
}

// TestMemoryStore_GetOrSet_ReturnsCopy ensures callers cannot mutate stored values.
func TestMemoryStore_GetOrSet_ReturnsCopy(t *testing.T) {
	t.Parallel()

	const key = "copy-key"

	store := NewMemoryStore(false, 0)
	require.NoError(t, store.Set(key, []byte("persisted")))

	actual, loaded, err := store.GetOrSet(key, "ignored")
	require.NoError(t, err)
	require.True(t, loaded)

	actualBytes := actual.([]byte)
	actualBytes[0] = 'X'

	next, err := store.Get(key)
	require.NoError(t, err)
	assert.Equal(t, []byte("persisted"), next.([]byte))
}

// TestMemoryStore_GetOrSet_InsertReturnsCopy ensures the inserted value is not exposed.
func TestMemoryStore_GetOrSet_InsertReturnsCopy(t *testing.T) {
	t.Parallel()

	const key = "insert-copy"

	store := NewMemoryStore(true, 0)

	actual, loaded, err := store.GetOrSet(key, []byte("fresh"))
	require.NoError(t, err)
	require.False(t, loaded)

	actualBytes := actual.([]byte)
	actualBytes[0] = 'X'

	next, err := store.Get(key)
	require.NoError(t, err)
	assert.Equal(t, []byte("fresh"), next.([]byte))
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
		store     = NewMemoryStore(true, 0)
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
		store     = NewMemoryStore(true, 0)
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

// TestMemoryStore_Swap_Basic verifies Swap returns (nil, false) for new keys,
// and (previous_value, true) when replacing existing keys.
func TestMemoryStore_Swap_Basic(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true, 0)

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

	store := NewMemoryStore(true, 0)
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

	store := NewMemoryStore(true, 0)

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
		store = NewMemoryStore(true, 0)
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

	store := NewMemoryStore(true, 0)
	require.NoError(t, store.Set("test-key", "test-value"))

	require.NoError(t, store.Delete("test-key"))

	shard := store.getShardByKey("test-key")
	shard.mu.RLock()
	_, exists := shard.container["test-key"]
	shard.mu.RUnlock()

	assert.False(t, exists, "key must be removed from container")

	require.NoError(t, store.Delete("non-existent"), "Delete on missing key must not error")
}

// TestMemoryStore_Exists verifies Exists correctly returns false for missing keys
// and true for keys that are present in the store.
func TestMemoryStore_Exists(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true, 0)

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

	store := NewMemoryStore(true, 0)

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
		store = NewMemoryStore(true, 0)
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

	store := NewMemoryStore(true, 0)
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
		store        = NewMemoryStore(true, 0)
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
		store         = NewMemoryStore(true, 0)
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

	store := NewMemoryStore(true, 0)

	entries := []struct{ key, value string }{
		{"key1", "value1"},
		{"key2", "value2"},
	}

	for _, e := range entries {
		require.NoError(t, store.Set(e.key, e.value))
	}

	require.NoError(t, store.Clear())
	assert.Equal(t, 0, totalKeysInStore(store), "store must be empty after Clear")
}

// TestMemoryStore_Size verifies Size returns 0 for empty stores and accurately
// reports the number of entries after insertions.
func TestMemoryStore_Size(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true, 0)

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

			store := NewMemoryStore(trackKeys, 0)
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
			assert.Equal(t, prefixKeys["beta"][3:], keysFromEntries(t, page.Entries))
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
				keysFromEntries(t, listAfterDelete),
				keysFromEntries(t, scanAfterDelete),
				"Scan/List keys must match after deletes",
			)
		})
	}
}

// TestMemoryStore_List verifies List returns entries in lexicographic order,
// correctly applies prefix filters and limits, and handles empty stores.
func TestMemoryStore_List(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true, 0)

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

	store := NewMemoryStore(true, 0)
	keys := []string{"key1", "key2", "key3"}

	for _, key := range keys {
		require.NoError(t, store.Set(key, "value"))
	}

	assert.Equal(t, len(keys), totalTrackedKeys(store))

	for _, key := range keys {
		assert.Truef(t, keyTrackedInStore(store, key), "key missing from index: %s", key)
	}

	require.NoError(t, store.Delete(keys[1]))

	assert.Equal(t, len(keys)-1, totalTrackedKeys(store))
	assert.False(t, keyTrackedInStore(store, keys[1]), "deleted key must not remain in index")

	require.NoError(t, store.Clear())

	assert.Equal(t, 0, totalTrackedKeys(store), "index must be empty after Clear")
}

// TestMemoryStore_RebuildKeyList_RandomKeyPrefix verifies RebuildKeyList correctly
// reconstructs the internal index, restoring RandomKey functionality with prefixes.
func TestMemoryStore_RebuildKeyList_RandomKeyPrefix(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true, 0)

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

	store := NewMemoryStore(true, 0)
	require.NoError(t, store.Close())
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

func totalKeysInStore(store *MemoryStore) int {
	var total int

	for _, shard := range store.shards {
		shard.mu.RLock()
		total += len(shard.container)
		shard.mu.RUnlock()
	}

	return total
}

func totalTrackedKeys(store *MemoryStore) int {
	var total int

	for _, shard := range store.shards {
		shard.mu.RLock()
		total += len(shard.keysList)
		shard.mu.RUnlock()
	}

	return total
}

func keyTrackedInStore(store *MemoryStore, key string) bool {
	shard := store.getShardByKey(key)

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	if shard.keysMap == nil {
		return false
	}

	_, exists := shard.keysMap[key]

	return exists
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

// mustListStore lists the store and panics if it fails.
func mustListStore(t *testing.T, store Store) []Entry {
	t.Helper()

	entries, err := store.List("", 0)
	require.NoError(t, err)

	return entries
}

// keysFromEntries returns the keys from a list of entries.
func keysFromEntries(t *testing.T, entries []Entry) []string {
	t.Helper()

	keys := make([]string, len(entries))

	for i, entry := range entries {
		keys[i] = entry.Key
	}

	return keys
}

// valuesFromEntries returns the values from a list of entries.
func valuesFromEntries(t *testing.T, entries []Entry) map[string]string {
	t.Helper()

	values := make(map[string]string, len(entries))

	for _, entry := range entries {
		values[entry.Key] = entryValueAsString(t, entry.Value)
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
func entryValueAsString(t *testing.T, value any) string {
	t.Helper()

	switch v := value.(type) {
	case []byte:
		return string(v)
	case string:
		return v
	default:
		return fmt.Sprintf("%v", v)
	}
}
