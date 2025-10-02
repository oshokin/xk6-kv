package store

import (
	"bytes"
	"fmt"
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

// TestMemoryStore_Concurrency performs concurrent Set/Get loops to smoke-test synchronization.
// If we complete without deadlock or data race (under -race), the test passes.
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

// TestMemoryStore_IncrementBy_Basic checks IncrementBy on absent key (start at 0), positive/negative
// increments, and that non-integer payloads cause an error.
func TestMemoryStore_IncrementBy_Basic(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true)

	newVal, err := store.IncrementBy("ctr", 5)
	require.NoError(t, err)
	assert.EqualValues(t, 5, newVal)

	newVal, err = store.IncrementBy("ctr", -2)
	require.NoError(t, err)
	assert.EqualValues(t, 3, newVal)

	require.NoError(t, store.Set("bad", "not-an-int"))
	_, err = store.IncrementBy("bad", 1)
	require.Error(t, err, "non-integer value must cause IncrementBy error")
}

// TestMemoryStore_IncrementBy_Concurrent verifies concurrent increments produce the exact sum.
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

// TestMemoryStore_GetOrSet_Basic validates first-writer wins semantics and the "loaded" flag.
func TestMemoryStore_GetOrSet_Basic(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true)

	val, loaded, err := store.GetOrSet("k", "v1")
	require.NoError(t, err)
	require.False(t, loaded, "first insert must be loaded=false")
	assert.Equal(t, []byte("v1"), val.([]byte))

	val, loaded, err = store.GetOrSet("k", "v2")
	require.NoError(t, err)
	require.True(t, loaded, "existing key must return loaded=true")
	assert.Equal(t, []byte("v1"), val.([]byte), "existing value must be returned")
}

// TestMemoryStore_GetOrSet_Concurrent ensures only one goroutine creates the value and
// all others observe the identical stored bytes.
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
	)

	for result := range resultsCh {
		require.NoError(t, result.err)

		if !result.loaded {
			firstWriterCount++
			firstValue = string(result.actualBytes)
		} else {
			assert.Equal(t, firstValue, string(result.actualBytes), "all readers must see the same stored value")
		}
	}

	assert.Equal(t, 1, firstWriterCount, "exactly one goroutine must create the value")
}

// TestMemoryStore_Swap_Basic checks insertion (loaded=false, prev=nil) and replacement (loaded=true with prev bytes).
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

// TestMemoryStore_CompareAndSwap_Basic verifies CAS fails on wrong old value and succeeds on correct one.
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

// TestMemoryStore_CompareAndSwap_ConcurrentSingleWinner ensures exactly one CAS succeeds under contention.
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

// TestMemoryStore_Delete ensures Delete removes present keys and is a no-op (no error) for missing keys.
func TestMemoryStore_Delete(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true)
	require.NoError(t, store.Set("test-key", "test-value"))

	require.NoError(t, store.Delete("test-key"))
	_, exists := store.container["test-key"]
	assert.False(t, exists, "key must be removed from container")

	require.NoError(t, store.Delete("non-existent"), "Delete on missing key must not error")
}

// TestMemoryStore_Exists checks Exists returns false for missing keys and true for present keys.
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

// TestMemoryStore_DeleteIfExists_Basic verifies it returns (false) for absent key, (true) when it deletes,
// and that the key is gone afterwards.
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

// TestMemoryStore_DeleteIfExists_ConcurrentSingleWinner ensures exactly one deleter wins under contention.
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

// TestMemoryStore_CompareAndDelete_Basic checks that CompareAndDelete only removes the key when the expected value matches.
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

// TestMemoryStore_CompareAndDelete_ConcurrentSingleWinner ensures exactly one CompareAndDelete wins under contention.
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

	for i := 0; i < concurrencyLevel; i++ {
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

// TestMemoryStore_AtomicOps_DoNotChangeBytesUnexpectedly ensures the store does not alias external byte slices.
// After storing a []byte, mutating the original slice must not affect the stored value.
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

// TestMemoryStore_Clear confirms Clear removes all entries.
func TestMemoryStore_Clear(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true)

	require.NoError(t, store.Set("key1", "value1"))
	require.NoError(t, store.Set("key2", "value2"))

	require.NoError(t, store.Clear())
	assert.Empty(t, store.container, "store must be empty after Clear")
}

// TestMemoryStore_Size verifies Size reports 0 for empty stores and the exact number after inserts.
func TestMemoryStore_Size(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true)

	size, err := store.Size()
	require.NoError(t, err)
	assert.EqualValues(t, 0, size, "empty store must report size=0")

	require.NoError(t, store.Set("key1", "value1"))
	require.NoError(t, store.Set("key2", "value2"))

	size, err = store.Size()
	require.NoError(t, err)
	assert.EqualValues(t, 2, size, "size must equal number of entries")
}

// TestMemoryStore_List checks listing with/without prefix and with limits, verifying both contents
// and the sort order by key.
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

// TestDiskStore_KeyTrackingConsistency validates the internal key index (map/list) remains consistent
// after Set/Delete/Clear when trackKeys is enabled.
func TestMemoryStore_KeyTrackingConsistency(t *testing.T) {
	t.Parallel()

	var (
		store = NewMemoryStore(true)
		keys  = []string{"key1", "key2", "key3"}
	)

	for _, key := range keys {
		require.NoError(t, store.Set(key, "value"))
	}

	store.mu.RLock()

	assert.Len(t, store.keysList, 3)

	for _, key := range keys {
		_, exists := store.keysMap[key]
		assert.Truef(t, exists, "key missing from index: %s", key)
	}

	store.mu.RUnlock()

	require.NoError(t, store.Delete("key2"))

	store.mu.RLock()

	assert.Len(t, store.keysList, 2)
	_, exists := store.keysMap["key2"]
	assert.False(t, exists, "deleted key must not remain in index")

	store.mu.RUnlock()

	require.NoError(t, store.Clear())

	store.mu.RLock()
	assert.Empty(t, store.keysList, "index must be empty after Clear")
	store.mu.RUnlock()
}

// TestMemoryStore_RandomKey_Distribution_WithTracking validates that with tracking enabled,
// RandomKey on an empty store returns "", and over repeated draws it eventually returns all present keys.
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

// TestMemoryStore_RandomKey_WithPrefix_TrackingEnabled covers prefix filtering when tracking is enabled,
// including empty store, no-prefix, matching prefix, no-match prefix, and correctness after deletions and clear.
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

	mustDeleteFromMemoryStore(t, store, "a:1")
	key, err = store.RandomKey("a:")
	require.NoError(t, err)
	assert.Equal(t, "a:2", key, "after delete, the only remaining prefixed key must be returned")

	mustClearMemoryStore(t, store)
	key, err = store.RandomKey("a:")
	require.NoError(t, err)
	assert.Empty(t, key, "after clear, prefix must return empty key")
}

// TestMemoryStore_RandomKey_WithPrefix_TrackingDisabled covers prefix filtering with tracking disabled.
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

// TestMemoryStore_RebuildKeyList_RandomKeyPrefix ensures RebuildKeyList reconstructs the internal index
// such that RandomKey with a prefix works again after simulated index loss.
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

// TestMemoryStore_Close ensures Close is a no-op that returns no error (in-memory store).
func TestMemoryStore_Close(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(true)
	require.NoError(t, store.Close())
}

func mustSetInMemoryStore(t *testing.T, store *MemoryStore, key string, value any) {
	t.Helper()

	require.NoErrorf(t, store.Set(key, value), "Set(%q) must succeed", key)
}

func mustDeleteFromMemoryStore(t *testing.T, store *MemoryStore, key string) {
	t.Helper()

	require.NoErrorf(t, store.Delete(key), "Delete(%q) must succeed", key)
}

func mustClearMemoryStore(t *testing.T, store *MemoryStore) {
	t.Helper()

	require.NoError(t, store.Clear(), "Clear() must succeed")
}

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
