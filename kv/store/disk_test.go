package store

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestDiskStore creates a temporary on-disk store bound to a provided path
// (or a temporary file when empty), optionally opens it, and registers cleanup.
// It returns the initialized store.
func newTestDiskStore(t *testing.T, trackKeys bool, path string, autoOpen bool) *DiskStore {
	t.Helper()

	diskPath := path
	if diskPath == "" {
		diskPath = filepath.Join(t.TempDir(), DefaultDiskStorePath)
	}

	store, err := NewDiskStore(trackKeys, diskPath)
	require.NoError(t, err, "NewDiskStore() must succeed for test path %q", diskPath)

	if autoOpen {
		require.NoError(t, store.Open())
	}

	t.Cleanup(func() {
		_ = store.Close()
	})

	return store
}

// TestNewDiskStore ensures NewDiskStore returns a non-nil, unopened store with the default path,
// a non-nil DB handle placeholder, and a zero reference count.
func TestNewDiskStore(t *testing.T) {
	t.Parallel()

	store, err := NewDiskStore(true, "")

	require.NoError(t, err, "NewDiskStore() must not fail with empty path")
	require.NotNil(t, store, "NewDiskStore() must not return nil")

	absDefault, pathErr := filepath.Abs(DefaultDiskStorePath)

	require.NoError(t, pathErr, "failed to resolve default disk path")
	assert.Equal(t, absDefault, store.path, "unexpected default path")
	require.NotNil(t, store.handle, "handle placeholder must be non-nil before open")

	assert.False(t, store.opened.Load(), "store must not be marked opened initially")
	assert.EqualValues(t, 0, store.refCount.Load(), "initial refCount must be zero")
}

// TestNewDiskStore_PathHandling ensures callers can override the disk path and that validation behaves as expected.
func TestNewDiskStore_PathHandling(t *testing.T) {
	t.Parallel()

	t.Run("custom path", func(t *testing.T) {
		t.Parallel()

		customPath := filepath.Join(t.TempDir(), "custom-db")
		store := newTestDiskStore(t, true, customPath, false)
		require.NotNil(t, store, "NewDiskStore() must not return nil")
		assert.Equal(t, customPath, store.path, "custom path must be honoured")
	})

	t.Run("empty path", func(t *testing.T) {
		t.Parallel()

		store, err := NewDiskStore(true, "")
		require.NoError(t, err, "NewDiskStore() must not fail when path is empty")
		require.NotNil(t, store, "NewDiskStore() must not return nil")

		absDefault, pathErr := filepath.Abs(DefaultDiskStorePath)
		require.NoError(t, pathErr, "failed to resolve default disk path")
		assert.Equal(t, absDefault, store.path, "empty path must fall back to default")
	})

	t.Run("directory path", func(t *testing.T) {
		t.Parallel()

		tempDir := t.TempDir()
		store, err := NewDiskStore(true, tempDir)
		require.ErrorContains(t, err, "is a directory", "directories must be rejected during validation")
		assert.Nil(t, store, "store must not be created when path is a directory")
	})

	t.Run("missing parent directory", func(t *testing.T) {
		t.Parallel()

		tempDir := t.TempDir()
		missingDir := filepath.Join(tempDir, "missing", "store.db")

		store := newTestDiskStore(t, true, missingDir, false)
		require.NotNil(t, store, "NewDiskStore() must not return nil")

		err := store.Open()
		require.NoError(t, err, "Open() must create missing parent directories")
		assert.Equal(t, missingDir, store.path, "path must remain unchanged (no fallback to default)")

		parentDir := filepath.Dir(missingDir)
		info, statErr := os.Stat(parentDir)
		require.NoError(t, statErr, "parent directory must exist after Open()")
		assert.True(t, info.IsDir(), "parent directory must be a directory")
	})
}

// TestDiskStore_Open_IdempotentAndConcurrent verifies that calling the internal open() concurrently
// is idempotent: the DB is opened once and concurrent Close() calls safely bring refCount back to zero.
func TestDiskStore_Open_IdempotentAndConcurrent(t *testing.T) {
	t.Parallel()

	const concurrencyLevel = 64

	var (
		store   = newTestDiskStore(t, true, "", false)
		errorCh = make(chan error, concurrencyLevel)
		wg      sync.WaitGroup
	)

	wg.Add(concurrencyLevel)

	// Open concurrently N times.
	for range concurrencyLevel {
		go func() {
			defer wg.Done()

			if err := store.Open(); err != nil {
				errorCh <- fmt.Errorf("open failed: %w", err)
			}
		}()
	}

	wg.Wait()
	close(errorCh)

	for err := range errorCh {
		require.NoError(t, err, "unexpected open error")
	}

	require.True(t, store.opened.Load(), "store must be marked opened")
	require.NotNil(t, store.handle, "handle must be non-nil after open")
	assert.EqualValues(t, concurrencyLevel, store.refCount.Load(), "refCount must equal number of open calls")

	// Close concurrently N times; end state must be fully closed (refCount == 0).
	errorCh = make(chan error, concurrencyLevel)
	wg = sync.WaitGroup{}
	wg.Add(concurrencyLevel)

	for range concurrencyLevel {
		go func() {
			defer wg.Done()

			if err := store.Close(); err != nil {
				errorCh <- fmt.Errorf("close failed: %w", err)
			}
		}()
	}

	wg.Wait()
	close(errorCh)

	for err := range errorCh {
		require.NoError(t, err, "unexpected close error")
	}

	assert.False(t, store.opened.Load(), "store must be closed after all closes")
	assert.EqualValues(t, 0, store.refCount.Load(), "refCount must be zero after all closes")
}

// TestDiskStore_ConcurrentOpen_DoesNotLeakRefCount verifies that concurrent API operations that
// auto-open the DB (here, Set) do not leak references and the refCount remains stable.
func TestDiskStore_ConcurrentOpen_DoesNotLeakRefCount(t *testing.T) {
	t.Parallel()

	const concurrencyLevel = 10

	var (
		store        = newTestDiskStore(t, true, "", true)
		startBarrier = make(chan struct{})
		errorCh      = make(chan error, concurrencyLevel)
		wg           sync.WaitGroup
	)

	wg.Add(concurrencyLevel)

	for i := range concurrencyLevel {
		go func(callerIndex int) {
			defer wg.Done()

			<-startBarrier

			if err := store.Set(fmt.Sprintf("key-%d", callerIndex), "value"); err != nil {
				errorCh <- err
			}
		}(i)
	}

	close(startBarrier)
	wg.Wait()
	close(errorCh)

	for err := range errorCh {
		require.NoError(t, err, "Set must not fail")
	}

	require.True(t, store.opened.Load(), "store must be opened after concurrent operations")
	assert.EqualValues(t, 1, store.refCount.Load(), "refCount must remain 1 regardless of callers")

	// Close once; store should fully close.
	require.NoError(t, store.Close())

	assert.EqualValues(t, 0, store.refCount.Load(), "refCount must be zero after close")
	assert.False(t, store.opened.Load(), "store must be closed after close")
}

// TestDiskStore_OpenClose_InterleavedRace stress-tests interleaved open()/Close() sequences from
// many goroutines to exercise locking and refcount logic under the race detector.
func TestDiskStore_OpenClose_InterleavedRace(t *testing.T) {
	t.Parallel()

	const (
		concurrencyLevel       = 16
		iterationsPerGoroutine = 200
	)

	var (
		store        = newTestDiskStore(t, true, "", false)
		startBarrier = make(chan struct{})
		errorCh      = make(chan error, concurrencyLevel*iterationsPerGoroutine*2)
		wg           sync.WaitGroup
	)

	wg.Add(concurrencyLevel)

	for range concurrencyLevel {
		go func() {
			defer wg.Done()

			<-startBarrier

			for range iterationsPerGoroutine {
				if err := store.Open(); err != nil {
					errorCh <- fmt.Errorf("open failed: %w", err)
					return
				}

				// Small jitter to diversify interleavings.
				time.Sleep(time.Microsecond)

				if err := store.Close(); err != nil {
					errorCh <- fmt.Errorf("close failed: %w", err)
					return
				}
			}
		}()
	}

	close(startBarrier)
	wg.Wait()
	close(errorCh)

	for err := range errorCh {
		require.NoError(t, err, "interleaved open/close must not fail")
	}

	// After matched open/close pairs, the store must be fully closed.
	assert.False(t, store.opened.Load(), "store should be closed")
	assert.EqualValues(t, 0, store.refCount.Load(), "refCount must be zero")

	// Extra Close() calls are harmless no-ops.
	require.NoError(t, store.Close(), "extra Close() must not error")
	assert.False(t, store.opened.Load(), "store must remain closed")
	assert.EqualValues(t, 0, store.refCount.Load(), "refCount must stay zero")
}

// TestDiskStore_ReopenAfterFullyClosed_OnDemand ensures a closed store rejects operations until
// Open is called again, and that Open reinitializes the handle.
func TestDiskStore_ReopenAfterFullyClosed_OnDemand(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)

	require.NoError(t, store.Set("key1", "value1"))

	require.NoError(t, store.Close(), "Close must succeed")

	require.False(t, store.opened.Load(), "store should be fully closed")
	require.EqualValues(t, 0, store.refCount.Load(), "refCount must be zero")

	err := store.Set("key2", "value2")
	require.Error(t, err, "operations must fail when store is closed")

	require.NoError(t, store.Open(), "Open must succeed after close")
	assert.True(t, store.opened.Load(), "store must be opened after reopen")

	require.NoError(t, store.Set("key3", "value3"))
	assert.EqualValues(t, 1, store.refCount.Load(), "refCount must be reset to 1 after reopen")
}

// TestDiskStore_GetSet_RoundtripAndTypes validates:
// 1) Get on a missing key returns an error;
// 2) string and []byte Set -> Get round-trip to the same bytes;
// 3) unsupported types cause an error on Set.
func TestDiskStore_GetSet_RoundtripAndTypes(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)

	// Missing key error.
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

	// Unsupported type should error.
	require.Error(t, store.Set("invalid-key", 123), "Set of unsupported type must error")
}

// TestDiskStore_Get_ReturnsDistinctBuffers ensures that Get returns distinct buffers for each call.
func TestDiskStore_Get_ReturnsDistinctBuffers(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	require.NoError(t, store.Set("value-key", "value"))

	firstAny, err := store.Get("value-key")
	require.NoError(t, err)

	secondAny, err := store.Get("value-key")
	require.NoError(t, err)

	first := firstAny.([]byte)
	second := secondAny.([]byte)

	require.Equal(t, []byte("value"), first)
	require.Equal(t, []byte("value"), second)

	if len(first) > 0 && len(second) > 0 {
		firstPtr := uintptr(unsafe.Pointer(&first[0]))
		secondPtr := uintptr(unsafe.Pointer(&second[0]))
		require.NotEqual(t, firstPtr, secondPtr, "Get must return freshly cloned buffers")
	}
}

func TestDiskStore_GetReturnsCopy(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	require.NoError(t, store.Set("k", "value"))

	raw, err := store.Get("k")
	require.NoError(t, err)

	buf := raw.([]byte)
	buf[0] = 'X'

	again, err := store.Get("k")
	require.NoError(t, err)
	assert.Equal(t, []byte("value"), again.([]byte), "mutating returned slice must not affect stored value")
}

// TestDiskStore_Get_IgnoresStaleIndex ensures Get falls back to Bolt when
// the tracking index temporarily misses a key.
func TestDiskStore_Get_IgnoresStaleIndex(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)

	const key = "user:42"
	require.NoError(t, store.Set(key, "value"))

	// Simulate a stale index (e.g., reader racing with a writer that
	// committed to Bolt but hasn't updated the in-memory map yet).
	store.keysLock.Lock()
	delete(store.keysMap, key)
	store.keysLock.Unlock()

	raw, err := store.Get(key)
	require.NoError(t, err, "Get must read through stale index")

	require.IsType(t, []byte{}, raw)
	assert.Equal(t, []byte("value"), raw.([]byte))
}

// TestDiskStore_Exists_IgnoresStaleIndex ensures Exists rechecks Bolt before
// returning false when the tracking map misses a key.
func TestDiskStore_Exists_IgnoresStaleIndex(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)

	const key = "user:exists"
	require.NoError(t, store.Set(key, "value"))

	// Simulate the index temporarily missing the key.
	store.keysLock.Lock()
	delete(store.keysMap, key)
	store.keysLock.Unlock()

	exists, err := store.Exists(key)
	require.NoError(t, err, "Exists must not fail when rebuilding view")
	assert.True(t, exists, "Exists must fall back to Bolt when index misses key")
}

// TestDiskStore_IncrementBy_Basic checks IncrementBy on absent key (start at 0), positive/negative
// increments, and that non-integer payloads cause an error.
func TestDiskStore_IncrementBy_Basic(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)

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

// TestDiskStore_IncrementBy_Concurrent verifies concurrent increments produce the exact sum.
func TestDiskStore_IncrementBy_Concurrent(t *testing.T) {
	t.Parallel()

	const (
		concurrencyLevel       = 1000
		delta            int64 = 1
	)

	var (
		store = newTestDiskStore(t, true, "", true)
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

// TestDiskStore_GetOrSet_Basic validates first-writer wins semantics and "loaded" flag.
func TestDiskStore_GetOrSet_Basic(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	value, loaded, err := store.GetOrSet("k", "v1")

	require.NoError(t, err)
	require.False(t, loaded, "first insert must be loaded=false")
	assert.Equal(t, []byte("v1"), value.([]byte))

	value, loaded, err = store.GetOrSet("k", "v2")

	require.NoError(t, err)
	require.True(t, loaded, "existing key must return loaded=true")
	assert.Equal(t, []byte("v1"), value.([]byte), "existing value must be returned")
}

// TestDiskStore_GetOrSet_Concurrent ensures only one goroutine creates the value and all others
// observe the very same stored bytes.
func TestDiskStore_GetOrSet_Concurrent(t *testing.T) {
	t.Parallel()

	const concurrencyLevel = 256

	type goroutineResult struct {
		actualBytes []byte
		loaded      bool
		err         error
	}

	var (
		store     = newTestDiskStore(t, true, "", true)
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

// TestDiskStore_Swap_Basic checks insertion (loaded=false, prev=nil) and replacement (loaded=true with prev bytes).
func TestDiskStore_Swap_Basic(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)

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

// TestDiskStore_CompareAndSwap_Basic verifies CAS fails on wrong old value and succeeds on correct one.
func TestDiskStore_CompareAndSwap_Basic(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
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

func TestDiskStore_CompareAndSwap_InsertWhenAbsent(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)

	ok, err := store.CompareAndSwap("lock", nil, "holder")
	require.NoError(t, err)
	assert.True(t, ok, "CAS should insert when expecting absence")

	got, err := store.Get("lock")
	require.NoError(t, err)
	assert.Equal(t, []byte("holder"), got.([]byte))

	ok, err = store.CompareAndSwap("lock", nil, "other")
	require.NoError(t, err)
	assert.False(t, ok, "subsequent CAS with nil expectation should fail")
}

// TestDiskStore_CompareAndSwap_ConcurrentSingleWinner ensures exactly one CAS succeeds under contention.
func TestDiskStore_CompareAndSwap_ConcurrentSingleWinner(t *testing.T) {
	t.Parallel()

	const concurrencyLevel = 200

	var (
		store = newTestDiskStore(t, true, "", true)
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

// TestDiskStore_Delete verifies Delete succeeds for present keys and is a no-op (no error) for missing keys.
func TestDiskStore_Delete(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	require.NoError(t, store.Set("test-key", "test-value"))

	require.NoError(t, store.Delete("test-key"))

	exists, err := store.Exists("test-key")
	require.NoError(t, err)
	assert.False(t, exists, "key should be removed")

	require.NoError(t, store.Delete("non-existent"), "Delete on missing key must not error")
}

// TestDiskStore_Exists checks Exists returns false for missing keys and true for present keys.
func TestDiskStore_Exists(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)

	exists, err := store.Exists("non-existent")
	require.NoError(t, err)
	assert.False(t, exists)

	require.NoError(t, store.Set("test-key", "test-value"))

	exists, err = store.Exists("test-key")
	require.NoError(t, err)
	assert.True(t, exists)
}

// TestDiskStore_DeleteIfExists_Basic verifies it returns "false" for absent key,
// "true" when it deletes, and the key is gone.
func TestDiskStore_DeleteIfExists_Basic(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)

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

// TestDiskStore_DeleteIfExists_ConcurrentSingleWinner ensures exactly one deleter wins under contention.
func TestDiskStore_DeleteIfExists_ConcurrentSingleWinner(t *testing.T) {
	t.Parallel()

	const concurrencyLevel = 128

	var (
		store = newTestDiskStore(t, true, "", true)
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

// TestDiskStore_CompareAndDelete_Basic checks that CompareAndDelete only removes the key when the expected value matches.
func TestDiskStore_CompareAndDelete_Basic(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
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

// TestDiskStore_CompareAndDelete_ConcurrentSingleWinner ensures exactly one CompareAndDelete wins under contention.
func TestDiskStore_CompareAndDelete_ConcurrentSingleWinner(t *testing.T) {
	t.Parallel()

	const concurrencyLevel = 120

	var (
		store        = newTestDiskStore(t, true, "", true)
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

// TestDiskStore_AtomicOps_DoNotChangeBytesUnexpectedly ensures the store does not alias external byte slices.
// After storing a []byte, mutating the original slice must not affect the stored value.
func TestDiskStore_AtomicOps_DoNotChangeBytesUnexpectedly(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	originalBytes := []byte("payload")

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

// TestDiskStore_Clear confirms Clear removes all entries and Size() returns zero afterwards.
func TestDiskStore_Clear(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)

	require.NoError(t, store.Set("key1", "value1"))
	require.NoError(t, store.Set("key2", "value2"))

	require.NoError(t, store.Clear())

	size, err := store.Size()
	require.NoError(t, err)
	assert.EqualValues(t, 0, size, "store must be empty after Clear")
}

// TestDiskStore_Size verifies Size reports 0 for empty stores and the exact number after inserts.
func TestDiskStore_Size(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)

	size, err := store.Size()
	require.NoError(t, err)
	assert.EqualValues(t, 0, size, "empty store must report size=0")

	require.NoError(t, store.Set("key1", "value1"))
	require.NoError(t, store.Set("key2", "value2"))

	size, err = store.Size()
	require.NoError(t, err)
	assert.EqualValues(t, 2, size, "size must equal number of entries")
}

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

func TestDiskStore_ScanReturnsCopy(t *testing.T) {
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
			assert.Equal(t, prefixKeys["beta"][3:], keysFromEntries(page.Entries))
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
				keysFromEntries(listAfterDelete),
				keysFromEntries(scanAfterDelete),
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

// TestDiskStore_List checks listing with/without prefix and with limits, verifying returned keys.
func TestDiskStore_List(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)

	entries, err := store.List("", 0)
	require.NoError(t, err)
	assert.Empty(t, entries, "empty store must list zero entries")

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

	entries, err = store.List("", 0)
	require.NoError(t, err)
	require.Len(t, entries, len(testData))

	keyMap := make(map[string]bool, len(entries))
	for _, entry := range entries {
		keyMap[entry.Key] = true
	}

	for key := range testData {
		assert.Truef(t, keyMap[key], "missing key in List: %s", key)
	}

	entries, err = store.List("prefix", 0)
	require.NoError(t, err)
	assert.Len(t, entries, 2)

	for _, entry := range entries {
		assert.Truef(t, strings.HasPrefix(entry.Key, "prefix"), "unexpected key without prefix: %s", entry.Key)
	}

	entries, err = store.List("", 2)
	require.NoError(t, err)
	assert.Len(t, entries, 2)

	entries, err = store.List("prefix", 1)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.True(t, strings.HasPrefix(entries[0].Key, "prefix"))
}

// TestDiskStore_KeyTrackingConsistency validates the internal key index (map/list) remains consistent
// after Set/Delete/Clear when trackKeys is enabled.
func TestDiskStore_KeyTrackingConsistency(t *testing.T) {
	t.Parallel()

	var (
		store = newTestDiskStore(t, true, "", true)
		keys  = []string{"key1", "key2", "key3"}
	)

	for _, key := range keys {
		require.NoError(t, store.Set(key, "value"))
	}

	store.keysLock.RLock()

	assert.Len(t, store.keysList, 3)

	for _, key := range keys {
		_, exists := store.keysMap[key]
		assert.Truef(t, exists, "key missing from index: %s", key)
	}

	store.keysLock.RUnlock()

	require.NoError(t, store.Delete("key2"))

	store.keysLock.RLock()

	assert.Len(t, store.keysList, 2)
	_, exists := store.keysMap["key2"]
	assert.False(t, exists, "deleted key must not remain in index")

	store.keysLock.RUnlock()

	require.NoError(t, store.Clear())

	store.keysLock.RLock()
	assert.Empty(t, store.keysList, "index must be empty after Clear")
	store.keysLock.RUnlock()
}

// TestDiskStore_RebuildKeyList ensures RebuildKeyList reconstructs the in-memory index from storage.
func TestDiskStore_RebuildKeyList(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)

	require.NoError(t, store.Set("key1", "value1"))
	require.NoError(t, store.Set("key2", "value2"))

	// Corrupt the in-memory index to simulate loss.
	store.keysLock.Lock()
	store.keysMap = make(map[string]int)
	store.keysList = []string{}
	store.keysLock.Unlock()

	require.NoError(t, store.RebuildKeyList(), "RebuildKeyList must succeed")

	store.keysLock.RLock()
	defer store.keysLock.RUnlock()

	assert.Len(t, store.keysList, 2)

	expected := map[string]bool{"key1": true, "key2": true}
	for _, key := range store.keysList {
		assert.Truef(t, expected[key], "unexpected key in index: %s", key)
	}
}

// TestDiskStore_RandomKey_WithTracking validates that with tracking enabled, RandomKey on an empty
// store returns "", and over repeated draws it eventually returns all present keys (sanity check).
func TestDiskStore_RandomKey_WithTracking(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)

	key, err := store.RandomKey("")
	require.NoError(t, err)
	assert.Empty(t, key, "empty store must return empty key")

	keys := []string{"key1", "key2", "key3"}
	for _, k := range keys {
		require.NoError(t, store.Set(k, "value"))
	}

	found := make(map[string]bool)

	for range 50 {
		key, err := store.RandomKey("")
		require.NoError(t, err)

		found[key] = true
	}

	for _, k := range keys {
		assert.Truef(t, found[k], "key not observed in random selections: %s", k)
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

	for _, k := range []string{"key1", "key2", "key3"} {
		require.NoError(t, store.Set(k, "value"))
	}

	key, err = store.RandomKey("")
	require.NoError(t, err)
	assert.NotEmpty(t, key, "non-empty store must return some key")
}

// TestDiskStore_RandomKey_ConcurrentOperations performs a mixed workload (Set/Get/Exists/RandomKey/Delete)
// concurrently to smoke-test synchronization with randomKey in the mix.
func TestDiskStore_RandomKey_ConcurrentOperations(t *testing.T) {
	t.Parallel()

	var (
		store = newTestDiskStore(t, true, "", true)
		wg    sync.WaitGroup
	)

	for i := range 1000 {
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

	require.NoError(t, store.Set("a:1", "v1"))
	require.NoError(t, store.Set("a:2", "v2"))
	require.NoError(t, store.Set("b:1", "v3"))

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

	require.NoError(t, store.Delete("a:1"))

	key, err = store.RandomKey("a:")
	require.NoError(t, err)
	assert.Equal(t, "a:2", key, "after delete, the only remaining prefixed key must be returned")
}

// TestDiskStore_RandomKey_WithPrefix_TrackingDisabled covers prefix filtering with tracking disabled.
func TestDiskStore_RandomKey_WithPrefix_TrackingDisabled(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, false, "", true)

	key, err := store.RandomKey("a:")
	require.NoError(t, err)
	assert.Empty(t, key, "empty store must return empty key")

	require.NoError(t, store.Set("a:1", "v1"))
	require.NoError(t, store.Set("a:2", "v2"))
	require.NoError(t, store.Set("b:1", "v3"))

	key, err = store.RandomKey("a:")
	require.NoError(t, err)
	require.NotEmpty(t, key)
	assert.True(t, strings.HasPrefix(key, "a:"), "must return key with prefix a:")

	key, err = store.RandomKey("z:")
	require.NoError(t, err)
	assert.Empty(t, key, "no-match prefix must return empty key")
}

// TestDiskStore_Close ensures Close marks the store as closed after at least one operation opened it.
func TestDiskStore_Close(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	require.NoError(t, store.Set("key", "value"))

	require.NoError(t, store.Close())
	assert.False(t, store.opened.Load(), "store must be marked closed")
}

// TestDiskStore_Close_ManyConcurrentClosersFromSingleOpen verifies that multiple concurrent closers
// after a single open are safe: one closes, others become harmless no-ops.
func TestDiskStore_Close_ManyConcurrentClosersFromSingleOpen(t *testing.T) {
	t.Parallel()

	const closers = 32

	store := newTestDiskStore(t, true, "", false)

	require.NoError(t, store.Open())
	require.True(t, store.opened.Load(), "store must be opened after open()")
	assert.EqualValues(t, 1, store.refCount.Load(), "refCount must be 1 after single open")

	var (
		errorCh = make(chan error, closers)
		wg      sync.WaitGroup
	)

	wg.Add(closers)

	for range closers {
		go func() {
			defer wg.Done()

			if err := store.Close(); err != nil {
				errorCh <- err
			}
		}()
	}

	wg.Wait()
	close(errorCh)

	for err := range errorCh {
		require.NoError(t, err, "concurrent close must not error")
	}

	assert.False(t, store.opened.Load(), "store must be closed")
	assert.EqualValues(t, 0, store.refCount.Load(), "refCount must be zero after close storm")
}

// TestDiskStore_RefCountIgnoresOperationCount ensures operations do not bump the refcount
// beyond 1 and Close fully shuts the store down.
func TestDiskStore_RefCountIgnoresOperationCount(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", false)
	require.NoError(t, store.Open())
	assert.EqualValues(t, 1, store.refCount.Load(), "Open must set refCount=1")

	require.NoError(t, store.Set("key", "value"))
	assert.EqualValues(t, 1, store.refCount.Load(), "mutating ops must not change refCount")

	_, err := store.Get("key")
	require.NoError(t, err)
	assert.EqualValues(t, 1, store.refCount.Load(), "refCount must remain 1")

	require.NoError(t, store.Close())
	assert.EqualValues(t, 0, store.refCount.Load(), "Close must drop refCount to 0")
	assert.False(t, store.opened.Load(), "store must be closed")

	require.NoError(t, store.Close())
	assert.EqualValues(t, 0, store.refCount.Load(), "additional Close must keep refCount at 0")
	assert.False(t, store.opened.Load(), "store must remain closed")
}

// TestDiskStore_GetOrSet_Delete_Interleave_NoPanic exercises a hot concurrency
// path that previously exposed a "slice bounds out of range [:-1]" panic in
// key-tracking code (when removing from an empty slice). While the panic was
// observed on the memory backend originally, we also keep an equivalent test
// for the disk backend to ensure uniform safety across implementations.
// The success criterion is simple: the test must complete without ANY panic.
func TestDiskStore_GetOrSet_Delete_Interleave_NoPanic(t *testing.T) {
	t.Parallel()

	const (
		testKey         = "order-new"
		iterationsCount = 5_000
		keysCount       = 100
	)

	var (
		store     = newTestDiskStore(t, true, "", true)
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
