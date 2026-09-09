package store

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func runNextCircularStoreTest(t *testing.T, fn func(t *testing.T, s Store)) {
	t.Helper()

	for _, factory := range claimForOwnerFactories() {
		t.Run(factory.name, func(t *testing.T) {
			t.Parallel()

			fn(t, factory.newStore(t))
		})
	}
}

func seedNextCircularValues(t *testing.T, s Store, keys ...string) {
	t.Helper()

	for _, key := range keys {
		require.NoErrorf(t, s.Set(key, []byte(key)), "Set(%q) must succeed", key)
	}
}

func requireNextCircular(t *testing.T, s Store, prefix string) *Entry {
	t.Helper()

	entry, err := s.NextCircular(prefix)
	require.NoError(t, err)
	require.NotNil(t, entry)

	return entry
}

func requireNextCircularKey(t *testing.T, s Store, prefix, expected string) {
	t.Helper()

	entry := requireNextCircular(t, s, prefix)
	assert.Equal(t, expected, entry.Key)
}

func requireNextCircularStringValue(t *testing.T, entry *Entry, expected string) {
	t.Helper()

	require.NotNil(t, entry)

	valueBytes, ok := entry.Value.([]byte)
	require.Truef(t, ok, "expected []byte value, got %T", entry.Value)
	assert.Equal(t, []byte(expected), valueBytes)
}

func TestStore_NextCircular_EmptyReturnsNil(t *testing.T) {
	t.Parallel()

	runNextCircularStoreTest(t, func(t *testing.T, s Store) {
		t.Helper()

		entry, err := s.NextCircular("")
		require.NoError(t, err)
		assert.Nil(t, entry)
	})
}

func TestStore_NextCircular_OneKeyRepeats(t *testing.T) {
	t.Parallel()

	runNextCircularStoreTest(t, func(t *testing.T, s Store) {
		t.Helper()

		seedNextCircularValues(t, s, "data:0001")

		for range 5 {
			requireNextCircularKey(t, s, "data:", "data:0001")
		}
	})
}

func TestStore_NextCircular_BasicWrapLexicographicOrder(t *testing.T) {
	t.Parallel()

	runNextCircularStoreTest(t, func(t *testing.T, s Store) {
		t.Helper()

		seedNextCircularValues(t, s, "item:2", "item:10", "item:1")

		expected := []string{
			"item:1",
			"item:10",
			"item:2",
			"item:1",
			"item:10",
			"item:2",
		}

		for _, key := range expected {
			requireNextCircularKey(t, s, "item:", key)
		}
	})
}

func TestStore_NextCircular_ZeroPaddedNumericOrder(t *testing.T) {
	t.Parallel()

	runNextCircularStoreTest(t, func(t *testing.T, s Store) {
		t.Helper()

		seedNextCircularValues(t, s, "data:000003", "data:000001", "data:000002")

		expected := []string{
			"data:000001",
			"data:000002",
			"data:000003",
			"data:000001",
		}

		for _, key := range expected {
			requireNextCircularKey(t, s, "data:", key)
		}
	})
}

func TestStore_NextCircular_PrefixFiltering(t *testing.T) {
	t.Parallel()

	runNextCircularStoreTest(t, func(t *testing.T, s Store) {
		t.Helper()

		seedNextCircularValues(t, s, "users:0001", "users:0002", "orders:0001")

		for range 4 {
			entry := requireNextCircular(t, s, "users:")
			assert.True(t, strings.HasPrefix(entry.Key, "users:"))
		}
	})
}

func TestStore_NextCircular_OverlappingPrefixesHaveIndependentCursors(t *testing.T) {
	t.Parallel()

	runNextCircularStoreTest(t, func(t *testing.T, s Store) {
		t.Helper()

		seedNextCircularValues(t, s, "user:a1", "user:a2", "user:b1")

		requireNextCircularKey(t, s, "user:", "user:a1")
		requireNextCircularKey(t, s, "user:", "user:a2")
		requireNextCircularKey(t, s, "user:a", "user:a1")
	})
}

func TestStore_NextCircular_EmptyPrefixHasOwnCursor(t *testing.T) {
	t.Parallel()

	runNextCircularStoreTest(t, func(t *testing.T, s Store) {
		t.Helper()

		seedNextCircularValues(t, s, "b", "a")

		requireNextCircularKey(t, s, "", "a")
		requireNextCircularKey(t, s, "", "b")
		requireNextCircularKey(t, s, "", "a")
	})
}

func TestStore_NextCircular_IgnoresLiveClaims(t *testing.T) {
	t.Parallel()

	runNextCircularStoreTest(t, func(t *testing.T, s Store) {
		t.Helper()

		seedNextCircularValues(t, s, "users:0001", "users:0002")

		claim, err := s.ClaimKey("users:0001", &ClaimOptions{TTLMs: 60_000})
		require.NoError(t, err)
		require.NotNil(t, claim)

		entry := requireNextCircular(t, s, "users:")
		assert.Equal(t, "users:0001", entry.Key)
	})
}

func TestStore_NextCircular_DoesNotShareStateWithClaimNext(t *testing.T) {
	t.Parallel()

	runNextCircularStoreTest(t, func(t *testing.T, s Store) {
		t.Helper()

		seedNextCircularValues(t, s, "data:0001", "data:0002")

		requireNextCircularKey(t, s, "data:", "data:0001")

		claim, err := s.ClaimNext(&ClaimOptions{
			Prefix: "data:",
			TTLMs:  60_000,
		})
		require.NoError(t, err)
		require.NotNil(t, claim)
		assert.Equal(t, "data:0001", claim.Key)

		requireNextCircularKey(t, s, "data:", "data:0002")
		requireNextCircularKey(t, s, "data:", "data:0001")
	})
}

func TestStore_NextCircular_ReturnsCurrentValueSnapshot(t *testing.T) {
	t.Parallel()

	runNextCircularStoreTest(t, func(t *testing.T, s Store) {
		t.Helper()

		require.NoError(t, s.Set("a", []byte("old")))
		require.NoError(t, s.Set("b", []byte("other")))

		first := requireNextCircular(t, s, "")
		assert.Equal(t, "a", first.Key)
		requireNextCircularStringValue(t, first, "old")

		require.NoError(t, s.Set("a", []byte("new")))

		second := requireNextCircular(t, s, "")
		assert.Equal(t, "b", second.Key)

		third := requireNextCircular(t, s, "")
		assert.Equal(t, "a", third.Key)
		requireNextCircularStringValue(t, third, "new")
	})
}

func TestStore_NextCircular_DeleteLastKeyUsesStrictlyGreaterResume(t *testing.T) {
	t.Parallel()

	runNextCircularStoreTest(t, func(t *testing.T, s Store) {
		t.Helper()

		seedNextCircularValues(t, s, "a", "b", "c")

		requireNextCircularKey(t, s, "", "a")
		requireNextCircularKey(t, s, "", "b")

		require.NoError(t, s.Delete("b"))

		requireNextCircularKey(t, s, "", "c")
		requireNextCircularKey(t, s, "", "a")
	})
}

func TestStore_NextCircular_InsertAfterCursorAppearsBeforeWrap(t *testing.T) {
	t.Parallel()

	runNextCircularStoreTest(t, func(t *testing.T, s Store) {
		t.Helper()

		seedNextCircularValues(t, s, "a", "c")

		requireNextCircularKey(t, s, "", "a")
		require.NoError(t, s.Set("b", []byte("b")))

		requireNextCircularKey(t, s, "", "b")
		requireNextCircularKey(t, s, "", "c")
		requireNextCircularKey(t, s, "", "a")
	})
}

func TestStore_NextCircular_InsertBeforeCursorAppearsAfterWrap(t *testing.T) {
	t.Parallel()

	runNextCircularStoreTest(t, func(t *testing.T, s Store) {
		t.Helper()

		seedNextCircularValues(t, s, "b", "c")

		requireNextCircularKey(t, s, "", "b")
		require.NoError(t, s.Set("a", []byte("a")))

		requireNextCircularKey(t, s, "", "c")
		requireNextCircularKey(t, s, "", "a")
		requireNextCircularKey(t, s, "", "b")
	})
}

func TestStore_NextCircular_ObservedEmptyResetsPosition(t *testing.T) {
	t.Parallel()

	runNextCircularStoreTest(t, func(t *testing.T, s Store) {
		t.Helper()

		seedNextCircularValues(t, s, "a", "b")

		requireNextCircularKey(t, s, "", "a")
		requireNextCircularKey(t, s, "", "b")

		require.NoError(t, s.Delete("a"))
		require.NoError(t, s.Delete("b"))

		entry, err := s.NextCircular("")
		require.NoError(t, err)
		require.Nil(t, entry)

		require.NoError(t, s.Set("a", []byte("a")))
		require.NoError(t, s.Set("b", []byte("b")))

		requireNextCircularKey(t, s, "", "a")
	})
}

func TestStore_NextCircular_ClearResetsPosition(t *testing.T) {
	t.Parallel()

	runNextCircularStoreTest(t, func(t *testing.T, s Store) {
		t.Helper()

		seedNextCircularValues(t, s, "a", "b", "c")

		requireNextCircularKey(t, s, "", "a")
		requireNextCircularKey(t, s, "", "b")

		require.NoError(t, s.Clear())
		seedNextCircularValues(t, s, "a", "b", "c")

		requireNextCircularKey(t, s, "", "a")
	})
}

func TestStore_NextCircular_RestoreResetsPosition(t *testing.T) {
	t.Parallel()

	runNextCircularStoreTest(t, func(t *testing.T, s Store) {
		t.Helper()

		seedNextCircularValues(t, s, "a", "b", "c")
		snapshotPath := filepath.Join(t.TempDir(), "circular-restore.kv")

		_, err := s.Backup(&BackupOptions{FileName: snapshotPath})
		require.NoError(t, err)

		requireNextCircularKey(t, s, "", "a")
		requireNextCircularKey(t, s, "", "b")

		_, err = s.Restore(&RestoreOptions{FileName: snapshotPath})
		require.NoError(t, err)

		requireNextCircularKey(t, s, "", "a")
	})
}

func TestDiskStore_NextCircular_NewInstanceStartsFromBeginning(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run(fmt.Sprintf("trackKeys=%t", trackKeys), func(t *testing.T) {
			t.Parallel()

			dbPath := filepath.Join(t.TempDir(), fmt.Sprintf("circular-%t.db", trackKeys))

			first, err := NewDiskStore(trackKeys, dbPath, nil)
			require.NoError(t, err)
			require.NoError(t, first.Open())

			seedNextCircularValues(t, first, "a", "b", "c")
			requireNextCircularKey(t, first, "", "a")
			require.NoError(t, first.Close())

			second, err := NewDiskStore(trackKeys, dbPath, nil)
			require.NoError(t, err)
			require.NoError(t, second.Open())
			t.Cleanup(func() {
				_ = second.Close()
			})

			requireNextCircularKey(t, second, "", "a")
		})
	}
}

func TestDiskStore_NextCircular_ReadOnlySupported(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run(fmt.Sprintf("trackKeys=%t", trackKeys), func(t *testing.T) {
			t.Parallel()

			dbPath := filepath.Join(t.TempDir(), fmt.Sprintf("readonly-circular-%t.db", trackKeys))
			writable := newTestDiskStore(t, trackKeys, dbPath, true)
			seedNextCircularValues(t, writable, "seed:1", "seed:2")
			require.NoError(t, writable.Close())

			readOnly, err := NewDiskStore(trackKeys, dbPath, &DiskConfig{
				ReadOnly: GetComparablePointer(true),
			})
			require.NoError(t, err)
			require.NoError(t, readOnly.Open())
			t.Cleanup(func() {
				_ = readOnly.Close()
			})

			requireNextCircularKey(t, readOnly, "seed:", "seed:1")
			requireNextCircularKey(t, readOnly, "seed:", "seed:2")
			requireNextCircularKey(t, readOnly, "seed:", "seed:1")
		})
	}
}

func TestStore_NextCircular_ConcurrentStaticDatasetDistribution(t *testing.T) {
	t.Parallel()

	runNextCircularStoreTest(t, func(t *testing.T, s Store) {
		t.Helper()

		const (
			totalKeys = 20
			callers   = 60
		)

		for index := 1; index <= totalKeys; index++ {
			key := fmt.Sprintf("data:%06d", index)
			require.NoError(t, s.Set(key, []byte(key)))
		}

		var (
			wg   sync.WaitGroup
			mu   sync.Mutex
			errs []error
			keys []string
		)

		for range callers {
			wg.Go(func() {
				entry, err := s.NextCircular("data:")
				if err != nil {
					mu.Lock()

					errs = append(errs, err)

					mu.Unlock()

					return
				}

				if entry == nil {
					mu.Lock()

					errs = append(errs, errors.New("nextCircular returned nil"))

					mu.Unlock()

					return
				}

				mu.Lock()

				keys = append(keys, entry.Key)

				mu.Unlock()
			})
		}

		wg.Wait()

		require.Emptyf(t, errs, "concurrent nextCircular errors: %v", errs)
		require.Len(t, keys, callers)

		counts := make(map[string]int, totalKeys)
		for _, key := range keys {
			counts[key]++
		}

		for index := 1; index <= totalKeys; index++ {
			key := fmt.Sprintf("data:%06d", index)
			assert.Equalf(t, 3, counts[key], "unexpected histogram count for %s", key)
		}
	})
}

func TestStore_NextCircular_ConcurrentDifferentPrefixesStayIndependent(t *testing.T) {
	t.Parallel()

	runNextCircularStoreTest(t, func(t *testing.T, s Store) {
		t.Helper()

		const (
			keysPerPrefix  = 10
			callsPerPrefix = 30
		)

		for index := 1; index <= keysPerPrefix; index++ {
			keyA := fmt.Sprintf("a:%06d", index)
			keyB := fmt.Sprintf("b:%06d", index)

			require.NoError(t, s.Set(keyA, []byte(keyA)))
			require.NoError(t, s.Set(keyB, []byte(keyB)))
		}

		type sample struct {
			prefix string
			key    string
		}

		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			results []sample
			errs    []error
		)

		runCall := func(prefix string) {
			entry, err := s.NextCircular(prefix)
			if err != nil {
				mu.Lock()

				errs = append(errs, err)

				mu.Unlock()

				return
			}

			if entry == nil {
				mu.Lock()

				errs = append(errs, fmt.Errorf("nextCircular returned nil for prefix %q", prefix))

				mu.Unlock()

				return
			}

			mu.Lock()

			results = append(results, sample{prefix: prefix, key: entry.Key})

			mu.Unlock()
		}

		for range callsPerPrefix {
			wg.Go(func() {
				runCall("a:")
			})
			wg.Go(func() {
				runCall("b:")
			})
		}

		wg.Wait()

		require.Emptyf(t, errs, "concurrent nextCircular errors: %v", errs)
		require.Len(t, results, callsPerPrefix*2)

		countsA := make(map[string]int, keysPerPrefix)
		countsB := make(map[string]int, keysPerPrefix)

		for _, result := range results {
			switch result.prefix {
			case "a:":
				if !strings.HasPrefix(result.key, "a:") {
					t.Fatalf("expected a: key, got %q", result.key)
				}

				countsA[result.key]++
			case "b:":
				if !strings.HasPrefix(result.key, "b:") {
					t.Fatalf("expected b: key, got %q", result.key)
				}

				countsB[result.key]++
			default:
				t.Fatalf("unexpected prefix tag %q", result.prefix)
			}
		}

		for index := 1; index <= keysPerPrefix; index++ {
			keyA := fmt.Sprintf("a:%06d", index)
			keyB := fmt.Sprintf("b:%06d", index)

			assert.Equal(t, 3, countsA[keyA], "prefix a histogram mismatch")
			assert.Equal(t, 3, countsB[keyB], "prefix b histogram mismatch")
		}
	})
}

func TestStore_NextCircular_ConcurrentClearRestore_NoRaceOrDeadlock(t *testing.T) {
	t.Parallel()

	runNextCircularStoreTest(t, func(t *testing.T, s Store) {
		t.Helper()

		seedDataset := func() error {
			for i := 1; i <= 16; i++ {
				key := fmt.Sprintf("data:%06d", i)
				if err := s.Set(key, []byte(key)); err != nil {
					return err
				}
			}

			return nil
		}

		require.NoError(t, seedDataset())
		snapshotPath := filepath.Join(t.TempDir(), "next-circular-race-restore.kv")

		_, err := s.Backup(&BackupOptions{FileName: snapshotPath})
		require.NoError(t, err)

		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			runErrs []error
		)

		recordErr := func(err error) {
			mu.Lock()

			runErrs = append(runErrs, err)

			mu.Unlock()
		}

		for range 8 {
			wg.Go(func() {
				for range 500 {
					entry, nextErr := s.NextCircular("data:")
					if nextErr != nil {
						recordErr(nextErr)
						return
					}

					if entry == nil {
						continue
					}

					if !strings.HasPrefix(entry.Key, "data:") {
						recordErr(fmt.Errorf("unexpected key %q", entry.Key))
						return
					}
				}
			})
		}

		wg.Go(func() {
			for i := range 24 {
				if i%2 == 0 {
					if clearErr := s.Clear(); clearErr != nil {
						recordErr(clearErr)
						return
					}

					if seedErr := seedDataset(); seedErr != nil {
						recordErr(seedErr)
						return
					}

					continue
				}

				if _, restoreErr := s.Restore(&RestoreOptions{FileName: snapshotPath}); restoreErr != nil {
					recordErr(restoreErr)
					return
				}
			}
		})

		wg.Wait()
		require.Emptyf(t, runErrs, "concurrent clear/restore errors: %v", runErrs)

		entry := requireNextCircular(t, s, "data:")
		assert.True(t, strings.HasPrefix(entry.Key, "data:"))
	})
}
