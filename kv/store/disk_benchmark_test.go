package store

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// newBenchmarkDiskStore creates a temporary on-disk store,
// binds it to a temporary file, and registers cleanup.
// It returns the initialized store and the temporary file path.
func newBenchmarkDiskStore(b *testing.B, trackKeys bool, namePattern string) *DiskStore {
	b.Helper()

	//nolint:forbidigo // this is just a test.
	tempFile, err := os.CreateTemp(b.TempDir(), namePattern)
	require.NoErrorf(b, err, "failed to create temporary file for disk store")

	_ = tempFile.Close()

	store, err := NewDiskStore(trackKeys, tempFile.Name())
	require.NoError(b, err, "NewDiskStore() must succeed for benchmark path")

	require.NoError(b, store.Open())

	b.Cleanup(func() {
		_ = store.Close()

		//nolint:forbidigo // this is just a test.
		_ = os.Remove(tempFile.Name())
	})

	return store
}

// seedDiskStore pre-populates the store with N keys "prefix{i}" -> "value-{i}".
// Seeding happens outside of timed regions in the benchmarks.
func seedDiskStore(b *testing.B, store *DiskStore, totalKeys int, keyPrefix string) {
	b.Helper()

	for index := range totalKeys {
		keyString := fmt.Sprintf("%s%d", keyPrefix, index)
		valueString := fmt.Sprintf("value-%d", index)

		require.NoErrorf(b, store.Set(keyString, valueString), "seed Set(%q) must succeed", keyString)
	}
}

// BenchmarkDiskStore_Get: read throughput on a warm store, with and without key tracking.
// Key tracking should not impact Get directly, but we expose any incidental overhead.
func BenchmarkDiskStore_Get(b *testing.B) {
	for _, trackKeys := range []bool{true, false} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			b.ReportAllocs()

			const totalSeedKeys = 1000

			store := newBenchmarkDiskStore(b, trackKeys, "diskstore-bench-get-*.db")

			b.StopTimer()
			seedDiskStore(b, store, totalSeedKeys, "key-")
			b.StartTimer()

			for i := range b.N {
				keyString := fmt.Sprintf("key-%d", i%totalSeedKeys)

				_, _ = store.Get(keyString)
			}
		})
	}
}

// BenchmarkDiskStore_Set: write/insert throughput, with and without key tracking.
// With tracking enabled, Set also updates the in-memory index.
func BenchmarkDiskStore_Set(b *testing.B) {
	for _, trackKeys := range []bool{true, false} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			b.ReportAllocs()

			store := newBenchmarkDiskStore(b, trackKeys, "diskstore-bench-set-*.db")

			b.ResetTimer()

			for i := range b.N {
				keyString := fmt.Sprintf("key-%d", i)

				_ = store.Set(keyString, "value")
			}
		})
	}
}

// BenchmarkDiskStore_IncrementBy: atomic integer increments with/without tracking.
func BenchmarkDiskStore_IncrementBy(b *testing.B) {
	for _, trackKeys := range []bool{true, false} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			b.ReportAllocs()

			store := newBenchmarkDiskStore(b, trackKeys, "diskstore-bench-increment-by-*.db")

			b.ResetTimer()

			for b.Loop() {
				_, _ = store.IncrementBy("ctr", 1)
			}
		})
	}
}

// BenchmarkDiskStore_GetOrSet: first-writer-wins and steady-state loaded=true path.
func BenchmarkDiskStore_GetOrSet(b *testing.B) {
	for _, trackKeys := range []bool{true, false} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			b.ReportAllocs()

			store := newBenchmarkDiskStore(b, trackKeys, "diskstore-bench-get-or-set-*.db")

			b.ResetTimer()

			for i := range b.N {
				valueString := "v" + strconv.Itoa(i)

				_, _, _ = store.GetOrSet("k", valueString)
			}
		})
	}
}

// BenchmarkDiskStore_Swap: unconditional replacement throughput.
// First call inserts (and updates index if tracking), later calls are replaces.
func BenchmarkDiskStore_Swap(b *testing.B) {
	for _, trackKeys := range []bool{true, false} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			b.ReportAllocs()

			store := newBenchmarkDiskStore(b, trackKeys, "diskstore-bench-swap-*.db")

			b.ResetTimer()

			for i := range b.N {
				_, _, _ = store.Swap("swap-k", strconv.Itoa(i))
			}
		})
	}
}

// BenchmarkDiskStore_CompareAndSwap: single-threaded CAS throughput.
func BenchmarkDiskStore_CompareAndSwap(b *testing.B) {
	for _, trackKeys := range []bool{true, false} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			b.ReportAllocs()

			store := newBenchmarkDiskStore(b, trackKeys, "diskstore-bench-compare-and-swap-*.db")
			require.NoError(b, store.Set("k", "0"))

			b.ResetTimer()

			for b.Loop() {
				_, _ = store.CompareAndSwap("k", "0", "1")
			}
		})
	}
}

// BenchmarkDiskStore_CompareAndSwap_Contention: CAS under parallel contention to show scalability.
func BenchmarkDiskStore_CompareAndSwap_Contention(b *testing.B) {
	for _, trackKeys := range []bool{true, false} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			b.ReportAllocs()

			store := newBenchmarkDiskStore(b, trackKeys, "diskstore-bench-compare-and-swap-contention-*.db")
			require.NoError(b, store.Set("k", "v0"))

			b.ResetTimer()
			b.RunParallel(func(parallelBench *testing.PB) {
				for parallelBench.Next() {
					_, _ = store.CompareAndSwap("k", "v0", "v1")
				}
			})
		})
	}
}

// BenchmarkDiskStore_RandomKey: cost of picking any random key in a large dataset.
// Compares tracking index vs no-tracking path.
func BenchmarkDiskStore_RandomKey(b *testing.B) {
	for _, trackKeys := range []bool{true, false} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			b.ReportAllocs()

			const genericKeys = 10_000

			store := newBenchmarkDiskStore(b, trackKeys, "diskstore-bench-random-key-*.db")

			b.StopTimer()
			seedDiskStore(b, store, genericKeys, "key-")
			b.StartTimer()

			for b.Loop() {
				_, _ = store.RandomKey("")
			}
		})
	}
}

// BenchmarkDiskStore_RandomKey_WithPrefix: select a random key from a subset (prefix).
// Compares indexed prefix selection (tracking) vs scan (no tracking).
func BenchmarkDiskStore_RandomKey_WithPrefix(b *testing.B) {
	for _, trackKeys := range []bool{true, false} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			b.ReportAllocs()

			store := newBenchmarkDiskStore(b, trackKeys, "diskstore-bench-random-key-with-prefix-*.db")

			// Seed 10k generic + 2k with "pfx-".
			b.StopTimer()

			seedDiskStore(b, store, 10_000, "key-")

			for index := range 2_000 {
				require.NoError(b, store.Set(fmt.Sprintf("pfx-%d", index), "value"))
			}

			b.StartTimer()

			for b.Loop() {
				_, _ = store.RandomKey("pfx-")
			}
		})
	}
}

// BenchmarkDiskStore_Delete: delete throughput across varying store sizes and tracking modes.
// Re-seeding is excluded from timing to isolate Delete cost.
func BenchmarkDiskStore_Delete(b *testing.B) {
	for _, trackKeys := range []bool{true, false} {
		for _, totalSize := range []int{10, 100, 1000} {
			b.Run(fmt.Sprintf("trackKeys=%v/size=%d", trackKeys, totalSize), func(b *testing.B) {
				b.ReportAllocs()

				store := newBenchmarkDiskStore(b, trackKeys, "diskstore-bench-delete-*.db")

				b.StopTimer()
				seedDiskStore(b, store, totalSize, "key-")
				b.StartTimer()

				for i := range b.N {
					keyString := fmt.Sprintf("key-%d", i%totalSize)
					_ = store.Delete(keyString)

					// Re-seed the deleted key for the next iteration (excluded from timing).
					if i < b.N-1 {
						b.StopTimer()
						require.NoError(b, store.Set(keyString, "value"))
						b.StartTimer()
					}
				}
			})
		}
	}
}

// BenchmarkDiskStore_Exists: membership checks on a pre-populated store.
// TrackKeys may or may not help (depends on the implementation), we benchmark both.
func BenchmarkDiskStore_Exists(b *testing.B) {
	for _, trackKeys := range []bool{true, false} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			b.ReportAllocs()

			const totalSeedKeys = 1000

			store := newBenchmarkDiskStore(b, trackKeys, "diskstore-bench-exists-*.db")

			b.StopTimer()
			seedDiskStore(b, store, totalSeedKeys, "key-")
			b.StartTimer()

			for i := range b.N {
				keyString := fmt.Sprintf("key-%d", i%totalSeedKeys)

				_, _ = store.Exists(keyString)
			}
		})
	}
}

// BenchmarkDiskStore_DeleteIfExists: conditional delete cost with and without tracking.
func BenchmarkDiskStore_DeleteIfExists(b *testing.B) {
	for _, trackKeys := range []bool{true, false} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			b.ReportAllocs()

			store := newBenchmarkDiskStore(b, trackKeys, "diskstore-bench-delete-if-exists-*.db")
			require.NoError(b, store.Set("k", "v"))

			b.ResetTimer()

			for b.Loop() {
				_, _ = store.DeleteIfExists("k")

				// Re-seed outside timing.
				b.StopTimer()
				require.NoError(b, store.Set("k", "v"))
				b.StartTimer()
			}
		})
	}
}

// BenchmarkDiskStore_CompareAndDelete: delete only if value matches; re-seeding excluded from timing.
func BenchmarkDiskStore_CompareAndDelete(b *testing.B) {
	for _, trackKeys := range []bool{true, false} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			b.ReportAllocs()

			store := newBenchmarkDiskStore(b, trackKeys, "diskstore-bench-compare-and-delete-*.db")
			require.NoError(b, store.Set("k", "v"))

			b.ResetTimer()

			for b.Loop() {
				_, _ = store.CompareAndDelete("k", "v")

				// Re-seed outside timing.
				b.StopTimer()
				require.NoError(b, store.Set("k", "v"))
				b.StartTimer()
			}
		})
	}
}

// BenchmarkDiskStore_Scan measures paginated scans across large datasets with and without tracking.
func BenchmarkDiskStore_Scan(b *testing.B) {
	const (
		totalPerPrefix = 10_000
		pageLimit      = 100
		resumeLimit    = 32
	)

	prefixes := []string{"user:", "order:", "misc:"}

	for _, trackKeys := range []bool{true, false} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			store := newBenchmarkDiskStore(b, trackKeys, "diskstore-bench-scan-*.db")

			b.StopTimer()

			for _, prefix := range prefixes {
				seedDiskStore(b, store, totalPerPrefix, prefix)
			}

			b.StartTimer()

			b.Run("PrefixUserLimit100", func(b *testing.B) {
				runScanBenchmark(b, store, "user:", "", pageLimit)
			})

			b.Run("PrefixUserUnlimited", func(b *testing.B) {
				runScanBenchmark(b, store, "user:", "", 0)
			})

			b.Run("PrefixOrderResume", func(b *testing.B) {
				startAfter := fmt.Sprintf("order:%05d", totalPerPrefix/2)
				runScanBenchmark(b, store, "order:", startAfter, resumeLimit)
			})
		})
	}
}

// BenchmarkDiskStore_List: list-all, list-by-prefix, and limit variants, for both tracking modes.
// Tracking should strongly influence performance by enabling index-based behavior.
func BenchmarkDiskStore_List(b *testing.B) {
	for _, trackKeys := range []bool{true, false} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			b.ReportAllocs()

			store := newBenchmarkDiskStore(b, trackKeys, "diskstore-bench-list-*.db")

			// Seed general and prefixed keys outside timing.
			b.StopTimer()

			seedDiskStore(b, store, 1000, "key-")
			seedDiskStore(b, store, 100, "prefix-")

			b.StartTimer()

			b.Run("ListAll", func(b *testing.B) {
				b.ReportAllocs()

				b.ResetTimer()

				for b.Loop() {
					_, _ = store.List("", 0)
				}
			})

			b.Run("ListWithPrefix", func(b *testing.B) {
				b.ReportAllocs()

				b.ResetTimer()

				for b.Loop() {
					_, _ = store.List("prefix", 0)
				}
			})

			b.Run("ListWithLimit", func(b *testing.B) {
				b.ReportAllocs()

				b.ResetTimer()

				for b.Loop() {
					_, _ = store.List("", 10)
				}
			})

			b.Run("ListWithPrefixAndLimit", func(b *testing.B) {
				b.ReportAllocs()

				b.ResetTimer()

				for b.Loop() {
					_, _ = store.List("prefix", 10)
				}
			})
		})
	}
}

// BenchmarkDiskStore_RebuildKeyList: measures cost of reconstructing the in-memory index from disk.
// This only makes sense when key tracking is enabled.
func BenchmarkDiskStore_RebuildKeyList(b *testing.B) {
	b.Run("trackKeys=true", func(b *testing.B) {
		b.ReportAllocs()

		store := newBenchmarkDiskStore(b, true, "diskstore-bench-rebuild-key-list-*.db")

		// Fill with keys.
		b.StopTimer()
		seedDiskStore(b, store, 10_000, "key-")
		b.StartTimer()

		for b.Loop() {
			_ = store.RebuildKeyList()
		}
	})
}

// BenchmarkDiskStore_Concurrent: mixed Get/Set under parallel load on a warm store.
// Demonstrates read/write interaction and scheduler effects; both tracking modes included.
func BenchmarkDiskStore_Concurrent(b *testing.B) {
	for _, trackKeys := range []bool{true, false} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			b.ReportAllocs()

			store := newBenchmarkDiskStore(b, trackKeys, "diskstore-bench-concurrent-*.db")

			b.StopTimer()
			seedDiskStore(b, store, 1000, "key-")
			b.StartTimer()

			b.RunParallel(func(parallelBench *testing.PB) {
				var i int
				for parallelBench.Next() {
					if i%2 == 0 {
						keyString := fmt.Sprintf("key-%d", i%1000)

						_, _ = store.Get(keyString)
					} else {
						keyString := fmt.Sprintf("key-%d", i%1000)
						valueString := fmt.Sprintf("value-%d", i)

						_ = store.Set(keyString, valueString)
					}

					i++
				}
			})
		})
	}
}

// BenchmarkDiskStore_AtomicConcurrent: mixed atomic operations under parallel load.
// Exercises IncrementBy, GetOrSet, Swap, CAS, DeleteIfExists, CompareAndDelete in one hot path.
func BenchmarkDiskStore_AtomicConcurrent(b *testing.B) {
	b.ReportAllocs()

	store := newBenchmarkDiskStore(b, true, "diskstore-bench-atomic-concurrent-*.db")

	b.RunParallel(func(parallelBench *testing.PB) {
		var i int
		for parallelBench.Next() {
			switch i % 6 {
			case 0:
				_, _ = store.IncrementBy("ctr", 1)
			case 1:
				_, _, _ = store.GetOrSet("once", "payload")
			case 2:
				_, _, _ = store.Swap("swap", strconv.Itoa(i))
			case 3:
				_, _ = store.CompareAndSwap("cas", "old", "new")
			case 4:
				_, _ = store.DeleteIfExists("del")

				// Re-seed (not excluded; acceptable in mixed workload).
				_ = store.Set("del", "v")
			default:
				_, _ = store.CompareAndDelete("cndel", "v")

				// Re-seed.
				_ = store.Set("cndel", "v")
			}

			i++
		}
	})
}
