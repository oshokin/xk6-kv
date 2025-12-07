package store

import (
	"fmt"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkMemoryStore_Get: measures read performance on a pre-populated store.
// Runs with and without key tracking to expose any incidental overhead.
func BenchmarkMemoryStore_Get(b *testing.B) {
	for _, trackKeys := range []bool{true, false} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			b.ReportAllocs()

			const totalSeedKeys = 1000

			memoryCfg := &MemoryConfig{TrackKeys: trackKeys}
			store := NewMemoryStore(memoryCfg)

			b.StopTimer()
			seedMemoryStore(b, store, totalSeedKeys, "key-")
			b.StartTimer()

			for i := range b.N {
				keyString := fmt.Sprintf("key-%d", i%totalSeedKeys)
				_, _ = store.Get(keyString)
			}
		})
	}
}

// BenchmarkMemoryStore_Set: measures write/insert performance with and without key tracking.
// Tracking adds index maintenance, so it should be visibly different.
func BenchmarkMemoryStore_Set(b *testing.B) {
	for _, trackKeys := range []bool{true, false} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			b.ReportAllocs()

			memoryCfg := &MemoryConfig{TrackKeys: trackKeys}
			store := NewMemoryStore(memoryCfg)

			b.ResetTimer()

			for i := range b.N {
				keyString := fmt.Sprintf("key-%d", i)
				_ = store.Set(keyString, "value")
			}
		})
	}
}

// BenchmarkMemoryStore_IncrementBy: measures atomic integer increments.
// Tracking is mostly irrelevant but is included to expose any fixed overhead difference.
func BenchmarkMemoryStore_IncrementBy(b *testing.B) {
	for _, trackKeys := range []bool{true, false} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			b.ReportAllocs()

			memoryCfg := &MemoryConfig{TrackKeys: trackKeys}
			store := NewMemoryStore(memoryCfg)

			b.ResetTimer()

			for b.Loop() {
				_, _ = store.IncrementBy("ctr", 1)
			}
		})
	}
}

// BenchmarkMemoryStore_GetOrSet: measures first-writer-wins path and steady-state "loaded=true" lookups.
// With tracking enabled, first insert updates the index; subsequent loads should be similar across modes.
func BenchmarkMemoryStore_GetOrSet(b *testing.B) {
	for _, trackKeys := range []bool{true, false} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			b.ReportAllocs()

			memoryCfg := &MemoryConfig{TrackKeys: trackKeys}
			store := NewMemoryStore(memoryCfg)

			b.ResetTimer()

			for i := range b.N {
				valueString := "v" + strconv.Itoa(i)

				_, _, _ = store.GetOrSet("k", valueString)
			}
		})
	}
}

// BenchmarkMemoryStore_Swap: measures unconditional replacement.
// With tracking, the first call inserts (index update), later calls are pure replace.
func BenchmarkMemoryStore_Swap(b *testing.B) {
	for _, trackKeys := range []bool{true, false} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			b.ReportAllocs()

			memoryCfg := &MemoryConfig{TrackKeys: trackKeys}
			store := NewMemoryStore(memoryCfg)

			b.ResetTimer()

			for i := range b.N {
				_, _, _ = store.Swap("k", strconv.Itoa(i))
			}
		})
	}
}

// BenchmarkMemoryStore_CompareAndSwap: single-threaded CAS throughput.
// First CAS succeeds; subsequent CAS attempts fail but still exercise the path.
func BenchmarkMemoryStore_CompareAndSwap(b *testing.B) {
	for _, trackKeys := range []bool{true, false} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			b.ReportAllocs()

			memoryCfg := &MemoryConfig{TrackKeys: trackKeys}
			store := NewMemoryStore(memoryCfg)
			require.NoError(b, store.Set("k", "0"))

			b.ResetTimer()

			for b.Loop() {
				_, _ = store.CompareAndSwap("k", "0", "1")
			}
		})
	}
}

// BenchmarkMemoryStore_CompareAndSwap_Contention: CAS under parallel contention.
// Shows scalability and retry behavior with many goroutines.
func BenchmarkMemoryStore_CompareAndSwap_Contention(b *testing.B) {
	b.ReportAllocs()

	memoryCfg := &MemoryConfig{TrackKeys: true}
	store := NewMemoryStore(memoryCfg)
	require.NoError(b, store.Set("k", "v0"))

	b.ResetTimer()
	b.RunParallel(func(parallelBench *testing.PB) {
		for parallelBench.Next() {
			_, _ = store.CompareAndSwap("k", "v0", "v1")
		}
	})
}

// BenchmarkMemoryStore_ShardsParallelSet measures parallel write throughput comparing shard counts.
func BenchmarkMemoryStore_ShardsParallelSet(b *testing.B) {
	const keySpace = 10_000_000

	keys := makeKeyPool(keySpace, "parallel-key-")
	payload := []byte("value")
	keyCount := uint64(len(keys))

	for _, shardCount := range shardBenchmarkTargets() {
		b.Run(fmt.Sprintf("shards=%d", shardCount), func(b *testing.B) {
			b.ReportAllocs()

			memoryCfg := &MemoryConfig{
				TrackKeys:  false,
				ShardCount: shardCount,
			}

			store := NewMemoryStore(memoryCfg)

			b.StopTimer()

			for _, key := range keys {
				require.NoError(b, store.Set(key, payload))
			}

			b.StartTimer()
			b.ResetTimer()

			var cursor atomic.Uint64

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					idx := cursor.Add(1) - 1
					key := keys[int(idx%keyCount)]

					_ = store.Set(key, payload)
				}
			})
		})
	}
}

// BenchmarkMemoryStore_ShardsParallelGet measures parallel read throughput across shard counts.
func BenchmarkMemoryStore_ShardsParallelGet(b *testing.B) {
	const keySpace = 10_000_000

	keys := makeKeyPool(keySpace, "parallel-key-")
	payload := []byte("value")
	keyCount := uint64(len(keys))

	for _, shardCount := range shardBenchmarkTargets() {
		b.Run(fmt.Sprintf("shards=%d", shardCount), func(b *testing.B) {
			b.ReportAllocs()

			memoryCfg := &MemoryConfig{
				TrackKeys:  false,
				ShardCount: shardCount,
			}

			store := NewMemoryStore(memoryCfg)

			b.StopTimer()

			for _, key := range keys {
				require.NoError(b, store.Set(key, payload))
			}

			b.StartTimer()
			b.ResetTimer()

			var cursor atomic.Uint64

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					idx := cursor.Add(1) - 1
					key := keys[int(idx%keyCount)]

					_, _ = store.Get(key)
				}
			})
		})
	}
}

// BenchmarkMemoryStore_Delete: delete throughput across varying store sizes.
// Re-seeding is excluded from timing to isolate delete cost.
func BenchmarkMemoryStore_Delete(b *testing.B) {
	for _, totalSize := range []int{10, 100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("Size=%d", totalSize), func(b *testing.B) {
			b.ReportAllocs()

			memoryCfg := &MemoryConfig{TrackKeys: true}
			store := NewMemoryStore(memoryCfg)

			b.StopTimer()
			seedMemoryStore(b, store, totalSize, "key-")
			b.StartTimer()

			for i := range b.N {
				keyString := fmt.Sprintf("key-%d", i%totalSize)
				_ = store.Delete(keyString)

				// Re-seed the deleted key for the next iteration (excluded from timing).
				if i < b.N-1 {
					b.StopTimer()

					valueString := fmt.Sprintf("value-%d", i%totalSize)
					require.NoError(b, store.Set(keyString, valueString))

					b.StartTimer()
				}
			}
		})
	}
}

// BenchmarkMemoryStore_Exists: membership checks on a pre-populated store.
// Includes both tracking modes for overhead comparison.
func BenchmarkMemoryStore_Exists(b *testing.B) {
	for _, trackKeys := range []bool{true, false} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			b.ReportAllocs()

			const totalSeedKeys = 1000

			memoryCfg := &MemoryConfig{TrackKeys: trackKeys}
			store := NewMemoryStore(memoryCfg)

			b.StopTimer()
			seedMemoryStore(b, store, totalSeedKeys, "key-")
			b.StartTimer()

			for i := range b.N {
				keyString := fmt.Sprintf("key-%d", i%totalSeedKeys)

				_, _ = store.Exists(keyString)
			}
		})
	}
}

// BenchmarkMemoryStore_DeleteIfExists: conditional delete cost with and without tracking.
// Re-seeding is excluded from timing to isolate DeleteIfExists cost.
func BenchmarkMemoryStore_DeleteIfExists(b *testing.B) {
	for _, trackKeys := range []bool{true, false} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			b.ReportAllocs()

			memoryCfg := &MemoryConfig{TrackKeys: trackKeys}
			store := NewMemoryStore(memoryCfg)
			require.NoError(b, store.Set("k", "v"))

			b.ResetTimer()

			for b.Loop() {
				_, _ = store.DeleteIfExists("k")

				b.StopTimer()

				// Re-seed.
				require.NoError(b, store.Set("k", "v"))

				b.StartTimer()
			}
		})
	}
}

// BenchmarkMemoryStore_CompareAndDelete: conditional delete if value matches.
// Re-seeding excluded from timing.
func BenchmarkMemoryStore_CompareAndDelete(b *testing.B) {
	for _, trackKeys := range []bool{true, false} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			b.ReportAllocs()

			memoryCfg := &MemoryConfig{TrackKeys: trackKeys}
			store := NewMemoryStore(memoryCfg)
			require.NoError(b, store.Set("k", "v"))

			b.ResetTimer()

			for b.Loop() {
				_, _ = store.CompareAndDelete("k", "v")

				b.StopTimer()

				// Re-seed.
				require.NoError(b, store.Set("k", "v"))

				b.StartTimer()
			}
		})
	}
}

// BenchmarkMemoryStore_List: list-all, list-by-prefix, and limit variants.
// Tracking mode should strongly influence performance by enabling index-based behavior.
func BenchmarkMemoryStore_List(b *testing.B) {
	for _, trackKeys := range []bool{true, false} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			b.ReportAllocs()

			memoryCfg := &MemoryConfig{TrackKeys: trackKeys}
			store := NewMemoryStore(memoryCfg)

			b.StopTimer()
			seedMemoryStore(b, store, 1000, "key-")
			seedMemoryStore(b, store, 100, "prefix-")
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

// BenchmarkMemoryStore_Concurrent: mixed Get/Set under parallel load on a warm store.
// Demonstrates read/write interaction and scheduler effects.
func BenchmarkMemoryStore_Concurrent(b *testing.B) {
	b.ReportAllocs()

	memoryCfg := &MemoryConfig{TrackKeys: true}
	store := NewMemoryStore(memoryCfg)

	b.StopTimer()
	seedMemoryStore(b, store, 1000, "key-")
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
}

// BenchmarkMemoryStore_AtomicConcurrent: mixed atomic operations under parallel load.
// Exercises IncrementBy, GetOrSet, Swap, CAS, DeleteIfExists, CompareAndDelete together.
func BenchmarkMemoryStore_AtomicConcurrent(b *testing.B) {
	b.ReportAllocs()

	memoryCfg := &MemoryConfig{TrackKeys: true}
	store := NewMemoryStore(memoryCfg)

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

// seedMemoryStore pre-populates the store with N keys "prefix{i}" -> "value-{i}".
// Seeding happens outside of timed regions in the benchmarks.
func seedMemoryStore(b *testing.B, store *MemoryStore, totalKeys int, keyPrefix string) {
	b.Helper()

	for index := range totalKeys {
		keyString := fmt.Sprintf("%s%d", keyPrefix, index)
		valueString := fmt.Sprintf("value-%d", index)

		require.NoErrorf(b, store.Set(keyString, valueString), "seed Set(%q) must succeed", keyString)
	}
}

// makeKeyPool deterministically materializes key strings up front to avoid per-iteration allocations.
func makeKeyPool(totalKeys int, prefix string) []string {
	keys := make([]string, totalKeys)

	for i := range keys {
		keys[i] = fmt.Sprintf("%s%d", prefix, i)
	}

	return keys
}

// shardBenchmarkTargets returns the shard counts we want to compare (1 and runtime.NumCPU()).
func shardBenchmarkTargets() []int {
	targets := []int{1}

	cpuShards := max(runtime.NumCPU(), 1)

	if cpuShards != 1 {
		targets = append(targets, cpuShards)
	}

	return targets
}
