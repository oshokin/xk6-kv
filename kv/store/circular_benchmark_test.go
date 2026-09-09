package store

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkStore_NextCircular_Sequential(b *testing.B) {
	for _, backend := range []string{"memory", "disk"} {
		for _, trackKeys := range []bool{false, true} {
			for _, datasetSize := range []int{1, 1_000, 10_000} {
				name := fmt.Sprintf("%s/trackKeys=%t/dataset=%d", backend, trackKeys, datasetSize)

				b.Run(name+"/prefix=all", func(b *testing.B) {
					store := newCircularBenchmarkStore(b, backend, trackKeys)
					seedCircularBenchmarkPrefix(b, store, "data:", datasetSize)
					runSequentialNextCircularBenchmark(b, store, "")
				})

				b.Run(name+"/prefix=users", func(b *testing.B) {
					store := newCircularBenchmarkStore(b, backend, trackKeys)
					seedCircularBenchmarkPrefix(b, store, "users:", datasetSize)
					seedCircularBenchmarkPrefix(b, store, "other:", datasetSize)
					runSequentialNextCircularBenchmark(b, store, "users:")
				})
			}
		}
	}
}

func BenchmarkStore_NextCircular_ParallelSamePrefix(b *testing.B) {
	for _, backend := range []string{"memory", "disk"} {
		for _, trackKeys := range []bool{false, true} {
			for _, datasetSize := range []int{1, 1_000, 10_000} {
				name := fmt.Sprintf("%s/trackKeys=%t/dataset=%d", backend, trackKeys, datasetSize)

				b.Run(name+"/prefix=users", func(b *testing.B) {
					store := newCircularBenchmarkStore(b, backend, trackKeys)
					seedCircularBenchmarkPrefix(b, store, "users:", datasetSize)
					seedCircularBenchmarkPrefix(b, store, "other:", datasetSize)
					runParallelSamePrefixNextCircularBenchmark(b, store, "users:")
				})
			}
		}
	}
}

func BenchmarkStore_NextCircular_ParallelDifferentPrefixes(b *testing.B) {
	for _, backend := range []string{"memory", "disk"} {
		for _, trackKeys := range []bool{false, true} {
			for _, datasetSize := range []int{1, 1_000, 10_000} {
				name := fmt.Sprintf("%s/trackKeys=%t/dataset=%d", backend, trackKeys, datasetSize)

				b.Run(name+"/prefix=feed:*", func(b *testing.B) {
					store := newCircularBenchmarkStore(b, backend, trackKeys)

					prefixes := make([]string, 16)
					entriesPerPrefix := max(1, datasetSize/len(prefixes))

					for i := range prefixes {
						prefixes[i] = fmt.Sprintf("feed:%02d:", i)
						seedCircularBenchmarkPrefix(b, store, prefixes[i], entriesPerPrefix)
					}

					runParallelDifferentPrefixesNextCircularBenchmark(b, store, prefixes)
				})
			}
		}
	}
}

func newCircularBenchmarkStore(b *testing.B, backend string, trackKeys bool) Store {
	b.Helper()

	switch backend {
	case "memory":
		memoryStore := NewMemoryStore(&MemoryConfig{TrackKeys: trackKeys})
		require.NoError(b, memoryStore.Open())
		b.Cleanup(func() {
			_ = memoryStore.Close()
		})

		return memoryStore
	case "disk":
		return newBenchmarkDiskStore(
			b,
			trackKeys,
			fmt.Sprintf("diskstore-bench-next-circular-%t-*.db", trackKeys),
		)
	default:
		b.Fatalf("unknown backend %q", backend)
		return nil
	}
}

func seedCircularBenchmarkPrefix(b *testing.B, s Store, prefix string, count int) {
	b.Helper()

	for i := 1; i <= count; i++ {
		key := fmt.Sprintf("%s%06d", prefix, i)
		value := fmt.Sprintf("value-%06d", i)
		require.NoErrorf(b, s.Set(key, value), "seed Set(%q) must succeed", key)
	}
}

func runSequentialNextCircularBenchmark(b *testing.B, s Store, prefix string) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		entry, err := s.NextCircular(prefix)
		if err != nil {
			b.Fatalf("NextCircular failed: %v", err)
		}

		if entry == nil {
			b.Fatalf("NextCircular returned nil for prefix %q", prefix)
		}
	}
}

func runParallelSamePrefixNextCircularBenchmark(b *testing.B, s Store, prefix string) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			entry, err := s.NextCircular(prefix)
			if err != nil {
				b.Fatalf("NextCircular failed: %v", err)
			}

			if entry == nil {
				b.Fatalf("NextCircular returned nil for prefix %q", prefix)
			}
		}
	})
}

func runParallelDifferentPrefixesNextCircularBenchmark(b *testing.B, s Store, prefixes []string) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()

	var nextWorker atomic.Uint64

	b.RunParallel(func(pb *testing.PB) {
		workerID := nextWorker.Add(1) - 1
		prefix := prefixes[int(workerID%uint64(len(prefixes)))]

		for pb.Next() {
			entry, err := s.NextCircular(prefix)
			if err != nil {
				b.Fatalf("NextCircular failed: %v", err)
			}

			if entry == nil {
				b.Fatalf("NextCircular returned nil for prefix %q", prefix)
			}
		}
	})
}
