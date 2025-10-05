package store

import (
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkOSTree_Insert measures the end-to-end build cost of inserting N prefixed keys
// into a fresh OSTree. The timed region includes creating a new tree and inserting all keys.
// We also set a bytes-per-iteration heuristic with b.SetBytes() based on total key bytes.
func BenchmarkOSTree_Insert(b *testing.B) {
	for _, totalKeyCount := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("N=%d", totalKeyCount), func(b *testing.B) {
			benchmarkKeys := makeBenchmarkTestKeys(totalKeyCount)

			// Precondition: constructor must succeed (outside timed region).
			require.NotNil(b, NewOSTree())

			// SetBytes heuristic: sum of key byte lengths inserted per iteration.
			var totalKeyBytes int
			for _, key := range benchmarkKeys {
				totalKeyBytes += len(key)
			}

			b.SetBytes(int64(totalKeyBytes))
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				tree := NewOSTree()

				// No require inside timed loop to avoid timing overhead.
				for _, key := range benchmarkKeys {
					tree.Insert(key)
				}
			}
		})
	}
}

// BenchmarkOSTree_Rank measures key -> rank lookups against a prebuilt tree of 100k keys.
// The timed region executes only Rank calls; the tree build and query set are outside the timer.
func BenchmarkOSTree_Rank(b *testing.B) {
	tree := NewOSTree()

	benchmarkKeys := makeBenchmarkTestKeys(100_000)
	for _, key := range benchmarkKeys {
		tree.Insert(key)
	}

	// Prepare a query key set (~50% of the corpus); any distribution works as long as we reuse it.
	queryKeysForRank := makeBenchmarkTestKeys(50_000)

	// Preconditions (outside timed region).
	require.NotNil(b, tree)
	require.Positive(b, tree.Len())
	require.NotEmpty(b, queryKeysForRank)

	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		key := queryKeysForRank[i%len(queryKeysForRank)]

		_ = tree.Rank(key)
	}
}

// BenchmarkOSTree_Rank_Parallel measures Rank lookups using b.RunParallel. The tree is read-only
// during the benchmark and shared across goroutines. The work distribution uses an atomic counter
// to avoid contention on a RNG and to keep things deterministic.
func BenchmarkOSTree_Rank_Parallel(b *testing.B) {
	tree := NewOSTree()

	benchmarkKeys := makeBenchmarkTestKeys(100_000)
	for _, key := range benchmarkKeys {
		tree.Insert(key)
	}

	queryKeysForRank := makeBenchmarkTestKeys(50_000)

	// Preconditions (outside timed region).
	require.NotNil(b, tree)
	require.Positive(b, tree.Len())
	require.NotEmpty(b, queryKeysForRank)

	var atomicQueryIndex uint64

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(parallelBench *testing.PB) {
		for parallelBench.Next() {
			index := nextAtomicModuloIndex(&atomicQueryIndex, len(queryKeysForRank))

			key := queryKeysForRank[index]
			_ = tree.Rank(key)
		}
	})
}

// BenchmarkOSTree_Kth measures random Kth(index) lookups against a prebuilt tree of 100k keys.
// The timed region executes only Kth calls; the tree build is outside the timer.
func BenchmarkOSTree_Kth(b *testing.B) {
	tree := NewOSTree()

	benchmarkKeys := makeBenchmarkTestKeys(100_000)
	for _, key := range benchmarkKeys {
		tree.Insert(key)
	}

	totalElementCount := tree.Len()

	// Preconditions (outside timed region).
	require.NotNil(b, tree)
	require.Positive(b, totalElementCount)

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		randomIndex := rand.IntN(totalElementCount)

		_, _ = tree.Kth(randomIndex)
	}
}

// BenchmarkOSTree_Kth_Parallel measures Kth lookups using b.RunParallel. We avoid RNG contention
// by using an atomic counter modulo the element count.
func BenchmarkOSTree_Kth_Parallel(b *testing.B) {
	tree := NewOSTree()

	benchmarkKeys := makeBenchmarkTestKeys(100_000)
	for _, key := range benchmarkKeys {
		tree.Insert(key)
	}

	totalElementCount := tree.Len()

	// Preconditions (outside timed region).
	require.NotNil(b, tree)
	require.Positive(b, totalElementCount)

	var atomicKIndex uint64

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(parallelBench *testing.PB) {
		for parallelBench.Next() {
			index := nextAtomicModuloIndex(&atomicKIndex, totalElementCount)

			_, _ = tree.Kth(index)
		}
	})
}

// BenchmarkOSTree_RangeBounds measures prefix range bound computation (low/high indices) over
// a prebuilt tree of 100k keys. The timed region executes only RangeBounds calls.
func BenchmarkOSTree_RangeBounds(b *testing.B) {
	tree := NewOSTree()

	benchmarkKeys := makeBenchmarkTestKeys(100_000)
	for _, key := range benchmarkKeys {
		tree.Insert(key)
	}

	prefixQueries := []string{"a", "ab", "user:", "zzz", ""}

	// Preconditions (outside timed region).
	require.NotNil(b, tree)
	require.Positive(b, tree.Len())
	require.NotEmpty(b, prefixQueries)

	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		prefix := prefixQueries[i%len(prefixQueries)]

		_, _ = tree.RangeBounds(prefix)
	}
}

// makeBenchmarkTestKeys returns a deterministic slice of keys with clustered prefixes
// to exercise prefix-sensitive operations (RangeBounds) and varying ranks.
// Pattern: a:, ab:, user:, z: - evenly distributed across the input size.
func makeBenchmarkTestKeys(totalKeyCount int) []string {
	benchmarkKeys := make([]string, totalKeyCount)

	for index := range totalKeyCount {
		switch index % 4 {
		case 0:
			benchmarkKeys[index] = fmt.Sprintf("a:%08d", index)
		case 1:
			benchmarkKeys[index] = fmt.Sprintf("ab:%08d", index)
		case 2:
			benchmarkKeys[index] = fmt.Sprintf("user:%08d", index)
		default:
			benchmarkKeys[index] = fmt.Sprintf("z:%08d", index)
		}
	}

	return benchmarkKeys
}

// nextAtomicModuloIndex atomically increments a uint64 counter and returns a bounded int index in [0, limit).
//
// Why this exists (and why it is safe):
//   - Gosec rule G115 warns about converting from uint64 -> int because it may overflow on 32-bit builds.
//   - We avoid overflow by FIRST reducing the uint64 value modulo `limit` in uint64 space, then converting the
//     reduced value to int. Since the reduced value is guaranteed to be in [0, limit), and `limit` is an int,
//     the conversion to int cannot overflow.
//   - This helper is intended for benchmarks that need a cheap, contention-free index generator without RNG.
//
// Concurrency:
//   - The function uses sync/atomic to get a unique increasing ticket per call and then folds it into range.
func nextAtomicModuloIndex(counter *uint64, limit int) int {
	if limit <= 0 {
		// Defensive: avoid division by zero, return 0 when there is no valid range.
		return 0
	}

	ticket := atomic.AddUint64(counter, 1) - 1

	return int(ticket % uint64(limit))
}
