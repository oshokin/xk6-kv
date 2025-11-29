package store

import (
	"fmt"
	"testing"
)

// BenchmarkShardHashSumBytes benchmarks the sum of bytes shard hash.
func BenchmarkShardHashSumBytes(b *testing.B) {
	benchmarkShardHash(b, sumBytesShardHash)
}

// BenchmarkShardHashXXHash benchmarks the xxhash shard hash.
func BenchmarkShardHashXXHash(b *testing.B) {
	benchmarkShardHash(b, xxhashShardHash)
}

// BenchmarkShardHashFNV benchmarks the fnv shard hash.
func BenchmarkShardHashFNV(b *testing.B) {
	benchmarkShardHash(b, fnvShardHash)
}

// benchmarkShardHash benchmarks the shard hash.
func benchmarkShardHash(b *testing.B, fn shardHashFunc) {
	b.Helper()

	var sink uint64

	b.ReportAllocs()
	b.ResetTimer()

	// Generate keys on-demand during the benchmark to avoid pre-allocation overhead.
	// This keeps memory usage low and makes the benchmark more realistic.
	for i := range b.N {
		key := fmt.Sprintf("bench-key-%04d-%08d", i, i*i)
		sink += fn(key)
	}
}
