package store

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

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
