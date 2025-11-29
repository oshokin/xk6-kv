package store

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkMemoryStore_RandomKey: measures cost of picking any random key in a large map.
// Compares tracking index vs no-tracking scan path.
func BenchmarkMemoryStore_RandomKey(b *testing.B) {
	for _, trackKeys := range []bool{true, false} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			b.ReportAllocs()

			const genericKeys = 10_000

			store := NewMemoryStore(trackKeys, 0)

			b.StopTimer()
			seedMemoryStore(b, store, genericKeys, "key-")
			b.StartTimer()

			for b.Loop() {
				_, _ = store.RandomKey("")
			}
		})
	}
}

// BenchmarkMemoryStore_RandomKey_WithPrefix: measures cost of selecting random keys from a subset.
// Compares indexed prefix selection (tracking) vs two-pass scan (no tracking).
func BenchmarkMemoryStore_RandomKey_WithPrefix(b *testing.B) {
	for _, trackKeys := range []bool{true, false} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			b.ReportAllocs()

			store := NewMemoryStore(trackKeys, 0)

			// Seed 10k generic + 2k with "pfx-" to simulate a dense subset.
			b.StopTimer()

			seedMemoryStore(b, store, 10_000, "key-")

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
