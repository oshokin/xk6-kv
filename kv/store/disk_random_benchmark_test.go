package store

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkDiskStore_RandomKey: cost of picking any random key in a large dataset.
// Compares tracking index vs no-tracking path.
func BenchmarkDiskStore_RandomKey(b *testing.B) {
	trackKeysModes := []bool{true, false}
	for trackKeysIndex := range trackKeysModes {
		trackKeys := trackKeysModes[trackKeysIndex]

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
	trackKeysModes := []bool{true, false}
	for trackKeysIndex := range trackKeysModes {
		trackKeys := trackKeysModes[trackKeysIndex]

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

// BenchmarkDiskStore_RandomKeys_WithPrefix measures batch random sampling from a prefix subset.
// It focuses on two practical regimes:
//   - K << M (small sample from large matching set)
//   - K ~ M (near-full unique sample)
func BenchmarkDiskStore_RandomKeys_WithPrefix(b *testing.B) {
	const (
		genericKeys  = 10_000
		prefixedKeys = 10_000
	)

	cases := []struct {
		name   string
		count  int64
		unique bool
	}{
		{name: "K<<M/unique=true", count: 10, unique: true},
		{name: "K~M/unique=true", count: prefixedKeys - 100, unique: true},
		{name: "K<<M/unique=false", count: 10, unique: false},
	}

	trackKeysModes := []bool{true, false}
	for trackKeysIndex := range trackKeysModes {
		trackKeys := trackKeysModes[trackKeysIndex]

		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			b.ReportAllocs()

			store := newBenchmarkDiskStore(b, trackKeys, "diskstore-bench-random-keys-with-prefix-*.db")

			b.StopTimer()
			seedDiskStore(b, store, genericKeys, "key-")

			for index := range prefixedKeys {
				require.NoError(b, store.Set(fmt.Sprintf("pfx-%d", index), "value"))
			}

			for caseIndex := range cases {
				benchmarkCase := cases[caseIndex]
				b.Run(benchmarkCase.name, func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()

					var keys []string

					for b.Loop() {
						keys, _ = store.RandomKeys("pfx-", benchmarkCase.count, benchmarkCase.unique)
					}

					b.StopTimer()
					require.NotEmpty(b, keys)
				})
			}
		})
	}
}
