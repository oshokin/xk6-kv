package store

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkMemoryStore_RandomKey: measures cost of picking any random key in a large map.
// Compares tracking index vs no-tracking scan path.
func BenchmarkMemoryStore_RandomKey(b *testing.B) {
	trackKeysModes := []bool{true, false}
	for trackKeysIndex := range trackKeysModes {
		trackKeys := trackKeysModes[trackKeysIndex]

		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			b.ReportAllocs()

			const genericKeys = 10_000

			memoryCfg := &MemoryConfig{TrackKeys: trackKeys}
			store := NewMemoryStore(memoryCfg)

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
	trackKeysModes := []bool{true, false}
	for trackKeysIndex := range trackKeysModes {
		trackKeys := trackKeysModes[trackKeysIndex]

		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			b.ReportAllocs()

			memoryCfg := &MemoryConfig{TrackKeys: trackKeys}
			store := NewMemoryStore(memoryCfg)

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

// BenchmarkMemoryStore_RandomKeys_WithPrefix measures batch random sampling from a prefix subset.
// It focuses on two practical regimes:
//   - K << M (small sample from large matching set)
//   - K ~ M (near-full unique sample)
func BenchmarkMemoryStore_RandomKeys_WithPrefix(b *testing.B) {
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

			memoryCfg := &MemoryConfig{TrackKeys: trackKeys}
			store := NewMemoryStore(memoryCfg)

			b.StopTimer()
			seedMemoryStore(b, store, genericKeys, "key-")

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

// BenchmarkMemoryStore_RandomKeys_TrackedShardLookupScaling stresses tracked
// randomKeys on high shard counts to expose keyFromShardRanges lookup behavior.
func BenchmarkMemoryStore_RandomKeys_TrackedShardLookupScaling(b *testing.B) {
	const (
		prefixedKeys = 20_000
		otherKeys    = 5_000
	)

	shardCounts := []int{32, 512, 4096}
	cases := []struct {
		name   string
		count  int64
		unique bool
	}{
		{name: "unique=true", count: 2_000, unique: true},
		{name: "unique=false", count: 2_000, unique: false},
	}

	for shardCountIndex := range shardCounts {
		shardCount := shardCounts[shardCountIndex]

		b.Run(fmt.Sprintf("shards=%d", shardCount), func(b *testing.B) {
			memoryCfg := &MemoryConfig{
				TrackKeys:  true,
				ShardCount: shardCount,
			}
			store := NewMemoryStore(memoryCfg)

			b.StopTimer()
			seedMemoryStore(b, store, prefixedKeys, "pfx-")
			seedMemoryStore(b, store, otherKeys, "other-")

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
					require.Len(b, keys, int(benchmarkCase.count))
				})
			}
		})
	}
}

// BenchmarkMemoryStore_KeyFromShardRanges_LookupStrategies compares linear,
// binary, and hybrid shard-range lookup strategies.
func BenchmarkMemoryStore_KeyFromShardRanges_LookupStrategies(b *testing.B) {
	const (
		prefixedKeys        = 20_000
		offsetsPerIteration = 2_000
	)

	shardCounts := []int{32, 512, 4096}

	for shardCountIndex := range shardCounts {
		shardCount := shardCounts[shardCountIndex]

		b.Run(fmt.Sprintf("shards=%d", shardCount), func(b *testing.B) {
			memoryCfg := &MemoryConfig{
				TrackKeys:  true,
				ShardCount: shardCount,
			}
			store := NewMemoryStore(memoryCfg)

			b.StopTimer()
			seedMemoryStore(b, store, prefixedKeys, "pfx-")
			store.lockAllShardReaders()
			b.Cleanup(store.unlockAllShardReaders)

			ranges, total := buildShardRandomRanges(store.shards, "pfx-")
			require.NotEmpty(b, ranges)
			require.Positive(b, total)

			offsets := make([]int, 0, offsetsPerIteration)
			for i := range offsetsPerIteration {
				offsets = append(offsets, i%total)
			}

			lookups := []struct {
				name string
				fn   func([]shardRandomRange, int) (string, bool)
			}{
				{name: "linear", fn: keyFromShardRangesLinearForBenchmark},
				{name: "binary", fn: keyFromShardRangesBinaryForBenchmark},
				{name: "hybrid", fn: keyFromShardRanges},
			}

			for lookupIndex := range lookups {
				lookup := lookups[lookupIndex]

				b.Run(lookup.name, func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()

					for b.Loop() {
						for _, offset := range offsets {
							key, ok := lookup.fn(ranges, offset)
							if !ok || key == "" {
								b.Fatalf("lookup failed for offset=%d", offset)
							}
						}
					}
				})
			}
		})
	}
}

func keyFromShardRangesLinearForBenchmark(ranges []shardRandomRange, globalOffset int) (string, bool) {
	if globalOffset < 0 || len(ranges) == 0 {
		return "", false
	}

	remaining := globalOffset

	for rangeIndex := range ranges {
		selectedRange := &ranges[rangeIndex]

		rangeWidth := selectedRange.right - selectedRange.left
		if remaining >= rangeWidth {
			remaining -= rangeWidth
			continue
		}

		return selectedRange.shard.ost.Kth(selectedRange.left + remaining)
	}

	return "", false
}

func keyFromShardRangesBinaryForBenchmark(ranges []shardRandomRange, globalOffset int) (string, bool) {
	if globalOffset < 0 || len(ranges) == 0 {
		return "", false
	}

	rangeIndex := sort.Search(len(ranges), func(index int) bool {
		return globalOffset < ranges[index].cumulative
	})
	if rangeIndex >= len(ranges) {
		return "", false
	}

	var previousCumulative int
	if rangeIndex > 0 {
		previousCumulative = ranges[rangeIndex-1].cumulative
	}

	localOffset := globalOffset - previousCumulative
	selectedRange := &ranges[rangeIndex]

	return selectedRange.shard.ost.Kth(selectedRange.left + localOffset)
}
