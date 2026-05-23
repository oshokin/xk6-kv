package store

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// allocationStatsBenchmarkTotalKeys is the total number of keys
// to seed the benchmark store with.
const allocationStatsBenchmarkTotalKeys = 10_000

// allocationStatsBenchmarkSink is a sink for the allocation stats benchmark.
// It is used to store the allocation stats for the benchmark.
//
//nolint:gochecknoglobals // this is a benchmark sink.
var allocationStatsBenchmarkSink *AllocationStats

// BenchmarkStore_AllocationStats_PrefixCoverageMatrix measures store allocation stats prefix coverage matrix.
func BenchmarkStore_AllocationStats_PrefixCoverageMatrix(b *testing.B) {
	prefixPercents := []int{1, 10, 100}
	prefix := "bench:users:"
	otherPrefix := "bench:other:"

	for _, trackKeys := range []bool{false, true} {
		b.Run(fmt.Sprintf("backend=memory/trackKeys=%v", trackKeys), func(b *testing.B) {
			for _, prefixPercent := range prefixPercents {
				b.Run(fmt.Sprintf("prefix=%d%%", prefixPercent), func(b *testing.B) {
					b.ReportAllocs()

					store := NewMemoryStore(&MemoryConfig{TrackKeys: trackKeys})
					matchingKeys := allocationStatsBenchmarkTotalKeys * prefixPercent / 100

					b.StopTimer()
					seedAllocationStatsBenchmarkStore(
						b,
						store,
						allocationStatsBenchmarkTotalKeys,
						matchingKeys,
						prefix,
						otherPrefix,
					)
					b.StartTimer()
					b.ResetTimer()

					for b.Loop() {
						snapshot, err := store.AllocationStats(prefix)
						if err != nil {
							b.Fatalf("AllocationStats failed: %v", err)
						}

						if snapshot == nil {
							b.Fatal("AllocationStats returned nil snapshot")
						}

						if snapshot.Total != int64(matchingKeys) {
							b.Fatalf(
								"AllocationStats total mismatch: got=%d want=%d",
								snapshot.Total,
								matchingKeys,
							)
						}

						allocationStatsBenchmarkSink = snapshot
					}
				})
			}
		})

		b.Run(fmt.Sprintf("backend=disk/trackKeys=%v", trackKeys), func(b *testing.B) {
			for _, prefixPercent := range prefixPercents {
				b.Run(fmt.Sprintf("prefix=%d%%", prefixPercent), func(b *testing.B) {
					b.ReportAllocs()

					store := newBenchmarkDiskStore(b, trackKeys, "diskstore-bench-allocation-stats-*.db")
					matchingKeys := allocationStatsBenchmarkTotalKeys * prefixPercent / 100

					b.StopTimer()
					seedAllocationStatsBenchmarkStore(
						b,
						store,
						allocationStatsBenchmarkTotalKeys,
						matchingKeys,
						prefix,
						otherPrefix,
					)
					b.StartTimer()
					b.ResetTimer()

					for b.Loop() {
						snapshot, err := store.AllocationStats(prefix)
						if err != nil {
							b.Fatalf("AllocationStats failed: %v", err)
						}

						if snapshot == nil {
							b.Fatal("AllocationStats returned nil snapshot")
						}

						if snapshot.Total != int64(matchingKeys) {
							b.Fatalf(
								"AllocationStats total mismatch: got=%d want=%d",
								snapshot.Total,
								matchingKeys,
							)
						}

						allocationStatsBenchmarkSink = snapshot
					}
				})
			}
		})
	}
}

// BenchmarkMemoryStore_AllocationStats_KeyspaceScaling measures memory store allocation stats keyspace scaling.
func BenchmarkMemoryStore_AllocationStats_KeyspaceScaling(b *testing.B) {
	prefix := "bench:users:"
	otherPrefix := "bench:other:"
	totalKeysVariants := []int{1_000, 10_000, 100_000}

	const matchingPercent = 10

	for _, trackKeys := range []bool{false, true} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			for _, totalKeys := range totalKeysVariants {
				b.Run(fmt.Sprintf("keys=%d/match=%d%%", totalKeys, matchingPercent), func(b *testing.B) {
					b.ReportAllocs()

					store := NewMemoryStore(&MemoryConfig{TrackKeys: trackKeys})
					matchingKeys := totalKeys * matchingPercent / 100

					b.StopTimer()
					seedAllocationStatsBenchmarkStore(
						b,
						store,
						totalKeys,
						matchingKeys,
						prefix,
						otherPrefix,
					)
					b.StartTimer()
					b.ResetTimer()

					for b.Loop() {
						snapshot, err := store.AllocationStats(prefix)
						if err != nil {
							b.Fatalf("AllocationStats failed: %v", err)
						}

						if snapshot == nil {
							b.Fatal("AllocationStats returned nil snapshot")
						}

						if snapshot.Total != int64(matchingKeys) {
							b.Fatalf(
								"AllocationStats total mismatch: got=%d want=%d",
								snapshot.Total,
								matchingKeys,
							)
						}

						allocationStatsBenchmarkSink = snapshot
					}
				})
			}
		})
	}
}

// BenchmarkStore_AllocationStats_10kKeys_NoClaims measures store allocation stats 10k keys no claims.
func BenchmarkStore_AllocationStats_10kKeys_NoClaims(b *testing.B) {
	benchmarkStoreAllocationStatsClaimDensity(b, 0)
}

// BenchmarkStore_AllocationStats_10kKeys_50PercentLiveClaims measures store allocation stats 10k keys 50 percent live claims.
func BenchmarkStore_AllocationStats_10kKeys_50PercentLiveClaims(b *testing.B) {
	benchmarkStoreAllocationStatsClaimDensity(b, 50)
}

// benchmarkStoreAllocationStatsClaimDensity is a test helper for benchmark store allocation stats claim density.
func benchmarkStoreAllocationStatsClaimDensity(b *testing.B, liveClaimPercent int) {
	b.Helper()
	require.GreaterOrEqual(b, liveClaimPercent, 0)
	require.LessOrEqual(b, liveClaimPercent, 100)

	const prefix = "bench:users:"

	type backendFactory struct {
		name     string
		newStore func(b *testing.B, trackKeys bool) Store
	}

	backends := []backendFactory{
		{
			name: "memory",
			newStore: func(_ *testing.B, trackKeys bool) Store {
				return NewMemoryStore(&MemoryConfig{TrackKeys: trackKeys})
			},
		},
		{
			name: "disk",
			newStore: func(b *testing.B, trackKeys bool) Store {
				b.Helper()

				return newBenchmarkDiskStore(b, trackKeys, "diskstore-bench-allocation-stats-density-*.db")
			},
		},
	}

	for _, backend := range backends {
		for _, trackKeys := range []bool{false, true} {
			b.Run(fmt.Sprintf("backend=%s/trackKeys=%v", backend.name, trackKeys), func(b *testing.B) {
				b.ReportAllocs()

				backendStore := backend.newStore(b, trackKeys)

				liveClaimCount := allocationStatsBenchmarkTotalKeys * liveClaimPercent / 100
				expectedClaimable := int64(allocationStatsBenchmarkTotalKeys - liveClaimCount)
				expectedClaimedLive := int64(liveClaimCount)

				b.StopTimer()
				seedAllocationStatsBenchmarkPrefixKeys(b, backendStore, allocationStatsBenchmarkTotalKeys, prefix)
				claimAllocationStatsBenchmarkPrefixKeys(b, backendStore, prefix, liveClaimCount)
				b.StartTimer()
				b.ResetTimer()

				for b.Loop() {
					snapshot, err := backendStore.AllocationStats(prefix)
					if err != nil {
						b.Fatalf("AllocationStats failed: %v", err)
					}

					if snapshot == nil {
						b.Fatal("AllocationStats returned nil snapshot")
					}

					if snapshot.Total != int64(allocationStatsBenchmarkTotalKeys) {
						b.Fatalf(
							"AllocationStats total mismatch: got=%d want=%d",
							snapshot.Total,
							allocationStatsBenchmarkTotalKeys,
						)
					}

					if snapshot.Claimable != expectedClaimable {
						b.Fatalf(
							"AllocationStats claimable mismatch: got=%d want=%d",
							snapshot.Claimable,
							expectedClaimable,
						)
					}

					if snapshot.ClaimedLive != expectedClaimedLive {
						b.Fatalf(
							"AllocationStats claimedLive mismatch: got=%d want=%d",
							snapshot.ClaimedLive,
							expectedClaimedLive,
						)
					}

					allocationStatsBenchmarkSink = snapshot
				}
			})
		}
	}
}

// seedAllocationStatsBenchmarkPrefixKeys is a test helper for seed allocation stats benchmark prefix keys.
func seedAllocationStatsBenchmarkPrefixKeys(b *testing.B, store Store, totalKeys int, prefix string) {
	b.Helper()
	require.Positive(b, totalKeys)

	for i := range totalKeys {
		key := fmt.Sprintf("%s%06d", prefix, i)
		require.NoError(b, store.Set(key, "value"))
	}
}

// claimAllocationStatsBenchmarkPrefixKeys is a test helper for claim allocation stats benchmark prefix keys.
func claimAllocationStatsBenchmarkPrefixKeys(b *testing.B, store Store, prefix string, claimCount int) {
	b.Helper()
	require.GreaterOrEqual(b, claimCount, 0)

	for i := range claimCount {
		key := fmt.Sprintf("%s%06d", prefix, i)
		claim, err := store.ClaimKey(key, &ClaimOptions{TTLMs: 60_000})
		require.NoError(b, err)
		require.NotNil(b, claim)
	}
}

// seedAllocationStatsBenchmarkStore is a test helper for seed allocation stats benchmark store.
func seedAllocationStatsBenchmarkStore(
	b *testing.B,
	store Store,
	totalKeys int,
	matchingKeys int,
	prefix string,
	otherPrefix string,
) {
	b.Helper()

	require.Positive(b, totalKeys)
	require.GreaterOrEqual(b, matchingKeys, 0)
	require.LessOrEqual(b, matchingKeys, totalKeys)

	for i := range matchingKeys {
		key := fmt.Sprintf("%s%06d", prefix, i)
		require.NoError(b, store.Set(key, "value"))
	}

	for i := matchingKeys; i < totalKeys; i++ {
		key := fmt.Sprintf("%s%06d", otherPrefix, i)
		require.NoError(b, store.Set(key, "value"))
	}
}
