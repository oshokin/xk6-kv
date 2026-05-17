package store

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

const expiredClaimsBenchKeys = 100_000

// BenchmarkDiskStore_ClaimRandom_AllocationMatrix measures ClaimRandom
// allocation/latency shape across trackKeys, prefix, and claim density.
func BenchmarkDiskStore_ClaimRandom_AllocationMatrix(b *testing.B) {
	trackKeysModes := []bool{true, false}
	cases := claimAllocationBenchmarkCases()

	for trackKeysIndex := range trackKeysModes {
		trackKeys := trackKeysModes[trackKeysIndex]

		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			for caseIndex := range cases {
				benchmarkCase := cases[caseIndex]

				b.Run(benchmarkCase.name(), func(b *testing.B) {
					b.ReportAllocs()

					store := newBenchmarkDiskStore(b, trackKeys, "diskstore-bench-claim-random-allocation-*.db")

					b.StopTimer()
					seedClaimBenchmarkDiskStore(b, store)
					preclaimBenchmarkDensity(b, store, benchmarkCase.prefix, benchmarkCase.claimsDensity)

					claimOptions := &ClaimOptions{
						Prefix: benchmarkCase.prefix,
						TTLMs:  claimAllocationBenchTTLMs,
					}

					b.StartTimer()
					b.ResetTimer()

					for b.Loop() {
						claim, err := store.ClaimRandom(claimOptions)
						if err != nil {
							b.Fatalf("ClaimRandom failed: %v", err)
						}

						if claim == nil {
							b.Fatalf("ClaimRandom returned nil for case %s", benchmarkCase.name())
						}

						b.StopTimer()

						released, err := store.ReleaseClaim(&ClaimRef{
							ID:    claim.ID,
							Key:   claim.Key,
							Token: claim.Token,
						})
						if err != nil {
							b.Fatalf("ReleaseClaim failed: %v", err)
						}

						if !released {
							b.Fatalf("ReleaseClaim returned false for claim %s", claim.ID)
						}

						b.StartTimer()
					}
				})
			}
		})
	}
}

// BenchmarkDiskStore_ClaimRandom_ExpiredClaims measures allocation latency when
// many stale leases exist and full cleanup is throttled out of the hot path.
func BenchmarkDiskStore_ClaimRandom_ExpiredClaims(b *testing.B) {
	for _, trackKeys := range []bool{true, false} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			b.ReportAllocs()

			store := newBenchmarkDiskStore(b, trackKeys, "diskstore-bench-claim-random-expired-*.db")

			b.StopTimer()
			seedExpiredClaimBenchmarkDiskStore(b, store, expiredClaimsBenchKeys, "key:")

			claimOptions := &ClaimOptions{
				Prefix: "key:",
				TTLMs:  claimAllocationBenchTTLMs,
			}

			b.StartTimer()
			b.ResetTimer()

			for b.Loop() {
				claim, err := store.ClaimRandom(claimOptions)
				if err != nil {
					b.Fatalf("ClaimRandom failed: %v", err)
				}

				if claim == nil {
					b.Fatal("ClaimRandom returned nil")
				}

				b.StopTimer()

				released, err := store.ReleaseClaim(claim.Ref())
				if err != nil {
					b.Fatalf("ReleaseClaim failed: %v", err)
				}

				if !released {
					b.Fatalf("ReleaseClaim returned false for claim %s", claim.ID)
				}

				store.lastClaimsCleanupUnixMilli.Store(time.Now().UnixMilli())
				b.StartTimer()
			}
		})
	}
}

// BenchmarkDiskStore_PopRandom_ExpiredClaims measures pop latency when many
// stale leases exist and full cleanup is throttled out of the hot path.
func BenchmarkDiskStore_PopRandom_ExpiredClaims(b *testing.B) {
	for _, trackKeys := range []bool{true, false} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			b.ReportAllocs()

			store := newBenchmarkDiskStore(b, trackKeys, "diskstore-bench-pop-random-expired-*.db")

			b.StopTimer()
			seedExpiredClaimBenchmarkDiskStore(b, store, expiredClaimsBenchKeys, "key:")

			b.StartTimer()
			b.ResetTimer()

			for b.Loop() {
				entry, err := store.PopRandom("key:")
				if err != nil {
					b.Fatalf("PopRandom failed: %v", err)
				}

				if entry == nil {
					b.Fatal("PopRandom returned nil")
				}

				b.StopTimer()

				if err := store.Set(entry.Key, entry.Value); err != nil {
					b.Fatalf("Set failed after PopRandom: %v", err)
				}

				store.lastClaimsCleanupUnixMilli.Store(time.Now().UnixMilli())
				b.StartTimer()
			}
		})
	}
}

// BenchmarkDiskStore_PopRandom_AllocationMatrix measures PopRandom
// allocation/latency shape across trackKeys, prefix, and claim density.
func BenchmarkDiskStore_PopRandom_AllocationMatrix(b *testing.B) {
	trackKeysModes := []bool{true, false}
	cases := claimAllocationBenchmarkCases()

	for trackKeysIndex := range trackKeysModes {
		trackKeys := trackKeysModes[trackKeysIndex]

		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			for caseIndex := range cases {
				benchmarkCase := cases[caseIndex]

				b.Run(benchmarkCase.name(), func(b *testing.B) {
					b.ReportAllocs()

					store := newBenchmarkDiskStore(b, trackKeys, "diskstore-bench-pop-random-allocation-*.db")

					b.StopTimer()
					seedClaimBenchmarkDiskStore(b, store)
					preclaimBenchmarkDensity(b, store, benchmarkCase.prefix, benchmarkCase.claimsDensity)

					b.StartTimer()
					b.ResetTimer()

					for b.Loop() {
						entry, err := store.PopRandom(benchmarkCase.prefix)
						if err != nil {
							b.Fatalf("PopRandom failed: %v", err)
						}

						if entry == nil {
							b.Fatalf("PopRandom returned nil for case %s", benchmarkCase.name())
						}

						b.StopTimer()

						if err := store.Set(entry.Key, entry.Value); err != nil {
							b.Fatalf("Set failed after PopRandom: %v", err)
						}

						b.StartTimer()
					}
				})
			}
		})
	}
}

// BenchmarkDiskStore_ClaimRandomMany_TrackKeysTrue_Count1 measures ClaimRandomMany
// allocation/latency shape when claiming 1 entry.
func BenchmarkDiskStore_ClaimRandomMany_TrackKeysTrue_Count1(b *testing.B) {
	benchmarkDiskStoreClaimRandomManyTrackKeysTrue(b, 1)
}

// BenchmarkDiskStore_ClaimRandomMany_TrackKeysTrue_Count10 measures ClaimRandomMany
// allocation/latency shape when claiming 10 entries.
func BenchmarkDiskStore_ClaimRandomMany_TrackKeysTrue_Count10(b *testing.B) {
	benchmarkDiskStoreClaimRandomManyTrackKeysTrue(b, 10)
}

// BenchmarkDiskStore_ClaimRandomMany_TrackKeysTrue_Count100 measures ClaimRandomMany
// allocation/latency shape when claiming 100 entries.
func BenchmarkDiskStore_ClaimRandomMany_TrackKeysTrue_Count100(b *testing.B) {
	benchmarkDiskStoreClaimRandomManyTrackKeysTrue(b, 100)
}

// BenchmarkDiskStore_ClaimRandomMany_TrackKeysTrue_Count1000 measures ClaimRandomMany
// allocation/latency shape when claiming 1000 entries.
func BenchmarkDiskStore_ClaimRandomMany_TrackKeysTrue_Count1000(b *testing.B) {
	benchmarkDiskStoreClaimRandomManyTrackKeysTrue(b, 1_000)
}

// BenchmarkDiskStore_ClaimRandomMany_TrackKeysTrue_Parallel measures ClaimRandomMany
// allocation/latency shape when claiming 1000 entries in parallel.
func BenchmarkDiskStore_ClaimRandomMany_TrackKeysTrue_Parallel(b *testing.B) {
	b.ReportAllocs()

	store := newBenchmarkDiskStore(b, true, "diskstore-bench-claim-random-many-trackkeys-true-parallel-*.db")

	b.StopTimer()
	seedDiskStore(b, store, 8_192, "user:")

	claimOptions := &ClaimManyOptions{
		Prefix: "user:",
		Count:  10,
		TTLMs:  claimAllocationBenchTTLMs,
	}

	b.StartTimer()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			claims, _ := store.ClaimRandomMany(claimOptions)
			for _, claim := range claims {
				_, _ = store.ReleaseClaim(claim.Ref())
			}
		}
	})
}

// BenchmarkDiskStore_RenewClaim_TrackKeysTrue_HeavyRenew measures RenewClaim
// allocation/latency shape when renewing a claim.
func BenchmarkDiskStore_RenewClaim_TrackKeysTrue_HeavyRenew(b *testing.B) {
	b.ReportAllocs()

	store := newBenchmarkDiskStore(b, true, "diskstore-bench-renew-claim-trackkeys-true-heavy-renew-*.db")

	b.StopTimer()
	seedDiskStore(b, store, 1, "user:")

	claim, err := store.ClaimKey("user:0", &ClaimOptions{
		Owner: "bench-renew",
		TTLMs: claimAllocationBenchTTLMs,
	})
	if err != nil {
		b.Fatalf("ClaimKey failed: %v", err)
	}

	if claim == nil {
		b.Fatal("ClaimKey returned nil")
	}

	ref := claim.Ref()
	renewOpts := &RenewClaimOptions{TTLMs: claimAllocationBenchTTLMs}

	b.StartTimer()
	b.ResetTimer()

	for b.Loop() {
		renewed, renewErr := store.RenewClaim(ref, renewOpts)
		if renewErr != nil {
			b.Fatalf("RenewClaim failed: %v", renewErr)
		}

		if !renewed {
			b.Fatal("RenewClaim returned false")
		}
	}
}

// seedClaimBenchmarkDiskStore seeds the benchmark disk store with user and other keys.
func seedClaimBenchmarkDiskStore(b *testing.B, s *DiskStore) {
	b.Helper()

	seedDiskStore(b, s, claimAllocationBenchUserKeys, "user:")
	seedDiskStore(b, s, claimAllocationBenchOtherKeys, "order:")
}

// seedExpiredClaimBenchmarkDiskStore seeds the benchmark disk store with expired claims.
func seedExpiredClaimBenchmarkDiskStore(b *testing.B, s *DiskStore, totalKeys int, prefix string) {
	b.Helper()

	seedDiskStore(b, s, totalKeys, prefix)

	now := time.Now().UnixMilli()
	err := s.handle.Update(func(tx *bolt.Tx) error {
		claimsBucket, err := s.ensureClaimsBucket(tx)
		if err != nil {
			return err
		}

		for index := range totalKeys {
			token := int64(index + 1)
			key := fmt.Sprintf("%s%d", prefix, index)

			if err := s.putDiskClaimTx(claimsBucket, &EntryClaim{
				ID:        claimIDFromToken(token),
				Key:       key,
				Token:     token,
				ExpiresAt: now - 1,
			}); err != nil {
				return err
			}
		}

		return nil
	})
	require.NoError(b, err)

	s.claimToken.Store(int64(totalKeys))
	s.lastClaimsCleanupUnixMilli.Store(now)
}

// benchmarkDiskStoreClaimRandomManyTrackKeysTrue benchmarks ClaimRandomMany
// allocation/latency shape when claiming entries in tracked mode.
func benchmarkDiskStoreClaimRandomManyTrackKeysTrue(b *testing.B, count int64) {
	b.Helper()
	b.ReportAllocs()

	store := newBenchmarkDiskStore(
		b,
		true,
		fmt.Sprintf("diskstore-bench-claim-random-many-trackkeys-true-count-%d-*.db", count),
	)

	totalKeys := claimAllocationBenchUserKeys

	minRequired := int(count) * 2
	if totalKeys < minRequired {
		totalKeys = minRequired
	}

	if totalKeys < 2_048 {
		totalKeys = 2_048
	}

	b.StopTimer()
	seedDiskStore(b, store, totalKeys, "user:")

	claimOptions := &ClaimManyOptions{
		Prefix: "user:",
		Count:  count,
		TTLMs:  claimAllocationBenchTTLMs,
	}

	b.StartTimer()
	b.ResetTimer()

	for b.Loop() {
		claims, err := store.ClaimRandomMany(claimOptions)
		if err != nil {
			b.Fatalf("ClaimRandomMany failed: %v", err)
		}

		if len(claims) == 0 {
			b.Fatal("ClaimRandomMany returned zero claims")
		}

		b.StopTimer()

		for _, claim := range claims {
			released, releaseErr := store.ReleaseClaim(claim.Ref())
			if releaseErr != nil {
				b.Fatalf("ReleaseClaim failed: %v", releaseErr)
			}

			if !released {
				b.Fatalf("ReleaseClaim returned false for claim %s", claim.ID)
			}
		}

		b.StartTimer()
	}
}
