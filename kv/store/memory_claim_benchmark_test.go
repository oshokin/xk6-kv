package store

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	// claimAllocationBenchUserKeys is a test const used by surrounding tests.
	claimAllocationBenchUserKeys = 512
	// claimAllocationBenchOtherKeys is a test const used by surrounding tests.
	claimAllocationBenchOtherKeys = 512
	// claimAllocationBenchTTLMs is a test const used by surrounding tests.
	claimAllocationBenchTTLMs = int64(3_600_000)
)

// claimAllocationBenchmarkCase is a test type used by claim allocation benchmark case tests.
type claimAllocationBenchmarkCase struct {
	// prefixLabel holds test state for claim allocation benchmark case.
	prefixLabel string
	// prefix holds test state for claim allocation benchmark case.
	prefix string
	// claimsDensity holds test state for claim allocation benchmark case.
	claimsDensity int
}

// claimAllocationBenchmarkCase.name implements name for claim allocation benchmark case test scenarios.
func (c claimAllocationBenchmarkCase) name() string {
	return fmt.Sprintf("prefix=%s/claims_density=%d%%", c.prefixLabel, c.claimsDensity)
}

// claimAllocationBenchmarkCases is a test helper for claim allocation benchmark cases.
func claimAllocationBenchmarkCases() []claimAllocationBenchmarkCase {
	prefixes := []struct {
		label  string
		prefix string
	}{
		{label: "empty", prefix: ""},
		{label: "user", prefix: "user:"},
	}
	densities := []int{0, 50, 90}

	cases := make([]claimAllocationBenchmarkCase, 0, len(prefixes)*len(densities))

	for prefixIndex := range prefixes {
		prefixCase := prefixes[prefixIndex]

		for densityIndex := range densities {
			density := densities[densityIndex]
			cases = append(cases, claimAllocationBenchmarkCase{
				prefixLabel:   prefixCase.label,
				prefix:        prefixCase.prefix,
				claimsDensity: density,
			})
		}
	}

	return cases
}

// claimNextAllocationBenchmarkCase is a test type used by claimNext allocation benchmark case tests.
type claimNextAllocationBenchmarkCase struct {
	// prefixLabel holds test state for claimNext allocation benchmark case.
	prefixLabel string
	// prefix holds test state for claimNext allocation benchmark case.
	prefix string
	// headClaimedDensity holds test state for claimNext allocation benchmark case.
	headClaimedDensity int
}

// claimNextAllocationBenchmarkCase.name implements name for claimNext allocation benchmark case test scenarios.
func (c claimNextAllocationBenchmarkCase) name() string {
	return fmt.Sprintf("prefix=%s/head_claimed=%d%%", c.prefixLabel, c.headClaimedDensity)
}

// claimNextAllocationBenchmarkCases is a test helper for claimNext allocation benchmark cases.
func claimNextAllocationBenchmarkCases() []claimNextAllocationBenchmarkCase {
	prefixes := []struct {
		label  string
		prefix string
	}{
		{label: "empty", prefix: ""},
		{label: "user", prefix: "user:"},
	}
	densities := []int{0, 50, 90}

	cases := make([]claimNextAllocationBenchmarkCase, 0, len(prefixes)*len(densities))

	for prefixIndex := range prefixes {
		prefixCase := prefixes[prefixIndex]

		for densityIndex := range densities {
			density := densities[densityIndex]
			cases = append(cases, claimNextAllocationBenchmarkCase{
				prefixLabel:        prefixCase.label,
				prefix:             prefixCase.prefix,
				headClaimedDensity: density,
			})
		}
	}

	return cases
}

// seedClaimBenchmarkMemoryStore is a test helper for seed claim benchmark memory store.
func seedClaimBenchmarkMemoryStore(b *testing.B, s *MemoryStore) {
	b.Helper()

	seedMemoryStore(b, s, claimAllocationBenchUserKeys, "user:")
	seedMemoryStore(b, s, claimAllocationBenchOtherKeys, "order:")
}

// seedOrderedClaimBenchmarkMemoryStore seeds sortable keys for ordered claim benchmarks.
func seedOrderedClaimBenchmarkMemoryStore(b *testing.B, s *MemoryStore) {
	b.Helper()

	for index := range claimAllocationBenchUserKeys {
		key := fmt.Sprintf("user:%06d", index+1)
		value := fmt.Sprintf("value-%06d", index+1)
		require.NoErrorf(b, s.Set(key, value), "seed Set(%q) must succeed", key)
	}

	for index := range claimAllocationBenchOtherKeys {
		key := fmt.Sprintf("order:%06d", index+1)
		value := fmt.Sprintf("value-%06d", index+1)
		require.NoErrorf(b, s.Set(key, value), "seed Set(%q) must succeed", key)
	}
}

// preclaimBenchmarkDensity is a test helper for preclaim benchmark density.
func preclaimBenchmarkDensity(b *testing.B, s Store, prefix string, claimsDensity int) {
	b.Helper()

	if claimsDensity <= 0 {
		return
	}

	total, err := s.Count(prefix)
	require.NoError(b, err)
	require.Positive(b, total)

	toClaim := int((total * int64(claimsDensity)) / 100)
	if toClaim <= 0 {
		return
	}

	claimOptions := &ClaimOptions{
		Prefix: prefix,
		Owner:  "benchmark-seed",
		TTLMs:  claimAllocationBenchTTLMs,
	}

	for i := range toClaim {
		claim, claimErr := s.ClaimRandom(claimOptions)
		require.NoError(b, claimErr)
		require.NotNilf(b, claim, "preclaim returned nil at step %d/%d", i+1, toClaim)
	}
}

// preclaimOrderedHeadDensity claims the lexicographic head of the selected keyspace.
func preclaimOrderedHeadDensity(b *testing.B, s Store, prefix string, headClaimedDensity int) {
	b.Helper()

	if headClaimedDensity <= 0 {
		return
	}

	keys, err := s.ListKeys(prefix, 0)
	require.NoError(b, err)
	require.NotEmpty(b, keys)

	toClaim := (len(keys) * headClaimedDensity) / 100
	if toClaim <= 0 {
		return
	}

	claimOptions := &ClaimOptions{
		Owner: "benchmark-seed",
		TTLMs: claimAllocationBenchTTLMs,
	}

	for i := range toClaim {
		claim, claimErr := s.ClaimKey(keys[i], claimOptions)
		require.NoError(b, claimErr)
		require.NotNilf(b, claim, "preclaim returned nil at step %d/%d", i+1, toClaim)
	}
}

// BenchmarkMemoryStore_ClaimRandom_AllocationMatrix measures ClaimRandom
// allocation/latency shape across trackKeys, prefix, and claim density.
func BenchmarkMemoryStore_ClaimRandom_AllocationMatrix(b *testing.B) {
	trackKeysModes := []bool{true, false}
	cases := claimAllocationBenchmarkCases()

	for trackKeysIndex := range trackKeysModes {
		trackKeys := trackKeysModes[trackKeysIndex]

		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			for caseIndex := range cases {
				benchmarkCase := cases[caseIndex]

				b.Run(benchmarkCase.name(), func(b *testing.B) {
					b.ReportAllocs()

					memoryCfg := &MemoryConfig{TrackKeys: trackKeys}
					store := NewMemoryStore(memoryCfg)

					b.StopTimer()
					seedClaimBenchmarkMemoryStore(b, store)
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

// BenchmarkMemoryStore_ClaimNext_AllocationMatrix measures ClaimNext latency
// across trackKeys, prefix, and head-claimed density.
func BenchmarkMemoryStore_ClaimNext_AllocationMatrix(b *testing.B) {
	trackKeysModes := []bool{true, false}
	cases := claimNextAllocationBenchmarkCases()

	for trackKeysIndex := range trackKeysModes {
		trackKeys := trackKeysModes[trackKeysIndex]

		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			for caseIndex := range cases {
				benchmarkCase := cases[caseIndex]

				b.Run(benchmarkCase.name(), func(b *testing.B) {
					b.ReportAllocs()

					memoryCfg := &MemoryConfig{TrackKeys: trackKeys}
					store := NewMemoryStore(memoryCfg)

					b.StopTimer()
					seedOrderedClaimBenchmarkMemoryStore(b, store)
					preclaimOrderedHeadDensity(
						b,
						store,
						benchmarkCase.prefix,
						benchmarkCase.headClaimedDensity,
					)

					claimOptions := &ClaimOptions{
						Prefix: benchmarkCase.prefix,
						TTLMs:  claimAllocationBenchTTLMs,
					}

					b.StartTimer()
					b.ResetTimer()

					for b.Loop() {
						claim, err := store.ClaimNext(claimOptions)
						if err != nil {
							b.Fatalf("ClaimNext failed: %v", err)
						}

						if claim == nil {
							b.Fatalf("ClaimNext returned nil for case %s", benchmarkCase.name())
						}

						b.StopTimer()

						released, err := store.ReleaseClaim(claim.Ref())
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

// BenchmarkMemoryStore_PopRandom_AllocationMatrix measures PopRandom
// allocation/latency shape across trackKeys, prefix, and claim density.
func BenchmarkMemoryStore_PopRandom_AllocationMatrix(b *testing.B) {
	trackKeysModes := []bool{true, false}
	cases := claimAllocationBenchmarkCases()

	for trackKeysIndex := range trackKeysModes {
		trackKeys := trackKeysModes[trackKeysIndex]

		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			for caseIndex := range cases {
				benchmarkCase := cases[caseIndex]

				b.Run(benchmarkCase.name(), func(b *testing.B) {
					b.ReportAllocs()

					memoryCfg := &MemoryConfig{TrackKeys: trackKeys}
					store := NewMemoryStore(memoryCfg)

					b.StopTimer()
					seedClaimBenchmarkMemoryStore(b, store)
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
