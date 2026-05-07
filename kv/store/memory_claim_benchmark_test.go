package store

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	claimAllocationBenchUserKeys  = 512
	claimAllocationBenchOtherKeys = 512
	claimAllocationBenchTTLMs     = int64(3_600_000)
)

type claimAllocationBenchmarkCase struct {
	prefixLabel   string
	prefix        string
	claimsDensity int
}

func (c claimAllocationBenchmarkCase) name() string {
	return fmt.Sprintf("prefix=%s/claims_density=%d%%", c.prefixLabel, c.claimsDensity)
}

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

func seedClaimBenchmarkMemoryStore(b *testing.B, s *MemoryStore) {
	b.Helper()

	seedMemoryStore(b, s, claimAllocationBenchUserKeys, "user:")
	seedMemoryStore(b, s, claimAllocationBenchOtherKeys, "order:")
}

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
