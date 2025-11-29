package store

import (
	"fmt"
	"testing"
)

// BenchmarkMemoryStore_Scan measures paginated scans across large datasets with and without tracking.
func BenchmarkMemoryStore_Scan(b *testing.B) {
	const (
		totalPerPrefix = 10_000
		pageLimit      = 100
		resumeLimit    = 32
	)

	prefixes := []string{"user:", "order:", "misc:"}

	for _, trackKeys := range []bool{true, false} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			store := NewMemoryStore(trackKeys, 0)

			b.StopTimer()

			for _, prefix := range prefixes {
				seedMemoryStore(b, store, totalPerPrefix, prefix)
			}

			b.StartTimer()

			b.Run("PrefixUserLimit100", func(b *testing.B) {
				runScanBenchmark(b, store, "user:", "", pageLimit)
			})

			b.Run("PrefixUserUnlimited", func(b *testing.B) {
				runScanBenchmark(b, store, "user:", "", 0)
			})

			b.Run("PrefixOrderResume", func(b *testing.B) {
				startAfter := fmt.Sprintf("order:%05d", totalPerPrefix/2)
				runScanBenchmark(b, store, "order:", startAfter, resumeLimit)
			})
		})
	}
}

// runScanBenchmark runs a scan benchmark.
func runScanBenchmark(b *testing.B, store Store, prefix, initialAfter string, limit int64) {
	b.Helper()
	b.ReportAllocs()

	for b.Loop() {
		after := initialAfter

		for {
			page, err := store.Scan(prefix, after, limit)
			if err != nil {
				b.Fatalf("scan failed: %v", err)
			}

			if len(page.Entries) == 0 || page.NextKey == "" {
				break
			}

			after = page.NextKey
		}
	}
}
