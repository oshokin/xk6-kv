package store

import (
	"fmt"
	"testing"
)

// BenchmarkDiskStore_Scan measures paginated scans across large datasets with and without tracking.
func BenchmarkDiskStore_Scan(b *testing.B) {
	const (
		totalPerPrefix = 10_000
		pageLimit      = 100
		resumeLimit    = 32
	)

	prefixes := []string{"user:", "order:", "misc:"}

	for _, trackKeys := range []bool{true, false} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			store := newBenchmarkDiskStore(b, trackKeys, "diskstore-bench-scan-*.db")

			b.StopTimer()

			for _, prefix := range prefixes {
				seedDiskStore(b, store, totalPerPrefix, prefix)
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
