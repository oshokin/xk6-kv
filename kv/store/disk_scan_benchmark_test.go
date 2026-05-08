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

// BenchmarkDiskStore_KeyReadAPIs measures public key-only reads and prefix
// counts. The size matrix keeps the index-vs-cursor trade-off visible when
// changing disk trackKeys behavior.
func BenchmarkDiskStore_KeyReadAPIs(b *testing.B) {
	const (
		scanLimit = 100
		listLimit = 0
	)

	sizes := []struct {
		name           string
		keysPerPrefix  int
		otherKeyPrefix string
	}{
		{name: "small", keysPerPrefix: 100, otherKeyPrefix: "order:"},
		{name: "medium", keysPerPrefix: 10_000, otherKeyPrefix: "order:"},
		{name: "large", keysPerPrefix: 100_000, otherKeyPrefix: "order:"},
	}

	for _, trackKeys := range []bool{true, false} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			for _, size := range sizes {
				b.Run(size.name, func(b *testing.B) {
					store := newBenchmarkDiskStore(b, trackKeys, "diskstore-bench-key-read-*.db")

					b.StopTimer()
					seedDiskStore(b, store, size.keysPerPrefix, "user:")
					seedDiskStore(b, store, size.keysPerPrefix, size.otherKeyPrefix)
					b.StartTimer()

					b.Run("ScanKeysLimit100", func(b *testing.B) {
						b.ReportAllocs()

						for b.Loop() {
							_, _ = store.ScanKeys("user:", "", scanLimit)
						}
					})

					b.Run("ListKeysAllPrefix", func(b *testing.B) {
						b.ReportAllocs()

						for b.Loop() {
							_, _ = store.ListKeys("user:", listLimit)
						}
					})

					b.Run("CountPrefix", func(b *testing.B) {
						b.ReportAllocs()

						for b.Loop() {
							_, _ = store.Count("user:")
						}
					})
				})
			}
		})
	}
}
