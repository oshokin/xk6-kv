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
			memoryCfg := &MemoryConfig{TrackKeys: trackKeys}
			store := NewMemoryStore(memoryCfg)

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

// BenchmarkMemoryScanKeys_NoTracking isolates key-only pagination costs in memory mode without tracking.
func BenchmarkMemoryScanKeys_NoTracking(b *testing.B) {
	const (
		totalPerPrefix = 10_000
		pageLimit      = 100
		resumeLimit    = 32
	)

	prefixes := []string{"user:", "order:", "misc:"}

	memoryCfg := &MemoryConfig{TrackKeys: false}
	store := NewMemoryStore(memoryCfg)

	b.StopTimer()

	for _, prefix := range prefixes {
		seedMemoryStore(b, store, totalPerPrefix, prefix)
	}

	b.StartTimer()

	b.Run("PrefixUserLimit100", func(b *testing.B) {
		runScanKeysBenchmark(b, store, "user:", "", pageLimit)
	})

	b.Run("PrefixUserUnlimited", func(b *testing.B) {
		runScanKeysBenchmark(b, store, "user:", "", 0)
	})

	b.Run("PrefixOrderResume", func(b *testing.B) {
		startAfter := fmt.Sprintf("order:%05d", totalPerPrefix/2)
		runScanKeysBenchmark(b, store, "order:", startAfter, resumeLimit)
	})
}

// BenchmarkMemoryScanKeys_NoTracking_Unlimited_100k measures memory scan keys no tracking unlimited 100k.
func BenchmarkMemoryScanKeys_NoTracking_Unlimited_100k(b *testing.B) {
	const totalPerPrefix = 100_000

	store := NewMemoryStore(&MemoryConfig{TrackKeys: false})

	b.StopTimer()
	seedMemoryStore(b, store, totalPerPrefix, "user:")
	b.StartTimer()

	runScanKeysBenchmark(b, store, "user:", "", 0)
}

// BenchmarkMemoryListKeys_NoTracking measures listKeys() costs in memory mode without tracking.
func BenchmarkMemoryListKeys_NoTracking(b *testing.B) {
	const totalPerPrefix = 10_000

	prefixes := []string{"user:", "order:", "misc:"}

	memoryCfg := &MemoryConfig{TrackKeys: false}
	store := NewMemoryStore(memoryCfg)

	b.StopTimer()

	for _, prefix := range prefixes {
		seedMemoryStore(b, store, totalPerPrefix, prefix)
	}

	b.StartTimer()

	b.Run("PrefixUserLimit100", func(b *testing.B) {
		runListKeysBenchmark(b, store, "user:", 100)
	})

	b.Run("PrefixUserUnlimited", func(b *testing.B) {
		runListKeysBenchmark(b, store, "user:", 0)
	})

	b.Run("AllLimit100", func(b *testing.B) {
		runListKeysBenchmark(b, store, "", 100)
	})
}

// runScanKeysBenchmark runs scan keys benchmark.
func runScanKeysBenchmark(b *testing.B, store Store, prefix, initialAfter string, limit int64) {
	b.Helper()
	b.ReportAllocs()

	for b.Loop() {
		after := initialAfter

		for {
			page, err := store.ScanKeys(prefix, after, limit)
			if err != nil {
				b.Fatalf("scan keys failed: %v", err)
			}

			if len(page.Keys) == 0 || page.NextKey == "" {
				break
			}

			after = page.NextKey
		}
	}
}

// runListKeysBenchmark runs list keys benchmark.
func runListKeysBenchmark(b *testing.B, store Store, prefix string, limit int64) {
	b.Helper()
	b.ReportAllocs()

	for b.Loop() {
		_, err := store.ListKeys(prefix, limit)
		if err != nil {
			b.Fatalf("list keys failed: %v", err)
		}
	}
}
