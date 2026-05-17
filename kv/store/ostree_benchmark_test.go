package store

import (
	"fmt"
	"math/rand/v2"
	"testing"
)

const (
	// ostreeBenchmarkKeyCount is the number of keys to benchmark the OSTree.
	ostreeBenchmarkKeyCount = 100_000
	// ostreeClaimedPercentNinety is the percentage of keys that are claimed.
	ostreeClaimedPercentNinety = 90
)

// ostreeBenchmarkMeta is a benchmark metadata type for the OSTree.
type ostreeBenchmarkMeta struct {
	// version is the version of the metadata.
	version int
}

// BenchmarkOSTree_Kth_100k benchmarks the Kth API of the OSTree.
func BenchmarkOSTree_Kth_100k(b *testing.B) {
	keys := buildOSTreeBenchmarkKeys()
	tree := buildOSTreeBenchmarkTree(keys)
	total := tree.Len()

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		idx := rand.IntN(total)
		_, _ = tree.Kth(idx)
	}
}

// BenchmarkOSTree_KthSelectable_100k_90PercentClaimed benchmarks the KthSelectable API of the OSTree.
func BenchmarkOSTree_KthSelectable_100k_90PercentClaimed(b *testing.B) {
	keys := buildOSTreeBenchmarkKeys()
	tree := buildOSTreeBenchmarkTree(keys)
	applyClaimDensity(tree, keys, ostreeClaimedPercentNinety)

	total := tree.SelectableLen()
	if total == 0 {
		b.Fatal("expected selectable keys")
	}

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		idx := rand.IntN(total)
		_, _ = tree.KthSelectable(idx)
	}
}

// BenchmarkOSTree_RangeBounds_100k benchmarks the RangeBounds API of the OSTree.
func BenchmarkOSTree_RangeBounds_100k(b *testing.B) {
	keys := buildOSTreeBenchmarkKeys()
	tree := buildOSTreeBenchmarkTree(keys)
	prefixes := []string{"user:", "user:000", "user:999", "missing:"}

	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		_, _ = tree.RangeBounds(prefixes[i%len(prefixes)])
	}
}

// BenchmarkOSTree_SelectableRangeBounds_100k_90PercentClaimed benchmarks the SelectableRangeBounds API of the OSTree.
func BenchmarkOSTree_SelectableRangeBounds_100k_90PercentClaimed(b *testing.B) {
	keys := buildOSTreeBenchmarkKeys()
	tree := buildOSTreeBenchmarkTree(keys)
	applyClaimDensity(tree, keys, ostreeClaimedPercentNinety)

	prefixes := []string{"user:", "user:000", "user:999", "missing:"}

	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		_, _ = tree.SelectableRangeBounds(prefixes[i%len(prefixes)])
	}
}

// BenchmarkOSTree_UpdateMeta_100k benchmarks the UpdateMeta API of the OSTree.
func BenchmarkOSTree_UpdateMeta_100k(b *testing.B) {
	keys := buildOSTreeBenchmarkKeys()

	tree := NewOSTreeOf[*ostreeBenchmarkMeta]()
	for _, key := range keys {
		tree.InsertWithMeta(key, nil)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		key := keys[i%len(keys)]
		tree.UpdateMeta(key, func(old *ostreeBenchmarkMeta) (*ostreeBenchmarkMeta, bool) {
			if old == nil {
				return &ostreeBenchmarkMeta{version: 1}, false
			}

			old.version++

			return old, false
		})
	}
}

// BenchmarkOSTree_ClearMeta_100k benchmarks the ClearMeta API of the OSTree.
func BenchmarkOSTree_ClearMeta_100k(b *testing.B) {
	keys := buildOSTreeBenchmarkKeys()

	tree := NewOSTreeOf[*ostreeBenchmarkMeta]()
	for _, key := range keys {
		tree.InsertWithMeta(key, nil)
	}

	applyMetaClaimDensity(tree, keys, ostreeClaimedPercentNinety)

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		tree.ClearMeta(nil)

		b.StopTimer()
		applyMetaClaimDensity(tree, keys, ostreeClaimedPercentNinety)
		b.StartTimer()
	}
}

// buildOSTreeBenchmarkKeys builds the keys for the OSTree benchmark.
func buildOSTreeBenchmarkKeys() []string {
	keys := make([]string, ostreeBenchmarkKeyCount)
	for i := range ostreeBenchmarkKeyCount {
		keys[i] = fmt.Sprintf("user:%06d", i)
	}

	return keys
}

// buildOSTreeBenchmarkTree builds the OSTree for the benchmark.
func buildOSTreeBenchmarkTree(keys []string) *OSTreeOf[emptyOSTMeta] {
	tree := NewOSTreeOf[emptyOSTMeta]()
	for _, key := range keys {
		tree.Insert(key)
	}

	return tree
}

// applyClaimDensity applies the claim density to the OSTree.
func applyClaimDensity(tree *OSTreeOf[emptyOSTMeta], keys []string, claimedPercent int) {
	claimed := (len(keys) * claimedPercent) / 100
	for i := range claimed {
		tree.SetSelectable(keys[i], false)
	}
}

// applyMetaClaimDensity applies the meta claim density to the OSTree.
func applyMetaClaimDensity(tree *OSTreeOf[*ostreeBenchmarkMeta], keys []string, claimedPercent int) {
	claimed := (len(keys) * claimedPercent) / 100
	for i := range claimed {
		version := i + 1
		tree.UpdateMeta(keys[i], func(_ *ostreeBenchmarkMeta) (*ostreeBenchmarkMeta, bool) {
			return &ostreeBenchmarkMeta{version: version}, false
		})
	}
}
