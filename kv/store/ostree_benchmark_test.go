package store

import (
	"fmt"
	"math/rand/v2"
	"testing"
)

func BenchmarkOSTree_Insert(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			keys := makeKeys(n)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tr := NewOSTree()
				for _, k := range keys {
					tr.Insert(k)
				}
			}
		})
	}
}

func BenchmarkOSTree_Rank(b *testing.B) {
	tr := NewOSTree()
	keys := makeKeys(100_000)
	for _, k := range keys {
		tr.Insert(k)
	}
	qs := makeKeys(50_000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := qs[i%len(qs)]
		_ = tr.Rank(k)
	}
}

func BenchmarkOSTree_Kth(b *testing.B) {
	tr := NewOSTree()
	keys := makeKeys(100_000)
	for _, k := range keys {
		tr.Insert(k)
	}
	n := tr.Len()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := rand.IntN(n) //nolint:gosec
		_, _ = tr.Kth(idx)
	}
}

func BenchmarkOSTree_RangeBounds(b *testing.B) {
	tr := NewOSTree()
	keys := makeKeys(100_000)
	for _, k := range keys {
		tr.Insert(k)
	}
	prefixes := []string{"a", "ab", "user:", "zzz", ""}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := prefixes[i%len(prefixes)]
		_, _ = tr.RangeBounds(p)
	}
}

func makeKeys(n int) []string {
	keys := make([]string, n)
	for i := range n {
		// Simple alpha keys with some clustered prefixes
		switch i % 4 {
		case 0:
			keys[i] = fmt.Sprintf("a:%08d", i)
		case 1:
			keys[i] = fmt.Sprintf("ab:%08d", i)
		case 2:
			keys[i] = fmt.Sprintf("user:%08d", i)
		default:
			keys[i] = fmt.Sprintf("z:%08d", i)
		}
	}

	return keys
}
