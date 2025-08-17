package store

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestOSTree_InsertRankSelect(t *testing.T) {
	t.Parallel()

	var (
		keys = []string{"a", "ab", "abc", "b", "ba", "z", "aa", "aaa"}
		got  = make([]string, 0, len(keys))
		tr   = NewOSTree()
	)

	for _, k := range keys {
		tr.Insert(k)
	}

	if tr.Len() != len(unique(keys)) {
		t.Fatalf("Len() mismatch: got %d", tr.Len())
	}

	// Kth should return sorted order (unique)
	sorted := unique(keys)
	sort.Strings(sorted)

	for i := range sorted {
		k, ok := tr.Kth(i)
		if !ok || k != sorted[i] {
			t.Fatalf("Kth(%d) = %q, ok=%v; want %q", i, k, ok, sorted[i])
		}

		got = append(got, k)
	}

	// Explicitly assert the full sequence, so 'got' is actually used
	if !reflect.DeepEqual(got, sorted) {
		t.Fatalf("in-order traversal mismatch: got %v; want %v", got, sorted)
	}

	// Rank should align with sorted positions
	for i, k := range sorted {
		if r := tr.Rank(k); r != i {
			t.Fatalf("Rank(%q)=%d; want %d", k, r, i)
		}
	}

	// Deletions keep order-statistics valid
	tr.Delete("ab")
	tr.Delete("z")

	exp := removeAll(sorted, "ab", "z")
	if tr.Len() != len(exp) {
		t.Fatalf("Len after delete = %d; want %d", tr.Len(), len(exp))
	}

	for i := range exp {
		k, ok := tr.Kth(i)
		if !ok || k != exp[i] {
			t.Fatalf("After delete: Kth(%d)=%q; want %q", i, k, exp[i])
		}
	}
}

func TestOSTree_RangeBounds(t *testing.T) {
	t.Parallel()

	var (
		tr   = NewOSTree()
		keys = []string{"a", "aa", "ab", "aba", "ac", "b", "ba"}
	)

	for _, k := range keys {
		tr.Insert(k)
	}

	sort.Strings(keys)

	assertRange := func(prefix string) {
		var (
			l, r = tr.RangeBounds(prefix)
			want []string
		)

		for _, k := range keys {
			if !strings.HasPrefix(k, prefix) {
				continue
			}

			want = append(want, k)
		}

		if r-l != len(want) {
			t.Fatalf("Range %q -> [%d,%d) size=%d; want %d", prefix, l, r, r-l, len(want))
		}

		for i := 0; i < len(want); i++ {
			k, ok := tr.Kth(l + i)
			if !ok || k != want[i] {
				t.Fatalf("Range %q Kth(%d)=%q; want %q", prefix, l+i, k, want[i])
			}
		}
	}

	assertRange("")
	assertRange("a")
	assertRange("ab")
	assertRange("aba")
	assertRange("ac")
	assertRange("b")
	assertRange("zz")
}

func TestNextPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   string
		want string
	}{
		{"", ""},
		{"a", "b"},
		{"a\xff", "b"},
		{"\xff", ""},
		{"ab", "ac"},
		{"ab\xff\xff", "ac"},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("%q", tc.in), func(t *testing.T) {
			t.Parallel()

			if got := nextPrefix(tc.in); got != tc.want {
				t.Fatalf("nextPrefix(%q)=%q; want %q", tc.in, got, tc.want)
			}
		})
	}
}

func unique[T comparable](in []T) []T {
	m := map[T]struct{}{}

	result := make([]T, 0, len(in))
	for _, s := range in {
		if _, ok := m[s]; ok {
			continue
		}

		m[s] = struct{}{}
		result = append(result, s)
	}

	return result
}

func removeAll[T comparable](src []T, dels ...T) []T {
	rm := map[T]struct{}{}
	for _, d := range dels {
		rm[d] = struct{}{}
	}

	result := make([]T, 0, len(src))
	for _, s := range src {
		if _, exists := rm[s]; exists {
			continue
		}

		result = append(result, s)
	}

	return result
}
