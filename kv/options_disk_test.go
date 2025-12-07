package kv

import "testing"

func TestDiskOptionsEqual(t *testing.T) {
	t.Parallel()

	boolTrue := getComparablePointer(true)
	boolFalse := getComparablePointer(false)
	freelistMapUpper := getComparablePointer("MAP")
	freelistMapLower := getComparablePointer("map")

	testCases := []struct {
		name   string
		left   *DiskOptions
		right  *DiskOptions
		expect bool
	}{
		{
			name:   "both nil",
			left:   nil,
			right:  nil,
			expect: true,
		},
		{
			name:   "nil vs empty struct",
			left:   nil,
			right:  new(DiskOptions),
			expect: true,
		},
		{
			name:   "empty struct vs empty struct",
			left:   new(DiskOptions),
			right:  new(DiskOptions),
			expect: true,
		},
		{
			name:   "nil vs explicitly set bool",
			left:   nil,
			right:  &DiskOptions{NoSync: boolFalse},
			expect: false,
		},
		{
			name:   "matching bools",
			left:   &DiskOptions{NoSync: boolTrue},
			right:  &DiskOptions{NoSync: getComparablePointer(true)},
			expect: true,
		},
		{
			name:   "matching mixed timeout types",
			left:   &DiskOptions{Timeout: "1s"},
			right:  &DiskOptions{Timeout: 1000},
			expect: true,
		},
		{
			name:   "matching mixed mmap size types",
			left:   &DiskOptions{InitialMmapSize: "1MiB"},
			right:  &DiskOptions{InitialMmapSize: 1024 * 1024},
			expect: true,
		},
		{
			name:   "normalized freelist types",
			left:   &DiskOptions{FreelistType: freelistMapUpper},
			right:  &DiskOptions{FreelistType: freelistMapLower},
			expect: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := tc.left.Equal(tc.right); got != tc.expect {
				t.Fatalf("left.Equal(right) = %t, want %t", got, tc.expect)
			}

			if got := tc.right.Equal(tc.left); got != tc.expect {
				t.Fatalf("right.Equal(left) = %t, want %t", got, tc.expect)
			}
		})
	}
}

func getComparablePointer[T any](v T) *T {
	return &v
}
