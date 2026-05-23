package kv

import "testing"

// TestMetricsOptionsEqual verifies that metrics options equal.
func TestMetricsOptionsEqual(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name   string
		left   *MetricsOptions
		right  *MetricsOptions
		expect bool
	}{
		{
			name:   "both nil",
			left:   nil,
			right:  nil,
			expect: true,
		},
		{
			name:   "nil vs zero struct",
			left:   nil,
			right:  new(MetricsOptions),
			expect: true,
		},
		{
			name:   "disabled vs disabled",
			left:   &MetricsOptions{Operations: false},
			right:  &MetricsOptions{Operations: false},
			expect: true,
		},
		{
			name:   "enabled vs enabled",
			left:   &MetricsOptions{Operations: true},
			right:  &MetricsOptions{Operations: true},
			expect: true,
		},
		{
			name:   "enabled vs disabled",
			left:   &MetricsOptions{Operations: true},
			right:  &MetricsOptions{Operations: false},
			expect: false,
		},
		{
			name:   "nil vs enabled",
			left:   nil,
			right:  &MetricsOptions{Operations: true},
			expect: false,
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
