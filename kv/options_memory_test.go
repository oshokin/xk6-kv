package kv

import "testing"

func TestMemoryOptionsEqual(t *testing.T) {
	t.Parallel()

	auto := &MemoryOptions{ShardCount: 0}
	negativeAuto := &MemoryOptions{ShardCount: -1}
	sharded := &MemoryOptions{ShardCount: 128}

	testCases := []struct {
		name   string
		left   *MemoryOptions
		right  *MemoryOptions
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
			right:  new(MemoryOptions),
			expect: true,
		},
		{
			name:   "nil vs auto shard count",
			left:   nil,
			right:  auto,
			expect: true,
		},
		{
			name:   "auto equivalents",
			left:   auto,
			right:  negativeAuto,
			expect: true,
		},
		{
			name:   "different shard counts",
			left:   sharded,
			right:  &MemoryOptions{ShardCount: 64},
			expect: false,
		},
		{
			name:   "matching shard counts",
			left:   sharded,
			right:  &MemoryOptions{ShardCount: 128},
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
