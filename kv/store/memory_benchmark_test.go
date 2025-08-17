package store

import (
	"fmt"
	"testing"
)

func BenchmarkMemoryStore_Get(b *testing.B) {
	store := NewMemoryStore(true)

	// Setup: Add some data to the store
	for i := range 1000 {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		err := store.Set(key, value)
		if err != nil {
			b.Fatalf("Failed to set up benchmark: %v", err)
		}
	}

	// Reset the timer before the actual benchmark
	b.ResetTimer()

	// Run the benchmark
	for i := range b.N {
		key := fmt.Sprintf("key-%d", i%1000)
		_, _ = store.Get(key)
	}
}

func BenchmarkMemoryStore_Set(b *testing.B) {
	cases := []struct {
		name      string
		trackKeys bool
	}{
		{"WithTrackKeys", true},
		{"WithoutTrackKeys", false},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			store := NewMemoryStore(tc.trackKeys)

			// Reset the timer before the actual benchmark
			b.ResetTimer()

			// Run the benchmark
			for i := range b.N {
				key := fmt.Sprintf("key-%d", i)
				_ = store.Set(key, "value")
			}
		})
	}
}

func BenchmarkMemoryStore_RandomKey(b *testing.B) {
	cases := []struct {
		name      string
		trackKeys bool
	}{
		{"WithTrackKeys", true},
		{"WithoutTrackKeys", false},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			store := NewMemoryStore(tc.trackKeys)

			for i := range 10_000 {
				_ = store.Set(fmt.Sprintf("key-%d", i), "value")
			}

			// Reset the timer before the actual benchmark
			b.ResetTimer()

			// Run the benchmark
			for i := 0; i < b.N; i++ {
				_, _ = store.RandomKey("")
			}
		})
	}
}

func BenchmarkMemoryStore_RandomKey_WithPrefix(b *testing.B) {
	cases := []struct {
		name      string
		trackKeys bool
	}{
		{"WithTrackKeys", true},
		{"WithoutTrackKeys", false},
	}

	for _, tc := range cases {
		// Run the benchmark
		b.Run(tc.name, func(b *testing.B) {
			store := NewMemoryStore(tc.trackKeys)
			// 10k generic + 2k with "pfx-" to simulate a subset
			for i := range 10_000 {
				_ = store.Set(fmt.Sprintf("key-%d", i), "value")

				if i < 2000 {
					_ = store.Set(fmt.Sprintf("pfx-%d", i), "value")
				}
			}

			// Reset the timer before the actual benchmark
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = store.RandomKey("pfx-")
			}
		})
	}
}

func BenchmarkMemoryStore_Delete(b *testing.B) {
	// Run the benchmark multiple times with different store sizes
	benchSizes := []int{10, 100, 1000, 10_000}

	for _, size := range benchSizes {
		b.Run(fmt.Sprintf("Size-%d", size), func(b *testing.B) {
			store := NewMemoryStore(true)

			// Setup: Add data to the store
			for i := range size {
				key := fmt.Sprintf("key-%d", i)
				value := fmt.Sprintf("value-%d", i)
				err := store.Set(key, value)
				if err != nil {
					b.Fatalf("Failed to set up benchmark: %v", err)
				}
			}

			// Reset the timer before the actual benchmark
			b.ResetTimer()

			// Run the benchmark
			for i := range b.N {
				key := fmt.Sprintf("key-%d", i%size)
				_ = store.Delete(key)

				// Re-add the key for the next iteration
				if i < b.N-1 {
					value := fmt.Sprintf("value-%d", i%size)
					_ = store.Set(key, value)
				}
			}
		})
	}
}

func BenchmarkMemoryStore_Exists(b *testing.B) {
	store := NewMemoryStore(true)

	// Setup: Add some data to the store
	for i := range 1000 {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		err := store.Set(key, value)
		if err != nil {
			b.Fatalf("Failed to set up benchmark: %v", err)
		}
	}

	// Reset the timer before the actual benchmark
	b.ResetTimer()

	// Run the benchmark
	for i := range b.N {
		key := fmt.Sprintf("key-%d", i%1000)
		_, _ = store.Exists(key)
	}
}

func BenchmarkMemoryStore_List(b *testing.B) {
	store := NewMemoryStore(true)

	// Setup: Add some data to the store
	for i := range 1000 {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		err := store.Set(key, value)
		if err != nil {
			b.Fatalf("Failed to set up benchmark: %v", err)
		}
	}

	// Add some data with a specific prefix
	for i := range 100 {
		key := fmt.Sprintf("prefix-%d", i)
		value := fmt.Sprintf("value-%d", i)
		err := store.Set(key, value)
		if err != nil {
			b.Fatalf("Failed to set up benchmark: %v", err)
		}
	}

	// Benchmark different List operations
	b.Run("ListAll", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			_, _ = store.List("", 0)
		}
	})

	b.Run("ListWithPrefix", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			_, _ = store.List("prefix", 0)
		}
	})

	b.Run("ListWithLimit", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			_, _ = store.List("", 10)
		}
	})

	b.Run("ListWithPrefixAndLimit", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			_, _ = store.List("prefix", 10)
		}
	})
}

func BenchmarkMemoryStore_Concurrent(b *testing.B) {
	store := NewMemoryStore(true)

	// Setup: Add some data to the store
	for i := range 1000 {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		err := store.Set(key, value)
		if err != nil {
			b.Fatalf("Failed to set up benchmark: %v", err)
		}
	}

	// Reset the timer before the actual benchmark
	b.ResetTimer()

	// Run the benchmark with multiple goroutines
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			// Alternate between Get and Set operations
			if i%2 == 0 {
				key := fmt.Sprintf("key-%d", i%1000)
				_, _ = store.Get(key)
			} else {
				key := fmt.Sprintf("key-%d", i%1000)
				value := fmt.Sprintf("value-%d", i)
				_ = store.Set(key, value)
			}
			i++
		}
	})
}
