package store

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMemoryStore_BlockMutationsAPI verifies that blockMutations correctly blocks all
// mutation operations with a custom error, and unblockMutations restores normal operation.
func TestMemoryStore_BlockMutationsAPI(t *testing.T) {
	t.Parallel()

	memoryCfg := &MemoryConfig{TrackKeys: true}
	store := NewMemoryStore(memoryCfg)

	require.NoError(t, store.blockMutations(errors.New("custom block")))

	err := store.Set("k1", "v1")
	require.EqualError(t, err, "custom block")

	store.unblockMutations()
	require.NoError(t, store.Set("k1", "v1"))

	require.NoError(t, store.blockMutations(nil))

	err = store.Set("k2", "v2")
	require.ErrorIs(t, err, ErrMutationBlocked)

	store.unblockMutations()
	require.NoError(t, store.Set("k2", "v2"))
}

// TestMemoryStore_BlockMutations_WaitGroupRace stress-tests concurrent blocking
// and mutation operations to detect race conditions in the WaitGroup synchronization.
func TestMemoryStore_BlockMutations_WaitGroupRace(t *testing.T) {
	t.Parallel()

	memoryCfg := &MemoryConfig{TrackKeys: false}
	store := NewMemoryStore(memoryCfg)

	// Trigger many concurrent writes + blocks to hit the race.
	for range 100 {
		var wg sync.WaitGroup

		// Launch 50 concurrent writers.
		for i := range 50 {
			wg.Add(1)

			go func(id int) {
				defer wg.Done()

				for range 10 {
					_ = store.Set(fmt.Sprintf("k%d", id), "v")
				}
			}(i)
		}

		// Concurrently block mutations.
		for range 10 {
			wg.Go(func() {
				_ = store.blockMutations(errors.New("test"))
				store.unblockMutations()
			})
		}

		wg.Wait()
	}
}
