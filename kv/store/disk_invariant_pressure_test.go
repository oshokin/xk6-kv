package store

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDiskStore_SetCASDeleteRandomKey_TrackKeysConsistency verifies that the store
// can set, compare-and-swap, and delete keys while maintaining consistent key tracking.
func TestDiskStore_SetCASDeleteRandomKey_TrackKeysConsistency(t *testing.T) {
	t.Parallel()

	const (
		prefix   = "mix:"
		keyCount = 64
	)

	iterations := scaledStressCount(1_200, 120)

	store := newTestDiskStore(t, true, "", true)
	startBarrier := make(chan struct{})
	errorCh := make(chan error, 4)

	var wg sync.WaitGroup
	wg.Add(4)

	go func() {
		defer wg.Done()

		<-startBarrier

		for i := range iterations {
			key := fmt.Sprintf("%s%03d", prefix, i%keyCount)
			if err := store.Set(key, fmt.Sprintf("value-%d", i)); err != nil {
				errorCh <- fmt.Errorf("set failed: %w", err)
				return
			}
		}
	}()

	go func() {
		defer wg.Done()

		<-startBarrier

		for i := range iterations {
			key := fmt.Sprintf("%s%03d", prefix, i%keyCount)
			casValue := fmt.Sprintf("cas-%d", i)

			if _, err := store.CompareAndSwap(key, nil, casValue); err != nil {
				errorCh <- fmt.Errorf("compare-and-swap(absent) failed: %w", err)
				return
			}

			if _, err := store.CompareAndSwap(key, casValue, casValue+"-next"); err != nil {
				errorCh <- fmt.Errorf("compare-and-swap(update) failed: %w", err)
				return
			}
		}
	}()

	go func() {
		defer wg.Done()

		<-startBarrier

		for i := range iterations {
			key := fmt.Sprintf("%s%03d", prefix, i%keyCount)
			if err := store.Delete(key); err != nil {
				errorCh <- fmt.Errorf("delete failed: %w", err)
				return
			}
		}
	}()

	go func() {
		defer wg.Done()

		<-startBarrier

		for range iterations {
			key, err := store.RandomKey(prefix)
			if err != nil {
				errorCh <- fmt.Errorf("randomKey failed: %w", err)
				return
			}

			if key != "" && !strings.HasPrefix(key, prefix) {
				errorCh <- fmt.Errorf("randomKey returned unexpected key: %s", key)
				return
			}
		}
	}()

	close(startBarrier)
	wg.Wait()
	close(errorCh)

	for err := range errorCh {
		require.NoError(t, err)
	}

	requireDiskTrackingMatchesStore(t, store)
}

// TestDiskStore_ClearSetRace_TrackKeysConsistency verifies that the store
// can clear and set keys while maintaining consistent key tracking.
func TestDiskStore_ClearSetRace_TrackKeysConsistency(t *testing.T) {
	t.Parallel()

	const (
		prefix   = "clear:"
		keyCount = 128
	)

	clearIterations := scaledStressCount(300, 40)
	setIterations := scaledStressCount(1_600, 160)

	store := newTestDiskStore(t, true, "", true)
	startBarrier := make(chan struct{})
	errorCh := make(chan error, 3)

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()

		<-startBarrier

		for range clearIterations {
			if err := store.Clear(); err != nil {
				errorCh <- fmt.Errorf("clear failed: %w", err)
				return
			}
		}
	}()

	go func() {
		defer wg.Done()

		<-startBarrier

		for i := range setIterations {
			key := fmt.Sprintf("%s%03d", prefix, i%keyCount)
			if err := store.Set(key, fmt.Sprintf("value-%d", i)); err != nil {
				errorCh <- fmt.Errorf("set failed: %w", err)
				return
			}
		}
	}()

	go func() {
		defer wg.Done()

		<-startBarrier

		for range setIterations {
			key, err := store.RandomKey(prefix)
			if err != nil {
				errorCh <- fmt.Errorf("randomKey failed: %w", err)
				return
			}

			if key != "" && !strings.HasPrefix(key, prefix) {
				errorCh <- fmt.Errorf("randomKey returned unexpected key: %s", key)
				return
			}
		}
	}()

	close(startBarrier)
	wg.Wait()
	close(errorCh)

	for err := range errorCh {
		require.NoError(t, err)
	}

	requireDiskTrackingMatchesStore(t, store)
}

// TestDiskStore_RebuildKeyList_UnderMutationPressure verifies that the store
// can rebuild the key list while under mutation pressure.
func TestDiskStore_RebuildKeyList_UnderMutationPressure(t *testing.T) {
	t.Parallel()

	const (
		prefix   = "rebuild:"
		keyCount = 96
	)

	mutationIterations := scaledStressCount(1_500, 150)
	rebuildIterations := scaledStressCount(220, 30)

	store := newTestDiskStore(t, true, "", true)
	for i := range keyCount {
		require.NoError(t, store.Set(fmt.Sprintf("%s%03d", prefix, i), "seed"))
	}

	startBarrier := make(chan struct{})
	errorCh := make(chan error, 3)

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()

		<-startBarrier

		for i := range mutationIterations {
			key := fmt.Sprintf("%s%03d", prefix, i%keyCount)
			if i%3 == 0 {
				if err := store.Delete(key); err != nil {
					errorCh <- fmt.Errorf("delete failed: %w", err)
					return
				}

				continue
			}

			if err := store.Set(key, fmt.Sprintf("value-%d", i)); err != nil {
				errorCh <- fmt.Errorf("set failed: %w", err)
				return
			}
		}
	}()

	go func() {
		defer wg.Done()

		<-startBarrier

		for range rebuildIterations {
			if err := store.RebuildKeyList(); err != nil {
				errorCh <- fmt.Errorf("rebuildKeyList failed: %w", err)
				return
			}
		}
	}()

	go func() {
		defer wg.Done()

		<-startBarrier

		for range mutationIterations {
			key, err := store.RandomKey(prefix)
			if err != nil {
				errorCh <- fmt.Errorf("randomKey failed: %w", err)
				return
			}

			if key != "" && !strings.HasPrefix(key, prefix) {
				errorCh <- fmt.Errorf("randomKey returned unexpected key: %s", key)
				return
			}
		}
	}()

	close(startBarrier)
	wg.Wait()
	close(errorCh)

	for err := range errorCh {
		require.NoError(t, err)
	}

	requireDiskTrackingMatchesStore(t, store)
}
