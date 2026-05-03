package store

import (
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryStore_SetMany_Empty(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store := NewMemoryStore(&MemoryConfig{TrackKeys: trackKeys})
			written, err := store.SetMany([]Entry{})
			require.NoError(t, err)
			assert.EqualValues(t, 0, written)

			size, err := store.Size()
			require.NoError(t, err)
			assert.EqualValues(t, 0, size)

			if trackKeys {
				requireMemoryTrackingMatchesStore(t, store)
			}
		})
	}
}

func TestMemoryStore_SetMany_WritesAndOverwrites(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store := NewMemoryStore(&MemoryConfig{TrackKeys: trackKeys})

			written, err := store.SetMany([]Entry{
				{Key: "user:1", Value: "one"},
				{Key: "user:2", Value: "two"},
			})
			require.NoError(t, err)
			assert.EqualValues(t, 2, written)

			written, err = store.SetMany([]Entry{
				{Key: "user:2", Value: "two-updated"},
				{Key: "user:3", Value: "three"},
			})
			require.NoError(t, err)
			assert.EqualValues(t, 2, written)

			size, err := store.Size()
			require.NoError(t, err)
			assert.EqualValues(t, 3, size)

			valueAny, err := store.Get("user:2")
			require.NoError(t, err)
			assert.Equal(t, []byte("two-updated"), valueAny.([]byte))

			if trackKeys {
				requireMemoryTrackingMatchesStore(t, store)
			}
		})
	}
}

func TestMemoryStore_SetMany_UnsupportedValueTypeDoesNotWrite(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(&MemoryConfig{TrackKeys: true})
	written, err := store.SetMany([]Entry{
		{Key: "ok", Value: "value"},
		{Key: "bad", Value: 123},
	})
	require.ErrorIs(t, err, ErrUnsupportedValueType)
	assert.EqualValues(t, 0, written)

	exists, existsErr := store.Exists("ok")
	require.NoError(t, existsErr)
	assert.False(t, exists, "batch must be all-or-nothing")
}

func TestMemoryStore_SetMany_DoesNotClearLiveClaim(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(&MemoryConfig{TrackKeys: true})
	require.NoError(t, store.Set("user:1", []byte("old")))

	claim, err := store.ClaimRandom(&ClaimOptions{
		Prefix: "user:",
		TTLMs:  60_000,
	})
	require.NoError(t, err)
	require.NotNil(t, claim)

	written, err := store.SetMany([]Entry{
		{Key: "user:1", Value: []byte("new")},
	})
	require.NoError(t, err)
	assert.EqualValues(t, 1, written)

	secondClaim, err := store.ClaimRandom(&ClaimOptions{
		Prefix: "user:",
		TTLMs:  60_000,
	})
	require.NoError(t, err)
	assert.Nil(t, secondClaim, "overwriting value must not release live claim")
}

func TestMemoryStore_SetMany_Concurrent_TrackKeysConsistency(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store := NewMemoryStore(&MemoryConfig{TrackKeys: trackKeys})

			writers := scaledStressCount(24, 8)
			batchesPerWriter := scaledStressCount(30, 10)

			const entriesPerBatch = 8

			startBarrier := make(chan struct{})
			errorCh := make(chan error, writers)

			var wg sync.WaitGroup
			wg.Add(writers)

			for writerID := range writers {
				go func(id int) {
					defer wg.Done()

					<-startBarrier

					for batchID := range batchesPerWriter {
						entries := make([]Entry, 0, entriesPerBatch)
						for entryID := range entriesPerBatch {
							entries = append(entries, Entry{
								Key: fmt.Sprintf("setmany:w:%03d:b:%03d:e:%02d", id, batchID, entryID),
								Value: fmt.Appendf(nil,
									"value-%03d-%03d-%02d",
									id,
									batchID,
									entryID,
								),
							})
						}

						if _, err := store.SetMany(entries); err != nil {
							errorCh <- fmt.Errorf("writer %d batch %d failed: %w", id, batchID, err)
							return
						}
					}
				}(writerID)
			}

			close(startBarrier)
			wg.Wait()
			close(errorCh)

			for err := range errorCh {
				require.NoError(t, err)
			}

			expectedTotal := int64(writers * batchesPerWriter * entriesPerBatch)

			size, err := store.Size()
			require.NoError(t, err)
			assert.Equal(t, expectedTotal, size)

			count := requireCountMatchesScan(t, store, "setmany:")
			assert.Equal(t, expectedTotal, count)

			if trackKeys {
				requireMemoryTrackingMatchesStore(t, store)
			}
		})
	}
}
