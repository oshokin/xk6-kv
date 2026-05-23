package store

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMemoryStore_DeleteByPrefix_RejectsEmptyPrefix verifies that memory store delete by prefix rejects empty prefix.
func TestMemoryStore_DeleteByPrefix_RejectsEmptyPrefix(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store := NewMemoryStore(&MemoryConfig{TrackKeys: trackKeys})

			result, err := store.DeleteByPrefix("", 1)
			require.Error(t, err)
			assert.Nil(t, result)
			require.ErrorIs(t, err, ErrKVOptionsInvalid)
		})
	}
}

// TestMemoryStore_DeleteByPrefix_RejectsInvalidLimit verifies that memory store delete by prefix rejects invalid limit.
func TestMemoryStore_DeleteByPrefix_RejectsInvalidLimit(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store := NewMemoryStore(&MemoryConfig{TrackKeys: trackKeys})

			for _, limit := range []int64{0, -1} {
				result, err := store.DeleteByPrefix("tmp:", limit)
				require.Error(t, err)
				assert.Nil(t, result)
				require.ErrorIs(t, err, ErrKVOptionsInvalid)
			}
		})
	}
}

// TestMemoryStore_DeleteByPrefix_DeletesLimitedBatchAndReportsNotDone verifies that memory store delete by prefix deletes limited batch and reports not done.
func TestMemoryStore_DeleteByPrefix_DeletesLimitedBatchAndReportsNotDone(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store := NewMemoryStore(&MemoryConfig{TrackKeys: trackKeys})
			_, err := store.SetMany([]Entry{
				{Key: "tmp:1", Value: []byte("1")},
				{Key: "tmp:2", Value: []byte("2")},
				{Key: "tmp:3", Value: []byte("3")},
				{Key: "user:1", Value: []byte("u")},
			})
			require.NoError(t, err)

			result, err := store.DeleteByPrefix("tmp:", 2)
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.EqualValues(t, 2, result.Deleted)
			assert.False(t, result.Done)

			tmpKeys, err := store.ListKeys("tmp:", 0)
			require.NoError(t, err)
			assert.Len(t, tmpKeys, 1)

			userKeys, err := store.ListKeys("user:", 0)
			require.NoError(t, err)
			assert.Equal(t, []string{"user:1"}, userKeys)
		})
	}
}

// TestMemoryStore_DeleteByPrefix_RepeatedCallsReportDone verifies that memory store delete by prefix repeated calls report done.
func TestMemoryStore_DeleteByPrefix_RepeatedCallsReportDone(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store := NewMemoryStore(&MemoryConfig{TrackKeys: trackKeys})
			_, err := store.SetMany([]Entry{
				{Key: "tmp:1", Value: []byte("1")},
				{Key: "tmp:2", Value: []byte("2")},
				{Key: "tmp:3", Value: []byte("3")},
			})
			require.NoError(t, err)

			first, err := store.DeleteByPrefix("tmp:", 2)
			require.NoError(t, err)
			require.NotNil(t, first)
			assert.EqualValues(t, 2, first.Deleted)
			assert.False(t, first.Done)

			second, err := store.DeleteByPrefix("tmp:", 2)
			require.NoError(t, err)
			require.NotNil(t, second)
			assert.EqualValues(t, 1, second.Deleted)
			assert.True(t, second.Done)

			keys, err := store.ListKeys("tmp:", 0)
			require.NoError(t, err)
			assert.Empty(t, keys)
		})
	}
}

// TestMemoryStore_DeleteByPrefix_DoesNotDeleteOtherPrefixes verifies that memory store delete by prefix does not delete other prefixes.
func TestMemoryStore_DeleteByPrefix_DoesNotDeleteOtherPrefixes(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(&MemoryConfig{TrackKeys: true})
	_, err := store.SetMany([]Entry{
		{Key: "tmp:1", Value: []byte("1")},
		{Key: "tmp:2", Value: []byte("2")},
		{Key: "user:1", Value: []byte("u")},
		{Key: "order:1", Value: []byte("o")},
	})
	require.NoError(t, err)

	result, err := store.DeleteByPrefix("tmp:", 10)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 2, result.Deleted)
	assert.True(t, result.Done)

	userExists, err := store.Exists("user:1")
	require.NoError(t, err)
	assert.True(t, userExists)

	orderExists, err := store.Exists("order:1")
	require.NoError(t, err)
	assert.True(t, orderExists)
}

// TestMemoryStore_DeleteByPrefix_CleansClaims verifies that memory store delete by prefix cleans claims.
func TestMemoryStore_DeleteByPrefix_CleansClaims(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(&MemoryConfig{TrackKeys: true})
	require.NoError(t, store.Set("tmp:1", []byte("value")))

	claim, err := store.ClaimRandom(&ClaimOptions{
		Prefix: "tmp:",
		TTLMs:  60_000,
	})
	require.NoError(t, err)
	require.NotNil(t, claim)

	result, err := store.DeleteByPrefix("tmp:", 10)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 1, result.Deleted)
	assert.True(t, result.Done)

	released, err := store.ReleaseClaim(&ClaimRef{
		ID:    claim.ID,
		Key:   claim.Key,
		Token: claim.Token,
	})
	require.NoError(t, err)
	assert.False(t, released)
}

// TestMemoryStore_DeleteByPrefix_UpdatesTracking verifies that memory store delete by prefix updates tracking.
func TestMemoryStore_DeleteByPrefix_UpdatesTracking(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(&MemoryConfig{TrackKeys: true})
	_, err := store.SetMany([]Entry{
		{Key: "tmp:1", Value: []byte("1")},
		{Key: "tmp:2", Value: []byte("2")},
		{Key: "user:1", Value: []byte("u")},
	})
	require.NoError(t, err)

	result, err := store.DeleteByPrefix("tmp:", 10)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 2, result.Deleted)
	assert.True(t, result.Done)

	requireMemoryTrackingMatchesStore(t, store)
}
