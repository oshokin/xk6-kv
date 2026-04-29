package store

import (
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestMemoryStore_Stats(t *testing.T) {
	t.Parallel()

	t.Run("track keys enabled", func(t *testing.T) {
		t.Parallel()

		store := NewMemoryStore(&MemoryConfig{TrackKeys: true})
		require.NoError(t, store.Set("user:1", "a"))
		require.NoError(t, store.Set("user:2", "b"))

		now := time.Now().UnixMilli()

		liveShard := store.getShardByKey("user:1")
		liveShard.mu.Lock()
		liveShard.claims["user:1"] = &memoryClaimRecord{
			ID:        "claim-live",
			Token:     1,
			ExpiresAt: now + 30_000,
		}
		liveShard.mu.Unlock()

		expiredShard := store.getShardByKey("user:2")
		expiredShard.mu.Lock()
		expiredShard.claims["user:2"] = &memoryClaimRecord{
			ID:        "claim-expired",
			Token:     2,
			ExpiresAt: now - 30_000,
		}
		expiredShard.mu.Unlock()

		snapshot, err := store.Stats()
		require.NoError(t, err)
		require.NotNil(t, snapshot)

		expectedSnapshot := &StatsSnapshot{
			Backend:   backendMemoryName,
			TrackKeys: true,
			Count:     2,
			Claims: ClaimStats{
				Live:    1,
				Expired: 1,
			},
			Index: &IndexStats{
				Enabled:    true,
				KeysList:   2,
				KeysMap:    2,
				OST:        2,
				Consistent: true,
			},
		}

		assert.Equal(t, expectedSnapshot, snapshot)
	})

	t.Run("track keys disabled", func(t *testing.T) {
		t.Parallel()

		store := NewMemoryStore(&MemoryConfig{TrackKeys: false})
		require.NoError(t, store.Set("user:1", "a"))

		snapshot, err := store.Stats()
		require.NoError(t, err)
		require.NotNil(t, snapshot)

		expectedSnapshot := &StatsSnapshot{
			Backend:   backendMemoryName,
			TrackKeys: false,
			Count:     1,
		}

		assert.Equal(t, expectedSnapshot, snapshot)
	})
}

func TestDiskStore_Stats(t *testing.T) {
	t.Parallel()

	t.Run("track keys enabled", func(t *testing.T) {
		t.Parallel()

		store := newTestDiskStore(t, true, "", true)
		require.NoError(t, store.Set("user:1", "a"))
		require.NoError(t, store.Set("user:2", "b"))

		now := time.Now().UnixMilli()
		err := store.handle.Update(func(tx *bolt.Tx) error {
			claimsBucket, err := ensureClaimsBucket(tx)
			if err != nil {
				return err
			}

			if err := putDiskClaimTx(claimsBucket, &EntryClaim{
				ID:        "claim-live",
				Key:       "user:1",
				Token:     1,
				ExpiresAt: now + 30_000,
			}); err != nil {
				return err
			}

			return putDiskClaimTx(claimsBucket, &EntryClaim{
				ID:        "claim-expired",
				Key:       "user:2",
				Token:     2,
				ExpiresAt: now - 30_000,
			})
		})
		require.NoError(t, err)

		snapshot, err := store.Stats()
		require.NoError(t, err)
		require.NotNil(t, snapshot)

		assert.Equal(t, backendDiskName, snapshot.Backend)
		assert.True(t, snapshot.TrackKeys)
		assert.EqualValues(t, 2, snapshot.Count)
		assert.EqualValues(t, 1, snapshot.Claims.Live)
		assert.EqualValues(t, 1, snapshot.Claims.Expired)

		require.NotNil(t, snapshot.Index)
		assert.True(t, snapshot.Index.Enabled)
		assert.EqualValues(t, 2, snapshot.Index.KeysList)
		assert.EqualValues(t, 2, snapshot.Index.KeysMap)
		assert.EqualValues(t, 2, snapshot.Index.OST)
		assert.True(t, snapshot.Index.Consistent)

		require.NotNil(t, snapshot.Disk)
		assert.Equal(t, store.path, snapshot.Disk.Path)
		assert.Positive(t, snapshot.Disk.SizeBytes)
	})

	t.Run("track keys disabled", func(t *testing.T) {
		t.Parallel()

		store := newTestDiskStore(t, false, "", true)
		require.NoError(t, store.Set("user:1", "a"))

		snapshot, err := store.Stats()
		require.NoError(t, err)
		require.NotNil(t, snapshot)

		assert.Equal(t, backendDiskName, snapshot.Backend)
		assert.False(t, snapshot.TrackKeys)
		assert.EqualValues(t, 1, snapshot.Count)
		assert.Nil(t, snapshot.Index)
		require.NotNil(t, snapshot.Disk)
		assert.Equal(t, store.path, snapshot.Disk.Path)
	})
}

func TestSerializedStore_Stats(t *testing.T) {
	t.Parallel()

	t.Run("json serialization", func(t *testing.T) {
		t.Parallel()

		store := NewSerializedStore(
			NewMemoryStore(&MemoryConfig{TrackKeys: true}),
			NewJSONSerializer(),
		)

		require.NoError(t, store.Set("user:1", map[string]any{"name": "alice"}))

		snapshot, err := store.Stats()
		require.NoError(t, err)
		require.NotNil(t, snapshot)

		expectedSnapshot := &StatsSnapshot{
			Backend:       backendMemoryName,
			Serialization: serializationJSONName,
			TrackKeys:     true,
			Count:         1,
			Index: &IndexStats{
				Enabled:    true,
				KeysList:   1,
				KeysMap:    1,
				OST:        1,
				Consistent: true,
			},
		}

		assert.Equal(t, expectedSnapshot, snapshot)
	})

	t.Run("string serialization", func(t *testing.T) {
		t.Parallel()

		store := NewSerializedStore(
			NewMemoryStore(&MemoryConfig{TrackKeys: true}),
			NewStringSerializer(),
		)

		require.NoError(t, store.Set("user:1", "alice"))

		snapshot, err := store.Stats()
		require.NoError(t, err)
		require.NotNil(t, snapshot)

		expectedSnapshot := &StatsSnapshot{
			Backend:       backendMemoryName,
			Serialization: serializationStringName,
			TrackKeys:     true,
			Count:         1,
			Index: &IndexStats{
				Enabled:    true,
				KeysList:   1,
				KeysMap:    1,
				OST:        1,
				Consistent: true,
			},
		}

		assert.Equal(t, expectedSnapshot, snapshot)
	})
}

func TestMemoryStore_Stats_LifecycleClaimsCleared(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store := NewMemoryStore(&MemoryConfig{TrackKeys: trackKeys})
			require.NoError(t, store.Set("user:1", "alpha"))

			claim, err := store.ClaimRandom(&ClaimOptions{
				Prefix: "user:",
				TTLMs:  60_000,
			})
			require.NoError(t, err)
			require.NotNil(t, claim)

			beforeClear, err := store.Stats()
			require.NoError(t, err)
			require.NotNil(t, beforeClear)
			assert.EqualValues(t, 1, beforeClear.Claims.Live)

			require.NoError(t, store.Clear())

			afterClear, err := store.Stats()
			require.NoError(t, err)
			require.NotNil(t, afterClear)
			assert.EqualValues(t, 0, afterClear.Count)
			assert.EqualValues(t, 0, afterClear.Claims.Live)
			assert.EqualValues(t, 0, afterClear.Claims.Expired)

			require.NoError(t, store.Set("user:1", "alpha"))

			snapshotPath := filepath.Join(t.TempDir(), "stats-memory-restore.kv")
			_, err = store.Backup(&BackupOptions{FileName: snapshotPath})
			require.NoError(t, err)

			claimBeforeRestore, err := store.ClaimRandom(&ClaimOptions{
				Prefix: "user:",
				TTLMs:  60_000,
			})
			require.NoError(t, err)
			require.NotNil(t, claimBeforeRestore)

			_, err = store.Restore(&RestoreOptions{FileName: snapshotPath})
			require.NoError(t, err)

			afterRestore, err := store.Stats()
			require.NoError(t, err)
			require.NotNil(t, afterRestore)
			assert.EqualValues(t, 1, afterRestore.Count)
			assert.EqualValues(t, 0, afterRestore.Claims.Live)
			assert.EqualValues(t, 0, afterRestore.Claims.Expired)
		})
	}
}

func TestDiskStore_Stats_LifecycleClaimsCleared(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
			require.NoError(t, store.Set("user:1", "alpha"))

			claim, err := store.ClaimRandom(&ClaimOptions{
				Prefix: "user:",
				TTLMs:  60_000,
			})
			require.NoError(t, err)
			require.NotNil(t, claim)

			beforeClear, err := store.Stats()
			require.NoError(t, err)
			require.NotNil(t, beforeClear)
			assert.EqualValues(t, 1, beforeClear.Claims.Live)

			require.NoError(t, store.Clear())

			afterClear, err := store.Stats()
			require.NoError(t, err)
			require.NotNil(t, afterClear)
			assert.EqualValues(t, 0, afterClear.Count)
			assert.EqualValues(t, 0, afterClear.Claims.Live)
			assert.EqualValues(t, 0, afterClear.Claims.Expired)

			require.NoError(t, store.Set("user:1", "alpha"))

			snapshotPath := filepath.Join(t.TempDir(), "stats-disk-restore.kv")
			_, err = store.Backup(&BackupOptions{FileName: snapshotPath})
			require.NoError(t, err)

			claimBeforeRestore, err := store.ClaimRandom(&ClaimOptions{
				Prefix: "user:",
				TTLMs:  60_000,
			})
			require.NoError(t, err)
			require.NotNil(t, claimBeforeRestore)

			_, err = store.Restore(&RestoreOptions{FileName: snapshotPath})
			require.NoError(t, err)

			afterRestore, err := store.Stats()
			require.NoError(t, err)
			require.NotNil(t, afterRestore)
			assert.EqualValues(t, 1, afterRestore.Count)
			assert.EqualValues(t, 0, afterRestore.Claims.Live)
			assert.EqualValues(t, 0, afterRestore.Claims.Expired)
		})
	}
}
