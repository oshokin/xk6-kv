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

// newReadOnlyDiskStoreFixture creates read only disk store fixture for tests.
func newReadOnlyDiskStoreFixture(t *testing.T, trackKeys bool) (*DiskStore, *ClaimRef) {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "readonly.db")
	writable := newTestDiskStore(t, trackKeys, dbPath, true)
	requirePopulateStore(
		t,
		writable,
		"seed:1", "alpha",
		"seed:2", "beta",
		"counter", "41",
	)

	claim, err := writable.ClaimRandom(&ClaimOptions{
		Prefix: "seed:1",
		TTLMs:  10_000,
	})
	require.NoError(t, err)
	require.NotNil(t, claim)
	require.NoError(t, writable.Close())

	readOnlyStore, err := NewDiskStore(trackKeys, dbPath, &DiskConfig{
		ReadOnly: GetComparablePointer(true),
	})
	require.NoError(t, err)
	require.NoError(t, readOnlyStore.Open())

	t.Cleanup(func() {
		_ = readOnlyStore.Close()
	})

	return readOnlyStore, &ClaimRef{
		ID:    claim.ID,
		Key:   claim.Key,
		Token: claim.Token,
	}
}

// putDurableClaimForTest is a test helper for put durable claim for test.
func putDurableClaimForTest(t *testing.T, store *DiskStore, claim *EntryClaim) {
	t.Helper()

	require.NotNil(t, claim)

	require.NoError(t, store.handle.Update(func(tx *bolt.Tx) error {
		claimsBucket, err := store.ensureClaimsBucket(tx)
		if err != nil {
			return err
		}

		return store.putDiskClaimTx(claimsBucket, claim)
	}))
}

// TestDiskStoreReadOnlyAllowsReads verifies that disk store read only allows reads.
func TestDiskStoreReadOnlyAllowsReads(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store, _ := newReadOnlyDiskStoreFixture(t, trackKeys)

			gotAny, err := store.Get("seed:1")
			require.NoError(t, err)
			assert.Equal(t, []byte("alpha"), gotAny.([]byte))

			entries, err := store.GetMany([]string{"seed:1", "missing"})
			require.NoError(t, err)
			require.Len(t, entries, 2)
			require.NotNil(t, entries[0])
			assert.Nil(t, entries[1])

			size, err := store.Size()
			require.NoError(t, err)
			assert.EqualValues(t, 3, size)
		})
	}
}

// TestDiskStoreReadOnlyRejectsMutations verifies that disk store read only rejects mutations.
func TestDiskStoreReadOnlyRejectsMutations(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store, _ := newReadOnlyDiskStoreFixture(t, trackKeys)

			testCases := []struct {
				name string
				run  func(*DiskStore) error
			}{
				{
					name: "set",
					run: func(s *DiskStore) error {
						return s.Set("blocked:set", "value")
					},
				},
				{
					name: "increment_by",
					run: func(s *DiskStore) error {
						_, err := s.IncrementBy("counter", 1)

						return err
					},
				},
				{
					name: "get_or_set",
					run: func(s *DiskStore) error {
						_, _, err := s.GetOrSet("blocked:getorset", "value")

						return err
					},
				},
				{
					name: "swap",
					run: func(s *DiskStore) error {
						_, _, err := s.Swap("seed:1", "swapped")

						return err
					},
				},
				{
					name: "delete",
					run: func(s *DiskStore) error {
						return s.Delete("seed:1")
					},
				},
				{
					name: "delete_if_exists",
					run: func(s *DiskStore) error {
						_, err := s.DeleteIfExists("seed:1")

						return err
					},
				},
				{
					name: "clear",
					run: func(s *DiskStore) error {
						return s.Clear()
					},
				},
				{
					name: "set_many",
					run: func(s *DiskStore) error {
						_, err := s.SetMany([]Entry{{Key: "batch:1", Value: "value"}})

						return err
					},
				},
				{
					name: "delete_many",
					run: func(s *DiskStore) error {
						_, err := s.DeleteMany([]string{"seed:1"})

						return err
					},
				},
				{
					name: "delete_by_prefix",
					run: func(s *DiskStore) error {
						_, err := s.DeleteByPrefix("seed:", 1)

						return err
					},
				},
				{
					name: "compare_and_swap",
					run: func(s *DiskStore) error {
						_, err := s.CompareAndSwap("seed:1", "alpha", "next")

						return err
					},
				},
				{
					name: "compare_and_delete",
					run: func(s *DiskStore) error {
						_, err := s.CompareAndDelete("seed:1", "alpha")

						return err
					},
				},
				{
					name: "pop_random",
					run: func(s *DiskStore) error {
						_, err := s.PopRandom("seed:")

						return err
					},
				},
				{
					name: "claim_random",
					run: func(s *DiskStore) error {
						_, err := s.ClaimRandom(&ClaimOptions{
							Prefix: "seed:",
							TTLMs:  10_000,
						})

						return err
					},
				},
			}

			for _, tt := range testCases {
				t.Run(tt.name, func(t *testing.T) {
					err := tt.run(store)
					require.Error(t, err)
					require.ErrorIs(t, err, ErrDiskStoreReadOnly)
				})
			}
		})
	}
}

// TestDiskStoreReadOnlyRejectsClaims verifies that disk store read only rejects claims.
func TestDiskStoreReadOnlyRejectsClaims(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store, claimRef := newReadOnlyDiskStoreFixture(t, trackKeys)

			released, err := store.ReleaseClaim(claimRef)
			require.Error(t, err)
			require.ErrorIs(t, err, ErrDiskStoreReadOnly)
			assert.False(t, released)

			completed, err := store.CompleteClaim(claimRef, nil)
			require.Error(t, err)
			require.ErrorIs(t, err, ErrDiskStoreReadOnly)
			assert.False(t, completed)
		})
	}
}

// TestDiskStoreReadOnlyRejectsRestore verifies that disk store read only rejects restore.
func TestDiskStoreReadOnlyRejectsRestore(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store, _ := newReadOnlyDiskStoreFixture(t, trackKeys)

			source := NewMemoryStore(&MemoryConfig{TrackKeys: true})
			require.NoError(t, source.Set("restored:key", "restored-value"))

			snapshotPath := filepath.Join(t.TempDir(), "readonly-restore-src.kv")
			_, err := source.Backup(&BackupOptions{FileName: snapshotPath})
			require.NoError(t, err)

			_, err = store.Restore(&RestoreOptions{FileName: snapshotPath})
			require.Error(t, err)
			require.ErrorIs(t, err, ErrDiskStoreReadOnly)
		})
	}
}

// TestDiskAllocationStats_ReadOnlyTrackKeysUsesDurableClaims verifies that disk allocation stats read only track keys uses durable claims.
func TestDiskAllocationStats_ReadOnlyTrackKeysUsesDurableClaims(t *testing.T) {
	t.Parallel()

	t.Run("live claim", func(t *testing.T) {
		t.Parallel()

		dbPath := filepath.Join(t.TempDir(), "allocation-readonly-live.db")
		writable := newTestDiskStore(t, true, dbPath, true)
		require.NoError(t, writable.Set("users:1", "alice"))

		putDurableClaimForTest(t, writable, &EntryClaim{
			ID:        "claim-live",
			Key:       "users:1",
			Token:     1,
			ExpiresAt: time.Now().Add(time.Minute).UnixMilli(),
		})
		require.NoError(t, writable.Close())

		readOnlyStore, err := NewDiskStore(true, dbPath, &DiskConfig{
			ReadOnly: GetComparablePointer(true),
		})
		require.NoError(t, err)
		require.NoError(t, readOnlyStore.Open())
		t.Cleanup(func() {
			_ = readOnlyStore.Close()
		})

		snapshot, err := readOnlyStore.AllocationStats("users:")
		require.NoError(t, err)
		require.NotNil(t, snapshot)
		assert.EqualValues(t, 1, snapshot.Total)
		assert.EqualValues(t, 0, snapshot.Claimable)
		assert.EqualValues(t, 1, snapshot.ClaimedLive)
		assert.EqualValues(t, 0, snapshot.ClaimedExpired)
	})

	t.Run("expired claim", func(t *testing.T) {
		t.Parallel()

		dbPath := filepath.Join(t.TempDir(), "allocation-readonly-expired.db")
		writable := newTestDiskStore(t, true, dbPath, true)
		require.NoError(t, writable.Set("users:1", "alice"))

		putDurableClaimForTest(t, writable, &EntryClaim{
			ID:        "claim-expired",
			Key:       "users:1",
			Token:     2,
			ExpiresAt: time.Now().Add(-time.Minute).UnixMilli(),
		})
		require.NoError(t, writable.Close())

		readOnlyStore, err := NewDiskStore(true, dbPath, &DiskConfig{
			ReadOnly: GetComparablePointer(true),
		})
		require.NoError(t, err)
		require.NoError(t, readOnlyStore.Open())
		t.Cleanup(func() {
			_ = readOnlyStore.Close()
		})

		snapshot, err := readOnlyStore.AllocationStats("users:")
		require.NoError(t, err)
		require.NotNil(t, snapshot)
		assert.EqualValues(t, 1, snapshot.Total)
		assert.EqualValues(t, 1, snapshot.Claimable)
		assert.EqualValues(t, 0, snapshot.ClaimedLive)
		assert.EqualValues(t, 1, snapshot.ClaimedExpired)
	})
}

// TestDiskStats_ReadOnlyTrackKeysUsesDurableClaims_Live verifies that disk stats read only track keys uses durable claims live.
func TestDiskStats_ReadOnlyTrackKeysUsesDurableClaims_Live(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "stats-readonly-live.db")
	writable := newTestDiskStore(t, true, dbPath, true)
	require.NoError(t, writable.Set("users:1", "alice"))

	putDurableClaimForTest(t, writable, &EntryClaim{
		ID:        "claim-live",
		Key:       "users:1",
		Token:     1,
		ExpiresAt: time.Now().Add(time.Minute).UnixMilli(),
	})
	require.NoError(t, writable.Close())

	readOnlyStore, err := NewDiskStore(true, dbPath, &DiskConfig{
		ReadOnly: GetComparablePointer(true),
	})
	require.NoError(t, err)
	require.NoError(t, readOnlyStore.Open())
	t.Cleanup(func() {
		_ = readOnlyStore.Close()
	})

	snapshot, err := readOnlyStore.Stats()
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	assert.EqualValues(t, 1, snapshot.Count)
	assert.EqualValues(t, 1, snapshot.Claims.Live)
	assert.EqualValues(t, 0, snapshot.Claims.Expired)
}

// TestDiskStats_ReadOnlyTrackKeysUsesDurableClaims_Expired verifies that disk stats read only track keys uses durable claims expired.
func TestDiskStats_ReadOnlyTrackKeysUsesDurableClaims_Expired(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "stats-readonly-expired.db")
	writable := newTestDiskStore(t, true, dbPath, true)
	require.NoError(t, writable.Set("users:1", "alice"))

	putDurableClaimForTest(t, writable, &EntryClaim{
		ID:        "claim-expired",
		Key:       "users:1",
		Token:     2,
		ExpiresAt: time.Now().Add(-time.Minute).UnixMilli(),
	})
	require.NoError(t, writable.Close())

	readOnlyStore, err := NewDiskStore(true, dbPath, &DiskConfig{
		ReadOnly: GetComparablePointer(true),
	})
	require.NoError(t, err)
	require.NoError(t, readOnlyStore.Open())
	t.Cleanup(func() {
		_ = readOnlyStore.Close()
	})

	snapshot, err := readOnlyStore.Stats()
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	assert.EqualValues(t, 1, snapshot.Count)
	assert.EqualValues(t, 0, snapshot.Claims.Live)
	assert.EqualValues(t, 1, snapshot.Claims.Expired)
}
