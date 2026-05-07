package store

import (
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
