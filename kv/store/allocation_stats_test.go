package store

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// allocationStatsStoreFactory configures store construction for allocation stats store factory tests.
type allocationStatsStoreFactory struct {
	// name identifies the name case under test.
	name string
	// backend identifies the backend case under test.
	backend string
	// trackKeys identifies the trackKeys case under test.
	trackKeys bool
	// newStore constructs the store instance for the allocation stats store factory case.
	newStore func(t *testing.T) Store
}

// allocationStatsFactories is a test helper for allocation stats factories.
func allocationStatsFactories() []allocationStatsStoreFactory {
	return []allocationStatsStoreFactory{
		{
			name:      "memory trackKeys=false",
			backend:   backendMemoryName,
			trackKeys: false,
			newStore: func(t *testing.T) Store {
				t.Helper()
				return NewMemoryStore(&MemoryConfig{TrackKeys: false})
			},
		},
		{
			name:      "memory trackKeys=true",
			backend:   backendMemoryName,
			trackKeys: true,
			newStore: func(t *testing.T) Store {
				t.Helper()
				return NewMemoryStore(&MemoryConfig{TrackKeys: true})
			},
		},
		{
			name:      "disk trackKeys=false",
			backend:   backendDiskName,
			trackKeys: false,
			newStore: func(t *testing.T) Store {
				t.Helper()
				return newTestDiskStore(t, false, "", true)
			},
		},
		{
			name:      "disk trackKeys=true",
			backend:   backendDiskName,
			trackKeys: true,
			newStore: func(t *testing.T) Store {
				t.Helper()
				return newTestDiskStore(t, true, "", true)
			},
		},
	}
}

// TestStore_AllocationStats_EmptyAndPrefixNoMatch verifies that store allocation stats empty and prefix no match.
func TestStore_AllocationStats_EmptyAndPrefixNoMatch(t *testing.T) {
	t.Parallel()

	for _, factory := range allocationStatsFactories() {
		t.Run(factory.name, func(t *testing.T) {
			t.Parallel()

			s := factory.newStore(t)

			allStats, err := s.AllocationStats("")
			require.NoError(t, err)
			require.NotNil(t, allStats)
			assert.Empty(t, allStats.Prefix)
			assert.Equal(t, factory.backend, allStats.Backend)
			assert.Equal(t, factory.trackKeys, allStats.TrackKeys)
			assert.EqualValues(t, 0, allStats.Total)
			assert.EqualValues(t, 0, allStats.Claimable)
			assert.EqualValues(t, 0, allStats.ClaimedLive)
			assert.EqualValues(t, 0, allStats.ClaimedExpired)

			prefixStats, err := s.AllocationStats("missing:")
			require.NoError(t, err)
			require.NotNil(t, prefixStats)
			assert.Equal(t, "missing:", prefixStats.Prefix)
			assert.Equal(t, factory.backend, prefixStats.Backend)
			assert.Equal(t, factory.trackKeys, prefixStats.TrackKeys)
			assert.EqualValues(t, 0, prefixStats.Total)
			assert.EqualValues(t, 0, prefixStats.Claimable)
			assert.EqualValues(t, 0, prefixStats.ClaimedLive)
			assert.EqualValues(t, 0, prefixStats.ClaimedExpired)
		})
	}
}

// TestStore_AllocationStats_PrefixLiveReleased verifies that store allocation stats prefix live released.
func TestStore_AllocationStats_PrefixLiveReleased(t *testing.T) {
	t.Parallel()

	for _, factory := range allocationStatsFactories() {
		t.Run(factory.name, func(t *testing.T) {
			t.Parallel()

			s := factory.newStore(t)

			require.NoError(t, s.Set("users:1", "a"))
			require.NoError(t, s.Set("users:2", "b"))
			require.NoError(t, s.Set("orders:1", "x"))

			claim, err := s.ClaimKey("users:1", &ClaimOptions{
				Owner: "test",
				TTLMs: 60_000,
			})
			require.NoError(t, err)
			require.NotNil(t, claim)

			liveStats, err := s.AllocationStats("users:")
			require.NoError(t, err)
			require.NotNil(t, liveStats)
			assert.EqualValues(t, 2, liveStats.Total)
			assert.EqualValues(t, 1, liveStats.Claimable)
			assert.EqualValues(t, 1, liveStats.ClaimedLive)
			assert.EqualValues(t, 0, liveStats.ClaimedExpired)

			released, err := s.ReleaseClaim(claim.Ref())
			require.NoError(t, err)
			require.True(t, released)

			releasedStats, err := s.AllocationStats("users:")
			require.NoError(t, err)
			require.NotNil(t, releasedStats)
			assert.EqualValues(t, 2, releasedStats.Total)
			assert.EqualValues(t, 2, releasedStats.Claimable)
			assert.EqualValues(t, 0, releasedStats.ClaimedLive)
			assert.EqualValues(t, 0, releasedStats.ClaimedExpired)
		})
	}
}

// TestStore_AllocationStats_ExpiredClaim verifies that store allocation stats expired claim.
func TestStore_AllocationStats_ExpiredClaim(t *testing.T) {
	t.Parallel()

	for _, factory := range allocationStatsFactories() {
		t.Run(factory.name, func(t *testing.T) {
			t.Parallel()

			s := factory.newStore(t)

			require.NoError(t, s.Set("users:1", "a"))

			claim, err := s.ClaimKey("users:1", &ClaimOptions{
				TTLMs: 5,
			})
			require.NoError(t, err)
			require.NotNil(t, claim)

			requireStoreClaimExpired(t, s, claim.Ref())

			snapshot, err := s.AllocationStats("users:")
			require.NoError(t, err)
			require.NotNil(t, snapshot)
			assert.EqualValues(t, 1, snapshot.Total)
			assert.EqualValues(t, 1, snapshot.Claimable)
			assert.EqualValues(t, 0, snapshot.ClaimedLive)
			assert.EqualValues(t, 1, snapshot.ClaimedExpired)
		})
	}
}

// TestStore_AllocationStats_DoesNotReapExpiredClaimsDuringRead verifies that store allocation stats does not reap expired claims during read.
func TestStore_AllocationStats_DoesNotReapExpiredClaimsDuringRead(t *testing.T) {
	t.Parallel()

	for _, factory := range allocationStatsFactories() {
		t.Run(factory.name, func(t *testing.T) {
			t.Parallel()

			s := factory.newStore(t)

			require.NoError(t, s.Set("users:1", "a"))

			claim, err := s.ClaimKey("users:1", &ClaimOptions{TTLMs: 5})
			require.NoError(t, err)
			require.NotNil(t, claim)
			requireStoreClaimExpired(t, s, claim.Ref())

			firstSnapshot, err := s.AllocationStats("users:")
			require.NoError(t, err)
			require.NotNil(t, firstSnapshot)
			assert.EqualValues(t, 1, firstSnapshot.Total)
			assert.EqualValues(t, 1, firstSnapshot.Claimable)
			assert.EqualValues(t, 0, firstSnapshot.ClaimedLive)
			assert.EqualValues(t, 1, firstSnapshot.ClaimedExpired)

			secondSnapshot, err := s.AllocationStats("users:")
			require.NoError(t, err)
			require.NotNil(t, secondSnapshot)
			assert.EqualValues(t, 1, secondSnapshot.Total)
			assert.EqualValues(t, 1, secondSnapshot.Claimable)
			assert.EqualValues(t, 0, secondSnapshot.ClaimedLive)
			assert.EqualValues(t, 1, secondSnapshot.ClaimedExpired)
		})
	}
}

// TestStore_AllocationStats_CompleteClaimDeleteModes verifies that store allocation stats complete claim delete modes.
func TestStore_AllocationStats_CompleteClaimDeleteModes(t *testing.T) {
	t.Parallel()

	for _, factory := range allocationStatsFactories() {
		t.Run(factory.name, func(t *testing.T) {
			t.Parallel()

			s := factory.newStore(t)

			require.NoError(t, s.Set("users:1", "a"))
			require.NoError(t, s.Set("users:2", "b"))

			claimDelete, err := s.ClaimKey("users:1", &ClaimOptions{TTLMs: 60_000})
			require.NoError(t, err)
			require.NotNil(t, claimDelete)

			claimKeep, err := s.ClaimKey("users:2", &ClaimOptions{TTLMs: 60_000})
			require.NoError(t, err)
			require.NotNil(t, claimKeep)

			completedDelete, err := s.CompleteClaim(claimDelete.Ref(), &CompleteClaimOptions{DeleteKey: true})
			require.NoError(t, err)
			require.True(t, completedDelete)

			completedKeep, err := s.CompleteClaim(claimKeep.Ref(), &CompleteClaimOptions{DeleteKey: false})
			require.NoError(t, err)
			require.True(t, completedKeep)

			snapshot, err := s.AllocationStats("users:")
			require.NoError(t, err)
			require.NotNil(t, snapshot)
			assert.EqualValues(t, 1, snapshot.Total)
			assert.EqualValues(t, 1, snapshot.Claimable)
			assert.EqualValues(t, 0, snapshot.ClaimedLive)
			assert.EqualValues(t, 0, snapshot.ClaimedExpired)
		})
	}
}

// TestStore_AllocationStats_StaleClaimsForMissingKeys verifies that store allocation stats stale claims for missing keys.
func TestStore_AllocationStats_StaleClaimsForMissingKeys(t *testing.T) {
	t.Parallel()

	for _, factory := range allocationStatsFactories() {
		t.Run(factory.name, func(t *testing.T) {
			t.Parallel()

			s := factory.newStore(t)

			injectStaleClaimForMissingKey(t, s, "users:stale:1")

			snapshot, err := s.AllocationStats("users:")
			require.NoError(t, err)
			require.NotNil(t, snapshot)

			if factory.backend == backendDiskName && factory.trackKeys {
				// trackKeys=true diagnostics intentionally report the operational tracked-index view.
				assert.EqualValues(t, 1, snapshot.Total)
				assert.EqualValues(t, 0, snapshot.Claimable)
				assert.EqualValues(t, 1, snapshot.ClaimedLive)
				assert.EqualValues(t, 0, snapshot.ClaimedExpired)

				return
			}

			assert.EqualValues(t, 0, snapshot.Total)
			assert.EqualValues(t, 0, snapshot.Claimable)
			assert.EqualValues(t, 0, snapshot.ClaimedLive)
			assert.EqualValues(t, 0, snapshot.ClaimedExpired)
		})
	}
}

// TestDiskStoreAllocationStats_TrackKeysTrue_UsesTrackedClaims verifies that disk store allocation stats track keys true uses tracked claims.
func TestDiskStoreAllocationStats_TrackKeysTrue_UsesTrackedClaims(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)

	require.NoError(t, store.Set("users:1", "a"))

	claim, err := store.ClaimKey("users:1", &ClaimOptions{TTLMs: 60_000})
	require.NoError(t, err)
	require.NotNil(t, claim)

	// Simulate out-of-band durable mutation without index rebuild:
	// tracked allocation diagnostics should still read tracked index state.
	require.NoError(t, store.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(store.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, store.bucket)
		}

		return bucket.Delete([]byte("users:1"))
	}))

	exists, err := store.Exists("users:1")
	require.NoError(t, err)
	assert.False(t, exists)

	snapshot, err := store.AllocationStats("users:")
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	assert.EqualValues(t, 1, snapshot.Total)
	assert.EqualValues(t, 0, snapshot.Claimable)
	assert.EqualValues(t, 1, snapshot.ClaimedLive)
	assert.EqualValues(t, 0, snapshot.ClaimedExpired)
}

// TestDiskStoreAllocationStats_TrackKeysTrue_ClaimableAfterRelease verifies that disk store allocation stats track keys true claimable after release.
func TestDiskStoreAllocationStats_TrackKeysTrue_ClaimableAfterRelease(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)

	require.NoError(t, store.Set("users:1", "a"))
	require.NoError(t, store.Set("users:2", "b"))

	claim, err := store.ClaimKey("users:1", &ClaimOptions{TTLMs: 60_000})
	require.NoError(t, err)
	require.NotNil(t, claim)

	liveSnapshot, err := store.AllocationStats("users:")
	require.NoError(t, err)
	require.NotNil(t, liveSnapshot)
	assert.EqualValues(t, 2, liveSnapshot.Total)
	assert.EqualValues(t, 1, liveSnapshot.Claimable)
	assert.EqualValues(t, 1, liveSnapshot.ClaimedLive)
	assert.EqualValues(t, 0, liveSnapshot.ClaimedExpired)

	released, err := store.ReleaseClaim(claim.Ref())
	require.NoError(t, err)
	require.True(t, released)

	releasedSnapshot, err := store.AllocationStats("users:")
	require.NoError(t, err)
	require.NotNil(t, releasedSnapshot)
	assert.EqualValues(t, 2, releasedSnapshot.Total)
	assert.EqualValues(t, 2, releasedSnapshot.Claimable)
	assert.EqualValues(t, 0, releasedSnapshot.ClaimedLive)
	assert.EqualValues(t, 0, releasedSnapshot.ClaimedExpired)
}

// TestDiskStoreAllocationStats_TrackKeysTrue_ExpiredClaimIsClaimable verifies that disk store allocation stats track keys true expired claim is claimable.
func TestDiskStoreAllocationStats_TrackKeysTrue_ExpiredClaimIsClaimable(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)

	require.NoError(t, store.Set("users:1", "a"))

	claim, err := store.ClaimKey("users:1", &ClaimOptions{TTLMs: 5})
	require.NoError(t, err)
	require.NotNil(t, claim)

	requireStoreClaimExpired(t, store, claim.Ref())

	snapshot, err := store.AllocationStats("users:")
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	assert.EqualValues(t, 1, snapshot.Total)
	assert.EqualValues(t, 1, snapshot.Claimable)
	assert.EqualValues(t, 0, snapshot.ClaimedLive)
	assert.EqualValues(t, 1, snapshot.ClaimedExpired)
}

// TestDiskStoreAllocationStats_TrackKeysFalse_BoltFallback verifies that disk store allocation stats track keys false bolt fallback.
func TestDiskStoreAllocationStats_TrackKeysFalse_BoltFallback(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, false, "", true)

	require.NoError(t, store.Set("users:1", "a"))

	claim, err := store.ClaimKey("users:1", &ClaimOptions{TTLMs: 60_000})
	require.NoError(t, err)
	require.NotNil(t, claim)

	liveSnapshot, err := store.AllocationStats("users:")
	require.NoError(t, err)
	require.NotNil(t, liveSnapshot)
	assert.EqualValues(t, 1, liveSnapshot.Total)
	assert.EqualValues(t, 0, liveSnapshot.Claimable)
	assert.EqualValues(t, 1, liveSnapshot.ClaimedLive)
	assert.EqualValues(t, 0, liveSnapshot.ClaimedExpired)

	released, err := store.ReleaseClaim(claim.Ref())
	require.NoError(t, err)
	require.True(t, released)

	releasedSnapshot, err := store.AllocationStats("users:")
	require.NoError(t, err)
	require.NotNil(t, releasedSnapshot)
	assert.EqualValues(t, 1, releasedSnapshot.Total)
	assert.EqualValues(t, 1, releasedSnapshot.Claimable)
	assert.EqualValues(t, 0, releasedSnapshot.ClaimedLive)
	assert.EqualValues(t, 0, releasedSnapshot.ClaimedExpired)
}

// TestDiskAllocationStats_TrackedIndexMissingKeyIsNotCounted verifies that disk allocation stats tracked index missing key is not counted.
func TestDiskAllocationStats_TrackedIndexMissingKeyIsNotCounted(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)

	require.NoError(t, store.Set("users:1", "a"))

	store.keysLock.Lock()
	require.NotNil(t, store.ost)
	store.ost.Delete("users:1")
	store.keysLock.Unlock()

	snapshot, err := store.AllocationStats("users:")
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	assert.EqualValues(t, 0, snapshot.Total)
	assert.EqualValues(t, 0, snapshot.Claimable)
	assert.EqualValues(t, 0, snapshot.ClaimedLive)
	assert.EqualValues(t, 0, snapshot.ClaimedExpired)

	statsBeforeRebuild, err := store.Stats()
	require.NoError(t, err)
	require.NotNil(t, statsBeforeRebuild)
	require.NotNil(t, statsBeforeRebuild.Index)
	assert.False(t, statsBeforeRebuild.Index.Consistent)

	require.NoError(t, store.RebuildKeyList())

	rebuiltSnapshot, err := store.AllocationStats("users:")
	require.NoError(t, err)
	require.NotNil(t, rebuiltSnapshot)
	assert.EqualValues(t, 1, rebuiltSnapshot.Total)
	assert.EqualValues(t, 1, rebuiltSnapshot.Claimable)
	assert.EqualValues(t, 0, rebuiltSnapshot.ClaimedLive)
	assert.EqualValues(t, 0, rebuiltSnapshot.ClaimedExpired)

	statsAfterRebuild, err := store.Stats()
	require.NoError(t, err)
	require.NotNil(t, statsAfterRebuild)
	require.NotNil(t, statsAfterRebuild.Index)
	assert.True(t, statsAfterRebuild.Index.Consistent)
}

// TestMemoryAllocationStats_TrackKeysTrue_FallsBackToContainerOnInconsistentIndex verifies that memory allocation stats track keys true falls back to container on inconsistent index.
func TestMemoryAllocationStats_TrackKeysTrue_FallsBackToContainerOnInconsistentIndex(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(&MemoryConfig{TrackKeys: true})
	require.NoError(t, store.Set("users:1", "a"))

	shard := store.getShardByKey("users:1")
	shard.mu.Lock()
	require.NotNil(t, shard.ost)
	shard.ost.Delete("users:1")
	shard.mu.Unlock()

	snapshot, err := store.AllocationStats("users:")
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	assert.EqualValues(t, 1, snapshot.Total)
	assert.EqualValues(t, 1, snapshot.Claimable)
	assert.EqualValues(t, 0, snapshot.ClaimedLive)
	assert.EqualValues(t, 0, snapshot.ClaimedExpired)

	statsBeforeRebuild, err := store.Stats()
	require.NoError(t, err)
	require.NotNil(t, statsBeforeRebuild)
	require.NotNil(t, statsBeforeRebuild.Index)
	assert.False(t, statsBeforeRebuild.Index.Consistent)

	require.NoError(t, store.RebuildKeyList())

	rebuiltSnapshot, err := store.AllocationStats("users:")
	require.NoError(t, err)
	require.NotNil(t, rebuiltSnapshot)
	assert.EqualValues(t, 1, rebuiltSnapshot.Total)
	assert.EqualValues(t, 1, rebuiltSnapshot.Claimable)
	assert.EqualValues(t, 0, rebuiltSnapshot.ClaimedLive)
	assert.EqualValues(t, 0, rebuiltSnapshot.ClaimedExpired)

	statsAfterRebuild, err := store.Stats()
	require.NoError(t, err)
	require.NotNil(t, statsAfterRebuild)
	require.NotNil(t, statsAfterRebuild.Index)
	assert.True(t, statsAfterRebuild.Index.Consistent)
}

// TestSerializedStore_AllocationStats verifies that serialized store allocation stats.
func TestSerializedStore_AllocationStats(t *testing.T) {
	t.Parallel()

	s := NewSerializedStore(
		NewMemoryStore(&MemoryConfig{TrackKeys: true}),
		NewJSONSerializer(),
	)

	require.NoError(t, s.Set("users:1", map[string]any{"name": "alice"}))

	snapshot, err := s.AllocationStats("users:")
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	assert.Equal(t, backendMemoryName, snapshot.Backend)
	assert.True(t, snapshot.TrackKeys)
	assert.Equal(t, "users:", snapshot.Prefix)
	assert.EqualValues(t, 1, snapshot.Total)
	assert.EqualValues(t, 1, snapshot.Claimable)
	assert.EqualValues(t, 0, snapshot.ClaimedLive)
	assert.EqualValues(t, 0, snapshot.ClaimedExpired)
}

// injectStaleClaimForMissingKey is a test helper for inject stale claim for missing key.
func injectStaleClaimForMissingKey(t *testing.T, s Store, key string) {
	t.Helper()

	require.NoError(t, s.Set(key, "value"))

	claim, err := s.ClaimKey(key, &ClaimOptions{TTLMs: 60_000})
	require.NoError(t, err)
	require.NotNil(t, claim)

	switch typed := s.(type) {
	case *MemoryStore:
		shard := typed.getShardByKey(key)
		shard.mu.Lock()
		delete(shard.container, key)
		shard.removeKeyTrackingLocked(key)
		shard.mu.Unlock()
	case *DiskStore:
		if typed.trackKeys {
			typed.keysLock.Lock()
			defer typed.keysLock.Unlock()
		}

		require.NoError(t, typed.handle.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(typed.bucket)
			if bucket == nil {
				return fmt.Errorf("%w: %s", ErrBucketNotFound, typed.bucket)
			}

			return bucket.Delete([]byte(key))
		}))
	default:
		t.Fatalf("unsupported store type for stale-claim injection: %T", s)
	}
}
