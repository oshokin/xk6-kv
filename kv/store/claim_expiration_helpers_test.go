package store

import (
	"container/heap"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// requireMemoryClaimExpired forces an existing memory claim to be expired.
func requireMemoryClaimExpired(t *testing.T, store *MemoryStore, key string) {
	t.Helper()

	shard := store.getShardByKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	record, ok := shard.claims[key]
	require.Truef(t, ok, "expected claim record for key %q", key)
	require.NotNil(t, record)

	record.ExpiresAt = 0
}

// requireDiskClaimExpired forces an existing disk claim to be expired.
func requireDiskClaimExpired(t *testing.T, store *DiskStore, ref *ClaimRef) {
	t.Helper()

	require.NotNil(t, ref)
	require.NotEmpty(t, ref.ID)
	require.NotEmpty(t, ref.Key)

	if store.trackKeys {
		store.keysLock.Lock()
		defer store.keysLock.Unlock()

		record, ok := store.ost.Meta(ref.Key)
		require.Truef(t, ok, "expected tracked record for key %q", ref.Key)
		require.NotNil(t, record)
		require.Equal(t, ref.ID, record.ID)
		require.Equal(t, ref.Token, record.Token)

		updated := updateTrackedClaimExpiry(store, ref.Key, ref.ID, ref.Token, 0)
		require.Truef(t, updated, "expected tracked claim update for key %q", ref.Key)

		updatedRecord, ok := store.ost.Meta(ref.Key)
		require.Truef(t, ok, "expected tracked record for key %q after expiration", ref.Key)
		require.NotNil(t, updatedRecord)

		heap.Push(&store.claimExpiry, diskClaimExpiryItem{
			Key:       ref.Key,
			ID:        ref.ID,
			Version:   updatedRecord.Version,
			ExpiresAt: 0,
		})

		return
	}

	require.NoError(t, store.handle.Update(func(tx *bolt.Tx) error {
		claimsBucket, err := store.ensureClaimsBucket(tx)
		if err != nil {
			return err
		}

		record, found, err := store.getDiskClaimByIDTx(claimsBucket, ref.ID)
		if err != nil {
			return err
		}

		if !found {
			return fmt.Errorf("claim %q not found", ref.ID)
		}

		if record.Key != ref.Key || record.Token != ref.Token {
			return fmt.Errorf(
				"claim mismatch for %q: got key=%q token=%d",
				ref.ID,
				record.Key,
				record.Token,
			)
		}

		record.ExpiresAt = 0

		payload, err := store.encodeDiskClaim(record)
		if err != nil {
			return err
		}

		return claimsBucket.Put(store.claimIDKey(record.ID), payload)
	}))
}

// requireStoreClaimExpired forces an existing claim to be expired for any concrete store backend.
func requireStoreClaimExpired(t *testing.T, store Store, ref *ClaimRef) {
	t.Helper()

	switch typed := store.(type) {
	case *MemoryStore:
		requireMemoryClaimExpired(t, typed, ref.Key)
	case *DiskStore:
		requireDiskClaimExpired(t, typed, ref)
	case *SerializedStore:
		requireStoreClaimExpired(t, typed.store, ref)
	default:
		t.Fatalf("unsupported store type for forced claim expiration: %T", store)
	}
}

// updateTrackedClaimExpiry updates the expiry of a tracked claim.
func updateTrackedClaimExpiry(
	store *DiskStore,
	key string,
	claimID string,
	token int64,
	expiresAt int64,
) bool {
	return store.ost.UpdateMeta(key, func(old *trackedDiskClaimRecord) (*trackedDiskClaimRecord, bool) {
		if old == nil || old.ID != claimID || old.Token != token {
			return old, old == nil
		}

		next := *old
		next.ExpiresAt = expiresAt

		return &next, false
	})
}
