package store

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkMemoryStore_ClaimForOwner_StickyHit(b *testing.B) {
	store := NewMemoryStore(&MemoryConfig{
		TrackKeys: true,
	})
	require.NoError(b, store.Open())
	require.NoError(b, store.Set("users:1", []byte(`{"id":1}`)))

	opts := &ClaimOptions{
		Prefix: "users:",
		Owner:  "scenario:a:vu:1",
		TTLMs:  MaxClaimTTLMs,
	}

	claim, err := store.ClaimForOwner(opts)
	require.NoError(b, err)
	require.NotNil(b, claim)

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		next, claimErr := store.ClaimForOwner(opts)
		if claimErr != nil || next == nil {
			b.Fatalf("claimForOwner: claim=%v err=%v", next, claimErr)
		}
	}
}

func BenchmarkMemoryStore_ClaimForOwner_ParallelDifferentOwners(b *testing.B) {
	store := NewMemoryStore(&MemoryConfig{
		TrackKeys: true,
	})
	require.NoError(b, store.Open())

	const owners = 256
	for i := range owners {
		key := fmt.Sprintf("users:%03d", i)
		require.NoError(b, store.Set(key, []byte(key)))
	}

	for i := range owners {
		owner := fmt.Sprintf("scenario:a:vu:%03d", i)
		claim, err := store.ClaimForOwner(&ClaimOptions{
			Prefix: "users:",
			Owner:  owner,
			TTLMs:  MaxClaimTTLMs,
		})
		require.NoError(b, err)
		require.NotNil(b, claim)
	}

	b.ReportAllocs()
	b.ResetTimer()

	var counter atomic.Uint64

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			index := int(counter.Add(1) % owners)
			owner := fmt.Sprintf("scenario:a:vu:%03d", index)

			claim, err := store.ClaimForOwner(&ClaimOptions{
				Prefix: "users:",
				Owner:  owner,
				TTLMs:  MaxClaimTTLMs,
			})
			if err != nil || claim == nil {
				b.Fatalf("claimForOwner owner=%s claim=%v err=%v", owner, claim, err)
			}
		}
	})
}

func BenchmarkDiskStore_ClaimForOwner_StickyHit_TrackKeysTrue(b *testing.B) {
	benchmarkDiskStoreClaimForOwnerStickyHit(b, true)
}

func BenchmarkDiskStore_ClaimForOwner_StickyHit_TrackKeysFalse(b *testing.B) {
	benchmarkDiskStoreClaimForOwnerStickyHit(b, false)
}

func benchmarkDiskStoreClaimForOwnerStickyHit(b *testing.B, trackKeys bool) {
	b.Helper()

	store := newBenchmarkDiskStore(b, trackKeys, "diskstore-bench-claim-for-owner-sticky-hit-*.db")
	require.NoError(b, store.Set("users:1", []byte(`{"id":1}`)))

	opts := &ClaimOptions{
		Prefix: "users:",
		Owner:  "scenario:a:vu:1",
		TTLMs:  MaxClaimTTLMs,
	}

	claim, err := store.ClaimForOwner(opts)
	require.NoError(b, err)
	require.NotNil(b, claim)

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		next, claimErr := store.ClaimForOwner(opts)
		if claimErr != nil || next == nil {
			b.Fatalf("claimForOwner: claim=%v err=%v", next, claimErr)
		}
	}
}
