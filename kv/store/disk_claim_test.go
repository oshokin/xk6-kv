package store

import (
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDiskStore_PopRandom_Empty_ReturnsNil verifies that disk store pop random empty returns nil.
func TestDiskStore_PopRandom_Empty_ReturnsNil(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
			entry, err := store.PopRandom("user:")
			require.NoError(t, err)
			assert.Nil(t, entry)
		})
	}
}

// TestDiskStore_PopRandom_WithPrefix_RemovesAndReturnsEntry verifies that disk store pop random with prefix removes and returns entry.
func TestDiskStore_PopRandom_WithPrefix_RemovesAndReturnsEntry(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
			requirePopulateStore(
				t,
				store,
				"user:1", "alpha",
				"user:2", "beta",
				"order:1", "gamma",
			)

			entry, err := store.PopRandom("user:")
			require.NoError(t, err)
			require.NotNil(t, entry)
			assert.True(t, strings.HasPrefix(entry.Key, "user:"))

			exists, err := store.Exists(entry.Key)
			require.NoError(t, err)
			assert.False(t, exists)

			count, err := store.Count("user:")
			require.NoError(t, err)
			assert.EqualValues(t, 1, count)
		})
	}
}

// TestDiskStore_PopRandom_SkipsLiveClaim verifies that disk store pop random skips live claim.
func TestDiskStore_PopRandom_SkipsLiveClaim(t *testing.T) {
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

			entry, err := store.PopRandom("user:")
			require.NoError(t, err)
			assert.Nil(t, entry, "popRandom must not pop a live claimed key")
		})
	}
}

// TestDiskStore_PopRandomTracked_CompleteError_ReleasesClaim verifies that disk store pop random tracked complete error releases claim.
func TestDiskStore_PopRandomTracked_CompleteError_ReleasesClaim(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	require.NoError(t, store.Set("user:1", "alpha"))

	claim, err := store.claimRandomTracked(&ClaimOptions{
		Prefix: "user:",
		TTLMs:  60_000,
	})
	require.NoError(t, err)
	require.NotNil(t, claim)

	originalBucket := store.bucket
	store.bucket = []byte("missing-bucket")

	err = store.completeTrackedPopClaim(claim)
	require.Error(t, err)

	store.bucket = originalBucket

	nextClaim, err := store.ClaimKey("user:1", &ClaimOptions{TTLMs: 60_000})
	require.NoError(t, err)
	require.NotNil(t, nextClaim, "tracked pop completion error must not leave a hidden live claim")
}

// TestDiskStore_PopRandomTracked_CompleteFalse_ReleasesClaim verifies that disk store pop random tracked complete false releases claim.
func TestDiskStore_PopRandomTracked_CompleteFalse_ReleasesClaim(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	require.NoError(t, store.Set("user:1", "alpha"))

	claim, err := store.claimRandomTracked(&ClaimOptions{
		Prefix: "user:",
		TTLMs:  60_000,
	})
	require.NoError(t, err)
	require.NotNil(t, claim)

	store.keysLock.Lock()
	store.ost.UpdateMeta(claim.Key, func(_ *trackedDiskClaimRecord) (*trackedDiskClaimRecord, bool) {
		return nil, true
	})
	store.keysLock.Unlock()

	err = store.completeTrackedPopClaim(claim)
	require.ErrorIs(t, err, ErrClaimCompletionFailed)
	require.ErrorContains(t, err, "releaseClaim")

	nextClaim, err := store.ClaimKey("user:1", &ClaimOptions{TTLMs: 60_000})
	require.NoError(t, err)
	require.NotNil(t, nextClaim, "tracked pop completion=false path must not keep claim hidden until TTL")
}

// TestDiskStore_PopRandom_AllowsExpiredClaim verifies that disk store pop random allows expired claim.
func TestDiskStore_PopRandom_AllowsExpiredClaim(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
			require.NoError(t, store.Set("user:1", "alpha"))

			claim, err := store.ClaimRandom(&ClaimOptions{
				Prefix: "user:",
				TTLMs:  5,
			})
			require.NoError(t, err)
			require.NotNil(t, claim)

			requireDiskClaimExpired(t, store, claim.Ref())

			entry, err := store.PopRandom("user:")
			require.NoError(t, err)
			require.NotNil(t, entry)
			assert.Equal(t, "user:1", entry.Key)
		})
	}
}

// TestDiskStore_PopRandom_ExpiredClaimAvailableWhenFullCleanupThrottled verifies that disk store pop random expired claim available when full cleanup throttled.
func TestDiskStore_PopRandom_ExpiredClaimAvailableWhenFullCleanupThrottled(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
			require.NoError(t, store.Set("user:1", "alpha"))

			claim, err := store.ClaimRandom(&ClaimOptions{
				Prefix: "user:",
				TTLMs:  5,
			})
			require.NoError(t, err)
			require.NotNil(t, claim)

			requireDiskClaimExpired(t, store, claim.Ref())
			store.lastClaimsCleanupUnixMilli.Store(time.Now().UnixMilli())

			entry, err := store.PopRandom("user:")
			require.NoError(t, err)
			require.NotNil(t, entry)
			assert.Equal(t, "user:1", entry.Key)
		})
	}
}

// TestDiskStore_ClaimRandom_ExcludesLiveClaim verifies that disk store claim random excludes live claim.
func TestDiskStore_ClaimRandom_ExcludesLiveClaim(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
			require.NoError(t, store.Set("user:1", "alpha"))

			claim, err := store.ClaimRandom(&ClaimOptions{
				Prefix: "user:",
				TTLMs:  10_000,
			})
			require.NoError(t, err)
			require.NotNil(t, claim)

			nextClaim, err := store.ClaimRandom(&ClaimOptions{
				Prefix: "user:",
				TTLMs:  10_000,
			})
			require.NoError(t, err)
			assert.Nil(t, nextClaim)
		})
	}
}

// TestDiskStore_ClaimRandom_TTLMustNotExceedMax verifies that disk store claim random ttl must not exceed max.
func TestDiskStore_ClaimRandom_TTLMustNotExceedMax(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
			require.NoError(t, store.Set("user:1", "alpha"))

			claim, err := store.ClaimRandom(&ClaimOptions{
				Prefix: "user:",
				TTLMs:  MaxClaimTTLMs,
			})
			require.NoError(t, err)
			require.NotNil(t, claim)

			released, err := store.ReleaseClaim(claim.Ref())
			require.NoError(t, err)
			require.True(t, released)

			claim, err = store.ClaimRandom(&ClaimOptions{
				Prefix: "user:",
				TTLMs:  MaxClaimTTLMs + 1,
			})
			require.ErrorIs(t, err, ErrKVOptionsInvalid)
			require.Nil(t, claim)
		})
	}
}

// TestDiskStore_ClaimRandom_OwnerMustNotExceedMax verifies that disk store claim random owner must not exceed max.
func TestDiskStore_ClaimRandom_OwnerMustNotExceedMax(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
			require.NoError(t, store.Set("user:1", "alpha"))

			owner := strings.Repeat("o", MaxClaimOwnerBytes)
			claim, err := store.ClaimRandom(&ClaimOptions{
				Prefix: "user:",
				Owner:  owner,
				TTLMs:  DefaultClaimTTLMs,
			})
			require.NoError(t, err)
			require.NotNil(t, claim)
			assert.Equal(t, owner, claim.Owner)

			released, err := store.ReleaseClaim(claim.Ref())
			require.NoError(t, err)
			require.True(t, released)

			claim, err = store.ClaimRandom(&ClaimOptions{
				Prefix: "user:",
				Owner:  strings.Repeat("o", MaxClaimOwnerBytes+1),
				TTLMs:  DefaultClaimTTLMs,
			})
			require.ErrorIs(t, err, ErrKVOptionsInvalid)
			require.Nil(t, claim)
		})
	}
}

// TestDiskStore_ClaimRandom_ExpiredClaimBecomesAvailable verifies that disk store claim random expired claim becomes available.
func TestDiskStore_ClaimRandom_ExpiredClaimBecomesAvailable(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
			require.NoError(t, store.Set("user:1", "alpha"))

			firstClaim, err := store.ClaimRandom(&ClaimOptions{
				Prefix: "user:",
				TTLMs:  5,
			})
			require.NoError(t, err)
			require.NotNil(t, firstClaim)

			requireDiskClaimExpired(t, store, firstClaim.Ref())

			secondClaim, err := store.ClaimRandom(&ClaimOptions{
				Prefix: "user:",
				TTLMs:  10_000,
			})
			require.NoError(t, err)
			require.NotNil(t, secondClaim)
			assert.Equal(t, firstClaim.Key, secondClaim.Key)
			assert.Greater(t, secondClaim.Token, firstClaim.Token)
		})
	}
}

// TestDiskStore_ClaimRandom_ExpiredClaimAvailableWhenFullCleanupThrottled verifies that disk store claim random expired claim available when full cleanup throttled.
func TestDiskStore_ClaimRandom_ExpiredClaimAvailableWhenFullCleanupThrottled(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
			require.NoError(t, store.Set("user:1", "alpha"))

			firstClaim, err := store.ClaimRandom(&ClaimOptions{
				Prefix: "user:",
				TTLMs:  5,
			})
			require.NoError(t, err)
			require.NotNil(t, firstClaim)

			requireDiskClaimExpired(t, store, firstClaim.Ref())
			store.lastClaimsCleanupUnixMilli.Store(time.Now().UnixMilli())

			secondClaim, err := store.ClaimRandom(&ClaimOptions{
				Prefix: "user:",
				TTLMs:  10_000,
			})
			require.NoError(t, err)
			require.NotNil(t, secondClaim)
			assert.Equal(t, firstClaim.Key, secondClaim.Key)
			assert.Greater(t, secondClaim.Token, firstClaim.Token)
		})
	}
}

// TestDiskStore_ClaimRandom_HighOccupancyReturnsOnlyFreeKey verifies that disk store claim random high occupancy returns only free key.
func TestDiskStore_ClaimRandom_HighOccupancyReturnsOnlyFreeKey(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
			freeKey := seedAndPreclaimAllButOne(t, store)

			claim, err := store.ClaimRandom(&ClaimOptions{
				Prefix: "key:",
				TTLMs:  30_000,
			})
			require.NoError(t, err)
			require.NotNil(t, claim)
			assert.Equal(t, freeKey, claim.Key)
		})
	}
}

// TestDiskStore_PopRandom_HighOccupancyReturnsOnlyFreeKey verifies that disk store pop random high occupancy returns only free key.
func TestDiskStore_PopRandom_HighOccupancyReturnsOnlyFreeKey(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
			freeKey := seedAndPreclaimAllButOne(t, store)

			entry, err := store.PopRandom("key:")
			require.NoError(t, err)
			require.NotNil(t, entry)
			assert.Equal(t, freeKey, entry.Key)
		})
	}
}

// TestDiskStore_ReleaseClaim_And_CompleteClaim verifies that disk store release claim and complete claim.
func TestDiskStore_ReleaseClaim_And_CompleteClaim(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
			requirePopulateStore(
				t,
				store,
				"user:1", "alpha",
				"user:2", "beta",
			)

			claim, err := store.ClaimRandom(&ClaimOptions{
				Prefix: "user:1",
				TTLMs:  10_000,
			})
			require.NoError(t, err)
			require.NotNil(t, claim)

			released, err := store.ReleaseClaim(&ClaimRef{
				ID:    claim.ID,
				Key:   claim.Key,
				Token: claim.Token,
			})
			require.NoError(t, err)
			assert.True(t, released)

			claimAgain, err := store.ClaimRandom(&ClaimOptions{
				Prefix: "user:1",
				TTLMs:  10_000,
			})
			require.NoError(t, err)
			require.NotNil(t, claimAgain)

			staleRelease, err := store.ReleaseClaim(&ClaimRef{
				ID:    claimAgain.ID,
				Key:   claimAgain.Key,
				Token: claimAgain.Token + 1,
			})
			require.NoError(t, err)
			assert.False(t, staleRelease)

			releasedAgain, err := store.ReleaseClaim(&ClaimRef{
				ID:    claimAgain.ID,
				Key:   claimAgain.Key,
				Token: claimAgain.Token,
			})
			require.NoError(t, err)
			assert.True(t, releasedAgain)

			completeClaim, err := store.ClaimRandom(&ClaimOptions{
				Prefix: "user:2",
				TTLMs:  10_000,
			})
			require.NoError(t, err)
			require.NotNil(t, completeClaim)

			completed, err := store.CompleteClaim(&ClaimRef{
				ID:    completeClaim.ID,
				Key:   completeClaim.Key,
				Token: completeClaim.Token,
			}, nil)
			require.NoError(t, err)
			assert.True(t, completed)

			exists, err := store.Exists(completeClaim.Key)
			require.NoError(t, err)
			assert.False(t, exists)

			claimKeep, err := store.ClaimRandom(&ClaimOptions{
				Prefix: "user:1",
				TTLMs:  10_000,
			})
			require.NoError(t, err)
			require.NotNil(t, claimKeep)

			kept, err := store.CompleteClaim(&ClaimRef{
				ID:    claimKeep.ID,
				Key:   claimKeep.Key,
				Token: claimKeep.Token,
			}, &CompleteClaimOptions{
				DeleteKey: false,
			})
			require.NoError(t, err)
			assert.True(t, kept)

			exists, err = store.Exists(claimKeep.Key)
			require.NoError(t, err)
			assert.True(t, exists)
		})
	}
}

// TestDiskStore_ClaimMetadata_DoesNotLeakIntoUserScan verifies that disk store claim metadata does not leak into user scan.
func TestDiskStore_ClaimMetadata_DoesNotLeakIntoUserScan(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
			require.NoError(t, store.Set("user:1", "alpha"))

			claim, err := store.ClaimRandom(&ClaimOptions{
				Prefix: "user:",
				TTLMs:  10_000,
			})
			require.NoError(t, err)
			require.NotNil(t, claim)

			page, err := store.Scan("", "", 0)
			require.NoError(t, err)
			require.Len(t, page.Entries, 1)
			assert.Equal(t, "user:1", page.Entries[0].Key)

			count, err := store.Count("")
			require.NoError(t, err)
			assert.EqualValues(t, 1, count)

			key, err := store.RandomKey("")
			require.NoError(t, err)
			assert.Equal(t, "user:1", key)
		})
	}
}

// TestDiskStore_PopRandom_Concurrent_NoDuplicateKeys verifies that disk store pop random concurrent no duplicate keys.
func TestDiskStore_PopRandom_Concurrent_NoDuplicateKeys(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			const keysCount = 120

			store := newTestDiskStore(t, trackKeys, "", true)
			for i := range keysCount {
				require.NoError(t, store.Set("key:"+strconv.Itoa(i), "value"))
			}

			var (
				mu       sync.Mutex
				wg       sync.WaitGroup
				seenKeys = make(map[string]struct{}, keysCount)
			)

			for range keysCount {
				wg.Go(func() {
					entry, err := store.PopRandom("key:")
					require.NoError(t, err)
					require.NotNil(t, entry)

					mu.Lock()
					_, duplicate := seenKeys[entry.Key]
					require.False(t, duplicate, "duplicate popped key: %s", entry.Key)
					seenKeys[entry.Key] = struct{}{}
					mu.Unlock()
				})
			}

			wg.Wait()
			assert.Len(t, seenKeys, keysCount)
		})
	}
}

// TestDiskStore_ClaimRandom_Concurrent_NoDuplicateLiveClaims verifies that disk store claim random concurrent no duplicate live claims.
func TestDiskStore_ClaimRandom_Concurrent_NoDuplicateLiveClaims(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			const keysCount = 120

			store := newTestDiskStore(t, trackKeys, "", true)
			for i := range keysCount {
				require.NoError(t, store.Set("key:"+strconv.Itoa(i), "value"))
			}

			var (
				mu       sync.Mutex
				wg       sync.WaitGroup
				seenKeys = make(map[string]struct{}, keysCount)
			)

			for range keysCount {
				wg.Go(func() {
					claim, err := store.ClaimRandom(&ClaimOptions{
						Prefix: "key:",
						TTLMs:  30_000,
					})
					require.NoError(t, err)
					require.NotNil(t, claim)

					mu.Lock()
					_, duplicate := seenKeys[claim.Key]
					require.False(t, duplicate, "duplicate claimed key: %s", claim.Key)
					seenKeys[claim.Key] = struct{}{}
					mu.Unlock()
				})
			}

			wg.Wait()
			assert.Len(t, seenKeys, keysCount)
		})
	}
}

// TestDiskStore_OpenClearRestore_ClearClaims verifies that disk store open clear restore clear claims.
func TestDiskStore_OpenClearRestore_ClearClaims(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			diskPath := filepath.Join(t.TempDir(), "claims-lifecycle.db")
			store := newTestDiskStore(t, trackKeys, diskPath, true)

			require.NoError(t, store.Set("user:1", "alpha"))
			claim, err := store.ClaimRandom(&ClaimOptions{
				Prefix: "user:",
				TTLMs:  60_000,
			})
			require.NoError(t, err)
			require.NotNil(t, claim)

			require.NoError(t, store.Close())
			require.NoError(t, store.Open())

			claimAfterOpen, err := store.ClaimRandom(&ClaimOptions{
				Prefix: "user:",
				TTLMs:  60_000,
			})
			require.NoError(t, err)
			require.NotNil(t, claimAfterOpen)

			require.NoError(t, store.Clear())
			require.NoError(t, store.Set("user:1", "alpha"))

			claimAfterClear, err := store.ClaimRandom(&ClaimOptions{
				Prefix: "user:",
				TTLMs:  60_000,
			})
			require.NoError(t, err)
			require.NotNil(t, claimAfterClear)

			releasedAfterClear, err := store.ReleaseClaim(&ClaimRef{
				ID:    claimAfterClear.ID,
				Key:   claimAfterClear.Key,
				Token: claimAfterClear.Token,
			})
			require.NoError(t, err)
			require.True(t, releasedAfterClear)

			claimBeforeSelfRestore, err := store.ClaimRandom(&ClaimOptions{
				Prefix: "user:",
				TTLMs:  60_000,
			})
			require.NoError(t, err)
			require.NotNil(t, claimBeforeSelfRestore)

			_, err = store.Restore(&RestoreOptions{
				FileName: store.path,
			})
			require.NoError(t, err)

			claimAfterSelfRestore, err := store.ClaimRandom(&ClaimOptions{
				Prefix: "user:",
				TTLMs:  60_000,
			})
			require.NoError(t, err)
			require.NotNil(t, claimAfterSelfRestore)
		})
	}
}

// TestDiskStore_ClaimKey_Behavior verifies that disk store claim key behavior.
func TestDiskStore_ClaimKey_Behavior(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
			require.NoError(t, store.Set("user:1", "alpha"))

			claim, err := store.ClaimKey("user:1", &ClaimOptions{TTLMs: 30_000})
			require.NoError(t, err)
			require.NotNil(t, claim)

			again, err := store.ClaimKey("user:1", &ClaimOptions{TTLMs: 30_000})
			require.NoError(t, err)
			require.Nil(t, again)

			missing, err := store.ClaimKey("missing", &ClaimOptions{TTLMs: 30_000})
			require.NoError(t, err)
			require.Nil(t, missing)
		})
	}
}

// TestDiskStore_RenewClaim_TokenStableAndExpiryExtended verifies that disk store renew claim token stable and expiry extended.
func TestDiskStore_RenewClaim_TokenStableAndExpiryExtended(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
			require.NoError(t, store.Set("user:1", "alpha"))

			claim, err := store.ClaimKey("user:1", &ClaimOptions{TTLMs: 200})
			require.NoError(t, err)
			require.NotNil(t, claim)

			oldExpiresAt := claim.ExpiresAt

			renewed, err := store.RenewClaim(claim.Ref(), &RenewClaimOptions{TTLMs: 60_000})
			require.NoError(t, err)
			require.True(t, renewed)

			released, err := store.ReleaseClaim(claim.Ref())
			require.NoError(t, err)
			require.True(t, released, "release with original token must still work after renewal")

			nextClaim, err := store.ClaimKey("user:1", &ClaimOptions{TTLMs: 30_000})
			require.NoError(t, err)
			require.NotNil(t, nextClaim)
			assert.Greater(t, nextClaim.ExpiresAt, oldExpiresAt)
		})
	}
}

// TestDiskStore_ClaimRandomMany_ReturnsUniqueFreeClaims verifies that disk store claim random many returns unique free claims.
func TestDiskStore_ClaimRandomMany_ReturnsUniqueFreeClaims(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
			requirePopulateStore(
				t,
				store,
				"user:1", "alpha",
				"user:2", "beta",
				"user:3", "gamma",
			)

			liveClaim, err := store.ClaimKey("user:1", &ClaimOptions{TTLMs: 60_000})
			require.NoError(t, err)
			require.NotNil(t, liveClaim)

			claims, err := store.ClaimRandomMany(&ClaimManyOptions{
				Prefix: "user:",
				Count:  10,
				TTLMs:  60_000,
			})
			require.NoError(t, err)
			require.Len(t, claims, 2)

			seen := make(map[string]struct{}, len(claims))
			for _, claim := range claims {
				_, exists := seen[claim.Key]
				require.False(t, exists, "claimRandomMany must not return duplicate keys")

				seen[claim.Key] = struct{}{}
				require.NotEqual(t, "user:1", claim.Key, "claimRandomMany must skip live claims")
			}
		})
	}
}

// TestDiskStore_ApplyTrackedClaimLocked_DoesNotOverwriteLiveClaim verifies that disk store apply tracked claim locked does not overwrite live claim.
func TestDiskStore_ApplyTrackedClaimLocked_DoesNotOverwriteLiveClaim(t *testing.T) {
	t.Parallel()

	store := newTestDiskStore(t, true, "", true)
	require.NoError(t, store.Set("user:1", "alpha"))

	expiresAt := time.Now().UnixMilli() + 60_000

	store.keysLock.Lock()
	first := store.applyTrackedClaimLocked("user:1", []byte("alpha"), "owner-a", expiresAt)
	second := store.applyTrackedClaimLocked("user:1", []byte("alpha"), "owner-b", expiresAt)
	record, ok := store.ost.Meta("user:1")
	store.keysLock.Unlock()

	require.NotNil(t, first)
	require.Nil(t, second, "tracked claim helper must not overwrite an existing live claim")
	require.True(t, ok)
	require.NotNil(t, record)
	assert.Equal(t, first.ID, record.ID)
	assert.Equal(t, first.Token, record.Token)
	assert.Equal(t, "owner-a", record.Owner)
}

// TestDiskStore_PopRandomMany_DeletesOnlyFreeMatchingKeys verifies that disk store pop random many deletes only free matching keys.
func TestDiskStore_PopRandomMany_DeletesOnlyFreeMatchingKeys(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{true, false} {
		t.Run("trackKeys="+strconv.FormatBool(trackKeys), func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, trackKeys, "", true)
			requirePopulateStore(
				t,
				store,
				"user:1", "alpha",
				"user:2", "beta",
				"user:3", "gamma",
				"order:1", "order",
			)

			liveClaim, err := store.ClaimKey("user:1", &ClaimOptions{TTLMs: 60_000})
			require.NoError(t, err)
			require.NotNil(t, liveClaim)

			entries, err := store.PopRandomMany("user:", 10)
			require.NoError(t, err)
			require.Len(t, entries, 2)

			seen := make(map[string]struct{}, len(entries))
			for _, entry := range entries {
				_, exists := seen[entry.Key]
				require.False(t, exists, "popRandomMany must not return duplicate keys")

				seen[entry.Key] = struct{}{}
				require.NotEqual(t, "user:1", entry.Key, "popRandomMany must skip live claims")
			}

			exists, err := store.Exists("user:1")
			require.NoError(t, err)
			require.True(t, exists, "live claimed key must remain after popRandomMany")
		})
	}
}
