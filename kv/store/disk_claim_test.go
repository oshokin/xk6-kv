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

			time.Sleep(10 * time.Millisecond)

			entry, err := store.PopRandom("user:")
			require.NoError(t, err)
			require.NotNil(t, entry)
			assert.Equal(t, "user:1", entry.Key)
		})
	}
}

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

			time.Sleep(10 * time.Millisecond)

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
