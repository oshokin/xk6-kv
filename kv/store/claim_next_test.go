package store

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func runClaimNextStoreTest(t *testing.T, fn func(t *testing.T, s Store)) {
	t.Helper()

	for _, factory := range claimForOwnerFactories() {
		t.Run(factory.name, func(t *testing.T) {
			t.Parallel()

			fn(t, factory.newStore(t))
		})
	}
}

func seedClaimNextValues(t *testing.T, s Store, keys ...string) {
	t.Helper()

	for _, key := range keys {
		require.NoErrorf(t, s.Set(key, []byte(key)), "Set(%q) must succeed", key)
	}
}

func requireClaimNext(t *testing.T, s Store, opts *ClaimOptions) *EntryClaim {
	t.Helper()

	claim, err := s.ClaimNext(opts)
	require.NoError(t, err)
	require.NotNil(t, claim)

	return claim
}

func TestStore_ClaimNext_EmptyReturnsNil(t *testing.T) {
	t.Parallel()

	runClaimNextStoreTest(t, func(t *testing.T, s Store) {
		t.Helper()

		claim, err := s.ClaimNext(nil)
		require.NoError(t, err)
		assert.Nil(t, claim)
	})
}

func TestStore_ClaimNext_BasicLexicographicOrder(t *testing.T) {
	t.Parallel()

	runClaimNextStoreTest(t, func(t *testing.T, s Store) {
		t.Helper()

		seedClaimNextValues(
			t,
			s,
			"jobs:2",
			"jobs:10",
			"jobs:1",
			"jobs:20",
		)

		opts := &ClaimOptions{
			Prefix: "jobs:",
			TTLMs:  60_000,
		}
		expected := []string{"jobs:1", "jobs:10", "jobs:2", "jobs:20"}

		for _, expectedKey := range expected {
			claim := requireClaimNext(t, s, opts)
			require.Equal(t, expectedKey, claim.Key)

			completed, err := s.CompleteClaim(claim.Ref(), &CompleteClaimOptions{
				DeleteKey: true,
			})
			require.NoError(t, err)
			require.True(t, completed)
		}

		exhausted, err := s.ClaimNext(opts)
		require.NoError(t, err)
		assert.Nil(t, exhausted)
	})
}

func TestStore_ClaimNext_ZeroPaddedNumericOrder(t *testing.T) {
	t.Parallel()

	runClaimNextStoreTest(t, func(t *testing.T, s Store) {
		t.Helper()

		seedClaimNextValues(
			t,
			s,
			"jobs:000003",
			"jobs:000001",
			"jobs:000002",
		)

		opts := &ClaimOptions{
			Prefix: "jobs:",
			TTLMs:  60_000,
		}
		expected := []string{
			"jobs:000001",
			"jobs:000002",
			"jobs:000003",
		}

		for _, expectedKey := range expected {
			claim := requireClaimNext(t, s, opts)
			require.Equal(t, expectedKey, claim.Key)

			completed, err := s.CompleteClaim(claim.Ref(), &CompleteClaimOptions{
				DeleteKey: true,
			})
			require.NoError(t, err)
			require.True(t, completed)
		}
	})
}

func TestStore_ClaimNext_PrefixScopedOrdering(t *testing.T) {
	t.Parallel()

	runClaimNextStoreTest(t, func(t *testing.T, s Store) {
		t.Helper()

		seedClaimNextValues(
			t,
			s,
			"jobs:000001",
			"jobs:000002",
			"users:000001",
		)

		opts := &ClaimOptions{
			Prefix: "jobs:",
			TTLMs:  60_000,
		}

		first := requireClaimNext(t, s, opts)
		require.Equal(t, "jobs:000001", first.Key)
		completed, err := s.CompleteClaim(first.Ref(), &CompleteClaimOptions{DeleteKey: true})
		require.NoError(t, err)
		require.True(t, completed)

		second := requireClaimNext(t, s, opts)
		require.Equal(t, "jobs:000002", second.Key)
		completed, err = s.CompleteClaim(second.Ref(), &CompleteClaimOptions{DeleteKey: true})
		require.NoError(t, err)
		require.True(t, completed)

		none, err := s.ClaimNext(opts)
		require.NoError(t, err)
		assert.Nil(t, none)
	})
}

func TestStore_ClaimNext_SkipsLiveHead(t *testing.T) {
	t.Parallel()

	runClaimNextStoreTest(t, func(t *testing.T, s Store) {
		t.Helper()

		seedClaimNextValues(
			t,
			s,
			"jobs:000001",
			"jobs:000002",
			"jobs:000003",
		)

		heldHead, err := s.ClaimKey("jobs:000001", &ClaimOptions{TTLMs: 60_000})
		require.NoError(t, err)
		require.NotNil(t, heldHead)

		next := requireClaimNext(t, s, &ClaimOptions{
			Prefix: "jobs:",
			TTLMs:  60_000,
		})
		assert.Equal(t, "jobs:000002", next.Key)

		releasedHead, err := s.ReleaseClaim(heldHead.Ref())
		require.NoError(t, err)
		require.True(t, releasedHead)

		releasedNext, err := s.ReleaseClaim(next.Ref())
		require.NoError(t, err)
		require.True(t, releasedNext)
	})
}

func TestStore_ClaimNext_ReleaseRequeuesHead(t *testing.T) {
	t.Parallel()

	runClaimNextStoreTest(t, func(t *testing.T, s Store) {
		t.Helper()

		seedClaimNextValues(t, s, "jobs:000001", "jobs:000002")

		opts := &ClaimOptions{
			Prefix: "jobs:",
			TTLMs:  60_000,
		}

		first := requireClaimNext(t, s, opts)

		released, err := s.ReleaseClaim(first.Ref())
		require.NoError(t, err)
		require.True(t, released)

		second := requireClaimNext(t, s, opts)
		assert.Equal(t, first.Key, second.Key)
		assert.NotEqual(t, first.ID, second.ID)
		assert.NotEqual(t, first.Token, second.Token)
	})
}

func TestStore_ClaimNext_CompleteDeleteFalseRequeuesHead(t *testing.T) {
	t.Parallel()

	runClaimNextStoreTest(t, func(t *testing.T, s Store) {
		t.Helper()

		seedClaimNextValues(t, s, "jobs:000001", "jobs:000002")

		opts := &ClaimOptions{
			Prefix: "jobs:",
			TTLMs:  60_000,
		}

		first := requireClaimNext(t, s, opts)

		completed, err := s.CompleteClaim(first.Ref(), &CompleteClaimOptions{
			DeleteKey: false,
		})
		require.NoError(t, err)
		require.True(t, completed)

		second := requireClaimNext(t, s, opts)
		assert.Equal(t, first.Key, second.Key)
		assert.NotEqual(t, first.ID, second.ID)
		assert.NotEqual(t, first.Token, second.Token)
	})
}

func TestStore_ClaimNext_CompleteDeleteTrueAdvances(t *testing.T) {
	t.Parallel()

	runClaimNextStoreTest(t, func(t *testing.T, s Store) {
		t.Helper()

		seedClaimNextValues(t, s, "jobs:000001", "jobs:000002")

		opts := &ClaimOptions{
			Prefix: "jobs:",
			TTLMs:  60_000,
		}

		first := requireClaimNext(t, s, opts)
		require.Equal(t, "jobs:000001", first.Key)

		completed, err := s.CompleteClaim(first.Ref(), &CompleteClaimOptions{
			DeleteKey: true,
		})
		require.NoError(t, err)
		require.True(t, completed)

		second := requireClaimNext(t, s, opts)
		require.Equal(t, "jobs:000002", second.Key)
	})
}

func TestStore_ClaimNext_ExpiredHeadBecomesAvailableAgain(t *testing.T) {
	t.Parallel()

	runClaimNextStoreTest(t, func(t *testing.T, s Store) {
		t.Helper()

		seedClaimNextValues(t, s, "jobs:000001", "jobs:000002")

		first := requireClaimNext(t, s, &ClaimOptions{
			Prefix: "jobs:",
			TTLMs:  60_000,
		})

		requireStoreClaimExpired(t, s, first.Ref())

		second := requireClaimNext(t, s, &ClaimOptions{
			Prefix: "jobs:",
			TTLMs:  60_000,
		})
		assert.Equal(t, first.Key, second.Key)
		assert.NotEqual(t, first.ID, second.ID)
		assert.NotEqual(t, first.Token, second.Token)
	})
}

func TestStore_ClaimNext_OwnerAndTTLPropagation(t *testing.T) {
	t.Parallel()

	runClaimNextStoreTest(t, func(t *testing.T, s Store) {
		t.Helper()

		seedClaimNextValues(t, s, "jobs:000001")

		const (
			ttlMs     = int64(60_000)
			tolerance = int64(2_500)
		)

		before := time.Now().UnixMilli()
		claim := requireClaimNext(t, s, &ClaimOptions{
			Prefix: "jobs:",
			Owner:  "worker:17",
			TTLMs:  ttlMs,
		})
		after := time.Now().UnixMilli()

		assert.Equal(t, "worker:17", claim.Owner)
		assert.GreaterOrEqual(t, claim.ExpiresAt, before+ttlMs-tolerance)
		assert.LessOrEqual(t, claim.ExpiresAt, after+ttlMs+tolerance)
	})
}

func TestStore_ClaimNext_ConcurrentSamePrefixGetsUniqueKeys(t *testing.T) {
	t.Parallel()

	runClaimNextStoreTest(t, func(t *testing.T, s Store) {
		t.Helper()

		const (
			totalKeys = 100
			callers   = 50
		)

		for i := 1; i <= totalKeys; i++ {
			key := fmt.Sprintf("jobs:%06d", i)
			require.NoError(t, s.Set(key, []byte(key)))
		}

		opts := &ClaimOptions{
			Prefix: "jobs:",
			TTLMs:  60_000,
		}

		var (
			wg     sync.WaitGroup
			mu     sync.Mutex
			claims []*EntryClaim
			errs   []error
		)

		for range callers {
			wg.Go(func() {
				claim, err := s.ClaimNext(opts)

				mu.Lock()
				defer mu.Unlock()

				if err != nil {
					errs = append(errs, err)
					return
				}

				if claim == nil {
					errs = append(errs, errors.New("claimNext returned nil"))
					return
				}

				claims = append(claims, claim)
			})
		}

		wg.Wait()

		require.Emptyf(t, errs, "concurrent claimNext errors: %v", errs)
		require.Len(t, claims, callers)

		seenIDs := make(map[string]struct{}, callers)
		seenKeys := make(map[string]struct{}, callers)
		keys := make([]string, 0, callers)

		for _, claim := range claims {
			seenIDs[claim.ID] = struct{}{}
			seenKeys[claim.Key] = struct{}{}
			keys = append(keys, claim.Key)
		}

		assert.Len(t, seenIDs, callers)
		assert.Len(t, seenKeys, callers)

		sort.Strings(keys)

		expected := make([]string, callers)
		for i := range callers {
			expected[i] = fmt.Sprintf("jobs:%06d", i+1)
		}

		assert.Equal(t, expected, keys)
	})
}

func TestStore_ClaimNext_ConcurrentReleaseAndCompleteDoesNotDoubleLease(t *testing.T) {
	t.Parallel()

	runClaimNextStoreTest(t, func(t *testing.T, s Store) {
		t.Helper()

		const totalKeys = 16

		for i := 1; i <= totalKeys; i++ {
			key := fmt.Sprintf("jobs:%06d", i)
			require.NoError(t, s.Set(key, []byte(key)))
		}

		const workerIterations = 200

		opts := &ClaimOptions{
			Prefix: "jobs:",
			TTLMs:  60_000,
		}

		liveClaimsByKey := make(map[string]string)

		var liveMu sync.Mutex

		claimStart := func(claim *EntryClaim) error {
			liveMu.Lock()
			defer liveMu.Unlock()

			if existingID, exists := liveClaimsByKey[claim.Key]; exists {
				return fmt.Errorf(
					"duplicate live key %q: existing claim=%s new claim=%s",
					claim.Key,
					existingID,
					claim.ID,
				)
			}

			liveClaimsByKey[claim.Key] = claim.ID

			return nil
		}

		claimEnd := func(claim *EntryClaim) {
			liveMu.Lock()
			defer liveMu.Unlock()

			delete(liveClaimsByKey, claim.Key)
		}

		errCh := make(chan error, workerIterations*2)

		var wg sync.WaitGroup

		completeWithoutDelete := func(claim *EntryClaim) error {
			completed, err := s.CompleteClaim(claim.Ref(), &CompleteClaimOptions{
				DeleteKey: false,
			})
			if err != nil {
				return err
			}

			if !completed {
				return fmt.Errorf("completeClaim returned false for %s", claim.ID)
			}

			return nil
		}

		releaseLiveClaim := func(claim *EntryClaim) error {
			released, err := s.ReleaseClaim(claim.Ref())
			if err != nil {
				return err
			}

			if !released {
				return fmt.Errorf("releaseClaim returned false for %s", claim.ID)
			}

			return nil
		}

		runWorker := func(name string, useCompleteWithoutDelete bool) {
			for range workerIterations {
				claim, err := s.ClaimNext(opts)
				if err != nil {
					errCh <- fmt.Errorf("%s claimNext failed: %w", name, err)
					return
				}

				if claim == nil {
					continue
				}

				if err := claimStart(claim); err != nil {
					errCh <- err
					return
				}

				// Stop test-level "live claim" tracking before lifecycle finalization.
				// Once release/complete succeeds, immediate re-lease of the same key is valid.
				claimEnd(claim)

				var opErr error

				if useCompleteWithoutDelete {
					opErr = completeWithoutDelete(claim)
				} else {
					opErr = releaseLiveClaim(claim)
				}

				if opErr != nil {
					errCh <- fmt.Errorf("%s lifecycle op failed: %w", name, opErr)
					return
				}
			}
		}

		wg.Go(func() {
			runWorker("release-worker", false)
		})

		wg.Go(func() {
			runWorker("complete-worker", true)
		})

		wg.Wait()
		close(errCh)

		for err := range errCh {
			require.NoError(t, err)
		}

		liveMu.Lock()
		assert.Empty(t, liveClaimsByKey)
		liveMu.Unlock()
	})
}

func TestStore_ClaimNext_SharesExclusivityWithClaimRandom(t *testing.T) {
	t.Parallel()

	runClaimNextStoreTest(t, func(t *testing.T, s Store) {
		t.Helper()

		opts := &ClaimOptions{
			Prefix: "jobs:",
			TTLMs:  60_000,
		}

		seedClaimNextValues(t, s, "jobs:000001", "jobs:000002")

		randomClaim := requireClaimNextFromRandom(t, s, opts)
		nextClaim := requireClaimNext(t, s, opts)
		assert.NotEqual(t, randomClaim.Key, nextClaim.Key)

		released, err := s.ReleaseClaim(randomClaim.Ref())
		require.NoError(t, err)
		require.True(t, released)

		released, err = s.ReleaseClaim(nextClaim.Ref())
		require.NoError(t, err)
		require.True(t, released)

		firstNext := requireClaimNext(t, s, opts)
		randomAfter := requireClaimNextFromRandom(t, s, opts)
		assert.NotEqual(t, firstNext.Key, randomAfter.Key)
	})
}

func TestStore_ClaimNext_SharesExclusivityWithClaimForOwner(t *testing.T) {
	t.Parallel()

	runClaimNextStoreTest(t, func(t *testing.T, s Store) {
		t.Helper()

		opts := &ClaimOptions{
			Prefix: "jobs:",
			TTLMs:  60_000,
		}

		seedClaimNextValues(t, s, "jobs:000001", "jobs:000002")

		ownerClaim, err := s.ClaimForOwner(&ClaimOptions{
			Prefix: "jobs:",
			Owner:  "owner:a",
			TTLMs:  60_000,
		})
		require.NoError(t, err)
		require.NotNil(t, ownerClaim)

		nextClaim := requireClaimNext(t, s, opts)
		assert.NotEqual(t, ownerClaim.Key, nextClaim.Key)

		released, err := s.ReleaseClaim(ownerClaim.Ref())
		require.NoError(t, err)
		require.True(t, released)

		released, err = s.ReleaseClaim(nextClaim.Ref())
		require.NoError(t, err)
		require.True(t, released)

		firstNext := requireClaimNext(t, s, opts)
		ownerAfter, err := s.ClaimForOwner(&ClaimOptions{
			Prefix: "jobs:",
			Owner:  "owner:b",
			TTLMs:  60_000,
		})
		require.NoError(t, err)
		require.NotNil(t, ownerAfter)
		assert.NotEqual(t, firstNext.Key, ownerAfter.Key)
	})
}

func requireClaimNextFromRandom(t *testing.T, s Store, opts *ClaimOptions) *EntryClaim {
	t.Helper()

	claim, err := s.ClaimRandom(opts)
	require.NoError(t, err)
	require.NotNil(t, claim)

	return claim
}
