package store

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type claimForOwnerStoreFactory struct {
	name      string
	trackKeys bool
	newStore  func(t *testing.T) Store
}

func claimForOwnerFactories() []*claimForOwnerStoreFactory {
	return []*claimForOwnerStoreFactory{
		{
			name:      "memory trackKeys=false",
			trackKeys: false,
			newStore: func(t *testing.T) Store {
				t.Helper()

				s := NewMemoryStore(&MemoryConfig{
					TrackKeys: false,
				})
				require.NoError(t, s.Open())

				return s
			},
		},
		{
			name:      "memory trackKeys=true",
			trackKeys: true,
			newStore: func(t *testing.T) Store {
				t.Helper()

				s := NewMemoryStore(&MemoryConfig{
					TrackKeys: true,
				})
				require.NoError(t, s.Open())

				return s
			},
		},
		{
			name:      "disk trackKeys=false",
			trackKeys: false,
			newStore: func(t *testing.T) Store {
				t.Helper()

				return newTestDiskStore(t, false, "", true)
			},
		},
		{
			name:      "disk trackKeys=true",
			trackKeys: true,
			newStore: func(t *testing.T) Store {
				t.Helper()

				return newTestDiskStore(t, true, "", true)
			},
		},
	}
}

func TestStore_ClaimForOwner_ReusesLiveClaim(t *testing.T) {
	t.Parallel()

	for _, factory := range claimForOwnerFactories() {
		t.Run(factory.name, func(t *testing.T) {
			t.Parallel()

			s := factory.newStore(t)
			require.NoError(t, s.Set("users:1", []byte("alice")))
			require.NoError(t, s.Set("users:2", []byte("bob")))

			opts := &ClaimOptions{
				Prefix: "users:",
				Owner:  "scenario:login:vu:1",
				TTLMs:  60_000,
			}

			first, err := s.ClaimForOwner(opts)
			require.NoError(t, err)
			require.NotNil(t, first)

			second, err := s.ClaimForOwner(opts)
			require.NoError(t, err)
			require.NotNil(t, second)

			assert.Equal(t, first.ID, second.ID)
			assert.Equal(t, first.Key, second.Key)
			assert.Equal(t, first.Token, second.Token)
			assert.Equal(t, first.Owner, second.Owner)
			assert.Equal(t, first.ExpiresAt, second.ExpiresAt)
		})
	}
}

func TestStore_ClaimForOwner_RequiresOwner(t *testing.T) {
	t.Parallel()

	for _, factory := range claimForOwnerFactories() {
		t.Run(factory.name, func(t *testing.T) {
			t.Parallel()

			s := factory.newStore(t)
			require.NoError(t, s.Set("users:1", []byte("alice")))

			claim, err := s.ClaimForOwner(nil)
			require.ErrorIs(t, err, ErrKVOptionsInvalid)
			require.Nil(t, claim)

			claim, err = s.ClaimForOwner(&ClaimOptions{
				Prefix: "users:",
				Owner:  "",
				TTLMs:  60_000,
			})
			require.ErrorIs(t, err, ErrKVOptionsInvalid)
			require.Nil(t, claim)

			claim, err = s.ClaimForOwner(&ClaimOptions{
				Prefix: "users:",
				Owner:  strings.Repeat("o", MaxClaimOwnerBytes+1),
				TTLMs:  60_000,
			})
			require.ErrorIs(t, err, ErrKVOptionsInvalid)
			require.Nil(t, claim)
		})
	}
}

func TestStore_ClaimForOwner_UsesDefaultTTLWhenZero(t *testing.T) {
	t.Parallel()

	for _, factory := range claimForOwnerFactories() {
		t.Run(factory.name, func(t *testing.T) {
			t.Parallel()

			s := factory.newStore(t)
			require.NoError(t, s.Set("users:1", []byte("alice")))

			claim, err := s.ClaimForOwner(&ClaimOptions{
				Prefix: "users:",
				Owner:  "vu:1",
				TTLMs:  0,
			})
			require.NoError(t, err)
			require.NotNil(t, claim)
			assert.Positive(t, claim.ExpiresAt)
		})
	}
}

func TestStore_ClaimForOwner_DoesNotImplicitlyRenew(t *testing.T) {
	t.Parallel()

	for _, factory := range claimForOwnerFactories() {
		t.Run(factory.name, func(t *testing.T) {
			t.Parallel()

			s := factory.newStore(t)
			require.NoError(t, s.Set("users:1", []byte("alice")))

			first, err := s.ClaimForOwner(&ClaimOptions{
				Prefix: "users:",
				Owner:  "vu:1",
				TTLMs:  60_000,
			})
			require.NoError(t, err)
			require.NotNil(t, first)

			second, err := s.ClaimForOwner(&ClaimOptions{
				Prefix: "users:",
				Owner:  "vu:1",
				TTLMs:  120_000,
			})
			require.NoError(t, err)
			require.NotNil(t, second)

			assert.Equal(t, first.ID, second.ID)
			assert.Equal(t, first.ExpiresAt, second.ExpiresAt)
		})
	}
}

func TestStore_ClaimForOwner_SameOwnerDifferentPrefixesIndependent(t *testing.T) {
	t.Parallel()

	for _, factory := range claimForOwnerFactories() {
		t.Run(factory.name, func(t *testing.T) {
			t.Parallel()

			s := factory.newStore(t)
			require.NoError(t, s.Set("users:a", []byte("alice")))
			require.NoError(t, s.Set("admins:a", []byte("admin")))

			userClaim, err := s.ClaimForOwner(&ClaimOptions{
				Prefix: "users:",
				Owner:  "vu:7",
				TTLMs:  60_000,
			})
			require.NoError(t, err)
			require.NotNil(t, userClaim)

			adminClaim, err := s.ClaimForOwner(&ClaimOptions{
				Prefix: "admins:",
				Owner:  "vu:7",
				TTLMs:  60_000,
			})
			require.NoError(t, err)
			require.NotNil(t, adminClaim)

			assert.NotEqual(t, userClaim.Key, adminClaim.Key)
			assert.NotEqual(t, userClaim.ID, adminClaim.ID)
		})
	}
}

func TestStore_ClaimForOwner_DifferentOwnersGetDifferentKeys(t *testing.T) {
	t.Parallel()

	for _, factory := range claimForOwnerFactories() {
		t.Run(factory.name, func(t *testing.T) {
			t.Parallel()

			s := factory.newStore(t)
			require.NoError(t, s.Set("users:01", []byte("u1")))
			require.NoError(t, s.Set("users:02", []byte("u2")))

			first, err := s.ClaimForOwner(&ClaimOptions{
				Prefix: "users:",
				Owner:  "vu:1",
				TTLMs:  60_000,
			})
			require.NoError(t, err)
			require.NotNil(t, first)

			second, err := s.ClaimForOwner(&ClaimOptions{
				Prefix: "users:",
				Owner:  "vu:2",
				TTLMs:  60_000,
			})
			require.NoError(t, err)
			require.NotNil(t, second)

			assert.NotEqual(t, first.Key, second.Key)
			assert.NotEqual(t, first.ID, second.ID)
		})
	}
}

func TestStore_ClaimForOwner_PoolExhaustedReturnsNil(t *testing.T) {
	t.Parallel()

	for _, factory := range claimForOwnerFactories() {
		t.Run(factory.name, func(t *testing.T) {
			t.Parallel()

			s := factory.newStore(t)
			require.NoError(t, s.Set("users:1", []byte("u1")))

			first, err := s.ClaimForOwner(&ClaimOptions{
				Prefix: "users:",
				Owner:  "vu:1",
				TTLMs:  60_000,
			})
			require.NoError(t, err)
			require.NotNil(t, first)

			second, err := s.ClaimForOwner(&ClaimOptions{
				Prefix: "users:",
				Owner:  "vu:2",
				TTLMs:  60_000,
			})
			require.NoError(t, err)
			require.Nil(t, second)
		})
	}
}

func TestStore_ClaimForOwner_PoolExhaustionIsNotCached(t *testing.T) {
	t.Parallel()

	for _, factory := range claimForOwnerFactories() {
		t.Run(factory.name, func(t *testing.T) {
			t.Parallel()

			s := factory.newStore(t)

			opts := &ClaimOptions{
				Prefix: "users:",
				Owner:  "vu:1",
				TTLMs:  60_000,
			}

			first, err := s.ClaimForOwner(opts)
			require.NoError(t, err)
			require.Nil(t, first)

			require.NoError(t, s.Set("users:1", []byte("u1")))

			second, err := s.ClaimForOwner(opts)
			require.NoError(t, err)
			require.NotNil(t, second)
			assert.Equal(t, "users:1", second.Key)
		})
	}
}

func TestStore_ClaimForOwner_ConcurrentSameOwnerGetsOneClaim(t *testing.T) {
	t.Parallel()

	for _, factory := range claimForOwnerFactories() {
		t.Run(factory.name, func(t *testing.T) {
			t.Parallel()

			s := factory.newStore(t)

			for i := range 100 {
				key := fmt.Sprintf("users:%03d", i)
				require.NoError(t, s.Set(key, []byte(key)))
			}

			const callers = 50

			results := make(chan *EntryClaim, callers)
			errs := make(chan error, callers)

			var wg sync.WaitGroup
			for range callers {
				wg.Go(func() {
					claim, err := s.ClaimForOwner(&ClaimOptions{
						Prefix: "users:",
						Owner:  "scenario:a:vu:17",
						TTLMs:  60_000,
					})
					if err != nil {
						errs <- err
						return
					}

					results <- claim
				})
			}

			wg.Wait()
			close(results)
			close(errs)

			for err := range errs {
				require.NoError(t, err)
			}

			var first *EntryClaim

			for claim := range results {
				require.NotNil(t, claim)

				if first == nil {
					first = claim
					continue
				}

				assert.Equal(t, first.ID, claim.ID)
				assert.Equal(t, first.Key, claim.Key)
				assert.Equal(t, first.Token, claim.Token)
			}

			stats, err := s.AllocationStats("users:")
			require.NoError(t, err)
			assert.EqualValues(t, 1, stats.ClaimedLive)
		})
	}
}

func TestStore_ClaimForOwner_ConcurrentDifferentOwnersGetUniqueKeys(t *testing.T) {
	t.Parallel()

	for _, factory := range claimForOwnerFactories() {
		t.Run(factory.name, func(t *testing.T) {
			t.Parallel()

			s := factory.newStore(t)

			const owners = 50

			for i := range owners {
				key := fmt.Sprintf("users:%03d", i)
				require.NoError(t, s.Set(key, []byte(key)))
			}

			type ownerClaim struct {
				owner string
				claim *EntryClaim
			}

			results := make(chan ownerClaim, owners)
			errs := make(chan error, owners)

			var wg sync.WaitGroup

			for i := range owners {
				owner := fmt.Sprintf("scenario:a:vu:%d", i)

				wg.Go(func() {
					claim, err := s.ClaimForOwner(&ClaimOptions{
						Prefix: "users:",
						Owner:  owner,
						TTLMs:  60_000,
					})
					if err != nil {
						errs <- err
						return
					}

					results <- ownerClaim{owner: owner, claim: claim}
				})
			}

			wg.Wait()
			close(results)
			close(errs)

			for err := range errs {
				require.NoError(t, err)
			}

			seenByOwner := make(map[string]*EntryClaim, owners)
			seenKeys := make(map[string]struct{}, owners)

			for item := range results {
				require.NotNil(t, item.claim)
				seenByOwner[item.owner] = item.claim

				_, duplicate := seenKeys[item.claim.Key]
				require.Falsef(t, duplicate, "duplicate key allocated: %s", item.claim.Key)
				seenKeys[item.claim.Key] = struct{}{}
			}

			require.Len(t, seenByOwner, owners)
			require.Len(t, seenKeys, owners)
		})
	}
}

func TestStore_ClaimForOwner_ReleaseInvalidatesBinding(t *testing.T) {
	t.Parallel()

	for _, factory := range claimForOwnerFactories() {
		t.Run(factory.name, func(t *testing.T) {
			t.Parallel()

			s := factory.newStore(t)
			require.NoError(t, s.Set("users:1", []byte("alice")))

			first, err := s.ClaimForOwner(&ClaimOptions{
				Prefix: "users:",
				Owner:  "vu:1",
				TTLMs:  60_000,
			})
			require.NoError(t, err)
			require.NotNil(t, first)

			released, err := s.ReleaseClaim(first.Ref())
			require.NoError(t, err)
			require.True(t, released)

			second, err := s.ClaimForOwner(&ClaimOptions{
				Prefix: "users:",
				Owner:  "vu:1",
				TTLMs:  60_000,
			})
			require.NoError(t, err)
			require.NotNil(t, second)

			assert.NotEqual(t, first.ID, second.ID)
			assert.NotEqual(t, first.Token, second.Token)
		})
	}
}

func TestStore_ClaimForOwner_CompleteDeleteFalseInvalidatesBinding(t *testing.T) {
	t.Parallel()

	for _, factory := range claimForOwnerFactories() {
		t.Run(factory.name, func(t *testing.T) {
			t.Parallel()

			s := factory.newStore(t)
			require.NoError(t, s.Set("users:1", []byte("alice")))

			first, err := s.ClaimForOwner(&ClaimOptions{
				Prefix: "users:",
				Owner:  "vu:1",
				TTLMs:  60_000,
			})
			require.NoError(t, err)
			require.NotNil(t, first)

			completed, err := s.CompleteClaim(first.Ref(), &CompleteClaimOptions{
				DeleteKey: false,
			})
			require.NoError(t, err)
			require.True(t, completed)

			second, err := s.ClaimForOwner(&ClaimOptions{
				Prefix: "users:",
				Owner:  "vu:1",
				TTLMs:  60_000,
			})
			require.NoError(t, err)
			require.NotNil(t, second)

			assert.Equal(t, first.Key, second.Key)
			assert.NotEqual(t, first.ID, second.ID)
			assert.Greater(t, second.Token, first.Token)
		})
	}
}

func TestStore_ClaimForOwner_CompleteDeleteTrueMayExhaustPool(t *testing.T) {
	t.Parallel()

	for _, factory := range claimForOwnerFactories() {
		t.Run(factory.name, func(t *testing.T) {
			t.Parallel()

			s := factory.newStore(t)
			require.NoError(t, s.Set("users:1", []byte("alice")))

			first, err := s.ClaimForOwner(&ClaimOptions{
				Prefix: "users:",
				Owner:  "vu:1",
				TTLMs:  60_000,
			})
			require.NoError(t, err)
			require.NotNil(t, first)

			completed, err := s.CompleteClaim(first.Ref(), &CompleteClaimOptions{
				DeleteKey: true,
			})
			require.NoError(t, err)
			require.True(t, completed)

			second, err := s.ClaimForOwner(&ClaimOptions{
				Prefix: "users:",
				Owner:  "vu:1",
				TTLMs:  60_000,
			})
			require.NoError(t, err)
			require.Nil(t, second)
		})
	}
}

func TestStore_ClaimForOwner_ExpirationInvalidatesBinding(t *testing.T) {
	t.Parallel()

	for _, factory := range claimForOwnerFactories() {
		t.Run(factory.name, func(t *testing.T) {
			t.Parallel()

			s := factory.newStore(t)
			require.NoError(t, s.Set("users:1", []byte("alice")))

			first, err := s.ClaimForOwner(&ClaimOptions{
				Prefix: "users:",
				Owner:  "vu:1",
				TTLMs:  60_000,
			})
			require.NoError(t, err)
			require.NotNil(t, first)

			requireStoreClaimExpired(t, s, first.Ref())

			second, err := s.ClaimForOwner(&ClaimOptions{
				Prefix: "users:",
				Owner:  "vu:1",
				TTLMs:  60_000,
			})
			require.NoError(t, err)
			require.NotNil(t, second)

			assert.Equal(t, first.Key, second.Key)
			assert.NotEqual(t, first.ID, second.ID)
			assert.NotEqual(t, first.Token, second.Token)
		})
	}
}

func TestStore_ClaimForOwner_ClaimRandomOwnerIsNotSticky(t *testing.T) {
	t.Parallel()

	for _, factory := range claimForOwnerFactories() {
		t.Run(factory.name, func(t *testing.T) {
			t.Parallel()

			s := factory.newStore(t)
			require.NoError(t, s.Set("users:1", []byte("alice")))
			require.NoError(t, s.Set("users:2", []byte("bob")))

			ordinary, err := s.ClaimRandom(&ClaimOptions{
				Prefix: "users:",
				Owner:  "vu:1",
				TTLMs:  60_000,
			})
			require.NoError(t, err)
			require.NotNil(t, ordinary)

			sticky, err := s.ClaimForOwner(&ClaimOptions{
				Prefix: "users:",
				Owner:  "vu:1",
				TTLMs:  60_000,
			})
			require.NoError(t, err)
			require.NotNil(t, sticky)

			assert.NotEqual(t, ordinary.ID, sticky.ID)
			assert.NotEqual(t, ordinary.Key, sticky.Key)
		})
	}
}

func TestStore_ClaimForOwner_RefreshesValueSnapshot(t *testing.T) {
	t.Parallel()

	for _, factory := range claimForOwnerFactories() {
		t.Run(factory.name, func(t *testing.T) {
			t.Parallel()

			s := factory.newStore(t)
			require.NoError(t, s.Set("users:1", []byte("initial")))

			first, err := s.ClaimForOwner(&ClaimOptions{
				Prefix: "users:",
				Owner:  "vu:1",
				TTLMs:  60_000,
			})
			require.NoError(t, err)
			require.NotNil(t, first)

			require.NoError(t, s.Set(first.Key, []byte("updated")))

			second, err := s.ClaimForOwner(&ClaimOptions{
				Prefix: "users:",
				Owner:  "vu:1",
				TTLMs:  60_000,
			})
			require.NoError(t, err)
			require.NotNil(t, second)

			assert.Equal(t, first.ID, second.ID)
			assert.Equal(t, first.Key, second.Key)
			assert.Equal(t, first.Token, second.Token)
			assert.Equal(t, []byte("updated"), second.Entry.Value)
		})
	}
}

func TestStore_ClaimForOwner_RenewReflectedOnNextLookup(t *testing.T) {
	t.Parallel()

	for _, factory := range claimForOwnerFactories() {
		t.Run(factory.name, func(t *testing.T) {
			t.Parallel()

			s := factory.newStore(t)
			require.NoError(t, s.Set("users:1", []byte("alice")))

			first, err := s.ClaimForOwner(&ClaimOptions{
				Prefix: "users:",
				Owner:  "vu:1",
				TTLMs:  500,
			})
			require.NoError(t, err)
			require.NotNil(t, first)

			renewed, err := s.RenewClaim(first.Ref(), &RenewClaimOptions{TTLMs: 120_000})
			require.NoError(t, err)
			require.True(t, renewed)

			second, err := s.ClaimForOwner(&ClaimOptions{
				Prefix: "users:",
				Owner:  "vu:1",
				TTLMs:  500,
			})
			require.NoError(t, err)
			require.NotNil(t, second)

			assert.Equal(t, first.ID, second.ID)
			assert.Equal(t, first.Key, second.Key)
			assert.Equal(t, first.Token, second.Token)
			assert.Greater(t, second.ExpiresAt, first.ExpiresAt)
		})
	}
}

func TestStore_ClaimForOwner_ClearLazyRepair(t *testing.T) {
	t.Parallel()

	for _, factory := range claimForOwnerFactories() {
		t.Run(factory.name, func(t *testing.T) {
			t.Parallel()

			s := factory.newStore(t)
			require.NoError(t, s.Set("users:1", []byte("alice")))

			first, err := s.ClaimForOwner(&ClaimOptions{
				Prefix: "users:",
				Owner:  "vu:1",
				TTLMs:  60_000,
			})
			require.NoError(t, err)
			require.NotNil(t, first)

			require.NoError(t, s.Clear())
			require.NoError(t, s.Set("users:1", []byte("alice")))

			second, err := s.ClaimForOwner(&ClaimOptions{
				Prefix: "users:",
				Owner:  "vu:1",
				TTLMs:  60_000,
			})
			require.NoError(t, err)
			require.NotNil(t, second)

			assert.NotEqual(t, first.ID, second.ID)
			assert.NotEqual(t, first.Token, second.Token)
		})
	}
}

func TestDiskStore_ClaimForOwner_CloseOpenLazyRepair(t *testing.T) {
	t.Parallel()

	for _, trackKeys := range []bool{false, true} {
		t.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(t *testing.T) {
			t.Parallel()

			s := newTestDiskStore(t, trackKeys, "", true)
			require.NoError(t, s.Set("users:1", []byte("alice")))

			first, err := s.ClaimForOwner(&ClaimOptions{
				Prefix: "users:",
				Owner:  "vu:1",
				TTLMs:  60_000,
			})
			require.NoError(t, err)
			require.NotNil(t, first)

			require.NoError(t, s.Close())
			require.NoError(t, s.Open())

			second, err := s.ClaimForOwner(&ClaimOptions{
				Prefix: "users:",
				Owner:  "vu:1",
				TTLMs:  60_000,
			})
			require.NoError(t, err)
			require.NotNil(t, second)

			assert.NotEqual(t, first.ID, second.ID)
			assert.NotEqual(t, first.Token, second.Token)
		})
	}
}
