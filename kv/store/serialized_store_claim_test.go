package store

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	corruptJSONKey   = "bad:1"
	corruptJSONValue = "{bad-json"
)

func TestSerializedStore_PopRandom_JSONDecodeErrorDoesNotDeleteOrLeakClaim(t *testing.T) {
	t.Parallel()

	for _, testCase := range serializedRawStoreCases() {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			raw := testCase.newStore(t)
			require.NoError(t, raw.Set(corruptJSONKey, []byte(corruptJSONValue)))

			serialized := NewSerializedStore(raw, NewJSONSerializer())

			entry, err := serialized.PopRandom("bad:")
			require.Nil(t, entry)
			require.ErrorIs(t, err, ErrSerializerDecodeFailed)
			requireRawCorruptJSONStillAvailable(t, raw)
		})
	}
}

func TestSerializedStore_ClaimRandom_JSONDecodeErrorReleasesClaim(t *testing.T) {
	t.Parallel()

	for _, testCase := range serializedRawStoreCases() {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			raw := testCase.newStore(t)
			require.NoError(t, raw.Set(corruptJSONKey, []byte(corruptJSONValue)))

			serialized := NewSerializedStore(raw, NewJSONSerializer())

			claim, err := serialized.ClaimRandom(&ClaimOptions{Prefix: "bad:"})
			require.Nil(t, claim)
			require.ErrorIs(t, err, ErrSerializerDecodeFailed)
			requireRawCorruptJSONStillAvailable(t, raw)
		})
	}
}

func TestSerializedStore_PopRandom_CompletionFailureReturnsSentinel(t *testing.T) {
	t.Parallel()

	serialized := NewSerializedStore(&popRandomCompletionFailureStore{}, NewStringSerializer())

	entry, err := serialized.PopRandom("user:")
	require.Nil(t, entry)
	require.ErrorIs(t, err, ErrClaimCompletionFailed)
	require.ErrorContains(t, err, "popRandom")
}

type serializedRawStoreCase struct {
	name     string
	newStore func(*testing.T) Store
}

func serializedRawStoreCases() []serializedRawStoreCase {
	return []serializedRawStoreCase{
		{
			name: "memory",
			newStore: func(t *testing.T) Store {
				t.Helper()

				store := NewMemoryStore(&MemoryConfig{TrackKeys: true})
				require.NoError(t, store.Open())

				return store
			},
		},
		{
			name: "disk",
			newStore: func(t *testing.T) Store {
				t.Helper()

				return newTestDiskStore(t, true, "", true)
			},
		},
	}
}

func requireRawCorruptJSONStillAvailable(t *testing.T, raw Store) {
	t.Helper()

	exists, err := raw.Exists(corruptJSONKey)
	require.NoError(t, err)
	require.True(t, exists)

	value, err := raw.Get(corruptJSONKey)
	require.NoError(t, err)
	//nolint:testifylint // autofix turns this to incorrect JSONEq call.
	require.Equal(t, []byte(corruptJSONValue), value)

	claim, err := raw.ClaimRandom(&ClaimOptions{Prefix: "bad:"})
	require.NoError(t, err)
	require.NotNil(t, claim)

	released, err := raw.ReleaseClaim(claim.Ref())
	require.NoError(t, err)
	require.True(t, released)
}

type popRandomCompletionFailureStore struct {
	Store
}

func (s *popRandomCompletionFailureStore) ClaimRandom(_ *ClaimOptions) (*EntryClaim, error) {
	return &EntryClaim{
		ID:    "claim-id",
		Key:   "user:1",
		Token: 1,
		Entry: Entry{
			Key:   "user:1",
			Value: []byte("value"),
		},
	}, nil
}

func (s *popRandomCompletionFailureStore) CompleteClaim(_ *ClaimRef, _ *CompleteClaimOptions) (bool, error) {
	return false, nil
}
