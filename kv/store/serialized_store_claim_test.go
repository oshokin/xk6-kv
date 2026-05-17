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

func TestSerializedStore_ClaimKey_JSONDecodeErrorReleasesClaim(t *testing.T) {
	t.Parallel()

	for _, testCase := range serializedRawStoreCases() {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			raw := testCase.newStore(t)
			require.NoError(t, raw.Set(corruptJSONKey, []byte(corruptJSONValue)))

			serialized := NewSerializedStore(raw, NewJSONSerializer())

			claim, err := serialized.ClaimKey(corruptJSONKey, &ClaimOptions{TTLMs: 30_000})
			require.Nil(t, claim)
			require.ErrorIs(t, err, ErrSerializerDecodeFailed)
			requireRawCorruptJSONStillAvailable(t, raw)
		})
	}
}

func TestSerializedStore_ClaimRandomMany_JSONDecodeErrorReleasesAllClaims(t *testing.T) {
	t.Parallel()

	for _, testCase := range serializedRawStoreCases() {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			raw := testCase.newStore(t)
			require.NoError(t, raw.Set("bad:1", []byte(corruptJSONValue)))
			require.NoError(t, raw.Set("bad:2", []byte(corruptJSONValue)))

			serialized := NewSerializedStore(raw, NewJSONSerializer())

			claims, err := serialized.ClaimRandomMany(&ClaimManyOptions{
				Prefix: "bad:",
				Count:  2,
				TTLMs:  30_000,
			})
			require.Nil(t, claims)
			require.ErrorIs(t, err, ErrSerializerDecodeFailed)

			rawClaims, err := raw.ClaimRandomMany(&ClaimManyOptions{
				Prefix: "bad:",
				Count:  2,
				TTLMs:  30_000,
			})
			require.NoError(t, err)
			require.Len(t, rawClaims, 2, "all claims must be released after decode failure")
		})
	}
}

func TestSerializedStore_PopRandomMany_JSONDecodeErrorDoesNotDeleteOrLeakClaims(t *testing.T) {
	t.Parallel()

	for _, testCase := range serializedRawStoreCases() {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			raw := testCase.newStore(t)
			require.NoError(t, raw.Set("bad:1", []byte(corruptJSONValue)))
			require.NoError(t, raw.Set("bad:2", []byte(corruptJSONValue)))

			serialized := NewSerializedStore(raw, NewJSONSerializer())

			entries, err := serialized.PopRandomMany("bad:", 2)
			require.Nil(t, entries)
			require.ErrorIs(t, err, ErrSerializerDecodeFailed)

			exists, err := raw.Exists("bad:1")
			require.NoError(t, err)
			require.True(t, exists)
			exists, err = raw.Exists("bad:2")
			require.NoError(t, err)
			require.True(t, exists)

			claims, err := raw.ClaimRandomMany(&ClaimManyOptions{
				Prefix: "bad:",
				Count:  2,
				TTLMs:  30_000,
			})
			require.NoError(t, err)
			require.Len(t, claims, 2, "claims must be released after popRandomMany decode failure")
		})
	}
}

func TestSerializedStore_PopRandom_CompletionFailureReturnsSentinel(t *testing.T) {
	t.Parallel()

	raw := &popRandomCompletionFailureStore{}
	serialized := NewSerializedStore(raw, NewStringSerializer())

	entry, err := serialized.PopRandom("user:")
	require.Nil(t, entry)
	require.ErrorIs(t, err, ErrClaimCompletionFailed)
	require.ErrorContains(t, err, "popRandom")
	require.Len(t, raw.released, 1, "single popRandom completion failure must release its claim")
}

func TestSerializedStore_PopRandom_CompleteError_ReleasesClaim(t *testing.T) {
	t.Parallel()

	raw := &popRandomCompleteErrorStore{}
	serialized := NewSerializedStore(raw, NewStringSerializer())

	entry, err := serialized.PopRandom("user:")
	require.Nil(t, entry)
	require.ErrorIs(t, err, ErrDiskStoreWriteFailed)
	require.Len(t, raw.released, 1, "single popRandom completion error must release its claim")
}

func TestSerializedStore_PopRandomMany_CompleteFailure_ReleasesRemainingClaims(t *testing.T) {
	t.Parallel()

	raw := &popRandomManyCompletionFailureStore{}
	serialized := NewSerializedStore(raw, NewStringSerializer())

	entries, err := serialized.PopRandomMany("user:", 3)
	require.Nil(t, entries)
	require.ErrorIs(t, err, ErrClaimCompletionFailed)

	require.Len(t, raw.released, 2, "failed + remaining claims must be released best effort")
	require.Equal(t, "claim-2", raw.released[0].ID)
	require.Equal(t, "claim-3", raw.released[1].ID)
}

func TestSerializedStore_PopRandomMany_CompletionFailure_JoinsReleaseError(t *testing.T) {
	t.Parallel()

	raw := &popRandomManyCompletionAndReleaseErrorStore{}
	serialized := NewSerializedStore(raw, NewStringSerializer())

	entries, err := serialized.PopRandomMany("user:", 3)
	require.Nil(t, entries)
	require.ErrorIs(t, err, ErrClaimCompletionFailed)
	require.ErrorIs(t, err, ErrDiskStoreWriteFailed)
	require.Len(t, raw.released, 2, "release must still be attempted for failed and remaining claims")
	require.Equal(t, "claim-2", raw.released[0].ID)
	require.Equal(t, "claim-3", raw.released[1].ID)
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
	released []*ClaimRef
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

func (s *popRandomCompletionFailureStore) ReleaseClaim(ref *ClaimRef) (bool, error) {
	s.released = append(s.released, &ClaimRef{
		ID:    ref.ID,
		Key:   ref.Key,
		Token: ref.Token,
	})

	return true, nil
}

type popRandomCompleteErrorStore struct {
	Store
	released []*ClaimRef
}

func (s *popRandomCompleteErrorStore) ClaimRandom(_ *ClaimOptions) (*EntryClaim, error) {
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

func (s *popRandomCompleteErrorStore) CompleteClaim(_ *ClaimRef, _ *CompleteClaimOptions) (bool, error) {
	return false, ErrDiskStoreWriteFailed
}

func (s *popRandomCompleteErrorStore) ReleaseClaim(ref *ClaimRef) (bool, error) {
	s.released = append(s.released, &ClaimRef{
		ID:    ref.ID,
		Key:   ref.Key,
		Token: ref.Token,
	})

	return true, nil
}

type popRandomManyCompletionFailureStore struct {
	Store
	completeCalls int
	released      []*ClaimRef
}

func (s *popRandomManyCompletionFailureStore) ClaimRandomMany(_ *ClaimManyOptions) ([]*EntryClaim, error) {
	return []*EntryClaim{
		{
			ID:    "claim-1",
			Key:   "user:1",
			Token: 1,
			Entry: Entry{Key: "user:1", Value: []byte("value-1")},
		},
		{
			ID:    "claim-2",
			Key:   "user:2",
			Token: 2,
			Entry: Entry{Key: "user:2", Value: []byte("value-2")},
		},
		{
			ID:    "claim-3",
			Key:   "user:3",
			Token: 3,
			Entry: Entry{Key: "user:3", Value: []byte("value-3")},
		},
	}, nil
}

func (s *popRandomManyCompletionFailureStore) CompleteClaim(_ *ClaimRef, _ *CompleteClaimOptions) (bool, error) {
	s.completeCalls++
	if s.completeCalls == 2 {
		return false, nil
	}

	return true, nil
}

func (s *popRandomManyCompletionFailureStore) ReleaseClaim(ref *ClaimRef) (bool, error) {
	s.released = append(s.released, &ClaimRef{
		ID:    ref.ID,
		Key:   ref.Key,
		Token: ref.Token,
	})

	return true, nil
}

type popRandomManyCompletionAndReleaseErrorStore struct {
	Store
	completeCalls int
	released      []*ClaimRef
}

func (s *popRandomManyCompletionAndReleaseErrorStore) ClaimRandomMany(_ *ClaimManyOptions) ([]*EntryClaim, error) {
	return []*EntryClaim{
		{
			ID:    "claim-1",
			Key:   "user:1",
			Token: 1,
			Entry: Entry{Key: "user:1", Value: []byte("value-1")},
		},
		{
			ID:    "claim-2",
			Key:   "user:2",
			Token: 2,
			Entry: Entry{Key: "user:2", Value: []byte("value-2")},
		},
		{
			ID:    "claim-3",
			Key:   "user:3",
			Token: 3,
			Entry: Entry{Key: "user:3", Value: []byte("value-3")},
		},
	}, nil
}

func (s *popRandomManyCompletionAndReleaseErrorStore) CompleteClaim(
	_ *ClaimRef,
	_ *CompleteClaimOptions,
) (bool, error) {
	s.completeCalls++
	if s.completeCalls == 2 {
		return false, nil
	}

	return true, nil
}

func (s *popRandomManyCompletionAndReleaseErrorStore) ReleaseClaim(ref *ClaimRef) (bool, error) {
	s.released = append(s.released, &ClaimRef{
		ID:    ref.ID,
		Key:   ref.Key,
		Token: ref.Token,
	})

	if ref.ID == "claim-2" {
		return false, ErrDiskStoreWriteFailed
	}

	return true, nil
}
