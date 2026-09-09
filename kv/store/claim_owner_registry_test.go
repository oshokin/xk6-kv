package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClaimForOwnerWithRegistry_StickyHitSkipsAllocate(t *testing.T) {
	t.Parallel()

	registry := &claimOwnerBindingRegistry{}
	opts := &ClaimOptions{
		Prefix: "users:",
		Owner:  "vu:1",
		TTLMs:  60_000,
	}

	expected := &EntryClaim{
		ID:  "c1",
		Key: "users:1",
		Entry: Entry{
			Key:   "users:1",
			Value: []byte("alice"),
		},
		Owner:     "vu:1",
		Token:     1,
		ExpiresAt: 999_999_999,
	}

	slot := registry.slot(opts.Prefix, opts.Owner)
	slot.ref = expected.Ref()

	allocateCalled := false

	claim, err := claimForOwnerWithRegistry(
		registry,
		opts,
		func(ref *ClaimRef, _ *ClaimOptions) (*EntryClaim, bool, error) {
			require.Equal(t, expected.Ref(), ref)
			return expected, true, nil
		},
		func(_ *ClaimOptions) (*EntryClaim, error) {
			allocateCalled = true
			return nil, nil //nolint:nilnil // test double for "should not allocate" path.
		},
	)
	require.NoError(t, err)
	require.NotNil(t, claim)
	assert.Equal(t, expected.ID, claim.ID)
	assert.False(t, allocateCalled)
	assert.Equal(t, expected.Ref(), slot.ref)
}

func TestClaimForOwnerWithRegistry_StaleBindingAllocatesNewClaim(t *testing.T) {
	t.Parallel()

	registry := &claimOwnerBindingRegistry{}
	opts := &ClaimOptions{
		Prefix: "users:",
		Owner:  "vu:1",
		TTLMs:  60_000,
	}

	stale := &ClaimRef{
		ID:    "old",
		Key:   "users:1",
		Token: 1,
	}
	slot := registry.slot(opts.Prefix, opts.Owner)
	slot.ref = stale

	allocated := &EntryClaim{
		ID:  "new",
		Key: "users:2",
		Entry: Entry{
			Key:   "users:2",
			Value: []byte("bob"),
		},
		Owner:     "vu:1",
		Token:     2,
		ExpiresAt: 999_999_999,
	}

	claim, err := claimForOwnerWithRegistry(
		registry,
		opts,
		func(ref *ClaimRef, _ *ClaimOptions) (*EntryClaim, bool, error) {
			require.Equal(t, stale, ref)
			return nil, false, nil
		},
		func(_ *ClaimOptions) (*EntryClaim, error) {
			return allocated, nil
		},
	)
	require.NoError(t, err)
	require.NotNil(t, claim)
	assert.Equal(t, allocated.ID, claim.ID)
	assert.Equal(t, allocated.Ref(), slot.ref)
}

func TestClaimForOwnerWithRegistry_LookupErrorKeepsReference(t *testing.T) {
	t.Parallel()

	registry := &claimOwnerBindingRegistry{}
	opts := &ClaimOptions{
		Prefix: "users:",
		Owner:  "vu:1",
		TTLMs:  60_000,
	}

	expectedRef := &ClaimRef{
		ID:    "c1",
		Key:   "users:1",
		Token: 1,
	}
	slot := registry.slot(opts.Prefix, opts.Owner)
	slot.ref = expectedRef

	expectedErr := errors.New("transient lookup error")
	allocateCalled := false

	claim, err := claimForOwnerWithRegistry(
		registry,
		opts,
		func(_ *ClaimRef, _ *ClaimOptions) (*EntryClaim, bool, error) {
			return nil, false, expectedErr
		},
		func(_ *ClaimOptions) (*EntryClaim, error) {
			allocateCalled = true
			return nil, nil //nolint:nilnil // test double for no-op allocation branch.
		},
	)
	require.ErrorIs(t, err, expectedErr)
	require.Nil(t, claim)
	assert.False(t, allocateCalled)
	assert.Equal(t, expectedRef, slot.ref)
}

func TestClaimForOwnerWithRegistry_AllocateNilReturnsNil(t *testing.T) {
	t.Parallel()

	registry := &claimOwnerBindingRegistry{}
	opts := &ClaimOptions{
		Prefix: "users:",
		Owner:  "vu:1",
		TTLMs:  60_000,
	}

	slot := registry.slot(opts.Prefix, opts.Owner)
	slot.ref = nil

	claim, err := claimForOwnerWithRegistry(
		registry,
		opts,
		func(_ *ClaimRef, _ *ClaimOptions) (*EntryClaim, bool, error) {
			return nil, false, nil
		},
		func(_ *ClaimOptions) (*EntryClaim, error) {
			return nil, nil //nolint:nilnil // exhausted allocation is represented by nil claim.
		},
	)
	require.NoError(t, err)
	require.Nil(t, claim)
	assert.Nil(t, slot.ref)
}

func TestClaimForOwnerWithRegistry_AllocateError(t *testing.T) {
	t.Parallel()

	registry := &claimOwnerBindingRegistry{}
	opts := &ClaimOptions{
		Prefix: "users:",
		Owner:  "vu:1",
		TTLMs:  60_000,
	}

	expectedErr := errors.New("allocation failure")

	claim, err := claimForOwnerWithRegistry(
		registry,
		opts,
		func(_ *ClaimRef, _ *ClaimOptions) (*EntryClaim, bool, error) {
			return nil, false, nil
		},
		func(_ *ClaimOptions) (*EntryClaim, error) {
			return nil, expectedErr
		},
	)
	require.ErrorIs(t, err, expectedErr)
	require.Nil(t, claim)

	slot := registry.slot(opts.Prefix, opts.Owner)
	assert.Nil(t, slot.ref)
}
