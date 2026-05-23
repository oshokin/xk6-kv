package store

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// emptyKeyOperation is a test type used by empty key operation tests.
type emptyKeyOperation struct {
	// name identifies the name case under test.
	name string
	// run holds test state for empty key operation.
	run func(Store) error
}

// TestMemoryStoreRejectsEmptyKeyForSingleKeyOperations verifies that memory store rejects empty key for single key operations.
func TestMemoryStoreRejectsEmptyKeyForSingleKeyOperations(t *testing.T) {
	t.Parallel()

	for _, operation := range singleKeyEmptyKeyOperations() {
		t.Run(operation.name, func(t *testing.T) {
			t.Parallel()

			store := NewMemoryStore(&MemoryConfig{TrackKeys: true})

			requireEmptyKeyRejectedWithoutMutation(t, store, operation)
		})
	}
}

// TestDiskStoreRejectsEmptyKeyForSingleKeyOperations verifies that disk store rejects empty key for single key operations.
func TestDiskStoreRejectsEmptyKeyForSingleKeyOperations(t *testing.T) {
	t.Parallel()

	for _, operation := range singleKeyEmptyKeyOperations() {
		t.Run(operation.name, func(t *testing.T) {
			t.Parallel()

			store := newTestDiskStore(t, true, "", true)

			requireEmptyKeyRejectedWithoutMutation(t, store, operation)
		})
	}
}

// singleKeyEmptyKeyOperations is a test helper for single key empty key operations.
func singleKeyEmptyKeyOperations() []emptyKeyOperation {
	return []emptyKeyOperation{
		{
			name: "Get",
			run: func(store Store) error {
				_, err := store.Get("")

				return err
			},
		},
		{
			name: "Set",
			run: func(store Store) error {
				return store.Set("", "value")
			},
		},
		{
			name: "IncrementBy",
			run: func(store Store) error {
				_, err := store.IncrementBy("", 1)

				return err
			},
		},
		{
			name: "GetOrSet",
			run: func(store Store) error {
				_, _, err := store.GetOrSet("", "value")

				return err
			},
		},
		{
			name: "Swap",
			run: func(store Store) error {
				_, _, err := store.Swap("", "value")

				return err
			},
		},
		{
			name: "Delete",
			run: func(store Store) error {
				return store.Delete("")
			},
		},
		{
			name: "Exists",
			run: func(store Store) error {
				_, err := store.Exists("")

				return err
			},
		},
		{
			name: "DeleteIfExists",
			run: func(store Store) error {
				_, err := store.DeleteIfExists("")

				return err
			},
		},
		{
			name: "CompareAndSwap",
			run: func(store Store) error {
				_, err := store.CompareAndSwap("", nil, "new")

				return err
			},
		},
		{
			name: "CompareAndSwapDetailed",
			run: func(store Store) error {
				_, err := store.CompareAndSwapDetailed("", nil, "new", true)

				return err
			},
		},
		{
			name: "CompareAndDelete",
			run: func(store Store) error {
				_, err := store.CompareAndDelete("", "old")

				return err
			},
		},
		{
			name: "CompareAndDeleteDetailed",
			run: func(store Store) error {
				_, err := store.CompareAndDeleteDetailed("", "old", true)

				return err
			},
		},
	}
}

// requireEmptyKeyRejectedWithoutMutation is a test helper for require empty key rejected without mutation.
func requireEmptyKeyRejectedWithoutMutation(t *testing.T, store Store, operation emptyKeyOperation) {
	t.Helper()

	require.ErrorIs(t, operation.run(store), ErrKeyEmpty)

	size, err := store.Size()
	require.NoError(t, err)
	require.Zero(t, size)
}
