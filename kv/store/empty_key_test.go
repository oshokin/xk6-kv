package store

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type emptyKeyOperation struct {
	name string
	run  func(Store) error
}

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

func requireEmptyKeyRejectedWithoutMutation(t *testing.T, store Store, operation emptyKeyOperation) {
	t.Helper()

	require.ErrorIs(t, operation.run(store), ErrKeyEmpty)

	size, err := store.Size()
	require.NoError(t, err)
	require.Zero(t, size)
}
