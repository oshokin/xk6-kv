package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCircularCursorRegistry_SlotIdentity(t *testing.T) {
	t.Parallel()

	registry := new(circularCursorRegistry)

	first := registry.slot("users:")
	second := registry.slot("users:")
	third := registry.slot("orders:")

	require.Same(t, first, second)
	require.NotSame(t, first, third)
}

func TestCircularCursorRegistry_ResetAllPreservesSlotObjects(t *testing.T) {
	t.Parallel()

	registry := new(circularCursorRegistry)
	slotBefore := registry.slot("a:")
	slotBefore.lastKey = "a:10"

	registry.resetAll()

	slotAfter := registry.slot("a:")
	require.Same(t, slotBefore, slotAfter)
	assert.Empty(t, slotAfter.lastKey)
}

func TestNextCircularWithRegistry_ScanErrorPreservesLastKey(t *testing.T) {
	t.Parallel()

	registry := new(circularCursorRegistry)
	registry.slot("data:").lastKey = "data:2"
	boom := errors.New("boom")

	var calls int

	entry, err := nextCircularWithRegistry(
		registry,
		"data:",
		func(_ string, _ string, _ int64) (*ScanPage, error) {
			calls++
			return nil, boom
		},
	)
	require.ErrorIs(t, err, boom)
	require.Nil(t, entry)
	assert.Equal(t, 1, calls)
	assert.Equal(t, "data:2", registry.slot("data:").lastKey)
}

func TestNextCircularWithRegistry_WrapScanErrorPreservesLastKey(t *testing.T) {
	t.Parallel()

	registry := new(circularCursorRegistry)
	registry.slot("users:").lastKey = "users:c"
	boom := errors.New("wrap failed")

	var calls int

	entry, err := nextCircularWithRegistry(
		registry,
		"users:",
		func(_ string, afterKey string, _ int64) (*ScanPage, error) {
			calls++

			switch afterKey {
			case "users:c":
				return &ScanPage{}, nil
			case "":
				return nil, boom
			default:
				return &ScanPage{}, nil
			}
		},
	)
	require.ErrorIs(t, err, boom)
	require.Nil(t, entry)
	assert.Equal(t, 2, calls)
	assert.Equal(t, "users:c", registry.slot("users:").lastKey)
}
