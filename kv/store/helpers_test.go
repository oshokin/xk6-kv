package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// Maximum int64 value as string.
	testMaxInt64String = "9223372036854775807"
	// Minimum int64 value as string.
	testMinInt64String = "-9223372036854775808"
	// Non-numeric string value.
	testNonNumeric = "not-a-number"
	// Maximum safe JavaScript integer.
	testMaxSafeJSInt = int64(9007199254740991)
)

// requireStoredStringValue verifies that the value stored in the store for
// the given key is equal to the expected string.
func requireStoredStringValue(t *testing.T, store Store, key, expected string) {
	t.Helper()

	valueAny, err := store.Get(key)
	require.NoError(t, err)

	valueBytes, ok := valueAny.([]byte)
	require.Truef(t, ok, "expected []byte, got %T", valueAny)

	assert.Equal(t, []byte(expected), valueBytes)
}
