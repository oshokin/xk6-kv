package kv

import (
	"testing"

	"github.com/grafana/sobek"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/v2/js/modulestest"
)

// TestImportSetManyEntries_NullRejects verifies that import set many entries null rejects.
func TestImportSetManyEntries_NullRejects(t *testing.T) {
	t.Parallel()

	entries, err := importSetManyEntries(sobek.Null())
	require.Error(t, err)
	assert.Nil(t, entries)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	require.Len(t, kvErr.Errors, 1)
	assert.Equal(t, setManyErrorNameInvalidEntries, kvErr.Errors[0].Name)
}

// TestImportSetManyEntries_ArrayRejects verifies that import set many entries array rejects.
func TestImportSetManyEntries_ArrayRejects(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	entries, err := importSetManyEntries(runtime.VU.Runtime().ToValue([]any{"a", "b"}))
	require.Error(t, err)
	assert.Nil(t, entries)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	require.Len(t, kvErr.Errors, 1)
	assert.Equal(t, setManyErrorNameInvalidEntries, kvErr.Errors[0].Name)
}

// TestImportSetManyEntries_EmptyObjectReturnsEmptySlice verifies that import set many entries empty object returns empty slice.
func TestImportSetManyEntries_EmptyObjectReturnsEmptySlice(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	entries, err := importSetManyEntries(runtime.VU.Runtime().ToValue(map[string]any{}))
	require.NoError(t, err)
	require.NotNil(t, entries)
	assert.Empty(t, entries)
}

// TestImportSetManyEntries_EmptyKeyRejectsWithDetails verifies that import set many entries empty key rejects with details.
func TestImportSetManyEntries_EmptyKeyRejectsWithDetails(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	entries, err := importSetManyEntries(runtime.VU.Runtime().ToValue(map[string]any{
		"": "value",
	}))
	require.Error(t, err)
	assert.Nil(t, entries)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Equal(t, "setMany validation failed: 1 invalid entry", kvErr.Message)
	require.Len(t, kvErr.Errors, 1)
	assert.Equal(t, setManyErrorNameEmptyKey, kvErr.Errors[0].Name)
	assert.Equal(t, "key must be a non-empty string", kvErr.Errors[0].Message)
}

// TestImportSetManyEntries_ValidObject verifies that import set many entries valid object.
func TestImportSetManyEntries_ValidObject(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	entries, err := importSetManyEntries(runtime.VU.Runtime().ToValue(map[string]any{
		"user:2": map[string]any{"name": "Ivan"},
		"user:1": map[string]any{"name": "Oleg"},
	}))
	require.NoError(t, err)
	require.Len(t, entries, 2)
	assert.Equal(t, "user:1", entries[0].Key, "entries are sorted for deterministic behavior")
	assert.Equal(t, "user:2", entries[1].Key, "entries are sorted for deterministic behavior")
}

// TestImportGetManyKeys_NullRejects verifies that import get many keys null rejects.
func TestImportGetManyKeys_NullRejects(t *testing.T) {
	t.Parallel()

	_, err := importGetManyKeys(sobek.Undefined())
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
}

// TestImportGetManyKeys_NonArrayRejects verifies that import get many keys non array rejects.
func TestImportGetManyKeys_NonArrayRejects(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	_, err := importGetManyKeys(runtime.VU.Runtime().ToValue(map[string]any{}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
}

// TestImportGetManyKeys_NonStringElementRejects verifies that import get many keys non string element rejects.
func TestImportGetManyKeys_NonStringElementRejects(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	_, err := importGetManyKeys(runtime.VU.Runtime().ToValue([]any{"ok", 123}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "keys[1]")
}

// TestImportGetManyKeys_EmptyArray verifies that import get many keys empty array.
func TestImportGetManyKeys_EmptyArray(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	keys, err := importGetManyKeys(runtime.VU.Runtime().ToValue([]any{}))
	require.NoError(t, err)
	assert.Empty(t, keys)
}

// TestImportGetManyKeys_ValidArray verifies that import get many keys valid array.
func TestImportGetManyKeys_ValidArray(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	keys, err := importGetManyKeys(runtime.VU.Runtime().ToValue([]any{"a", "b"}))
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, keys)
}

// TestImportGetManyKeys_AllowsEmptyString verifies that import get many keys allows empty string.
func TestImportGetManyKeys_AllowsEmptyString(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	keys, err := importGetManyKeys(runtime.VU.Runtime().ToValue([]any{"a", "", "b"}))
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "", "b"}, keys)
}

// TestImportDeleteManyKeys_NullRejects verifies that import delete many keys null rejects.
func TestImportDeleteManyKeys_NullRejects(t *testing.T) {
	t.Parallel()

	_, err := importDeleteManyKeys(sobek.Undefined())
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "deleteMany")
}

// TestImportDeleteManyKeys_NonArrayRejects verifies that import delete many keys non array rejects.
func TestImportDeleteManyKeys_NonArrayRejects(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	_, err := importDeleteManyKeys(runtime.VU.Runtime().ToValue(map[string]any{}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "deleteMany")
}

// TestImportDeleteManyKeys_NonStringElementRejects verifies that import delete many keys non string element rejects.
func TestImportDeleteManyKeys_NonStringElementRejects(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	_, err := importDeleteManyKeys(runtime.VU.Runtime().ToValue([]any{"ok", 123}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "deleteMany")
	assert.Contains(t, kvErr.Message, "keys[1]")
}

// TestImportDeleteManyKeys_EmptyStringRejects verifies that import delete many keys empty string rejects.
func TestImportDeleteManyKeys_EmptyStringRejects(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	_, err := importDeleteManyKeys(runtime.VU.Runtime().ToValue([]any{"ok", ""}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "deleteMany")
	assert.Contains(t, kvErr.Message, "keys[1]")
	assert.Contains(t, kvErr.Message, "non-empty")
}

// TestImportDeleteManyKeys_EmptyArray verifies that import delete many keys empty array.
func TestImportDeleteManyKeys_EmptyArray(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	keys, err := importDeleteManyKeys(runtime.VU.Runtime().ToValue([]any{}))
	require.NoError(t, err)
	assert.Empty(t, keys)
}

// TestImportDeleteManyKeys_ValidArray verifies that import delete many keys valid array.
func TestImportDeleteManyKeys_ValidArray(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	keys, err := importDeleteManyKeys(runtime.VU.Runtime().ToValue([]any{"a", "b"}))
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, keys)
}
