package kv

import (
	"testing"

	"github.com/grafana/sobek"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/js/modulestest"
)

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

func TestImportSetManyEntries_EmptyObjectReturnsEmptySlice(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	entries, err := importSetManyEntries(runtime.VU.Runtime().ToValue(map[string]any{}))
	require.NoError(t, err)
	require.NotNil(t, entries)
	assert.Empty(t, entries)
}

func TestImportSetManyEntries_EmptyKeyAllowed(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	entries, err := importSetManyEntries(runtime.VU.Runtime().ToValue(map[string]any{
		"": "value",
	}))
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Empty(t, entries[0].Key)
	assert.Equal(t, "value", entries[0].Value)
}

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
