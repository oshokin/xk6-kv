package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSerializedStore_SetMany_WritesAll(t *testing.T) {
	t.Parallel()

	serialized := newTestSerializedMemoryStore(t)
	written, err := serialized.SetMany([]Entry{
		{Key: "user:1", Value: map[string]any{"name": "Alice"}},
		{Key: "user:2", Value: map[string]any{"name": "Bob"}},
	})
	require.NoError(t, err)
	assert.EqualValues(t, 2, written)

	first, err := serialized.Get("user:1")
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"name": "Alice"}, first)
}

func TestSerializedStore_SetMany_SerializationErrorWritesNothing(t *testing.T) {
	t.Parallel()

	base := NewMemoryStore(&MemoryConfig{TrackKeys: true})
	serialized := NewSerializedStore(base, NewJSONSerializer())

	written, err := serialized.SetMany([]Entry{
		{Key: "ok", Value: map[string]any{"name": "Oleg"}},
		{Key: "bad", Value: func() {}},
	})

	require.Error(t, err)
	assert.EqualValues(t, 0, written)

	var entryListErr *EntryListError
	require.ErrorAs(t, err, &entryListErr)
	assert.Equal(t, EntryListErrorKindSerialization, entryListErr.Kind)
	require.Len(t, entryListErr.Errors, 1)
	assert.Equal(t, "bad", entryListErr.Errors[0].Key)
	assert.Equal(t, EntryErrorNameSerializer, entryListErr.Errors[0].Name)

	exists, existsErr := base.Exists("ok")
	require.NoError(t, existsErr)
	assert.False(t, exists, "serialization failures must not partially write")
}

func TestSerializedStore_GetMany_DeserializesValuesAndPreservesNulls(t *testing.T) {
	t.Parallel()

	serialized := newTestSerializedMemoryStore(t)

	written, err := serialized.SetMany([]Entry{
		{Key: "user:1", Value: map[string]any{"name": "Alice"}},
		{Key: "user:2", Value: map[string]any{"name": "Bob"}},
	})
	require.NoError(t, err)
	assert.EqualValues(t, 2, written)

	entries, err := serialized.GetMany([]string{"user:2", "missing", "user:1"})
	require.NoError(t, err)
	require.Len(t, entries, 3)

	require.NotNil(t, entries[0])
	assert.Equal(t, "user:2", entries[0].Key)
	assert.Equal(t, "Bob", entries[0].Value.(map[string]any)["name"])

	assert.Nil(t, entries[1])

	require.NotNil(t, entries[2])
	assert.Equal(t, "user:1", entries[2].Key)
	assert.Equal(t, "Alice", entries[2].Value.(map[string]any)["name"])
}

func TestSerializedStore_GetMany_JSONNullAndMissingBothReturnNil(t *testing.T) {
	t.Parallel()

	serialized := newTestSerializedMemoryStore(t)

	written, err := serialized.SetMany([]Entry{
		{Key: "json:null", Value: nil},
	})
	require.NoError(t, err)
	assert.EqualValues(t, 1, written)

	entries, err := serialized.GetMany([]string{"missing", "json:null"})
	require.NoError(t, err)
	require.Len(t, entries, 2)

	assert.Nil(t, entries[0], "missing keys must remain nil entries")
	require.NotNil(t, entries[1], "stored JSON null exists as an entry")
	assert.Nil(t, entries[1].Value, "JSON null deserializes to nil value")
}

func TestSerializedStore_DeleteMany_DelegatesAndReturnsCounts(t *testing.T) {
	t.Parallel()

	mem := NewMemoryStore(&MemoryConfig{TrackKeys: true})
	serialized := NewSerializedStore(mem, NewJSONSerializer())

	_, err := serialized.SetMany([]Entry{
		{Key: "user:1", Value: map[string]any{"name": "Alice"}},
		{Key: "user:2", Value: map[string]any{"name": "Bob"}},
	})
	require.NoError(t, err)

	result, err := serialized.DeleteMany([]string{"user:1", "missing", "user:2"})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 2, result.Deleted)
	assert.EqualValues(t, 1, result.Missing)
}
