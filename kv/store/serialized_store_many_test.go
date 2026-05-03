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
