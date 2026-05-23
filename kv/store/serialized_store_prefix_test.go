package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSerializedStore_DeleteByPrefix_DelegatesWithoutDeserializingValues verifies that serialized store delete by prefix delegates without deserializing values.
func TestSerializedStore_DeleteByPrefix_DelegatesWithoutDeserializingValues(t *testing.T) {
	t.Parallel()

	mem := NewMemoryStore(&MemoryConfig{TrackKeys: true})
	serialized := NewSerializedStore(mem, NewJSONSerializer())

	require.NoError(t, mem.Set("tmp:bad-json", []byte("{")))

	result, err := serialized.DeleteByPrefix("tmp:", 10)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 1, result.Deleted)
	assert.True(t, result.Done)
}
