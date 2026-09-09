package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSerializedStore_NextCircular_DeserializesCurrentValues(t *testing.T) {
	t.Parallel()

	for _, testCase := range serializedRawStoreCases() {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			raw := testCase.newStore(t)
			serialized := NewSerializedStore(raw, NewJSONSerializer())

			written, err := serialized.SetMany([]Entry{
				{Key: "a", Value: map[string]any{"id": 1}},
				{Key: "b", Value: map[string]any{"id": 2}},
			})
			require.NoError(t, err)
			require.EqualValues(t, 2, written)

			first, err := serialized.NextCircular("")
			require.NoError(t, err)
			require.NotNil(t, first)
			assert.Equal(t, "a", first.Key)
			assert.Equal(t, map[string]any{"id": float64(1)}, first.Value)

			second, err := serialized.NextCircular("")
			require.NoError(t, err)
			require.NotNil(t, second)
			assert.Equal(t, "b", second.Key)
			assert.Equal(t, map[string]any{"id": float64(2)}, second.Value)

			third, err := serialized.NextCircular("")
			require.NoError(t, err)
			require.NotNil(t, third)
			assert.Equal(t, "a", third.Key)
			assert.Equal(t, map[string]any{"id": float64(1)}, third.Value)
		})
	}
}

func TestSerializedStore_NextCircular_DecodeErrorReturnsSerializerError(t *testing.T) {
	t.Parallel()

	for _, testCase := range serializedRawStoreCases() {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			raw := testCase.newStore(t)
			serialized := NewSerializedStore(raw, NewJSONSerializer())
			require.NoError(t, raw.Set("bad:0001", []byte("{")))

			entry, err := serialized.NextCircular("bad:")
			require.Nil(t, entry)
			require.Error(t, err)
			require.ErrorIs(t, err, ErrSerializerDecodeFailed)
			require.ErrorContains(t, err, "key bad:0001")
		})
	}
}
