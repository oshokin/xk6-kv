package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type rawEncodeErrorSerializer struct {
	err error
}

func (s *rawEncodeErrorSerializer) Serialize(_ any) ([]byte, error) {
	return nil, s.err
}

func (s *rawEncodeErrorSerializer) Deserialize(data []byte) (any, error) {
	return string(data), nil
}

func (s *rawEncodeErrorSerializer) Type() string {
	return "raw-error"
}

func newSerializedStoreWithRawEncodeError(t *testing.T, err error) *SerializedStore {
	t.Helper()

	base := NewMemoryStore(&MemoryConfig{TrackKeys: true})
	require.NoError(t, base.Open())

	return NewSerializedStore(base, &rawEncodeErrorSerializer{err: err})
}

func TestSerializedStore_Set_NormalizesSerializerEncodeError(t *testing.T) {
	t.Parallel()

	rootErr := errors.New("serialize failed")
	store := newSerializedStoreWithRawEncodeError(t, rootErr)

	err := store.Set("k", "v")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSerializerEncodeFailed)
	require.ErrorIs(t, err, rootErr)
}

func TestSerializedStore_CompareAndSwapDetailed_NormalizesSerializerEncodeError(t *testing.T) {
	t.Parallel()

	rootErr := errors.New("serialize failed")
	store := newSerializedStoreWithRawEncodeError(t, rootErr)

	_, err := store.CompareAndSwapDetailed("k", nil, "next", true)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSerializerEncodeFailed)
	require.ErrorIs(t, err, rootErr)
}

func TestSerializedStore_CompareAndDeleteDetailed_NormalizesSerializerEncodeError(t *testing.T) {
	t.Parallel()

	rootErr := errors.New("serialize failed")
	store := newSerializedStoreWithRawEncodeError(t, rootErr)

	_, err := store.CompareAndDeleteDetailed("k", "expected", true)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSerializerEncodeFailed)
	require.ErrorIs(t, err, rootErr)
}
