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

func TestSerializedStore_NormalizesSerializerEncodeError(t *testing.T) {
	t.Parallel()

	assertSerializedStoreEncodeError(t, "Set", func(s *SerializedStore) error {
		return s.Set("k", "v")
	})
	assertSerializedStoreEncodeError(t, "CompareAndSwapDetailed", func(s *SerializedStore) error {
		_, err := s.CompareAndSwapDetailed("k", nil, "next", true)

		return err
	})
	assertSerializedStoreEncodeError(t, "CompareAndDeleteDetailed", func(s *SerializedStore) error {
		_, err := s.CompareAndDeleteDetailed("k", "expected", true)

		return err
	})
}

func assertSerializedStoreEncodeError(t *testing.T, name string, run func(*SerializedStore) error) {
	t.Helper()

	t.Run(name, func(t *testing.T) {
		t.Parallel()

		rootErr := errors.New("serialize failed")
		store := newSerializedStoreWithRawEncodeError(t, rootErr)

		err := run(store)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrSerializerEncodeFailed)
		require.ErrorIs(t, err, rootErr)
	})
}
