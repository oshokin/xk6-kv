package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// rawEncodeErrorSerializer is a test serializer used by raw encode error serializer tests.
type rawEncodeErrorSerializer struct {
	// err holds test state for raw encode error serializer.
	err error
}

// rawEncodeErrorSerializer implements serializer serialize for raw encode error serializer.
func (s *rawEncodeErrorSerializer) Serialize(_ any) ([]byte, error) {
	return nil, s.err
}

// rawEncodeErrorSerializer implements serializer deserialize for raw encode error serializer.
func (s *rawEncodeErrorSerializer) Deserialize(data []byte) (any, error) {
	return string(data), nil
}

// rawEncodeErrorSerializer implements serializer type for raw encode error serializer.
func (s *rawEncodeErrorSerializer) Type() string {
	return "raw-error"
}

// newSerializedStoreWithRawEncodeError creates serialized store with raw encode error for tests.
func newSerializedStoreWithRawEncodeError(t *testing.T, err error) *SerializedStore {
	t.Helper()

	base := NewMemoryStore(&MemoryConfig{TrackKeys: true})
	require.NoError(t, base.Open())

	return NewSerializedStore(base, &rawEncodeErrorSerializer{err: err})
}

// TestSerializedStore_NormalizesSerializerEncodeError verifies that serialized store normalizes serializer encode error.
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

// assertSerializedStoreEncodeError asserts serialized store encode error.
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
