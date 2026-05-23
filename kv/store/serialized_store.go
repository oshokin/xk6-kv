package store

import (
	"errors"
	"fmt"
)

// SerializedStore wraps an underlying Store and transparently applies a Serializer
// to values on write/read. This allows callers to work with rich types while the
// Store persists/returns raw bytes (or strings) internally.
type SerializedStore struct {
	// store is the underlying key-value backend.
	store Store

	// serializer encodes/decodes arbitrary values to/from bytes for persistence.
	serializer Serializer
}

// NewSerializedStore constructs a SerializedStore over the given Store and Serializer.
// It does not take ownership of the Store; Close must be called explicitly if needed.
// The serializer is immutable for the store lifetime because changing it would
// make already persisted values unreadable or incorrectly decoded.
func NewSerializedStore(store Store, serializer Serializer) *SerializedStore {
	return &SerializedStore{
		store:      store,
		serializer: serializer,
	}
}

// Open ensures the underlying store is ready before running operations.
func (s *SerializedStore) Open() error {
	return s.store.Open()
}

// Close forwards the close operation to the underlying store (if it has resources to release).
// Memory stores typically implement this as a no-op.
func (s *SerializedStore) Close() error {
	return s.store.Close()
}

// writeAndDecodeLoadedValue serializes value, writes via write, and decodes loaded bytes.
func (s *SerializedStore) writeAndDecodeLoadedValue(
	key string,
	value any,
	write func(string, any) (any, bool, error),
) (any, bool, error) {
	serializedValue, err := s.serializeWriteValue(value)
	if err != nil {
		return nil, false, err
	}

	rawValue, loaded, err := write(key, serializedValue)
	if err != nil || !loaded {
		return nil, loaded, err
	}

	decoded, err := s.deserializeValue(rawValue)

	return decoded, true, err
}

// normalizeSerializerEncodeError wraps encode failures with ErrSerializerEncodeFailed.
func (s *SerializedStore) normalizeSerializerEncodeError(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, ErrSerializerEncodeFailed) {
		return err
	}

	return fmt.Errorf("%w: %w", ErrSerializerEncodeFailed, err)
}

// serializeWriteValue encodes value for storage using the configured serializer.
func (s *SerializedStore) serializeWriteValue(value any) ([]byte, error) {
	serializedValue, err := s.serializer.Serialize(value)
	if err != nil {
		return nil, s.normalizeSerializerEncodeError(err)
	}

	return serializedValue, nil
}

// deserializeValue decodes a raw value coming from the underlying store.
// Supported raw input types are: []byte and string. Any other type is returned
// as-is (assumed already deserialized by the underlying store).
// This handles different store implementations that may return different raw types.
func (s *SerializedStore) deserializeValue(raw any) (any, error) {
	switch v := raw.(type) {
	// Handle byte slice values: most common case from disk-backed stores.
	// No cloning needed: stores already return copies via slices.Clone.
	case []byte:
		return s.serializer.Deserialize(v)
	// Handle string values from stores that don't use byte slices.
	// Convert to []byte for deserialization.
	case string:
		return s.serializer.Deserialize([]byte(v))
	default:
		// Already a structured value (e.g., MemoryStore can keep native values).
		// No deserialization needed: return as-is.
		return raw, nil
	}
}

// deserializeEntries applies deserializeValue to each Entry in a batch returned
// by List, preserving keys and converting values as needed.
// No cloning is performed here: stores already return defensive copies of data.
func (s *SerializedStore) deserializeEntries(rawEntries []Entry) ([]Entry, error) {
	entries := make([]Entry, len(rawEntries))

	for i, e := range rawEntries {
		value, err := s.deserializeValue(e.Value)
		if err != nil {
			return nil, fmt.Errorf("%w: key %s: %w", ErrSerializerDecodeFailed, e.Key, err)
		}

		entries[i] = Entry{
			Key:   e.Key,
			Value: value,
		}
	}

	return entries, nil
}
