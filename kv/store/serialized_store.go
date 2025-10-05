package store

import "fmt"

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
func NewSerializedStore(store Store, serializer Serializer) *SerializedStore {
	return &SerializedStore{
		store:      store,
		serializer: serializer,
	}
}

// Get fetches the raw value from the underlying store and deserializes it using
// the configured serializer. If the underlying store returns a type other than
// []byte or string, that value is returned as-is.
func (s *SerializedStore) Get(key string) (any, error) {
	// Get the raw value from the store.
	rawValue, err := s.store.Get(key)
	if err != nil {
		return nil, err
	}

	return s.deserializeValue(rawValue)
}

// Set serializes the provided value to bytes and stores it under the given key.
func (s *SerializedStore) Set(key string, value any) error {
	// Serialize the value.
	serializedValue, err := s.serializer.Serialize(value)
	if err != nil {
		return fmt.Errorf("failed to serialize value: %w", err)
	}

	// Store the serialized value.
	return s.store.Set(key, serializedValue)
}

// IncrementBy delegates to the underlying store's atomic integer increment.
// No serialization is applied (the counter value is maintained by the store).
func (s *SerializedStore) IncrementBy(key string, delta int64) (int64, error) {
	return s.store.IncrementBy(key, delta)
}

// GetOrSet atomically sets the key to the serialized value if absent, or returns
// the existing value if present. The returned value is always deserialized (if
// raw type is []byte/string), and "loaded" indicates whether the value existed.
func (s *SerializedStore) GetOrSet(key string, value any) (any, bool, error) {
	serializedValue, err := s.serializer.Serialize(value)
	if err != nil {
		return nil, false, fmt.Errorf("failed to serialize value: %w", err)
	}

	rawValue, loaded, err := s.store.GetOrSet(key, serializedValue)
	if err != nil {
		return nil, false, err
	}

	// Decode the returned value (either existing or just-stored).
	decoded, err := s.deserializeValue(rawValue)

	return decoded, loaded, err
}

// Swap replaces the current value for a key with the serialized new value and
// returns the previous value (deserialized when applicable). The 'loaded' flag
// is false when the key did not previously exist.
func (s *SerializedStore) Swap(key string, value any) (any, bool, error) {
	serializedValue, err := s.serializer.Serialize(value)
	if err != nil {
		return nil, false, fmt.Errorf("failed to serialize value: %w", err)
	}

	prevRaw, loaded, err := s.store.Swap(key, serializedValue)
	if err != nil || !loaded {
		// Either an error occurred, or the key didn't exist (loaded=false).
		return nil, loaded, err
	}

	// Decode the returned value.
	prevDecoded, err := s.deserializeValue(prevRaw)

	return prevDecoded, true, err
}

// CompareAndSwap performs an atomic CAS using serialized 'oldValue' and 'newValue'.
// It returns true on successful swap. No deserialization is needed here.
func (s *SerializedStore) CompareAndSwap(key string, oldValue any, newValue any) (bool, error) {
	oldSerializedValue, err := s.serializer.Serialize(oldValue)
	if err != nil {
		return false, err
	}

	newSerializedValue, err := s.serializer.Serialize(newValue)
	if err != nil {
		return false, err
	}

	return s.store.CompareAndSwap(key, oldSerializedValue, newSerializedValue)
}

// Delete removes the key from the underlying store (no serialization involved).
func (s *SerializedStore) Delete(key string) error {
	return s.store.Delete(key)
}

// Exists returns whether the key is present in the underlying store.
func (s *SerializedStore) Exists(key string) (bool, error) {
	return s.store.Exists(key)
}

// DeleteIfExists deletes the key if present and returns whether the deletion happened.
func (s *SerializedStore) DeleteIfExists(key string) (bool, error) {
	return s.store.DeleteIfExists(key)
}

// CompareAndDelete deletes the key only if its current serialized value equals
// the serialized "oldValue". It returns true on successful deletion.
func (s *SerializedStore) CompareAndDelete(key string, oldValue any) (bool, error) {
	serializedOldValue, err := s.serializer.Serialize(oldValue)
	if err != nil {
		return false, err
	}

	return s.store.CompareAndDelete(key, serializedOldValue)
}

// Clear removes all keys from the underlying store.
func (s *SerializedStore) Clear() error {
	return s.store.Clear()
}

// Size returns the number of keys currently in the underlying store.
func (s *SerializedStore) Size() (int64, error) {
	return s.store.Size()
}

// List returns entries (Key, Value) filtered by prefix and limited by 'limit'.
// All values are deserialized when the raw type is []byte or string; otherwise
// they are returned unchanged.
func (s *SerializedStore) List(prefix string, limit int64) ([]Entry, error) {
	// Get the raw entries from the underlying store.
	rawEntries, err := s.store.List(prefix, limit)
	if err != nil {
		return nil, err
	}

	// Deserialize each entry's value.
	return s.deserializeEntries(rawEntries)
}

// RandomKey returns a random key (optionally constrained by "prefix") from the underlying store.
// An empty string and nil error are returned when there is no match.
func (s *SerializedStore) RandomKey(prefix string) (string, error) {
	return s.store.RandomKey(prefix)
}

// RebuildKeyList asks the underlying store to rebuild any in-memory key indices
// from durable storage. Primarily useful for disk-backed stores.
func (s *SerializedStore) RebuildKeyList() error {
	return s.store.RebuildKeyList()
}

// Close forwards the close operation to the underlying store (if it has resources to release).
// Memory stores typically implement this as a no-op.
func (s *SerializedStore) Close() error {
	return s.store.Close()
}

// GetSerializer returns the currently configured serializer.
func (s *SerializedStore) GetSerializer() Serializer {
	return s.serializer
}

// SetSerializer replaces the current serializer with a new one.
// Changing serializers while there are already-serialized values in
// the store may make old entries unreadable with the new serializer.
// Coordinate such changes carefully, and avoid concurrent reads
// during a swap unless you control compatibility semantics externally.
func (s *SerializedStore) SetSerializer(serializer Serializer) {
	s.serializer = serializer
}

// deserializeValue decodes a raw value coming from the underlying store.
// Supported raw input types are: []byte and string. Any other type is returned
// as-is (assumed already deserialized by the underlying store).
func (s *SerializedStore) deserializeValue(raw any) (any, error) {
	switch v := raw.(type) {
	// Handle byte slice values.
	case []byte:
		return s.serializer.Deserialize(v)
	// Handle string values from stores that don't use byte slices.
	case string:
		return s.serializer.Deserialize([]byte(v))
	default:
		// Already a structured value (e.g., MemoryStore can keep native values).
		return raw, nil
	}
}

// deserializeEntries applies deserializeValue to each Entry in a batch returned
// by List, preserving keys and converting values as needed.
func (s *SerializedStore) deserializeEntries(rawEntries []Entry) ([]Entry, error) {
	entries := make([]Entry, len(rawEntries))

	for i, e := range rawEntries {
		val, err := s.deserializeValue(e.Value)
		if err != nil {
			return nil, fmt.Errorf("deserialize value for key %s: %w", e.Key, err)
		}

		entries[i] = Entry{Key: e.Key, Value: val}
	}

	return entries, nil
}
