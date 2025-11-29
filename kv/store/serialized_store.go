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

// Open ensures the underlying store is ready before running operations.
func (s *SerializedStore) Open() error {
	return s.store.Open()
}

// Get fetches the raw value from the underlying store and deserializes it using
// the configured serializer. If the underlying store returns a type other than
// []byte or string, that value is returned as-is.
func (s *SerializedStore) Get(key string) (any, error) {
	// Get the raw value from the store (typically []byte from serialization).
	rawValue, err := s.store.Get(key)
	if err != nil {
		return nil, err
	}

	// Deserialize to convert raw bytes back to the original type.
	return s.deserializeValue(rawValue)
}

// Set serializes the provided value to bytes and stores it under the given key.
func (s *SerializedStore) Set(key string, value any) error {
	// Serialize the value.
	serializedValue, err := s.serializer.Serialize(value)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrSerializerEncodeFailed, err)
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
		return nil, false, fmt.Errorf("%w: %w", ErrSerializerEncodeFailed, err)
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
		return nil, false, fmt.Errorf("%w: %w", ErrSerializerEncodeFailed, err)
	}

	prevRaw, loaded, err := s.store.Swap(key, serializedValue)
	if err != nil || !loaded {
		// Either an error occurred, or the key didn't exist (loaded=false).
		// In both cases, return nil for previous value (no previous value to decode).
		return nil, loaded, err
	}

	// Decode the returned value: key existed, so we have a previous value to deserialize.
	prevDecoded, err := s.deserializeValue(prevRaw)

	return prevDecoded, true, err
}

// CompareAndSwap performs an atomic CAS using serialized 'oldValue' and 'newValue'.
// It returns true on successful swap. No deserialization is needed here.
func (s *SerializedStore) CompareAndSwap(key string, oldValue any, newValue any) (bool, error) {
	var (
		oldSerializedValue []byte
		err                error
	)

	if oldValue != nil {
		oldSerializedValue, err = s.serializer.Serialize(oldValue)
		if err != nil {
			return false, err
		}
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

// Scan returns a page of key-value pairs, ordered lexicographically.
// If prefix is non-empty, only keys starting with prefix are considered.
// If afterKey is non-empty, scanning starts strictly after it; otherwise from the first key.
// If limit > 0, at most limit entries are returned; if limit <= 0, all matching entries are returned.
// Values are deserialized when the raw type is []byte or string; otherwise returned unchanged.
// Returns a ScanPage with Entries and NextKey (set to the last key when more results exist; empty when done).
func (s *SerializedStore) Scan(prefix, afterKey string, limit int64) (*ScanPage, error) {
	// Get the raw page from the underlying store.
	rawPage, err := s.store.Scan(prefix, afterKey, limit)
	if err != nil {
		return nil, err
	}

	// Deserialize each entry's value.
	entries, err := s.deserializeEntries(rawPage.Entries)
	if err != nil {
		return nil, err
	}

	return &ScanPage{
		Entries: entries,
		NextKey: rawPage.NextKey,
	}, nil
}

// List returns key-value pairs whose keys start with prefix, sorted lexicographically.
// If limit > 0, at most limit entries are returned; if limit <= 0, all matching entries are returned.
// Values are deserialized. This delegates to Scan internally.
func (s *SerializedStore) List(prefix string, limit int64) ([]Entry, error) {
	page, err := s.Scan(prefix, "", limit)
	if err != nil {
		return nil, err
	}

	return page.Entries, nil
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

// Backup streams the underlying store contents into a BoltDB snapshot.
func (s *SerializedStore) Backup(opts *BackupOptions) (*BackupSummary, error) {
	return s.store.Backup(opts)
}

// Restore replaces the underlying store contents with a previously exported snapshot.
func (s *SerializedStore) Restore(opts *RestoreOptions) (*RestoreSummary, error) {
	return s.store.Restore(opts)
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
