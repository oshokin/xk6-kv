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

// GetMany fetches raw values for keys and deserializes each non-missing entry.
// Missing keys are represented as nil entries.
func (s *SerializedStore) GetMany(keys []string) ([]*Entry, error) {
	entries, err := s.store.GetMany(keys)
	if err != nil {
		return nil, err
	}

	decoded := make([]*Entry, len(entries))

	for i, entry := range entries {
		if entry == nil {
			continue
		}

		value, err := s.deserializeValue(entry.Value)
		if err != nil {
			return nil, err
		}

		decoded[i] = &Entry{
			Key:   entry.Key,
			Value: value,
		}
	}

	return decoded, nil
}

// Set serializes the provided value to bytes and stores it under the given key.
func (s *SerializedStore) Set(key string, value any) error {
	serializedValue, err := s.serializeWriteValue(value)
	if err != nil {
		return err
	}

	// Store the serialized value.
	return s.store.Set(key, serializedValue)
}

// SetMany serializes all values first and writes only when every entry succeeds.
func (s *SerializedStore) SetMany(entries []Entry) (int64, error) {
	encoded := make([]Entry, 0, len(entries))
	entryErrors := make([]EntryError, 0)

	for i := range entries {
		serializedValue, err := s.serializeWriteValue(entries[i].Value)
		if err != nil {
			entryErrors = append(entryErrors, EntryError{
				Key:     entries[i].Key,
				Name:    EntryErrorNameSerializer,
				Message: err.Error(),
			})

			continue
		}

		encoded = append(encoded, Entry{
			Key:   entries[i].Key,
			Value: serializedValue,
		})
	}

	if len(entryErrors) > 0 {
		return 0, NewEntryListError("setMany", EntryListErrorKindSerialization, entryErrors)
	}

	return s.store.SetMany(encoded)
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
	return s.writeAndDecodeLoadedValue(key, value, s.store.GetOrSet)
}

// Swap replaces the current value for a key with the serialized new value and
// returns the previous value (deserialized when applicable). The 'loaded' flag
// is false when the key did not previously exist.
func (s *SerializedStore) Swap(key string, value any) (any, bool, error) {
	return s.writeAndDecodeLoadedValue(key, value, s.store.Swap)
}

// CompareAndSwap performs an atomic CAS using serialized 'oldValue' and 'newValue'.
// It returns true on successful swap. No deserialization is needed here.
func (s *SerializedStore) CompareAndSwap(key string, oldValue any, newValue any) (bool, error) {
	result, err := s.CompareAndSwapDetailed(key, oldValue, newValue, false)
	if err != nil {
		return false, err
	}

	return result.Swapped, nil
}

// CompareAndSwapDetailed performs CAS with serialized values and returns a
// structured result. Current is deserialized when included.
func (s *SerializedStore) CompareAndSwapDetailed(
	key string,
	oldValue any,
	newValue any,
	includeCurrentOnMismatch bool,
) (*CompareAndSwapDetailedResult, error) {
	var (
		oldSerializedValue []byte
		oldCompareValue    any
		err                error
	)

	// Preserve CAS absent-key sentinel semantics by passing a real nil interface
	// to the underlying store when oldValue is null/undefined from JS.
	oldCompareValue = nil

	if oldValue != nil {
		oldSerializedValue, err = s.serializeWriteValue(oldValue)
		if err != nil {
			return nil, err
		}

		oldCompareValue = oldSerializedValue
	}

	newSerializedValue, err := s.serializeWriteValue(newValue)
	if err != nil {
		return nil, err
	}

	result, err := s.store.CompareAndSwapDetailed(key, oldCompareValue, newSerializedValue, includeCurrentOnMismatch)
	if err != nil {
		return nil, err
	}

	if result.ShouldIncludeCurrent() {
		decodedCurrent, decodeErr := s.deserializeValue(result.Current)
		if decodeErr != nil {
			return nil, decodeErr
		}

		result.Current = decodedCurrent
	}

	return result, nil
}

// Delete removes the key from the underlying store (no serialization involved).
func (s *SerializedStore) Delete(key string) error {
	return s.store.Delete(key)
}

// DeleteMany deletes explicit keys and returns delete/missing counts.
func (s *SerializedStore) DeleteMany(keys []string) (*DeleteManyResult, error) {
	return s.store.DeleteMany(keys)
}

// DeleteByPrefix deletes up to limit keys by non-empty prefix.
func (s *SerializedStore) DeleteByPrefix(prefix string, limit int64) (*DeleteByPrefixResult, error) {
	return s.store.DeleteByPrefix(prefix, limit)
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
	result, err := s.CompareAndDeleteDetailed(key, oldValue, false)
	if err != nil {
		return false, err
	}

	return result.Deleted, nil
}

// CompareAndDeleteDetailed performs compare-and-delete with serialized values
// and returns a structured result. Current is deserialized when included.
func (s *SerializedStore) CompareAndDeleteDetailed(
	key string,
	oldValue any,
	includeCurrentOnMismatch bool,
) (*CompareAndDeleteDetailedResult, error) {
	serializedOldValue, err := s.serializeWriteValue(oldValue)
	if err != nil {
		return nil, err
	}

	result, err := s.store.CompareAndDeleteDetailed(key, serializedOldValue, includeCurrentOnMismatch)
	if err != nil {
		return nil, err
	}

	if result.ShouldIncludeCurrent() {
		decodedCurrent, decodeErr := s.deserializeValue(result.Current)
		if decodeErr != nil {
			return nil, decodeErr
		}

		result.Current = decodedCurrent
	}

	return result, nil
}

// Clear removes all keys from the underlying store.
func (s *SerializedStore) Clear() error {
	return s.store.Clear()
}

// Size returns the number of keys currently in the underlying store.
func (s *SerializedStore) Size() (int64, error) {
	return s.store.Size()
}

// Count returns the number of keys matching prefix in the underlying store.
// Count("") is equivalent to Size().
func (s *SerializedStore) Count(prefix string) (int64, error) {
	return s.store.Count(prefix)
}

// ScanKeys returns matching keys without cloning, deserializing, or returning values.
func (s *SerializedStore) ScanKeys(prefix, afterKey string, limit int64) (*KeyScanPage, error) {
	return s.store.ScanKeys(prefix, afterKey, limit)
}

// ListKeys returns matching keys without cloning, deserializing, or returning values.
func (s *SerializedStore) ListKeys(prefix string, limit int64) ([]string, error) {
	return s.store.ListKeys(prefix, limit)
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

// RandomKeys returns random key names from the underlying store.
func (s *SerializedStore) RandomKeys(prefix string, count int64, unique bool) ([]string, error) {
	return s.store.RandomKeys(prefix, count, unique)
}

// PopRandom atomically selects and removes a random matching entry and deserializes its value.
func (s *SerializedStore) PopRandom(prefix string) (*Entry, error) {
	entry, err := s.store.PopRandom(prefix)
	if err != nil || entry == nil {
		return entry, err
	}

	decoded, err := s.deserializeValue(entry.Value)
	if err != nil {
		return nil, err
	}

	entry.Value = decoded

	return entry, nil
}

// ClaimRandom atomically leases a random matching entry and deserializes its value.
func (s *SerializedStore) ClaimRandom(opts *ClaimOptions) (*EntryClaim, error) {
	claim, err := s.store.ClaimRandom(opts)
	if err != nil || claim == nil {
		return claim, err
	}

	decoded, err := s.deserializeValue(claim.Entry.Value)
	if err != nil {
		return nil, err
	}

	claim.Entry.Value = decoded

	return claim, nil
}

// ReleaseClaim delegates claim release to the underlying store.
func (s *SerializedStore) ReleaseClaim(ref *ClaimRef) (bool, error) {
	return s.store.ReleaseClaim(ref)
}

// CompleteClaim delegates claim completion to the underlying store.
func (s *SerializedStore) CompleteClaim(ref *ClaimRef, opts *CompleteClaimOptions) (bool, error) {
	return s.store.CompleteClaim(ref, opts)
}

// RebuildKeyList asks the underlying store to rebuild any in-memory key indices
// from durable storage. Primarily useful for disk-backed stores.
func (s *SerializedStore) RebuildKeyList() error {
	return s.store.RebuildKeyList()
}

// Stats returns a diagnostic snapshot and enriches it with serializer details.
func (s *SerializedStore) Stats() (*StatsSnapshot, error) {
	snapshot, err := s.store.Stats()
	if err != nil {
		return nil, err
	}

	if snapshot == nil {
		return nil, errors.New("store stats returned nil snapshot")
	}

	snapshot.Serialization = s.serializer.Type()

	return snapshot, nil
}

// Backup streams the underlying store contents into a bbolt snapshot.
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

func normalizeSerializerEncodeError(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, ErrSerializerEncodeFailed) {
		return err
	}

	return fmt.Errorf("%w: %w", ErrSerializerEncodeFailed, err)
}

func (s *SerializedStore) serializeWriteValue(value any) ([]byte, error) {
	serializedValue, err := s.serializer.Serialize(value)
	if err != nil {
		return nil, normalizeSerializerEncodeError(err)
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
