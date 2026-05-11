package store

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
	serializedValue, err := s.serializeWriteValue(value)
	if err != nil {
		return nil, false, err
	}

	rawValue, loaded, err := s.store.GetOrSet(key, serializedValue)
	if err != nil {
		return nil, false, err
	}

	if !loaded {
		rawValue = serializedValue
	}

	decoded, err := s.deserializeValue(rawValue)

	return decoded, loaded, err
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
