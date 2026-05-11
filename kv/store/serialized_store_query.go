package store

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
