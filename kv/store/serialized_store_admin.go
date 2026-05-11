package store

import "errors"

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
