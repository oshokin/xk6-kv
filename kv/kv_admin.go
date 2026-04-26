package kv

import (
	"github.com/grafana/sobek"

	"github.com/oshokin/xk6-kv/kv/store"
)

// Clear returns a Promise that resolves to true after removing all keys.
// Depending on the backend, this may be an expensive O(n) operation.
func (k *KV) Clear() *sobek.Promise {
	return k.runAsyncWithStore(
		func(s store.Store) (any, error) {
			return true, s.Clear()
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

// Size returns a Promise that resolves to the number of keys currently stored.
func (k *KV) Size() *sobek.Promise {
	return k.runAsyncWithStore(
		func(s store.Store) (any, error) {
			return s.Size()
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

// RebuildKeyList returns a Promise that resolves to true after rebuilding any in-memory
// indexes used by the underlying store (no-op where unsupported).
//
// This is primarily useful for disk-based backends when key-tracking is enabled
// and a rebuild is needed after a crash or manual intervention.
func (k *KV) RebuildKeyList() *sobek.Promise {
	return k.runAsyncWithStore(
		func(s store.Store) (any, error) {
			return true, s.RebuildKeyList()
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

// Backup streams the entire dataset into a bbolt snapshot file.
// Options allow overriding the destination path and enabling best-effort mode.
// Backup creates a point-in-time snapshot of the KV store and writes it to disk.
// Returns a Promise that resolves to a backup summary with operation metrics.
// The summary uses camelCase field names for JavaScript convention compatibility.
func (k *KV) Backup(options sobek.Value) *sobek.Promise {
	backupOptions := importBackupOptions(k.vu.Runtime(), options)

	return k.runAsyncWithStore(
		func(s store.Store) (any, error) {
			storeSummary, err := s.Backup(&store.BackupOptions{
				FileName:              backupOptions.FileName,
				AllowConcurrentWrites: backupOptions.AllowConcurrentWrites,
			})
			if err != nil {
				return nil, err
			}

			if storeSummary == nil {
				return nil, unexpectedStoreOutput("store.Backup")
			}

			return storeSummary, nil
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

// Restore replaces the current dataset with a snapshot.
// Restore loads a previously created snapshot back into the KV store.
// Returns a Promise that resolves to a restore summary with operation metrics.
// The summary uses camelCase field names for JavaScript convention compatibility.
func (k *KV) Restore(options sobek.Value) *sobek.Promise {
	restoreOptions := importRestoreOptions(k.vu.Runtime(), options)

	return k.runAsyncWithStore(
		func(s store.Store) (any, error) {
			storeSummary, err := s.Restore(&store.RestoreOptions{
				FileName:   restoreOptions.FileName,
				MaxEntries: restoreOptions.MaxEntries,
				MaxBytes:   restoreOptions.MaxBytes,
			})
			if err != nil {
				return nil, err
			}

			if storeSummary == nil {
				return nil, unexpectedStoreOutput("store.Restore")
			}

			return storeSummary, nil
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

// Close closes the underlying store. It is synchronous because the caller usually
// needs to know the outcome immediately (e.g., test tear-down).
func (k *KV) Close() error {
	if k.store == nil {
		return k.databaseNotOpenError()
	}

	k.closeOnce.Do(func() {
		k.closeErr = k.store.Close()
	})

	return k.closeErr
}
