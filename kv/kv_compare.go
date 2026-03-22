package kv

import (
	"github.com/grafana/sobek"

	"github.com/oshokin/xk6-kv/kv/store"
)

// CompareAndSwap (CAS) returns a Promise that resolves to true iff the value at "key"
// equals "oldValue" and is atomically replaced by "newValue".
// Otherwise resolves to false.
// Passing null/undefined (JS) for oldValue means "swap only if the key does not exist",
// mirroring sync/atomic.CompareAndSwap semantics in Go.
func (k *KV) CompareAndSwap(key, oldValue, newValue sobek.Value) *sobek.Promise {
	var (
		keyString   = key.String()
		exportedOld = oldValue.Export()
		exportedNew = newValue.Export()
	)

	return k.runAsyncWithStore(
		func(s store.Store) (any, error) {
			return s.CompareAndSwap(keyString, exportedOld, exportedNew)
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

// CompareAndSwapDetailed returns a Promise that resolves to an object describing
// CAS outcome:
//   - Success: { swapped: true, reason: "swapped" }
//   - Mismatch: { swapped: false, reason: "mismatch", existed: boolean, current?: any }
//
// current is included only when options.includeCurrentOnMismatch is true and the
// key existed at compare time.
func (k *KV) CompareAndSwapDetailed(key, oldValue, newValue, options sobek.Value) *sobek.Promise {
	var (
		keyString                = key.String()
		exportedOld              = oldValue.Export()
		exportedNew              = newValue.Export()
		includeCurrentOnMismatch = importIncludeCurrentOnMismatchOption(k.vu.Runtime(), options)
	)

	return k.runAsyncWithStore(
		func(s store.Store) (any, error) {
			detailed, err := s.CompareAndSwapDetailed(keyString, exportedOld, exportedNew, includeCurrentOnMismatch)
			if err != nil {
				return nil, err
			}

			if detailed == nil {
				return nil, unexpectedStoreOutput("store.CompareAndSwapDetailed")
			}

			return compareAndSwapDetailedPayload(detailed), nil
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

// Delete returns a Promise that resolves to true after deleting 'key'.
//
// Note: Deleting a non-existent key still resolves to true to keep the API simple.
// If you need to know whether deletion actually happened, use DeleteIfExists.
func (k *KV) Delete(key sobek.Value) *sobek.Promise {
	keyString := key.String()

	return k.runAsyncWithStore(
		func(s store.Store) (any, error) {
			return true, s.Delete(keyString)
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

// Exists returns a Promise that resolves to true if the key exists, false otherwise.
func (k *KV) Exists(key sobek.Value) *sobek.Promise {
	keyString := key.String()

	return k.runAsyncWithStore(
		func(s store.Store) (any, error) {
			return s.Exists(keyString)
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

// DeleteIfExists returns a Promise that resolves to true only if the key was present
// and has been deleted; false if it was absent.
func (k *KV) DeleteIfExists(key sobek.Value) *sobek.Promise {
	keyString := key.String()

	return k.runAsyncWithStore(
		func(s store.Store) (any, error) {
			return s.DeleteIfExists(keyString)
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

// CompareAndDelete returns a Promise that resolves to true if the current value at "key"
// equals "oldValue" and the key was deleted atomically; otherwise false.
func (k *KV) CompareAndDelete(key, old sobek.Value) *sobek.Promise {
	keyString := key.String()
	exportedOld := old.Export()

	return k.runAsyncWithStore(
		func(s store.Store) (any, error) {
			return s.CompareAndDelete(keyString, exportedOld)
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

// CompareAndDeleteDetailed returns a Promise that resolves to an object describing
// compare-and-delete outcome:
//   - Success: { deleted: true, reason: "deleted" }
//   - Mismatch: { deleted: false, reason: "mismatch", existed: boolean, current?: any }
//
// current is included only when options.includeCurrentOnMismatch is true and the
// key existed at compare time.
func (k *KV) CompareAndDeleteDetailed(key, old, options sobek.Value) *sobek.Promise {
	var (
		keyString                = key.String()
		exportedOld              = old.Export()
		includeCurrentOnMismatch = importIncludeCurrentOnMismatchOption(k.vu.Runtime(), options)
	)

	return k.runAsyncWithStore(
		func(s store.Store) (any, error) {
			detailed, err := s.CompareAndDeleteDetailed(keyString, exportedOld, includeCurrentOnMismatch)
			if err != nil {
				return nil, err
			}

			if detailed == nil {
				return nil, unexpectedStoreOutput("store.CompareAndDeleteDetailed")
			}

			return compareAndDeleteDetailedPayload(detailed), nil
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

// SetIfAbsent atomically sets value only when key is currently absent.
// Returns true if the value was inserted, false if the key already existed.
func (k *KV) SetIfAbsent(key, value sobek.Value) *sobek.Promise {
	keyString := key.String()
	exportedValue := value.Export()

	return k.runAsyncWithStore(
		func(s store.Store) (any, error) {
			return s.CompareAndSwap(keyString, nil, exportedValue)
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}
