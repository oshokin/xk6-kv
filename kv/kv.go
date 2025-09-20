package kv

import (
	"fmt"

	"github.com/grafana/sobek"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/js/promises"

	"github.com/oshokin/xk6-kv/kv/store"
)

// KV is the JavaScript-facing wrapper around an internal key-value store (memory or disk).
// Each exported method:
//
//   - Binds to the VU (virtual user) runtime via Sobek.
//   - Validates that an underlying store is available (not nil).
//   - Executes the blocking store operation in a separate goroutine.
//   - Resolves/rejects a Sobek Promise back on the VU event loop.
//
// Why this pattern?
// k6 VUs can invoke these methods concurrently. Spinning the blocking calls in a goroutine
// keeps the VU event loop responsive while the backing store enforces atomicity/thread-safety.
//

// KV is the key-value facade exposed to k6 scripts.
type KV struct {
	// store is the backing implementation (memory or disk), possibly wrapped with
	// serialization or synchronization decorators by the caller.
	store store.Store

	// vu is the owning k6 VU that provides the Sobek runtime and event loop.
	vu modules.VU
}

// NewKV constructs a new KV bound to the given VU and backing Store.
//
// The caller is responsible for providing any desired decorators around the store
// (e.g. serialization wrappers).
func NewKV(vu modules.VU, s store.Store) *KV {
	return &KV{
		vu:    vu,
		store: s,
	}
}

// Get returns a Promise that resolves to the value stored under the provided key.
//
// Rejection cases:
//   - The database is not open.
//   - The key does not exist (error is forwarded from the store).
func (k *KV) Get(key sobek.Value) *sobek.Promise {
	keyString := key.String()

	return k.runAsyncWithStore(
		func(rt *sobek.Runtime, backingStore store.Store) (sobek.Value, error) {
			storedValue, err := backingStore.Get(keyString)
			if err != nil {
				return nil, err
			}

			return rt.ToValue(storedValue), nil
		})
}

// Set returns a Promise that resolves to the *same* JavaScript value that was provided.
//
// Behavior:
//   - creates the key if absent,
//   - overwrites existing value if present.
//
// Rejection cases:
//   - database is not open,
//   - store-level serialization/validation errors.
func (k *KV) Set(key sobek.Value, value sobek.Value) *sobek.Promise {
	keyString := key.String()

	// Convert Sobek value to Go 'any' for the store.
	exportedValue := value.Export()

	return k.runAsyncWithStore(
		func(_ *sobek.Runtime, backingStore store.Store) (sobek.Value, error) {
			if err := backingStore.Set(keyString, exportedValue); err != nil {
				return nil, err
			}

			// Resolve with the original JS value for ergonomic symmetry.
			return value, nil
		})
}

// IncrementBy returns a Promise that resolves to the new integer value (int64) after atomically
// adding "delta" to the current value at "key".
//
// Notes:
//   - Absent keys are treated as 0 prior to the increment.
//   - The existing value must parse as an integer; otherwise the Promise is rejected.
func (k *KV) IncrementBy(key sobek.Value, delta sobek.Value) *sobek.Promise {
	keyString := key.String()

	return k.runAsyncWithStore(
		func(rt *sobek.Runtime, backingStore store.Store) (sobek.Value, error) {
			var deltaInt int64

			if err := rt.ExportTo(delta, &deltaInt); err != nil {
				return nil, NewError(ValueNumberRequiredError, fmt.Sprintf("delta must be a number: %v", err))
			}

			newValue, err := backingStore.IncrementBy(keyString, deltaInt)
			if err != nil {
				return nil, err
			}

			return rt.ToValue(newValue), nil
		})
}

// GetOrSet returns a Promise that resolves to an object: { value: any, loaded: boolean }.
//
// Semantics:
//   - If the key exists -> { value: existing, loaded: true }.
//   - If the key is absent -> stores the provided value and resolves { value: stored, loaded: false }.
func (k *KV) GetOrSet(key sobek.Value, value sobek.Value) *sobek.Promise {
	keyString := key.String()
	exportedValue := value.Export()

	return k.runAsyncWithStore(
		func(rt *sobek.Runtime, backingStore store.Store) (sobek.Value, error) {
			actualValue, wasLoaded, err := backingStore.GetOrSet(keyString, exportedValue)
			if err != nil {
				return nil, err
			}

			return makeJSObject(rt, func(obj *sobek.Object) error {
				if err := obj.Set("value", rt.ToValue(actualValue)); err != nil {
					return err
				}

				return obj.Set("loaded", wasLoaded)
			})
		})
}

// Swap returns a Promise that resolves to: { previous: any|null, loaded: boolean }.
//
// Semantics:
//   - If key existed -> it is replaced; resolves previous value with loaded=true.
//   - If key was absent -> it is created; resolves previous=null with loaded=false.
func (k *KV) Swap(key sobek.Value, value sobek.Value) *sobek.Promise {
	keyString := key.String()
	exportedValue := value.Export()

	return k.runAsyncWithStore(
		func(rt *sobek.Runtime, backingStore store.Store) (sobek.Value, error) {
			previousValue, wasLoaded, err := backingStore.Swap(keyString, exportedValue)
			if err != nil {
				return nil, err
			}

			return makeJSObject(rt, func(obj *sobek.Object) error {
				// Use explicit null to avoid "undefined" surprises in JS.
				if wasLoaded {
					if err := obj.Set("previous", rt.ToValue(previousValue)); err != nil {
						return err
					}
				} else {
					if err := obj.Set("previous", sobek.Null()); err != nil {
						return err
					}
				}

				return obj.Set("loaded", wasLoaded)
			})
		})
}

// CompareAndSwap (CAS) returns a Promise that resolves to true iff the value at "key"
// equals "oldValue" and is atomically replaced by "newValue".
// Otherwise resolves to false.
func (k *KV) CompareAndSwap(key, oldValue, newValue sobek.Value) *sobek.Promise {
	var (
		keyString   = key.String()
		exportedOld = oldValue.Export()
		exportedNew = newValue.Export()
	)

	return k.runAsyncWithStore(
		func(rt *sobek.Runtime, backingStore store.Store) (sobek.Value, error) {
			swapped, err := backingStore.CompareAndSwap(keyString, exportedOld, exportedNew)
			if err != nil {
				return nil, err
			}

			return rt.ToValue(swapped), nil
		})
}

// Delete returns a Promise that resolves to true after deleting 'key'.
//
// Note: Deleting a non-existent key still resolves to true to keep the API simple.
// If you need to know whether deletion actually happened, use DeleteIfExists.
func (k *KV) Delete(key sobek.Value) *sobek.Promise {
	keyString := key.String()

	return k.runAsyncWithStore(
		func(rt *sobek.Runtime, backingStore store.Store) (sobek.Value, error) {
			if err := backingStore.Delete(keyString); err != nil {
				return nil, err
			}

			return rt.ToValue(true), nil
		})
}

// Exists returns a Promise that resolves to true if the key exists, false otherwise.
func (k *KV) Exists(key sobek.Value) *sobek.Promise {
	keyString := key.String()

	return k.runAsyncWithStore(
		func(rt *sobek.Runtime, backingStore store.Store) (sobek.Value, error) {
			exists, err := backingStore.Exists(keyString)
			if err != nil {
				return nil, err
			}

			return rt.ToValue(exists), nil
		})
}

// DeleteIfExists returns a Promise that resolves to true only if the key was present
// and has been deleted; false if it was absent.
func (k *KV) DeleteIfExists(key sobek.Value) *sobek.Promise {
	keyString := key.String()

	return k.runAsyncWithStore(
		func(rt *sobek.Runtime, backingStore store.Store) (sobek.Value, error) {
			deleted, err := backingStore.DeleteIfExists(keyString)
			if err != nil {
				return nil, err
			}

			return rt.ToValue(deleted), nil
		})
}

// CompareAndDelete returns a Promise that resolves to true if the current value at "key"
// equals "oldValue" and the key was deleted atomically; otherwise false.
func (k *KV) CompareAndDelete(key, old sobek.Value) *sobek.Promise {
	var (
		keyString   = key.String()
		exportedOld = old.Export()
	)

	return k.runAsyncWithStore(
		func(rt *sobek.Runtime, backingStore store.Store) (sobek.Value, error) {
			deleted, err := backingStore.CompareAndDelete(keyString, exportedOld)
			if err != nil {
				return nil, err
			}

			return rt.ToValue(deleted), nil
		})
}

// Clear returns a Promise that resolves to true after removing all keys.
// Depending on the backend, this may be an expensive O(n) operation.
func (k *KV) Clear() *sobek.Promise {
	return k.runAsyncWithStore(
		func(rt *sobek.Runtime, backingStore store.Store) (sobek.Value, error) {
			if err := backingStore.Clear(); err != nil {
				return nil, err
			}

			return rt.ToValue(true), nil
		})
}

// Size returns a Promise that resolves to the number of keys currently stored.
// On some backends this can be O(n).
func (k *KV) Size() *sobek.Promise {
	return k.runAsyncWithStore(
		func(rt *sobek.Runtime, backingStore store.Store) (sobek.Value, error) {
			size, err := backingStore.Size()
			if err != nil {
				return nil, err
			}

			return rt.ToValue(size), nil
		})
}

// Close closes the underlying store. It is synchronous because the caller usually
// needs to know the outcome immediately (e.g., test tear-down).
func (k *KV) Close() error {
	if k.store == nil {
		return k.databaseNotOpenError()
	}

	return k.store.Close()
}

// List returns a Promise that resolves to an array of { key, value } objects,
// ordered lexicographically by key. Options support prefix and limit.
func (k *KV) List(options sobek.Value) *sobek.Promise {
	listOptions := ImportListOptions(k.vu.Runtime(), options)

	return k.runAsyncWithStore(
		func(rt *sobek.Runtime, backingStore store.Store) (sobek.Value, error) {
			entries, err := backingStore.List(listOptions.Prefix, listOptions.Limit)
			if err != nil {
				return nil, err
			}

			jsEntries := make([]ListEntry, len(entries))
			for i, entry := range entries {
				jsEntries[i] = ListEntry{
					Key:   entry.Key,
					Value: entry.Value,
				}
			}

			return rt.ToValue(jsEntries), nil
		})
}

// RandomKey returns a Promise that resolves to a random key as a string.
// If the store is empty or no keys match the optional prefix, resolves to "" (empty string).
func (k *KV) RandomKey(options sobek.Value) *sobek.Promise {
	randomKeyOptions := ImportRandomKeyOptions(k.vu.Runtime(), options)

	return k.runAsyncWithStore(
		func(rt *sobek.Runtime, backingStore store.Store) (sobek.Value, error) {
			keyString, err := backingStore.RandomKey(randomKeyOptions.Prefix)
			if err != nil {
				return nil, err
			}

			return rt.ToValue(keyString), nil
		})
}

// RebuildKeyList returns a Promise that resolves to true after rebuilding any in-memory
// indexes used by the underlying store (no-op where unsupported).
//
// This is primarily useful for disk-based backends when key-tracking is enabled
// and a rebuild is needed after a crash or manual intervention.
func (k *KV) RebuildKeyList() *sobek.Promise {
	return k.runAsyncWithStore(func(rt *sobek.Runtime, backingStore store.Store) (sobek.Value, error) {
		if err := backingStore.RebuildKeyList(); err != nil {
			return nil, err
		}

		return rt.ToValue(true), nil
	})
}

// ListEntry is the JavaScript-facing representation of a key-value pair returned by List().
type ListEntry struct {
	// Key is the entry key (List results are lexicographically ordered by this).
	Key string `json:"key"`
	// Value is the stored value (type depends on the store's serializer).
	Value any `json:"value"`
}

// ListOptions describes filters for List(); all fields are optional.
type ListOptions struct {
	// Prefix selects only keys that start with the given string.
	Prefix string `json:"prefix"`

	// Limit is the maximum number of entries to return; <= 0 means "no limit".
	Limit int64 `json:"limit"`

	// limitSet indicates whether Limit was explicitly provided from JS.
	limitSet bool
}

// ImportListOptions converts a Sobek value into ListOptions, accepting null/undefined
// and partial objects. Unknown fields are ignored.
func ImportListOptions(rt *sobek.Runtime, options sobek.Value) ListOptions {
	listOptions := ListOptions{}

	// If no options are passed, return the default options.
	if common.IsNullish(options) {
		return listOptions
	}

	// Interpret the options as a plain object from JS.
	optionsObj := options.ToObject(rt)

	listOptions.Prefix = optionsObj.Get("prefix").String()

	limitValue := optionsObj.Get("limit")
	if limitValue == nil {
		return listOptions
	}

	var (
		parsedLimit int64
		err         = rt.ExportTo(limitValue, &parsedLimit)
	)

	if err == nil {
		listOptions.Limit = parsedLimit
		listOptions.limitSet = true
	}

	return listOptions
}

// RandomKeyOptions holds the optional prefix filter for RandomKey().
type RandomKeyOptions struct {
	// Prefix restricts random selection to keys beginning with this string.
	Prefix string `json:"prefix"`
}

// ImportRandomKeyOptions converts a Sobek value into RandomKeyOptions.
// Accepts null/undefined and partial objects; missing fields default to zero-values.
func ImportRandomKeyOptions(rt *sobek.Runtime, options sobek.Value) RandomKeyOptions {
	randomKeyOptions := RandomKeyOptions{}
	if common.IsNullish(options) {
		return randomKeyOptions
	}

	optionsObj := options.ToObject(rt)
	randomKeyOptions.Prefix = optionsObj.Get("prefix").String()

	return randomKeyOptions
}

// databaseNotOpenError produces a consistent error when the backing store is nil.
func (k *KV) databaseNotOpenError() error {
	return NewError(DatabaseNotOpenError, "database is not open")
}

// runAsyncWithStore is a small template that:
//
//  1. creates a Promise bound to the current VU,
//  2. checks that the store is available,
//  3. executes the given operation in a goroutine,
//  4. resolves/rejects the Promise with the JS-ready value.
//
// The provided function must return a Sobek value that is safe to pass to JS
// (typically produced with rt.ToValue(...) or a fully-constructed JS object).
func (k *KV) runAsyncWithStore(
	operation func(rt *sobek.Runtime, backingStore store.Store) (sobek.Value, error),
) *sobek.Promise {
	promise, resolve, reject := promises.New(k.vu)

	go func() {
		if k.store == nil {
			reject(k.databaseNotOpenError())
			return
		}

		jsRuntime := k.vu.Runtime()

		// Convert the Go value to a JavaScript value for the VU runtime.
		jsValue, err := operation(jsRuntime, k.store)
		if err != nil {
			reject(err)
			return
		}

		resolve(jsValue)
	}()

	return promise
}

// makeJSObject is a tiny helper to create an object in the Sobek runtime and
// allow a setter function to populate it field-by-field.
func makeJSObject(rt *sobek.Runtime, setter func(obj *sobek.Object) error) (sobek.Value, error) {
	obj := rt.NewObject()
	if err := setter(obj); err != nil {
		return nil, err
	}

	return obj, nil
}
