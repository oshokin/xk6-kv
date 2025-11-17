package kv

import (
	"encoding/base64"
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
// Threading model:
//
//   - All blocking store work occurs in a goroutine (off the VU event loop).
//   - For converting Go results to JavaScript values, we use k.vu.Runtime().ToValue(...)
//     in the same goroutine, matching the original upstream plugin's pattern that
//     is known to work with this version of Sobek/k6.
//
// This avoids corrupting Sobek's internal VM state and prevents panics like
// "slice bounds out of range [:-1]" in vm.popTryFrame under high concurrency.
//
// Why this pattern?
// k6 VUs can invoke these methods concurrently. Spinning the blocking calls in a goroutine
// keeps the VU event loop responsive while the backing store enforces atomicity/thread-safety.

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

// Small result helpers we convert to JS on the event loop (via ToValue).
// These structs map cleanly to JS objects through Sobek's ToValue encoder.
type getOrSetResult struct {
	Value  any  `json:"value"`
	Loaded bool `json:"loaded"`
}

type swapResult struct {
	Previous any  `json:"previous"`
	Loaded   bool `json:"loaded"`
}

// Get returns a Promise that resolves to the value stored under the provided key.
//
// Rejection cases:
//   - The database is not open.
//   - The key does not exist (error is forwarded from the store).
func (k *KV) Get(key sobek.Value) *sobek.Promise {
	keyString := key.String()

	return k.runAsyncWithStore(
		func(store store.Store) (any, error) {
			return store.Get(keyString)
		},

		// Convert the Go value to a JavaScript value for the VU runtime.
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
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
		func(store store.Store) (any, error) {
			return value, store.Set(keyString, exportedValue)
		},

		// Resolve with the exact same JS value the user passed in (identity/symmetry).
		func(_ *sobek.Runtime, _ any) sobek.Value {
			return value
		},
	)
}

// IncrementBy returns a Promise that resolves to the new integer value (int64) after atomically
// adding "delta" to the current value at "key".
//
// Notes:
//   - Absent keys are treated as 0 prior to the increment.
//   - The existing value must parse as an integer; otherwise the Promise is rejected.
func (k *KV) IncrementBy(key sobek.Value, delta sobek.Value) *sobek.Promise {
	keyString := key.String()

	// We must not rely on rt.ExportTo(...) here because we are not on the VU event loop.
	// We instead use sobek.Value.Export(), then coerce to int64 synchronously before we spawn the goroutine.
	deltaInt, err := exportToInt64(delta)
	if err != nil {
		// No promises.Reject(...) in this environment; create a promise and reject it immediately.
		p, _, reject := promises.New(k.vu)
		reject(NewError(ValueNumberRequiredError, fmt.Sprintf("delta must be a number: %v", err)))

		return p
	}

	return k.runAsyncWithStore(
		func(store store.Store) (any, error) {
			return store.IncrementBy(keyString, deltaInt)
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
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
		func(store store.Store) (any, error) {
			actualValue, wasLoaded, err := store.GetOrSet(keyString, exportedValue)
			if err != nil {
				return nil, err
			}

			return getOrSetResult{
				Value:  actualValue,
				Loaded: wasLoaded,
			}, nil
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
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
		func(store store.Store) (any, error) {
			previousValue, loaded, err := store.Swap(keyString, exportedValue)
			if err != nil {
				return nil, err
			}

			return swapResult{
				Previous: previousValue,
				Loaded:   loaded,
			}, nil
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

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
		func(store store.Store) (any, error) {
			return store.CompareAndSwap(keyString, exportedOld, exportedNew)
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
		func(store store.Store) (any, error) {
			return true, store.Delete(keyString)
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
		func(store store.Store) (any, error) {
			return store.Exists(keyString)
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
		func(store store.Store) (any, error) {
			return store.DeleteIfExists(keyString)
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
		func(store store.Store) (any, error) {
			return store.CompareAndDelete(keyString, exportedOld)
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

// Clear returns a Promise that resolves to true after removing all keys.
// Depending on the backend, this may be an expensive O(n) operation.
func (k *KV) Clear() *sobek.Promise {
	return k.runAsyncWithStore(
		func(store store.Store) (any, error) {
			return true, store.Clear()
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

// Size returns a Promise that resolves to the number of keys currently stored.
// On some backends this can be O(n).
func (k *KV) Size() *sobek.Promise {
	return k.runAsyncWithStore(
		func(store store.Store) (any, error) {
			return store.Size()
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

	return k.store.Close()
}

// Scan returns a Promise that resolves to { entries: [], cursor: string, done: bool }.
// It supports cursor-based pagination over keys, ordered lexicographically.
// Pass the cursor from a previous result to continue; omit it (or pass "") to start fresh.
func (k *KV) Scan(options sobek.Value) *sobek.Promise {
	scanOptions := ImportScanOptions(k.vu.Runtime(), options)

	return k.runAsyncWithStore(
		func(s store.Store) (any, error) {
			var afterKey string

			if scanOptions.Cursor != "" {
				raw, err := base64.StdEncoding.DecodeString(scanOptions.Cursor)
				if err != nil {
					return nil, fmt.Errorf("invalid cursor for scan(): %w", err)
				}

				afterKey = string(raw)
			}

			page, err := s.Scan(scanOptions.Prefix, afterKey, scanOptions.Limit)
			if err != nil {
				return nil, err
			}

			jsEntries := make([]ListEntry, len(page.Entries))
			for i, entry := range page.Entries {
				jsEntries[i] = ListEntry{
					Key:   entry.Key,
					Value: entry.Value,
				}
			}

			var (
				cursor string
				done   bool
			)

			if page.NextKey != "" {
				cursor = base64.StdEncoding.EncodeToString([]byte(page.NextKey))
				done = false
			} else {
				cursor = ""
				done = true
			}

			return ScanResult{
				Entries: jsEntries,
				Cursor:  cursor,
				Done:    done,
			}, nil
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

// List returns a Promise that resolves to an array of { key, value } objects,
// ordered lexicographically by key. Options support prefix and limit.
func (k *KV) List(options sobek.Value) *sobek.Promise {
	// Import list options from JavaScript (safe on the VU thread now).
	listOptions := ImportListOptions(k.vu.Runtime(), options)

	return k.runAsyncWithStore(
		func(store store.Store) (any, error) {
			entries, err := store.List(listOptions.Prefix, listOptions.Limit)
			if err != nil {
				return nil, err
			}

			// Convert entries to the JS-facing struct.
			// ToValue will map it to an array of objects.
			jsEntries := make([]ListEntry, len(entries))
			for i, entry := range entries {
				jsEntries[i] = ListEntry{
					Key:   entry.Key,
					Value: entry.Value,
				}
			}

			return jsEntries, nil
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

// RandomKey returns a Promise that resolves to a random key as a string.
// If the store is empty or no keys match the optional prefix, resolves to "" (empty string).
func (k *KV) RandomKey(options sobek.Value) *sobek.Promise {
	randomKeyOptions := ImportRandomKeyOptions(k.vu.Runtime(), options)

	return k.runAsyncWithStore(
		func(store store.Store) (any, error) {
			return store.RandomKey(randomKeyOptions.Prefix)
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
		func(store store.Store) (any, error) {
			return true, store.RebuildKeyList()
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

// ListEntry is the JavaScript-facing representation of a key-value pair returned by List().
type ListEntry struct {
	// Key is the entry key (List results are lexicographically ordered by this).
	Key string `json:"key"`

	// Value is the stored value (type depends on the store's serializer).
	Value any `json:"value"`
}

// ScanOptions holds optional filters for Scan().
type ScanOptions struct {
	// Prefix selects only keys that start with the given string.
	Prefix string `json:"prefix"`

	// Limit is the maximum number of entries to return in a single page;
	// <= 0 means "no limit" (effectively behaves like list()).
	Limit int64 `json:"limit"`

	// Cursor is an opaque continuation token previously returned from Scan().
	// It is a base64-encoded representation of the last key in the previous page.
	Cursor string `json:"cursor"`

	// isLimitSet indicates whether Limit was explicitly provided from JS.
	isLimitSet bool
}

// ScanResult is the JS-facing result of kv.scan().
type ScanResult struct {
	// Entries holds the page of key/value pairs.
	Entries []ListEntry `json:"entries"`

	// Cursor is an opaque continuation token. The first call should pass an
	// empty cursor (or omit it); subsequent calls should pass the cursor from
	// the previous result. When Cursor is empty, the scan is complete.
	Cursor string `json:"cursor"`

	// Done is true when the scan is complete (i.e., Cursor == "").
	Done bool `json:"done"`
}

// ImportScanOptions converts a Sobek value into ScanOptions.
// Accepts null/undefined and partial objects; unknown fields are ignored.
func ImportScanOptions(rt *sobek.Runtime, options sobek.Value) ScanOptions {
	scanOptions := ScanOptions{}

	if common.IsNullish(options) {
		return scanOptions
	}

	optionsObj := options.ToObject(rt)

	scanOptions.Prefix = optionsObj.Get("prefix").String()
	scanOptions.Cursor = optionsObj.Get("cursor").String()

	limitValue := optionsObj.Get("limit")
	if limitValue == nil {
		return scanOptions
	}

	var (
		parsedLimit int64
		err         = rt.ExportTo(limitValue, &parsedLimit)
	)
	if err == nil {
		scanOptions.Limit = parsedLimit
		scanOptions.isLimitSet = true
	}

	return scanOptions
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
//  1. Creates a Promise bound to the current VU,
//  2. Checks that the store is available,
//  3. Executes the given operation in a goroutine,
//  4. Converts the Go result to a Sobek value using k.vu.Runtime().ToValue(...),
//  5. Resolves/rejects the Promise.
func (k *KV) runAsyncWithStore(
	operation func(store store.Store) (any, error),
	toJS func(rt *sobek.Runtime, result any) sobek.Value,
) *sobek.Promise {
	promise, resolve, reject := promises.New(k.vu)

	go func() {
		if k.store == nil {
			reject(k.databaseNotOpenError())
			return
		}

		// Run the blocking store operation in this goroutine.
		goResult, err := operation(k.store)
		if err != nil {
			reject(err)
			return
		}

		// Convert to JS using the VU's Sobek runtime and resolve the promise.
		jsRuntime := k.vu.Runtime()
		jsValue := toJS(jsRuntime, goResult)

		resolve(jsValue)
	}()

	return promise
}

// exportToInt64 converts a Sobek value (we are on the caller's thread here) into int64
// WITHOUT using rt.ExportTo in worker goroutines. This accepts a few numeric shapes commonly
// produced by JS -> Go marshaling.
func exportToInt64(v sobek.Value) (int64, error) {
	switch x := v.Export().(type) {
	case int64:
		return x, nil
	case int32:
		return int64(x), nil
	case int:
		return int64(x), nil
	case float64:
		return int64(x), nil
	case float32:
		return int64(x), nil
	default:
		return 0, fmt.Errorf("unsupported numeric type: %T", x)
	}
}
