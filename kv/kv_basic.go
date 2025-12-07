package kv

import (
	"fmt"

	"github.com/grafana/sobek"

	"github.com/oshokin/xk6-kv/kv/store"
)

type (
	// getOrSetResult is the JS-facing result of getOrSet().
	getOrSetResult struct {
		Value  any  `js:"value"`
		Loaded bool `js:"loaded"`
	}

	// swapResult is the JS-facing result of swap().
	swapResult struct {
		Previous any  `js:"previous"`
		Loaded   bool `js:"loaded"`
	}
)

// Get returns a Promise that resolves to the value stored under the provided key.
//
// Rejection cases:
//   - The database is not open.
//   - The key does not exist (error is forwarded from the store).
func (k *KV) Get(key sobek.Value) *sobek.Promise {
	keyString := key.String()

	return k.runAsyncWithStore(
		func(s store.Store) (any, error) {
			return s.Get(keyString)
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
		func(s store.Store) (any, error) {
			return value, s.Set(keyString, exportedValue)
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
		// We are still on the VU's thread when exportToInt64() rejects the delta,
		// so we have to hand craft a promise and enqueue its rejection back onto the event loop.
		rt := k.vu.Runtime()
		promise, _, reject := rt.NewPromise()
		callback := k.vu.RegisterCallback()

		kvErr := NewError(ValueNumberRequiredError, fmt.Sprintf("delta must be a number: %v", err))

		callback(func() error {
			return reject(kvErr.ToSobekValue(rt))
		})

		return promise
	}

	return k.runAsyncWithStore(
		func(s store.Store) (any, error) {
			return s.IncrementBy(keyString, deltaInt)
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
		func(s store.Store) (any, error) {
			actualValue, wasLoaded, err := s.GetOrSet(keyString, exportedValue)
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
		func(s store.Store) (any, error) {
			previousValue, loaded, err := s.Swap(keyString, exportedValue)
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
