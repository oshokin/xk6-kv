package kv

import (
	"fmt"
	"math"

	"github.com/grafana/sobek"
	"go.k6.io/k6/js/modules"

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

// databaseNotOpenError produces a consistent error when the backing store is nil.
func (k *KV) databaseNotOpenError() *Error {
	return NewError(DatabaseNotOpenError, "database is not open")
}

// runAsyncWithStore executes a blocking store operation on a worker goroutine
// and bridges its result back to JavaScript by resolving a Sobek promise on the
// VU's event loop. This indirection is required because:
//   - Sobek promises (rt.NewPromise) are not goroutine-safe; resolve/reject must
//     always run on the event loop thread.
//   - k6 extensions are responsible for scheduling their callbacks via
//     VU.RegisterCallback(), otherwise multiple goroutines will race inside the
//     VM and panic.
//   - We still want the expensive store work off the event loop so VUs stay
//     responsive under heavy contention.
func (k *KV) runAsyncWithStore(
	operation func(store store.Store) (any, error),
	toJS func(rt *sobek.Runtime, result any) sobek.Value,
) *sobek.Promise {
	// Capture the VU runtime and create a promise whose resolve/reject
	// must run on the event loop thread (Sobek promises are not goroutine-safe).
	rt := k.vu.Runtime()
	promise, resolve, reject := rt.NewPromise()

	// Grab the VU's RegisterCallback hook so we can enqueue work back onto
	// the event loop after the store operation completes.
	callback := k.vu.RegisterCallback()

	runOnEventLoop := func(fn func() error) {
		callback(fn)
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				panicErr := fmt.Errorf("panic recovered during store operation: %v", r)

				runOnEventLoop(func() error {
					return reject(classifyError(panicErr).ToSobekValue(rt))
				})
			}
		}()

		if k.store == nil {
			runOnEventLoop(func() error {
				return reject(k.databaseNotOpenError().ToSobekValue(rt))
			})

			return
		}

		// Run the blocking storage call on a worker goroutine so the event
		// loop remains responsive under contention.
		goResult, err := operation(k.store)
		if err != nil {
			runOnEventLoop(func() error {
				return reject(classifyError(err).ToSobekValue(rt))
			})

			return
		}

		// Marshal the result back to JS by enqueueing a callback that converts
		// the Go value and resolves the promise on the event loop thread.
		runOnEventLoop(func() error {
			jsValue := toJS(rt, goResult)

			return resolve(jsValue)
		})
	}()

	return promise
}

// exportToInt64 converts a Sobek value (we are on the caller's thread here)
// into int64 WITHOUT using rt.ExportTo in worker goroutines.
// This accepts a few numeric shapes commonly produced by JS -> Go marshaling.
func exportToInt64(v sobek.Value) (int64, error) {
	const (
		// maxInt64Float is the maximum float64 value that can be converted to int64 without overflow.
		maxInt64Float = float64(int64(^uint64(0) >> 1))
		// minInt64Float is the minimum float64 value that can be converted to int64 without overflow.
		minInt64Float = -maxInt64Float - 1
	)

	switch x := v.Export().(type) {
	case int64:
		return x, nil
	case int32:
		return int64(x), nil
	case int:
		return int64(x), nil
	case float64:
		// Check for non-finite numbers and out of int64 range.
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return 0, fmt.Errorf("non-finite number: %v", x)
		}

		// Check for out of int64 range.
		if x > maxInt64Float || x < minInt64Float {
			return 0, fmt.Errorf("number out of int64 range: %v", x)
		}

		return int64(x), nil
	case float32:
		// Check for non-finite numbers and out of int64 range.
		if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
			return 0, fmt.Errorf("non-finite number: %v", x)
		}

		// Check for out of int64 range.
		// Note: float32 is converted to float64 to avoid overflow.
		if float64(x) > maxInt64Float || float64(x) < minInt64Float {
			return 0, fmt.Errorf("number out of int64 range: %v", x)
		}

		return int64(x), nil
	default:
		return 0, fmt.Errorf("unsupported numeric type: %T", x)
	}
}
