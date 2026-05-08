package kv

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

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
//   - All blocking store work occurs in worker goroutines (off the VU event loop).
//   - Promise settle (resolve/reject) and Go->JS conversion (toJS/ToValue) run only
//     on the VU event loop through RegisterCallback callbacks.
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

	// stateMetrics emits reportStats() gauges into the k6 metrics pipeline.
	// It is populated by openKv() when k6 registry access is available.
	stateMetrics *kvStateMetrics

	// operationMetrics emits per-operation k6 custom metrics when enabled.
	operationMetrics *kvOperationMetrics

	// vu is the owning k6 VU that provides the Sobek runtime and event loop.
	vu modules.VU

	// closeOnce ensures per-handle close is idempotent.
	// This prevents accidental repeated Close() calls from decrementing
	// shared backend refcounts multiple times.
	closeOnce sync.Once
	// closed marks this JavaScript-facing handle as closed.
	// Once set, all async operations reject with StoreClosedError, regardless
	// of backend-specific close behavior.
	closed atomic.Bool
	// closeErr captures the first close result and is returned on subsequent calls.
	closeErr error
}

// asyncStoreObserver is a function that observes the result of an asynchronous store operation.
// It is used to emit metrics for the operation.
type asyncStoreObserver func(result any, err error, duration time.Duration)

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

// storeHandleClosedError produces a consistent error when this KV handle has been closed.
func (k *KV) storeHandleClosedError() *Error {
	return NewError(StoreClosedError, "kv store handle is closed")
}

func (k *KV) preflightStoreError() *Error {
	if k.store == nil {
		return k.databaseNotOpenError()
	}

	if k.closed.Load() {
		return k.storeHandleClosedError()
	}

	return nil
}

// rejectedPromise creates an already-rejected Promise on the VU event loop.
func (k *KV) rejectedPromise(err error) *sobek.Promise {
	rt := k.vu.Runtime()
	promise, _, reject := rt.NewPromise()
	callback := k.vu.RegisterCallback()

	kvErr := classifyError(err)

	callback(func() error {
		return reject(kvErr.ToSobekValue(rt))
	})

	return promise
}

// rejectedPromiseObserved emits failed operation metrics and returns a rejected promise.
func (k *KV) rejectedPromiseObserved(op string, err error) *sobek.Promise {
	if k.operationMetrics != nil && err != nil {
		classified := classifyError(err)

		errorType := string(UnknownError)
		if classified != nil {
			errorType = string(classified.Name)
		}

		k.operationMetrics.emit(k.vu.Context(), k.vu.State(), kvOperationSample{
			operation: op,
			failed:    true,
			errorType: errorType,
		})
	}

	return k.rejectedPromise(err)
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
	return k.runAsyncWithStoreAndObserver(operation, toJS, nil)
}

func (k *KV) runAsyncWithStoreAndObserver(
	operation func(store store.Store) (any, error),
	toJS func(rt *sobek.Runtime, result any) sobek.Value,
	observer asyncStoreObserver,
) *sobek.Promise {
	// Capture the VU runtime and create a promise whose resolve/reject
	// must run on the event loop thread (Sobek promises are not goroutine-safe).
	rt := k.vu.Runtime()
	promise, resolve, reject := rt.NewPromise()

	// Grab the VU's RegisterCallback hook so we can enqueue work back onto
	// the event loop after the store operation completes.
	var (
		callback   = k.vu.RegisterCallback()
		settleOnce sync.Once
	)

	runOnEventLoop := func(fn func() error) {
		settleOnce.Do(func() {
			callback(fn)
		})
	}

	finishAsync := k.beginAsyncOperation()

	go func() {
		defer finishAsync()

		startedAt := time.Now()
		observe := func(result any, err error) {
			if observer != nil {
				observer(result, err, time.Since(startedAt))
			}
		}

		defer func() {
			if r := recover(); r != nil {
				panicErr := fmt.Errorf("panic recovered during store operation: %v", r)
				observe(nil, panicErr)

				runOnEventLoop(func() error {
					return reject(classifyError(panicErr).ToSobekValue(rt))
				})
			}
		}()

		if err := k.preflightStoreError(); err != nil {
			observe(nil, err)

			runOnEventLoop(func() error {
				return reject(err.ToSobekValue(rt))
			})

			return
		}

		// Run the blocking storage call on a worker goroutine so the event
		// loop remains responsive under contention.
		goResult, err := operation(k.store)
		if err != nil {
			observe(nil, err)

			runOnEventLoop(func() error {
				return reject(classifyError(err).ToSobekValue(rt))
			})

			return
		}

		observe(goResult, nil)

		// Marshal the result back to JS by enqueueing a callback that converts
		// the Go value and resolves the promise on the event loop thread.
		runOnEventLoop(func() error {
			jsValue := toJS(rt, goResult)

			return resolve(jsValue)
		})
	}()

	return promise
}

func (k *KV) beginAsyncOperation() func() {
	if k.operationMetrics == nil {
		return func() {}
	}

	ctx := k.vu.Context()
	state := k.vu.State()

	k.operationMetrics.addAsyncInFlight(ctx, state, 1)

	return func() {
		k.operationMetrics.addAsyncInFlight(ctx, state, -1)
	}
}

// runAsyncWithStoreObserved wraps runAsyncWithStore with per-operation metric emission.
func (k *KV) runAsyncWithStoreObserved(
	op string,
	operation func(store store.Store) (any, error),
	toJS func(rt *sobek.Runtime, result any) sobek.Value,
) *sobek.Promise {
	if k.operationMetrics == nil {
		return k.runAsyncWithStore(operation, toJS)
	}

	ctx := k.vu.Context()
	state := k.vu.State()

	return k.runAsyncWithStoreAndObserver(
		operation,
		toJS,
		func(result any, err error, duration time.Duration) {
			errorType := ""
			if err != nil {
				errorType = string(classifyError(err).Name)
			}

			k.operationMetrics.emit(ctx, state, kvOperationSample{
				operation:   op,
				duration:    duration,
				failed:      err != nil,
				errorType:   errorType,
				emptyResult: isEmptyAllocationResult(op, result),
			})
		},
	)
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

		if math.Trunc(x) != x {
			return 0, fmt.Errorf("number must be an integer: %v", x)
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

		if math.Trunc(float64(x)) != float64(x) {
			return 0, fmt.Errorf("number must be an integer: %v", x)
		}

		return int64(x), nil
	default:
		return 0, fmt.Errorf("unsupported numeric type: %T", x)
	}
}
