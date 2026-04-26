package kv

import (
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/grafana/sobek"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/js/modulestest"

	"github.com/oshokin/xk6-kv/kv/store"
)

func TestKVAsync_Set_ResolvesPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.set("async:key", "async-value")
			.then((value) => {
				if (value !== "async-value") {
					throw new Error("unexpected resolved value");
				}
			});
	`)
}

func TestKVAsync_GetMissingKey_RejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.get("missing")
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "KeyNotFoundError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)
}

func TestKVAsync_Get_PanicInStore_RejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	panicStore := panicGetStore{
		Store: store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
	}
	kv := NewKV(runtime.VU, panicStore)

	runKVScript(t, runtime, kv, `
		__kv.get("panic-key")
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "UnknownError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
				if (!String(err.message).includes("panic recovered during store operation")) {
					throw new Error("panic context missing");
				}
			});
	`)
}

func TestKVAsync_CallbackEnqueue_ExactlyOnce(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	originalRegisterCallback := runtime.VU.RegisterCallbackField

	var (
		registerCount atomic.Int64
		enqueueCount  atomic.Int64
	)

	runtime.VU.RegisterCallbackField = func() func(func() error) {
		registerCount.Add(1)

		enqueue := originalRegisterCallback()

		return func(f func() error) {
			enqueueCount.Add(1)
			enqueue(f)
		}
	}

	runKVScript(t, runtime, kv, `
		__kv.set("callback:key", "value")
			.then((value) => {
				if (value !== "value") {
					throw new Error("unexpected resolved value");
				}
			});
	`)

	assert.EqualValues(t, 1, registerCount.Load(), "RegisterCallback must be called exactly once")
	assert.EqualValues(t, 1, enqueueCount.Load(), "enqueue callback must be called exactly once")
}

func TestKVAsync_ClosedDiskStore_RejectsWithoutHang(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	diskStore, err := store.NewDiskStore(
		true,
		filepath.Join(t.TempDir(), "async-closed.db"),
		nil,
	)
	require.NoError(t, err)

	kv := NewKV(runtime.VU, diskStore)

	runKVScript(t, runtime, kv, `
		__kv.get("closed-key")
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "DiskStoreOpenError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)
}

func TestKV_Close_IdempotentPerHandle(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	countingStore := &closeCountingStore{
		Store: store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
	}
	kv := NewKV(runtime.VU, countingStore)

	require.NoError(t, kv.Close())
	require.NoError(t, kv.Close())
	require.NoError(t, kv.Close())

	assert.EqualValues(t, 1, countingStore.closeCalls.Load(), "underlying Store.Close must run once per KV handle")
}

func TestKVAsync_IncrementBy_InvalidDelta_RejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.incrementBy("counter", "NaN-value")
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "ValueNumberRequiredError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)
}

func TestKVAsync_Count_ResolvesPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		Promise.all([
			__kv.set("user:1", "a"),
			__kv.set("user:2", "b"),
			__kv.set("order:1", "c")
		])
			.then(() => __kv.count("user:"))
			.then((count) => {
				if (count !== 2) {
					throw new Error("unexpected count for user prefix");
				}
				return __kv.count();
			})
			.then((countAll) => {
				if (countAll !== 3) {
					throw new Error("unexpected total count");
				}
			});
	`)
}

func TestKVAsync_Set_Concurrent_NoRaceOrPanic(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	memoryStore := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	kv := NewKV(runtime.VU, memoryStore)

	const operations = 1_000

	promises := make([]*sobek.Promise, operations)

	err := runtime.EventLoop.Start(func() error {
		rt := runtime.VU.Runtime()

		for i := range operations {
			key := rt.ToValue(fmt.Sprintf("bulk:%04d", i))
			value := rt.ToValue(fmt.Sprintf("value-%04d", i))

			promises[i] = kv.Set(key, value)
		}

		return nil
	})
	runtime.EventLoop.WaitOnRegistered()

	require.NoError(t, err)

	for i, promise := range promises {
		require.NotNilf(t, promise, "promise at index %d must be initialized", i)
		assert.Equalf(t, sobek.PromiseStateFulfilled, promise.State(), "promise at index %d must resolve", i)
	}

	size, sizeErr := memoryStore.Size()
	require.NoError(t, sizeErr)
	assert.EqualValues(t, operations, size, "all concurrent writes must be persisted")
}

func runKVScript(t *testing.T, runtime *modulestest.Runtime, kv *KV, script string) {
	t.Helper()

	require.NoError(t, runtime.VU.Runtime().Set("__kv", kv))

	_, err := runtime.RunOnEventLoop(script)
	require.NoError(t, err)
}

type panicGetStore struct {
	store.Store
}

func (s panicGetStore) Get(_ string) (any, error) {
	panic("boom")
}

type closeCountingStore struct {
	store.Store
	closeCalls atomic.Int64
}

func (s *closeCountingStore) Close() error {
	s.closeCalls.Add(1)

	return s.Store.Close()
}
