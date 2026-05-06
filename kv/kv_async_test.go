package kv

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/grafana/sobek"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/js/modulestest"
	"go.k6.io/k6/lib"
	k6metrics "go.k6.io/k6/metrics"

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

func TestKVAsync_SetMany_ResolvesWrittenCount(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	runKVScript(t, runtime, kv, `
		__kv.setMany({
			"user:1": { name: "Alice" },
			"user:2": { name: "Bob" },
		})
			.then((result) => {
				if (!result || result.written !== 2) {
					throw new Error("unexpected written count");
				}
				return Promise.all([
					__kv.get("user:1"),
					__kv.get("user:2"),
				]);
			})
			.then((values) => {
				if (values[0].name !== "Alice" || values[1].name !== "Bob") {
					throw new Error("unexpected values after setMany");
				}
			});
	`)
}

func TestKVAsync_SetMany_EmptyObjectResolvesZero(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.setMany({})
			.then((result) => {
				if (!result || result.written !== 0) {
					throw new Error("expected written=0");
				}
			});
	`)
}

func TestKVAsync_SetMany_InvalidShapeRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.setMany([])
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
				if (!Array.isArray(err.errors) || err.errors.length === 0) {
					throw new Error("expected errors array");
				}
				if (err.errors[0].name !== "InvalidEntries") {
					throw new Error("unexpected entry error");
				}
			});
	`)
}

func TestKVAsync_Set_EmptyKeyRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.set("", "value")
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)
}

func TestKVAsync_SetMany_EmptyKeyRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	runKVScript(t, runtime, kv, `
		__kv.setMany({ "": "batch-value" })
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
				if (!Array.isArray(err.errors) || err.errors.length !== 1) {
					throw new Error("expected single setMany entry error");
				}
				if (err.errors[0].name !== "EmptyKey") {
					throw new Error("unexpected setMany entry error name");
				}
			});
	`)
}

func TestKVAsync_SetMany_SerializationFailureRejectsAndWritesNothing(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	runKVScript(t, runtime, kv, `
		__kv.setMany({
			"ok": { name: "Alice" },
			"bad": () => {},
		})
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "SerializerError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
				if (!Array.isArray(err.errors) || err.errors.length !== 1) {
					throw new Error("expected single entry serialization error");
				}
				if (err.errors[0].name !== "SerializerError") {
					throw new Error("unexpected entry error name");
				}
			})
			.then(() => __kv.exists("ok"))
			.then((exists) => {
				if (exists !== false) {
					throw new Error("setMany must be all-or-nothing");
				}
			});
	`)
}

func TestKVAsync_GetMany_ResolvesItemsAndExistsFlags(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	runKVScript(t, runtime, kv, `
		__kv.setMany({
			"user:1": { name: "Alice" },
			"user:2": { name: "Bob" },
		})
			.then(() => __kv.getMany(["user:2", "missing", "user:1"]))
			.then((items) => {
				if (!Array.isArray(items) || items.length !== 3) {
					throw new Error("unexpected getMany result shape");
				}
				if (items[0].key !== "user:2" || items[0].exists !== true || items[0].value.name !== "Bob") {
					throw new Error("unexpected first value");
				}
				if (items[1].key !== "missing" || items[1].exists !== false || items[1].value !== null) {
					throw new Error("missing key must have exists=false and value=null");
				}
				if (items[2].key !== "user:1" || items[2].exists !== true || items[2].value.name !== "Alice") {
					throw new Error("unexpected third value");
				}
			});
	`)
}

func TestKVAsync_GetMany_EmptyArrayResolvesEmptyArray(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.getMany([])
			.then((items) => {
				if (!Array.isArray(items) || items.length !== 0) {
					throw new Error("expected empty array");
				}
			});
	`)
}

func TestKVAsync_GetMany_InvalidShapeRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.getMany({})
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)
}

func TestKVAsync_GetMany_InvalidElementRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.getMany(["ok", 123])
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
				if (!String(err.message).includes("keys[1]")) {
					throw new Error("error message must mention invalid index");
				}
			});
	`)
}

func TestKVAsync_GetMany_DuplicateKeys(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	runKVScript(t, runtime, kv, `
		__kv.set("user:1", { name: "Alice" })
			.then(() => __kv.getMany(["user:1", "user:1"]))
			.then((items) => {
				if (items.length !== 2) {
					throw new Error("expected two results");
				}
				if (
					items[0].key !== "user:1" ||
					items[1].key !== "user:1" ||
					items[0].exists !== true ||
					items[1].exists !== true ||
					items[0].value.name !== "Alice" ||
					items[1].value.name !== "Alice"
				) {
					throw new Error("duplicate keys must return duplicate values");
				}
			});
	`)
}

func TestKVAsync_GetMany_DistinguishesMissingAndStoredJSONNull(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	runKVScript(t, runtime, kv, `
		__kv.setMany({
			"json:null": null,
		})
			.then(() => __kv.getMany(["missing", "json:null"]))
			.then((items) => {
				if (items[0].key !== "missing" || items[0].exists !== false || items[0].value !== null) {
					throw new Error("missing key must have exists=false and value=null");
				}
				if (items[1].key !== "json:null" || items[1].exists !== true || items[1].value !== null) {
					throw new Error("stored JSON null must have exists=true and value=null");
				}
			});
	`)
}

func TestKVAsync_DeleteMany_ResolvesDeletedAndMissingCounts(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	runKVScript(t, runtime, kv, `
		__kv.setMany({
			"user:1": { name: "Alice" },
			"user:2": { name: "Bob" },
		})
			.then(() => __kv.deleteMany(["user:1", "user:2", "missing"]))
			.then((result) => {
				if (!result || result.deleted !== 2 || result.missing !== 1) {
					throw new Error("unexpected deleteMany result");
				}
			});
	`)
}

func TestKVAsync_DeleteMany_RemovesKeys(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	runKVScript(t, runtime, kv, `
		__kv.setMany({
			"user:1": { name: "Alice" },
			"user:2": { name: "Bob" },
		})
			.then(() => __kv.deleteMany(["user:1", "user:2"]))
			.then(() => __kv.getMany(["user:1", "user:2"]))
			.then((items) => {
				if (!Array.isArray(items) || items.length !== 2) {
					throw new Error("unexpected getMany shape after deleteMany");
				}
				if (items[0].exists !== false || items[1].exists !== false) {
					throw new Error("deleteMany must remove requested keys");
				}
			});
	`)
}

func TestKVAsync_DeleteMany_EmptyArrayResolvesZeroCounts(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.deleteMany([])
			.then((result) => {
				if (!result || result.deleted !== 0 || result.missing !== 0) {
					throw new Error("expected {deleted:0, missing:0}");
				}
			});
	`)
}

func TestKVAsync_DeleteMany_DuplicateKeys(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	runKVScript(t, runtime, kv, `
		__kv.set("user:1", { name: "Alice" })
			.then(() => __kv.deleteMany(["user:1", "user:1"]))
			.then((result) => {
				if (!result || result.deleted !== 1 || result.missing !== 1) {
					throw new Error("duplicate deleteMany semantics broken");
				}
			});
	`)
}

func TestKVAsync_DeleteMany_InvalidShapeRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.deleteMany({})
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
				if (!String(err.message).includes("deleteMany")) {
					throw new Error("message must mention deleteMany");
				}
			});
	`)
}

func TestKVAsync_DeleteMany_InvalidElementRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.deleteMany(["ok", 123])
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
				if (!String(err.message).includes("keys[1]")) {
					throw new Error("error message must mention invalid index");
				}
			});
	`)
}

func TestKVAsync_DeleteMany_EmptyKeyRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.deleteMany([""])
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
				if (!String(err.message).includes("non-empty")) {
					throw new Error("error message must mention non-empty key");
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
			.then(() => __kv.count({ prefix: "user:" }))
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

func TestKVAsync_Count_InvalidOptions_RejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.count("user:")
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)
}

func TestKVAsync_ListKeys_ResolvesOrderedKeys(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	runKVScript(t, runtime, kv, `
		__kv.setMany({
			"user:2": { name: "Bob" },
			"order:1": { id: 1 },
			"user:1": { name: "Alice" },
		})
			.then(() => __kv.listKeys())
			.then((keys) => {
				if (!Array.isArray(keys)) {
					throw new Error("listKeys must resolve array");
				}
				const expected = ["order:1", "user:1", "user:2"];
				if (JSON.stringify(keys) !== JSON.stringify(expected)) {
					throw new Error("unexpected keys: " + JSON.stringify(keys));
				}
			});
	`)
}

func TestKVAsync_ListKeys_PrefixAndLimit(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	runKVScript(t, runtime, kv, `
		__kv.setMany({
			"user:3": 3,
			"user:1": 1,
			"user:2": 2,
			"order:1": 1,
		})
			.then(() => __kv.listKeys({ prefix: "user:", limit: 2 }))
			.then((keys) => {
				const expected = ["user:1", "user:2"];
				if (JSON.stringify(keys) !== JSON.stringify(expected)) {
					throw new Error("unexpected keys: " + JSON.stringify(keys));
				}
			});
	`)
}

func TestKVAsync_ListKeys_EmptyStore(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.listKeys()
			.then((keys) => {
				if (!Array.isArray(keys) || keys.length !== 0) {
					throw new Error("expected empty keys array");
				}
			});
	`)
}

func TestKVAsync_ListKeys_InvalidShapeRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.listKeys([])
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)
}

func TestKVAsync_ListKeys_InvalidPrefixRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.listKeys({ prefix: 123 })
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)
}

func TestKVAsync_ListKeys_FractionalLimitRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.listKeys({ limit: 1.5 })
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)
}

func TestKVAsync_DeleteByPrefix_DeletesLimitedBatchAndReportsNotDone(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	runKVScript(t, runtime, kv, `
		__kv.setMany({
			"tmp:1": { value: 1 },
			"tmp:2": { value: 2 },
			"tmp:3": { value: 3 },
			"user:1": { value: 4 },
		})
			.then(() => __kv.deleteByPrefix({ prefix: "tmp:", limit: 2 }))
			.then((result) => {
				if (!result || result.deleted !== 2 || result.done !== false) {
					throw new Error("unexpected deleteByPrefix result");
				}
				return Promise.all([
					__kv.listKeys({ prefix: "tmp:" }),
					__kv.listKeys({ prefix: "user:" }),
				]);
			})
			.then(([tmpKeys, userKeys]) => {
				if (!Array.isArray(tmpKeys) || tmpKeys.length !== 1) {
					throw new Error("expected one tmp key after limited delete");
				}
				if (JSON.stringify(userKeys) !== JSON.stringify(["user:1"])) {
					throw new Error("non-target prefix must stay intact");
				}
			});
	`)
}

func TestKVAsync_DeleteByPrefix_RepeatedCallsReportDone(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	runKVScript(t, runtime, kv, `
		__kv.setMany({
			"tmp:1": { value: 1 },
			"tmp:2": { value: 2 },
			"tmp:3": { value: 3 },
		})
			.then(() => __kv.deleteByPrefix({ prefix: "tmp:", limit: 2 }))
			.then((first) => {
				if (!first || first.deleted !== 2 || first.done !== false) {
					throw new Error("unexpected first result");
				}
				return __kv.deleteByPrefix({ prefix: "tmp:", limit: 2 });
			})
			.then((second) => {
				if (!second || second.deleted !== 1 || second.done !== true) {
					throw new Error("unexpected second result");
				}
				return __kv.listKeys({ prefix: "tmp:" });
			})
			.then((keys) => {
				if (!Array.isArray(keys) || keys.length !== 0) {
					throw new Error("tmp keys should be fully removed");
				}
			});
	`)
}

func TestKVAsync_DeleteByPrefix_InvalidShapeRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.deleteByPrefix([])
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)
}

func TestKVAsync_DeleteByPrefix_EmptyPrefixRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.deleteByPrefix({ prefix: "", limit: 1 })
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
				if (!String(err.message).includes("non-empty")) {
					throw new Error("error message must mention non-empty prefix");
				}
			});
	`)
}

func TestKVAsync_DeleteByPrefix_MissingLimitRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.deleteByPrefix({ prefix: "tmp:" })
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)
}

func TestKVAsync_DeleteByPrefix_InvalidLimitRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.deleteByPrefix({ prefix: "tmp:", limit: 0 })
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)
}

func TestKVAsync_DeleteByPrefix_FractionalLimitRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.deleteByPrefix({ prefix: "tmp:", limit: 1.5 })
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)
}

func TestKVAsync_Stats_ResolvesSnapshot(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	runKVScript(t, runtime, kv, `
		Promise.all([
			__kv.set("user:1", { name: "Alice" }),
			__kv.set("user:2", { name: "Bob" })
		])
			.then(() => __kv.stats())
			.then((snapshot) => {
				if (!snapshot) {
					throw new Error("missing stats snapshot");
				}
				if (snapshot.backend !== "memory") {
					throw new Error("unexpected backend");
				}
				if (snapshot.serialization !== "json") {
					throw new Error("unexpected serialization");
				}
				if (snapshot.trackKeys !== true) {
					throw new Error("unexpected trackKeys");
				}
				if (snapshot.count !== 2) {
					throw new Error("unexpected key count");
				}
				if (!snapshot.claims || typeof snapshot.claims.live !== "number" || typeof snapshot.claims.expired !== "number") {
					throw new Error("invalid claim stats");
				}
				if (!snapshot.index || snapshot.index.enabled !== true || snapshot.index.consistent !== true) {
					throw new Error("invalid index stats: " + JSON.stringify(snapshot.index));
				}
			});
	`)
}

func TestKVAsync_Stats_NullsUnavailableOptionalFields(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: false}),
			store.NewJSONSerializer(),
		),
	)

	runKVScript(t, runtime, kv, `
		__kv.set("user:1", { name: "Alice" })
			.then(() => __kv.stats())
			.then((snapshot) => {
				if (!snapshot) {
					throw new Error("missing stats snapshot");
				}
				if (snapshot.index !== null) {
					throw new Error("index must be null when trackKeys=false");
				}
				if (snapshot.disk !== null) {
					throw new Error("disk must be null for memory backend");
				}
			});
	`)
}

func TestKVAsync_ReportStats_ResolvesAndEmitsMetrics(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	registry := runtime.VU.InitEnv().Registry
	rootTags := registry.RootTagSet()

	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	var err error

	kv.stateMetrics, err = newKVStateMetrics(registry)
	require.NoError(t, err)

	samples := make(chan k6metrics.SampleContainer, 32)
	runtime.MoveToVUContext(&lib.State{
		BuiltinMetrics: runtime.BuiltinMetrics,
		Samples:        samples,
		Tags:           lib.NewVUStateTags(rootTags),
	})

	runKVScript(t, runtime, kv, `
		Promise.all([
			__kv.set("user:1", { name: "Alice" }),
			__kv.set("user:2", { name: "Bob" })
		])
			.then(() => __kv.reportStats())
			.then((result) => {
				if (result !== undefined) {
					throw new Error("reportStats must resolve undefined");
				}
			});
	`)

	metricHits := make(map[string]int)

	for _, container := range k6metrics.GetBufferedSamples(samples) {
		for _, sample := range container.GetSamples() {
			metricHits[sample.Metric.Name]++
		}
	}

	assert.Equal(t, 1, metricHits[metricKVKeys])
	assert.Equal(t, 1, metricHits[metricKVClaimsLive])
	assert.Equal(t, 1, metricHits[metricKVClaimsExpired])
	assert.Equal(t, 3, metricHits[metricKVIndexKeys])
	assert.Equal(t, 1, metricHits[metricKVIndexConsistent])
	assert.Zero(t, metricHits[metricKVDiskSizeBytes])
}

func TestKVAsync_ReportStats_MetricsUnavailable_RejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	runKVScript(t, runtime, kv, `
		__kv.reportStats()
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "MetricsUnavailableError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)
}

func TestKVAsync_OperationMetrics_EmitsExpectedSamples(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	registry := runtime.VU.InitEnv().Registry
	rootTags := registry.RootTagSet()

	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	var err error

	kv.operationMetrics, err = newKVOperationMetrics(registry, Options{
		Backend:       BackendMemory,
		Serialization: SerializationJSON,
		TrackKeys:     true,
		Metrics:       &MetricsOptions{Operations: true},
	})
	require.NoError(t, err)

	samples := make(chan k6metrics.SampleContainer, 64)
	runtime.MoveToVUContext(&lib.State{
		BuiltinMetrics: runtime.BuiltinMetrics,
		Samples:        samples,
		Tags:           lib.NewVUStateTags(rootTags),
	})

	runKVScript(t, runtime, kv, `
		__kv.set("user:1", { name: "Alice" })
			.then(() => __kv.get("user:1"))
			.then(() => __kv.randomKey({ prefix: "missing:" }))
			.then((key) => {
				if (key !== "") {
					throw new Error("expected empty random key");
				}
				return __kv.get("missing")
					.catch((err) => {
						if (!err || err.name !== "KeyNotFoundError") {
							throw new Error("unexpected error class: " + String(err && err.name));
						}
					});
			});
	`)

	metricHits := make(map[string]int)

	var (
		failedNonZero int
		failedZero    int
		emptyNonZero  int
		emptyZero     int
	)

	for _, container := range k6metrics.GetBufferedSamples(samples) {
		for _, sample := range container.GetSamples() {
			metricHits[sample.Metric.Name]++

			switch sample.Metric.Name {
			case metricKVOperationFailed:
				if sample.Value > 0 {
					failedNonZero++
				} else {
					failedZero++
				}
			case metricKVEmptyResult:
				if sample.Value > 0 {
					emptyNonZero++
				} else {
					emptyZero++
				}
			}
		}
	}

	assert.Equal(t, 4, metricHits[metricKVOperationsTotal])
	assert.Equal(t, 4, metricHits[metricKVOperationDuration])
	assert.Equal(t, 4, metricHits[metricKVOperationFailed])
	assert.Equal(t, 1, metricHits[metricKVErrorsTotal])
	assert.Equal(t, 1, metricHits[metricKVEmptyResult])

	assert.Equal(t, 1, failedNonZero, "one operation should fail")
	assert.Equal(t, 3, failedZero, "three operations should succeed")
	assert.Equal(t, 1, emptyNonZero, "one allocation op should return empty")
	assert.Zero(t, emptyZero, "empty-result metric should be emitted only for allocation operations")
}

func TestKVAsync_OperationMetrics_EmitsValidationError(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	registry := runtime.VU.InitEnv().Registry
	rootTags := registry.RootTagSet()

	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	var err error

	kv.operationMetrics, err = newKVOperationMetrics(registry, Options{
		Backend:       BackendMemory,
		Serialization: SerializationJSON,
		TrackKeys:     true,
		Metrics:       &MetricsOptions{Operations: true},
	})
	require.NoError(t, err)

	samples := make(chan k6metrics.SampleContainer, 32)
	runtime.MoveToVUContext(&lib.State{
		BuiltinMetrics: runtime.BuiltinMetrics,
		Samples:        samples,
		Tags:           lib.NewVUStateTags(rootTags),
	})

	runKVScript(t, runtime, kv, `
		__kv.count("bad")
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)

	metricHits := make(map[string]int)

	var (
		seenOperationFailed bool
		seenOperationTotal  bool
		seenDuration        bool
		seenErrorsTotal     bool
	)

	for _, container := range k6metrics.GetBufferedSamples(samples) {
		for _, sample := range container.GetSamples() {
			metricHits[sample.Metric.Name]++

			switch sample.Metric.Name {
			case metricKVOperationFailed:
				assert.InDelta(t, 1.0, sample.Value, 1e-9)
				op, ok := sample.Tags.Get(tagOp)
				require.True(t, ok)
				assert.Equal(t, opCount, op)

				seenOperationFailed = true
			case metricKVOperationsTotal:
				assert.InDelta(t, 1.0, sample.Value, 1e-9)
				op, ok := sample.Tags.Get(tagOp)
				require.True(t, ok)
				assert.Equal(t, opCount, op)

				status, ok := sample.Tags.Get(tagStatus)
				require.True(t, ok)
				assert.Equal(t, statusError, status)

				seenOperationTotal = true
			case metricKVOperationDuration:
				op, ok := sample.Tags.Get(tagOp)
				require.True(t, ok)
				assert.Equal(t, opCount, op)

				status, ok := sample.Tags.Get(tagStatus)
				require.True(t, ok)
				assert.Equal(t, statusError, status)

				seenDuration = true
			case metricKVErrorsTotal:
				assert.InDelta(t, 1.0, sample.Value, 1e-9)
				op, ok := sample.Tags.Get(tagOp)
				require.True(t, ok)
				assert.Equal(t, opCount, op)

				errorType, ok := sample.Tags.Get(tagErrorType)
				require.True(t, ok)
				assert.Equal(t, string(InvalidOptionsError), errorType)

				seenErrorsTotal = true
			}
		}
	}

	assert.Equal(t, 1, metricHits[metricKVOperationsTotal])
	assert.Equal(t, 1, metricHits[metricKVOperationDuration])
	assert.Equal(t, 1, metricHits[metricKVOperationFailed])
	assert.Equal(t, 1, metricHits[metricKVErrorsTotal])
	assert.Zero(t, metricHits[metricKVEmptyResult])

	assert.True(t, seenOperationFailed)
	assert.True(t, seenOperationTotal)
	assert.True(t, seenDuration)
	assert.True(t, seenErrorsTotal)
}

func TestKVAsync_SetMany_OperationMetrics(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	registry := runtime.VU.InitEnv().Registry
	rootTags := registry.RootTagSet()

	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	var err error

	kv.operationMetrics, err = newKVOperationMetrics(registry, Options{
		Backend:       BackendMemory,
		Serialization: SerializationJSON,
		TrackKeys:     true,
		Metrics:       &MetricsOptions{Operations: true},
	})
	require.NoError(t, err)

	samples := make(chan k6metrics.SampleContainer, 32)
	runtime.MoveToVUContext(&lib.State{
		BuiltinMetrics: runtime.BuiltinMetrics,
		Samples:        samples,
		Tags:           lib.NewVUStateTags(rootTags),
	})

	runKVScript(t, runtime, kv, `
		__kv.setMany({
			"user:1": { name: "Alice" },
			"user:2": { name: "Bob" },
		})
			.then((result) => {
				if (!result || result.written !== 2) {
					throw new Error("unexpected written count");
				}
			});
	`)

	var (
		seenTotal    bool
		seenDuration bool
		seenFailed   bool
	)

	for _, container := range k6metrics.GetBufferedSamples(samples) {
		for _, sample := range container.GetSamples() {
			op, hasOp := sample.Tags.Get(tagOp)
			if !hasOp || op != opSetMany {
				continue
			}

			switch sample.Metric.Name {
			case metricKVOperationsTotal:
				status, ok := sample.Tags.Get(tagStatus)
				require.True(t, ok)
				assert.Equal(t, statusOK, status)

				seenTotal = true
			case metricKVOperationDuration:
				status, ok := sample.Tags.Get(tagStatus)
				require.True(t, ok)
				assert.Equal(t, statusOK, status)

				seenDuration = true
			case metricKVOperationFailed:
				assert.InDelta(t, 0.0, sample.Value, 1e-9)

				seenFailed = true
			}
		}
	}

	assert.True(t, seenTotal)
	assert.True(t, seenDuration)
	assert.True(t, seenFailed)
}

func TestKVAsync_GetMany_OperationMetrics(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	registry := runtime.VU.InitEnv().Registry
	rootTags := registry.RootTagSet()

	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	var err error

	kv.operationMetrics, err = newKVOperationMetrics(registry, Options{
		Backend:       BackendMemory,
		Serialization: SerializationJSON,
		TrackKeys:     true,
		Metrics:       &MetricsOptions{Operations: true},
	})
	require.NoError(t, err)

	samples := make(chan k6metrics.SampleContainer, 32)
	runtime.MoveToVUContext(&lib.State{
		BuiltinMetrics: runtime.BuiltinMetrics,
		Samples:        samples,
		Tags:           lib.NewVUStateTags(rootTags),
	})

	runKVScript(t, runtime, kv, `
		__kv.setMany({
			"user:1": { name: "Alice" },
			"user:2": { name: "Bob" },
			"json:null": null,
		})
			.then(() => __kv.getMany(["user:1", "missing", "json:null"]))
			.then((items) => {
				if (!Array.isArray(items) || items.length !== 3) {
					throw new Error("unexpected getMany shape");
				}
				if (items[1].exists !== false || items[1].value !== null) {
					throw new Error("missing item must have exists=false");
				}
				if (items[2].exists !== true || items[2].value !== null) {
					throw new Error("stored null item must have exists=true");
				}
			});
	`)

	var (
		seenTotal    bool
		seenDuration bool
		seenFailed   bool
	)

	for _, container := range k6metrics.GetBufferedSamples(samples) {
		for _, sample := range container.GetSamples() {
			op, hasOp := sample.Tags.Get(tagOp)
			if !hasOp || op != opGetMany {
				continue
			}

			switch sample.Metric.Name {
			case metricKVOperationsTotal:
				status, ok := sample.Tags.Get(tagStatus)
				require.True(t, ok)
				assert.Equal(t, statusOK, status)

				seenTotal = true
			case metricKVOperationDuration:
				status, ok := sample.Tags.Get(tagStatus)
				require.True(t, ok)
				assert.Equal(t, statusOK, status)

				seenDuration = true
			case metricKVOperationFailed:
				assert.InDelta(t, 0.0, sample.Value, 1e-9)

				seenFailed = true
			}
		}
	}

	assert.True(t, seenTotal)
	assert.True(t, seenDuration)
	assert.True(t, seenFailed)
}

func TestKVAsync_DeleteMany_OperationMetrics(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	registry := runtime.VU.InitEnv().Registry
	rootTags := registry.RootTagSet()

	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	var err error

	kv.operationMetrics, err = newKVOperationMetrics(registry, Options{
		Backend:       BackendMemory,
		Serialization: SerializationJSON,
		TrackKeys:     true,
		Metrics:       &MetricsOptions{Operations: true},
	})
	require.NoError(t, err)

	samples := make(chan k6metrics.SampleContainer, 32)
	runtime.MoveToVUContext(&lib.State{
		BuiltinMetrics: runtime.BuiltinMetrics,
		Samples:        samples,
		Tags:           lib.NewVUStateTags(rootTags),
	})

	runKVScript(t, runtime, kv, `
		__kv.setMany({
			"user:1": { name: "Alice" },
			"user:2": { name: "Bob" },
		})
			.then(() => __kv.deleteMany(["user:1", "user:2", "missing"]))
			.then((result) => {
				if (!result || result.deleted !== 2 || result.missing !== 1) {
					throw new Error("unexpected deleteMany result");
				}
			});
	`)

	var (
		seenTotal    bool
		seenDuration bool
		seenFailed   bool
	)

	for _, container := range k6metrics.GetBufferedSamples(samples) {
		for _, sample := range container.GetSamples() {
			op, hasOp := sample.Tags.Get(tagOp)
			if !hasOp || op != opDeleteMany {
				continue
			}

			switch sample.Metric.Name {
			case metricKVOperationsTotal:
				status, ok := sample.Tags.Get(tagStatus)
				require.True(t, ok)
				assert.Equal(t, statusOK, status)

				seenTotal = true
			case metricKVOperationDuration:
				status, ok := sample.Tags.Get(tagStatus)
				require.True(t, ok)
				assert.Equal(t, statusOK, status)

				seenDuration = true
			case metricKVOperationFailed:
				assert.InDelta(t, 0.0, sample.Value, 1e-9)

				seenFailed = true
			}
		}
	}

	assert.True(t, seenTotal)
	assert.True(t, seenDuration)
	assert.True(t, seenFailed)
}

func TestKVAsync_DeleteByPrefix_OperationMetrics(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	registry := runtime.VU.InitEnv().Registry
	rootTags := registry.RootTagSet()

	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	var err error

	kv.operationMetrics, err = newKVOperationMetrics(registry, Options{
		Backend:       BackendMemory,
		Serialization: SerializationJSON,
		TrackKeys:     true,
		Metrics:       &MetricsOptions{Operations: true},
	})
	require.NoError(t, err)

	samples := make(chan k6metrics.SampleContainer, 32)
	runtime.MoveToVUContext(&lib.State{
		BuiltinMetrics: runtime.BuiltinMetrics,
		Samples:        samples,
		Tags:           lib.NewVUStateTags(rootTags),
	})

	runKVScript(t, runtime, kv, `
		__kv.setMany({
			"tmp:1": { value: 1 },
			"tmp:2": { value: 2 },
			"tmp:3": { value: 3 },
		})
			.then(() => __kv.deleteByPrefix({ prefix: "tmp:", limit: 2 }))
			.then((result) => {
				if (!result || result.deleted !== 2 || result.done !== false) {
					throw new Error("unexpected deleteByPrefix result");
				}
			});
	`)

	var (
		seenTotal    bool
		seenDuration bool
		seenFailed   bool
	)

	for _, container := range k6metrics.GetBufferedSamples(samples) {
		for _, sample := range container.GetSamples() {
			op, hasOp := sample.Tags.Get(tagOp)
			if !hasOp || op != opDeleteByPrefix {
				continue
			}

			switch sample.Metric.Name {
			case metricKVOperationsTotal:
				status, ok := sample.Tags.Get(tagStatus)
				require.True(t, ok)
				assert.Equal(t, statusOK, status)

				seenTotal = true
			case metricKVOperationDuration:
				status, ok := sample.Tags.Get(tagStatus)
				require.True(t, ok)
				assert.Equal(t, statusOK, status)

				seenDuration = true
			case metricKVOperationFailed:
				assert.InDelta(t, 0.0, sample.Value, 1e-9)

				seenFailed = true
			}
		}
	}

	assert.True(t, seenTotal)
	assert.True(t, seenDuration)
	assert.True(t, seenFailed)
}

func TestKVAsync_ListKeys_OperationMetrics(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	registry := runtime.VU.InitEnv().Registry
	rootTags := registry.RootTagSet()

	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	var err error

	kv.operationMetrics, err = newKVOperationMetrics(registry, Options{
		Backend:       BackendMemory,
		Serialization: SerializationJSON,
		TrackKeys:     true,
		Metrics:       &MetricsOptions{Operations: true},
	})
	require.NoError(t, err)

	samples := make(chan k6metrics.SampleContainer, 32)
	runtime.MoveToVUContext(&lib.State{
		BuiltinMetrics: runtime.BuiltinMetrics,
		Samples:        samples,
		Tags:           lib.NewVUStateTags(rootTags),
	})

	runKVScript(t, runtime, kv, `
		__kv.setMany({
			"user:1": { name: "Alice" },
			"user:2": { name: "Bob" },
		})
			.then(() => __kv.listKeys({ prefix: "user:" }))
			.then((keys) => {
				if (!Array.isArray(keys) || keys.length !== 2) {
					throw new Error("unexpected listKeys result");
				}
			});
	`)

	var (
		seenTotal    bool
		seenDuration bool
		seenFailed   bool
	)

	for _, container := range k6metrics.GetBufferedSamples(samples) {
		for _, sample := range container.GetSamples() {
			op, hasOp := sample.Tags.Get(tagOp)
			if !hasOp || op != opListKeys {
				continue
			}

			switch sample.Metric.Name {
			case metricKVOperationsTotal:
				status, ok := sample.Tags.Get(tagStatus)
				require.True(t, ok)
				assert.Equal(t, statusOK, status)

				seenTotal = true
			case metricKVOperationDuration:
				status, ok := sample.Tags.Get(tagStatus)
				require.True(t, ok)
				assert.Equal(t, statusOK, status)

				seenDuration = true
			case metricKVOperationFailed:
				assert.InDelta(t, 0.0, sample.Value, 1e-9)

				seenFailed = true
			}
		}
	}

	assert.True(t, seenTotal)
	assert.True(t, seenDuration)
	assert.True(t, seenFailed)
}

func TestKVAsync_DeleteByPrefix_InvalidOptionsMetrics(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	registry := runtime.VU.InitEnv().Registry
	rootTags := registry.RootTagSet()

	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	var err error

	kv.operationMetrics, err = newKVOperationMetrics(registry, Options{
		Backend:       BackendMemory,
		Serialization: SerializationJSON,
		TrackKeys:     true,
		Metrics:       &MetricsOptions{Operations: true},
	})
	require.NoError(t, err)

	samples := make(chan k6metrics.SampleContainer, 32)
	runtime.MoveToVUContext(&lib.State{
		BuiltinMetrics: runtime.BuiltinMetrics,
		Samples:        samples,
		Tags:           lib.NewVUStateTags(rootTags),
	})

	runKVScript(t, runtime, kv, `
		__kv.deleteByPrefix({ prefix: "", limit: 1 })
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)

	var (
		seenTotal      bool
		seenDuration   bool
		seenFailed     bool
		seenErrors     bool
		seenEmpty      bool
		seenFailedZero bool
	)

	for _, container := range k6metrics.GetBufferedSamples(samples) {
		for _, sample := range container.GetSamples() {
			op, ok := sample.Tags.Get(tagOp)
			if !ok || op != opDeleteByPrefix {
				continue
			}

			switch sample.Metric.Name {
			case metricKVOperationsTotal:
				status, hasStatus := sample.Tags.Get(tagStatus)
				require.True(t, hasStatus)
				assert.Equal(t, statusError, status)
				assert.InDelta(t, 1.0, sample.Value, 1e-9)

				seenTotal = true
			case metricKVOperationDuration:
				status, hasStatus := sample.Tags.Get(tagStatus)
				require.True(t, hasStatus)
				assert.Equal(t, statusError, status)

				seenDuration = true
			case metricKVOperationFailed:
				if sample.Value == 0 {
					seenFailedZero = true
				}

				assert.InDelta(t, 1.0, sample.Value, 1e-9)

				seenFailed = true
			case metricKVErrorsTotal:
				errorType, hasErrorType := sample.Tags.Get(tagErrorType)
				require.True(t, hasErrorType)
				assert.Equal(t, string(InvalidOptionsError), errorType)
				assert.InDelta(t, 1.0, sample.Value, 1e-9)

				seenErrors = true
			case metricKVEmptyResult:
				seenEmpty = true
			}
		}
	}

	assert.True(t, seenTotal)
	assert.True(t, seenDuration)
	assert.True(t, seenFailed)
	assert.True(t, seenErrors)
	assert.False(t, seenEmpty, "deleteByPrefix should not emit empty-result metrics")
	assert.False(t, seenFailedZero, "failed deleteByPrefix should not emit failed=0 samples")
}

func TestKVAsync_ListKeys_InvalidShapeMetrics(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	registry := runtime.VU.InitEnv().Registry
	rootTags := registry.RootTagSet()

	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	var err error

	kv.operationMetrics, err = newKVOperationMetrics(registry, Options{
		Backend:       BackendMemory,
		Serialization: SerializationJSON,
		TrackKeys:     true,
		Metrics:       &MetricsOptions{Operations: true},
	})
	require.NoError(t, err)

	samples := make(chan k6metrics.SampleContainer, 32)
	runtime.MoveToVUContext(&lib.State{
		BuiltinMetrics: runtime.BuiltinMetrics,
		Samples:        samples,
		Tags:           lib.NewVUStateTags(rootTags),
	})

	runKVScript(t, runtime, kv, `
		__kv.listKeys([])
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)

	var (
		seenTotal      bool
		seenDuration   bool
		seenFailed     bool
		seenErrors     bool
		seenEmpty      bool
		seenFailedZero bool
	)

	for _, container := range k6metrics.GetBufferedSamples(samples) {
		for _, sample := range container.GetSamples() {
			op, ok := sample.Tags.Get(tagOp)
			if !ok || op != opListKeys {
				continue
			}

			switch sample.Metric.Name {
			case metricKVOperationsTotal:
				status, hasStatus := sample.Tags.Get(tagStatus)
				require.True(t, hasStatus)
				assert.Equal(t, statusError, status)
				assert.InDelta(t, 1.0, sample.Value, 1e-9)

				seenTotal = true
			case metricKVOperationDuration:
				status, hasStatus := sample.Tags.Get(tagStatus)
				require.True(t, hasStatus)
				assert.Equal(t, statusError, status)

				seenDuration = true
			case metricKVOperationFailed:
				if sample.Value == 0 {
					seenFailedZero = true
				}

				assert.InDelta(t, 1.0, sample.Value, 1e-9)

				seenFailed = true
			case metricKVErrorsTotal:
				errorType, hasErrorType := sample.Tags.Get(tagErrorType)
				require.True(t, hasErrorType)
				assert.Equal(t, string(InvalidOptionsError), errorType)
				assert.InDelta(t, 1.0, sample.Value, 1e-9)

				seenErrors = true
			case metricKVEmptyResult:
				seenEmpty = true
			}
		}
	}

	assert.True(t, seenTotal)
	assert.True(t, seenDuration)
	assert.True(t, seenFailed)
	assert.True(t, seenErrors)
	assert.False(t, seenEmpty, "listKeys should not emit empty-result metrics")
	assert.False(t, seenFailedZero, "failed listKeys should not emit failed=0 samples")
}

func TestKVAsync_DeleteMany_InvalidShapeMetrics(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	registry := runtime.VU.InitEnv().Registry
	rootTags := registry.RootTagSet()

	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	var err error

	kv.operationMetrics, err = newKVOperationMetrics(registry, Options{
		Backend:       BackendMemory,
		Serialization: SerializationJSON,
		TrackKeys:     true,
		Metrics:       &MetricsOptions{Operations: true},
	})
	require.NoError(t, err)

	samples := make(chan k6metrics.SampleContainer, 32)
	runtime.MoveToVUContext(&lib.State{
		BuiltinMetrics: runtime.BuiltinMetrics,
		Samples:        samples,
		Tags:           lib.NewVUStateTags(rootTags),
	})

	runKVScript(t, runtime, kv, `
		__kv.deleteMany({})
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)

	var (
		seenTotal      bool
		seenDuration   bool
		seenFailed     bool
		seenErrors     bool
		seenEmpty      bool
		seenFailedZero bool
	)

	for _, container := range k6metrics.GetBufferedSamples(samples) {
		for _, sample := range container.GetSamples() {
			op, ok := sample.Tags.Get(tagOp)
			if !ok || op != opDeleteMany {
				continue
			}

			switch sample.Metric.Name {
			case metricKVOperationsTotal:
				status, hasStatus := sample.Tags.Get(tagStatus)
				require.True(t, hasStatus)
				assert.Equal(t, statusError, status)
				assert.InDelta(t, 1.0, sample.Value, 1e-9)

				seenTotal = true
			case metricKVOperationDuration:
				status, hasStatus := sample.Tags.Get(tagStatus)
				require.True(t, hasStatus)
				assert.Equal(t, statusError, status)

				seenDuration = true
			case metricKVOperationFailed:
				if sample.Value == 0 {
					seenFailedZero = true
				}

				assert.InDelta(t, 1.0, sample.Value, 1e-9)

				seenFailed = true
			case metricKVErrorsTotal:
				errorType, hasErrorType := sample.Tags.Get(tagErrorType)
				require.True(t, hasErrorType)
				assert.Equal(t, string(InvalidOptionsError), errorType)
				assert.InDelta(t, 1.0, sample.Value, 1e-9)

				seenErrors = true
			case metricKVEmptyResult:
				seenEmpty = true
			}
		}
	}

	assert.True(t, seenTotal)
	assert.True(t, seenDuration)
	assert.True(t, seenFailed)
	assert.True(t, seenErrors)
	assert.False(t, seenEmpty, "deleteMany should not emit empty-result metrics")
	assert.False(t, seenFailedZero, "failed deleteMany should not emit failed=0 samples")
}

func TestKVAsync_DeleteMany_EmptyKeyMetrics(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	registry := runtime.VU.InitEnv().Registry
	rootTags := registry.RootTagSet()

	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	var err error

	kv.operationMetrics, err = newKVOperationMetrics(registry, Options{
		Backend:       BackendMemory,
		Serialization: SerializationJSON,
		TrackKeys:     true,
		Metrics:       &MetricsOptions{Operations: true},
	})
	require.NoError(t, err)

	samples := make(chan k6metrics.SampleContainer, 32)
	runtime.MoveToVUContext(&lib.State{
		BuiltinMetrics: runtime.BuiltinMetrics,
		Samples:        samples,
		Tags:           lib.NewVUStateTags(rootTags),
	})

	runKVScript(t, runtime, kv, `
		__kv.deleteMany([""])
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)

	var (
		seenTotal      bool
		seenDuration   bool
		seenFailed     bool
		seenErrors     bool
		seenEmpty      bool
		seenFailedZero bool
	)

	for _, container := range k6metrics.GetBufferedSamples(samples) {
		for _, sample := range container.GetSamples() {
			op, ok := sample.Tags.Get(tagOp)
			if !ok || op != opDeleteMany {
				continue
			}

			switch sample.Metric.Name {
			case metricKVOperationsTotal:
				status, hasStatus := sample.Tags.Get(tagStatus)
				require.True(t, hasStatus)
				assert.Equal(t, statusError, status)
				assert.InDelta(t, 1.0, sample.Value, 1e-9)

				seenTotal = true
			case metricKVOperationDuration:
				status, hasStatus := sample.Tags.Get(tagStatus)
				require.True(t, hasStatus)
				assert.Equal(t, statusError, status)

				seenDuration = true
			case metricKVOperationFailed:
				if sample.Value == 0 {
					seenFailedZero = true
				}

				assert.InDelta(t, 1.0, sample.Value, 1e-9)

				seenFailed = true
			case metricKVErrorsTotal:
				errorType, hasErrorType := sample.Tags.Get(tagErrorType)
				require.True(t, hasErrorType)
				assert.Equal(t, string(InvalidOptionsError), errorType)
				assert.InDelta(t, 1.0, sample.Value, 1e-9)

				seenErrors = true
			case metricKVEmptyResult:
				seenEmpty = true
			}
		}
	}

	assert.True(t, seenTotal)
	assert.True(t, seenDuration)
	assert.True(t, seenFailed)
	assert.True(t, seenErrors)
	assert.False(t, seenEmpty, "deleteMany should not emit empty-result metrics")
	assert.False(t, seenFailedZero, "failed deleteMany should not emit failed=0 samples")
}

func TestKVAsync_GetMany_InvalidShapeMetrics(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	registry := runtime.VU.InitEnv().Registry
	rootTags := registry.RootTagSet()

	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	var err error

	kv.operationMetrics, err = newKVOperationMetrics(registry, Options{
		Backend:       BackendMemory,
		Serialization: SerializationJSON,
		TrackKeys:     true,
		Metrics:       &MetricsOptions{Operations: true},
	})
	require.NoError(t, err)

	samples := make(chan k6metrics.SampleContainer, 32)
	runtime.MoveToVUContext(&lib.State{
		BuiltinMetrics: runtime.BuiltinMetrics,
		Samples:        samples,
		Tags:           lib.NewVUStateTags(rootTags),
	})

	runKVScript(t, runtime, kv, `
		__kv.getMany({})
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)

	var (
		seenTotal      bool
		seenDuration   bool
		seenFailed     bool
		seenErrors     bool
		seenEmpty      bool
		seenFailedZero bool
	)

	for _, container := range k6metrics.GetBufferedSamples(samples) {
		for _, sample := range container.GetSamples() {
			op, ok := sample.Tags.Get(tagOp)
			if !ok || op != opGetMany {
				continue
			}

			switch sample.Metric.Name {
			case metricKVOperationsTotal:
				status, hasStatus := sample.Tags.Get(tagStatus)
				require.True(t, hasStatus)
				assert.Equal(t, statusError, status)
				assert.InDelta(t, 1.0, sample.Value, 1e-9)

				seenTotal = true
			case metricKVOperationDuration:
				status, hasStatus := sample.Tags.Get(tagStatus)
				require.True(t, hasStatus)
				assert.Equal(t, statusError, status)

				seenDuration = true
			case metricKVOperationFailed:
				if sample.Value == 0 {
					seenFailedZero = true
				}

				assert.InDelta(t, 1.0, sample.Value, 1e-9)

				seenFailed = true
			case metricKVErrorsTotal:
				errorType, hasErrorType := sample.Tags.Get(tagErrorType)
				require.True(t, hasErrorType)
				assert.Equal(t, string(InvalidOptionsError), errorType)
				assert.InDelta(t, 1.0, sample.Value, 1e-9)

				seenErrors = true
			case metricKVEmptyResult:
				seenEmpty = true
			}
		}
	}

	assert.True(t, seenTotal)
	assert.True(t, seenDuration)
	assert.True(t, seenFailed)
	assert.True(t, seenErrors)
	assert.False(t, seenEmpty, "getMany should not emit empty-result metrics")
	assert.False(t, seenFailedZero, "failed getMany should not emit failed=0 samples")
}

func TestKVAsync_SetMany_ValidationFailureMetrics(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	registry := runtime.VU.InitEnv().Registry
	rootTags := registry.RootTagSet()

	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	var err error

	kv.operationMetrics, err = newKVOperationMetrics(registry, Options{
		Backend:       BackendMemory,
		Serialization: SerializationJSON,
		TrackKeys:     true,
		Metrics:       &MetricsOptions{Operations: true},
	})
	require.NoError(t, err)

	samples := make(chan k6metrics.SampleContainer, 32)
	runtime.MoveToVUContext(&lib.State{
		BuiltinMetrics: runtime.BuiltinMetrics,
		Samples:        samples,
		Tags:           lib.NewVUStateTags(rootTags),
	})

	runKVScript(t, runtime, kv, `
		__kv.setMany([])
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)

	var (
		seenTotal      bool
		seenDuration   bool
		seenFailed     bool
		seenErrors     bool
		seenEmpty      bool
		seenFailedZero bool
	)

	for _, container := range k6metrics.GetBufferedSamples(samples) {
		for _, sample := range container.GetSamples() {
			op, ok := sample.Tags.Get(tagOp)
			if !ok || op != opSetMany {
				continue
			}

			switch sample.Metric.Name {
			case metricKVOperationsTotal:
				status, hasStatus := sample.Tags.Get(tagStatus)
				require.True(t, hasStatus)
				assert.Equal(t, statusError, status)
				assert.InDelta(t, 1.0, sample.Value, 1e-9)

				seenTotal = true
			case metricKVOperationDuration:
				status, hasStatus := sample.Tags.Get(tagStatus)
				require.True(t, hasStatus)
				assert.Equal(t, statusError, status)

				seenDuration = true
			case metricKVOperationFailed:
				if sample.Value == 0 {
					seenFailedZero = true
				}

				assert.InDelta(t, 1.0, sample.Value, 1e-9)

				seenFailed = true
			case metricKVErrorsTotal:
				errorType, hasErrorType := sample.Tags.Get(tagErrorType)
				require.True(t, hasErrorType)
				assert.Equal(t, string(InvalidOptionsError), errorType)
				assert.InDelta(t, 1.0, sample.Value, 1e-9)

				seenErrors = true
			case metricKVEmptyResult:
				seenEmpty = true
			}
		}
	}

	assert.True(t, seenTotal)
	assert.True(t, seenDuration)
	assert.True(t, seenFailed)
	assert.True(t, seenErrors)
	assert.False(t, seenEmpty, "setMany should not emit empty-result metrics")
	assert.False(t, seenFailedZero, "failed setMany should not emit failed=0 samples")
}

func TestKVAsync_SetMany_SerializationFailureMetrics(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	registry := runtime.VU.InitEnv().Registry
	rootTags := registry.RootTagSet()

	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	var err error

	kv.operationMetrics, err = newKVOperationMetrics(registry, Options{
		Backend:       BackendMemory,
		Serialization: SerializationJSON,
		TrackKeys:     true,
		Metrics:       &MetricsOptions{Operations: true},
	})
	require.NoError(t, err)

	samples := make(chan k6metrics.SampleContainer, 32)
	runtime.MoveToVUContext(&lib.State{
		BuiltinMetrics: runtime.BuiltinMetrics,
		Samples:        samples,
		Tags:           lib.NewVUStateTags(rootTags),
	})

	runKVScript(t, runtime, kv, `
		__kv.setMany({
			"ok": { name: "Alice" },
			"bad": () => {},
		})
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "SerializerError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)

	var (
		seenTotal    bool
		seenDuration bool
		seenFailed   bool
		seenErrors   bool
		seenEmpty    bool
	)

	for _, container := range k6metrics.GetBufferedSamples(samples) {
		for _, sample := range container.GetSamples() {
			op, ok := sample.Tags.Get(tagOp)
			if !ok || op != opSetMany {
				continue
			}

			switch sample.Metric.Name {
			case metricKVOperationsTotal:
				status, hasStatus := sample.Tags.Get(tagStatus)
				require.True(t, hasStatus)
				assert.Equal(t, statusError, status)
				assert.InDelta(t, 1.0, sample.Value, 1e-9)

				seenTotal = true
			case metricKVOperationDuration:
				status, hasStatus := sample.Tags.Get(tagStatus)
				require.True(t, hasStatus)
				assert.Equal(t, statusError, status)

				seenDuration = true
			case metricKVOperationFailed:
				assert.InDelta(t, 1.0, sample.Value, 1e-9)

				seenFailed = true
			case metricKVErrorsTotal:
				errorType, hasErrorType := sample.Tags.Get(tagErrorType)
				require.True(t, hasErrorType)
				assert.Equal(t, string(SerializerError), errorType)
				assert.InDelta(t, 1.0, sample.Value, 1e-9)

				seenErrors = true
			case metricKVEmptyResult:
				seenEmpty = true
			}
		}
	}

	assert.True(t, seenTotal)
	assert.True(t, seenDuration)
	assert.True(t, seenFailed)
	assert.True(t, seenErrors)
	assert.False(t, seenEmpty, "setMany should not emit empty-result metrics")
}

func TestKVAsync_ExportJSONL_ResolvesSummary(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	fileName := filepath.Join(t.TempDir(), "export.jsonl")
	require.NoError(t, runtime.VU.Runtime().Set("fileName", fileName))

	runKVScript(t, runtime, kv, `
		__kv.setMany({
			"user:1": { name: "Alice" },
			"user:2": { name: "Bob" },
			"order:1": { total: 42 },
		})
			.then(() => __kv.exportJSONL({ fileName: fileName, prefix: "user:" }))
			.then((result) => {
				if (!result || typeof result !== "object") {
					throw new Error("missing exportJSONL result");
				}
				if (result.exported !== 2) {
					throw new Error("unexpected exported count");
				}
				if (result.fileName !== fileName) {
					throw new Error("unexpected fileName");
				}
				if (result.bytesWritten <= 0) {
					throw new Error("expected bytesWritten > 0");
				}
			});
	`)
}

func TestKVAsync_ExportJSONL_InvalidShapeRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.exportJSONL([])
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)
}

func TestKVAsync_ExportJSONL_MissingFileNameRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.exportJSONL({ prefix: "user:" })
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)
}

func TestKVAsync_ExportJSONL_FractionalLimitRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.exportJSONL({ fileName: "./x.jsonl", limit: 1.5 })
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)
}

func TestKVAsync_ExportJSONL_OperationMetrics(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	registry := runtime.VU.InitEnv().Registry
	rootTags := registry.RootTagSet()

	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	var err error

	kv.operationMetrics, err = newKVOperationMetrics(registry, Options{
		Backend:       BackendMemory,
		Serialization: SerializationJSON,
		TrackKeys:     true,
		Metrics:       &MetricsOptions{Operations: true},
	})
	require.NoError(t, err)

	samples := make(chan k6metrics.SampleContainer, 32)
	runtime.MoveToVUContext(&lib.State{
		BuiltinMetrics: runtime.BuiltinMetrics,
		Samples:        samples,
		Tags:           lib.NewVUStateTags(rootTags),
	})

	fileName := filepath.Join(t.TempDir(), "export.jsonl")
	require.NoError(t, runtime.VU.Runtime().Set("fileName", fileName))

	runKVScript(t, runtime, kv, `
		__kv.setMany({
			"user:1": { name: "Alice" },
			"user:2": { name: "Bob" },
		})
			.then(() => __kv.exportJSONL({ fileName: fileName, prefix: "user:" }))
			.then((result) => {
				if (!result || result.exported !== 2) {
					throw new Error("unexpected exportJSONL result");
				}
			});
	`)

	var (
		seenTotal    bool
		seenDuration bool
		seenFailed   bool
	)

	for _, container := range k6metrics.GetBufferedSamples(samples) {
		for _, sample := range container.GetSamples() {
			op, hasOp := sample.Tags.Get(tagOp)
			if !hasOp || op != opExportJSONL {
				continue
			}

			switch sample.Metric.Name {
			case metricKVOperationsTotal:
				status, ok := sample.Tags.Get(tagStatus)
				require.True(t, ok)
				assert.Equal(t, statusOK, status)

				seenTotal = true
			case metricKVOperationDuration:
				status, ok := sample.Tags.Get(tagStatus)
				require.True(t, ok)
				assert.Equal(t, statusOK, status)

				seenDuration = true
			case metricKVOperationFailed:
				assert.InDelta(t, 0.0, sample.Value, 1e-9)

				seenFailed = true
			}
		}
	}

	assert.True(t, seenTotal)
	assert.True(t, seenDuration)
	assert.True(t, seenFailed)
}

func TestKVAsync_ExportJSONL_InvalidOptionsMetrics(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	registry := runtime.VU.InitEnv().Registry
	rootTags := registry.RootTagSet()

	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	var err error

	kv.operationMetrics, err = newKVOperationMetrics(registry, Options{
		Backend:       BackendMemory,
		Serialization: SerializationJSON,
		TrackKeys:     true,
		Metrics:       &MetricsOptions{Operations: true},
	})
	require.NoError(t, err)

	samples := make(chan k6metrics.SampleContainer, 32)
	runtime.MoveToVUContext(&lib.State{
		BuiltinMetrics: runtime.BuiltinMetrics,
		Samples:        samples,
		Tags:           lib.NewVUStateTags(rootTags),
	})

	runKVScript(t, runtime, kv, `
		__kv.exportJSONL([])
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)

	var (
		seenTotal      bool
		seenDuration   bool
		seenFailed     bool
		seenErrors     bool
		seenEmpty      bool
		seenFailedZero bool
	)

	for _, container := range k6metrics.GetBufferedSamples(samples) {
		for _, sample := range container.GetSamples() {
			op, ok := sample.Tags.Get(tagOp)
			if !ok || op != opExportJSONL {
				continue
			}

			switch sample.Metric.Name {
			case metricKVOperationsTotal:
				status, hasStatus := sample.Tags.Get(tagStatus)
				require.True(t, hasStatus)
				assert.Equal(t, statusError, status)
				assert.InDelta(t, 1.0, sample.Value, 1e-9)

				seenTotal = true
			case metricKVOperationDuration:
				status, hasStatus := sample.Tags.Get(tagStatus)
				require.True(t, hasStatus)
				assert.Equal(t, statusError, status)

				seenDuration = true
			case metricKVOperationFailed:
				if sample.Value == 0 {
					seenFailedZero = true
				}

				assert.InDelta(t, 1.0, sample.Value, 1e-9)

				seenFailed = true
			case metricKVErrorsTotal:
				errorType, hasErrorType := sample.Tags.Get(tagErrorType)
				require.True(t, hasErrorType)
				assert.Equal(t, string(InvalidOptionsError), errorType)
				assert.InDelta(t, 1.0, sample.Value, 1e-9)

				seenErrors = true
			case metricKVEmptyResult:
				seenEmpty = true
			}
		}
	}

	assert.True(t, seenTotal)
	assert.True(t, seenDuration)
	assert.True(t, seenFailed)
	assert.True(t, seenErrors)
	assert.False(t, seenEmpty, "exportJSONL should not emit empty-result metrics")
	assert.False(t, seenFailedZero, "failed exportJSONL should not emit failed=0 samples")
}

func TestKVAsync_ImportJSONL_ResolvesSummary(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	fileName := filepath.Join(t.TempDir(), "import.jsonl")
	//nolint:forbidigo // test fixture creation requires file I/O.
	require.NoError(t, os.WriteFile(fileName, []byte(
		`{"key":"user:1","value":{"name":"Alice"}}`+"\n"+
			`{"key":"user:2","value":{"name":"Bob"}}`+"\n",
	), 0o644))

	require.NoError(t, runtime.VU.Runtime().Set("fileName", fileName))

	runKVScript(t, runtime, kv, `
		__kv.importJSONL({ fileName: fileName, batchSize: 1 })
			.then((result) => {
				if (!result || typeof result !== "object") {
					throw new Error("missing importJSONL result");
				}
				if (result.imported !== 2) {
					throw new Error("unexpected imported count");
				}
				if (result.fileName !== fileName) {
					throw new Error("unexpected fileName");
				}
				if (result.bytesRead <= 0) {
					throw new Error("expected bytesRead > 0");
				}
				return __kv.getMany(["user:1", "user:2"]);
			})
			.then((items) => {
				if (!Array.isArray(items) || items.length !== 2) {
					throw new Error("unexpected getMany shape");
				}
				if (items[0].exists !== true || items[1].exists !== true) {
					throw new Error("missing imported keys");
				}
			});
	`)
}

func TestKVAsync_ImportJSONL_InvalidShapeRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.importJSONL([])
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)
}

func TestKVAsync_ImportJSONL_MissingFileNameRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.importJSONL({ batchSize: 1 })
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)
}

func TestKVAsync_ImportJSONL_FractionalLimitRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.importJSONL({ fileName: "./x.jsonl", limit: 1.5 })
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)
}

func TestKVAsync_ImportJSONL_FractionalBatchSizeRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.importJSONL({ fileName: "./x.jsonl", batchSize: 1.5 })
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)
}

func TestKVAsync_ImportJSONL_FileNotFoundRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.importJSONL({ fileName: "./tmp/does-not-exist-import-jsonl.jsonl" })
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "SnapshotNotFoundError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)
}

func TestKVAsync_ImportJSONL_MalformedJSONRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	fileName := filepath.Join(t.TempDir(), "malformed-import.jsonl")
	//nolint:forbidigo // test fixture creation requires file I/O.
	require.NoError(t, os.WriteFile(fileName, []byte(
		`{"key":"user:1","value":{"name":"Alice"}}`+"\n"+
			`{"key":"user:2","value":`+"\n",
	), 0o644))

	require.NoError(t, runtime.VU.Runtime().Set("fileName", fileName))

	runKVScript(t, runtime, kv, `
		__kv.importJSONL({ fileName: fileName, batchSize: 1 })
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "ValueParseError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)
}

func TestKVAsync_ImportJSONL_BlankLineRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	fileName := filepath.Join(t.TempDir(), "blank-import.jsonl")
	//nolint:forbidigo // test fixture creation requires file I/O.
	require.NoError(t, os.WriteFile(fileName, []byte(
		`{"key":"user:1","value":{"name":"Alice"}}`+"\n"+
			"\n"+
			`{"key":"user:2","value":{"name":"Bob"}}`+"\n",
	), 0o644))

	require.NoError(t, runtime.VU.Runtime().Set("fileName", fileName))

	runKVScript(t, runtime, kv, `
		__kv.importJSONL({ fileName: fileName, batchSize: 1 })
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "ValueParseError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)
}

func TestKVAsync_ImportJSONL_OperationMetrics(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	registry := runtime.VU.InitEnv().Registry
	rootTags := registry.RootTagSet()

	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	var err error

	kv.operationMetrics, err = newKVOperationMetrics(registry, Options{
		Backend:       BackendMemory,
		Serialization: SerializationJSON,
		TrackKeys:     true,
		Metrics:       &MetricsOptions{Operations: true},
	})
	require.NoError(t, err)

	samples := make(chan k6metrics.SampleContainer, 32)
	runtime.MoveToVUContext(&lib.State{
		BuiltinMetrics: runtime.BuiltinMetrics,
		Samples:        samples,
		Tags:           lib.NewVUStateTags(rootTags),
	})

	fileName := filepath.Join(t.TempDir(), "import.jsonl")
	//nolint:forbidigo // test fixture creation requires file I/O.
	require.NoError(t, os.WriteFile(fileName, []byte(
		`{"key":"user:1","value":{"name":"Alice"}}`+"\n"+
			`{"key":"user:2","value":{"name":"Bob"}}`+"\n",
	), 0o644))
	require.NoError(t, runtime.VU.Runtime().Set("fileName", fileName))

	runKVScript(t, runtime, kv, `
		__kv.importJSONL({ fileName: fileName, batchSize: 1 })
			.then((result) => {
				if (!result || result.imported !== 2) {
					throw new Error("unexpected importJSONL result");
				}
			});
	`)

	var (
		seenTotal    bool
		seenDuration bool
		seenFailed   bool
	)

	for _, container := range k6metrics.GetBufferedSamples(samples) {
		for _, sample := range container.GetSamples() {
			op, hasOp := sample.Tags.Get(tagOp)
			if !hasOp || op != opImportJSONL {
				continue
			}

			switch sample.Metric.Name {
			case metricKVOperationsTotal:
				status, ok := sample.Tags.Get(tagStatus)
				require.True(t, ok)
				assert.Equal(t, statusOK, status)

				seenTotal = true
			case metricKVOperationDuration:
				status, ok := sample.Tags.Get(tagStatus)
				require.True(t, ok)
				assert.Equal(t, statusOK, status)

				seenDuration = true
			case metricKVOperationFailed:
				assert.InDelta(t, 0.0, sample.Value, 1e-9)

				seenFailed = true
			}
		}
	}

	assert.True(t, seenTotal)
	assert.True(t, seenDuration)
	assert.True(t, seenFailed)
}

func TestKVAsync_ImportJSONL_InvalidOptionsMetrics(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	registry := runtime.VU.InitEnv().Registry
	rootTags := registry.RootTagSet()

	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewJSONSerializer(),
		),
	)

	var err error

	kv.operationMetrics, err = newKVOperationMetrics(registry, Options{
		Backend:       BackendMemory,
		Serialization: SerializationJSON,
		TrackKeys:     true,
		Metrics:       &MetricsOptions{Operations: true},
	})
	require.NoError(t, err)

	samples := make(chan k6metrics.SampleContainer, 32)
	runtime.MoveToVUContext(&lib.State{
		BuiltinMetrics: runtime.BuiltinMetrics,
		Samples:        samples,
		Tags:           lib.NewVUStateTags(rootTags),
	})

	runKVScript(t, runtime, kv, `
		__kv.importJSONL([])
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)

	var (
		seenTotal      bool
		seenDuration   bool
		seenFailed     bool
		seenErrors     bool
		seenEmpty      bool
		seenFailedZero bool
	)

	for _, container := range k6metrics.GetBufferedSamples(samples) {
		for _, sample := range container.GetSamples() {
			op, ok := sample.Tags.Get(tagOp)
			if !ok || op != opImportJSONL {
				continue
			}

			switch sample.Metric.Name {
			case metricKVOperationsTotal:
				status, hasStatus := sample.Tags.Get(tagStatus)
				require.True(t, hasStatus)
				assert.Equal(t, statusError, status)
				assert.InDelta(t, 1.0, sample.Value, 1e-9)

				seenTotal = true
			case metricKVOperationDuration:
				status, hasStatus := sample.Tags.Get(tagStatus)
				require.True(t, hasStatus)
				assert.Equal(t, statusError, status)

				seenDuration = true
			case metricKVOperationFailed:
				if sample.Value == 0 {
					seenFailedZero = true
				}

				assert.InDelta(t, 1.0, sample.Value, 1e-9)

				seenFailed = true
			case metricKVErrorsTotal:
				errorType, hasErrorType := sample.Tags.Get(tagErrorType)
				require.True(t, hasErrorType)
				assert.Equal(t, string(InvalidOptionsError), errorType)
				assert.InDelta(t, 1.0, sample.Value, 1e-9)

				seenErrors = true
			case metricKVEmptyResult:
				seenEmpty = true
			}
		}
	}

	assert.True(t, seenTotal)
	assert.True(t, seenDuration)
	assert.True(t, seenFailed)
	assert.True(t, seenErrors)
	assert.False(t, seenEmpty, "importJSONL should not emit empty-result metrics")
	assert.False(t, seenFailedZero, "failed importJSONL should not emit failed=0 samples")
}

func TestKVAsync_AllOptionsMethods_InvalidOptionsType_RejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		function expectInvalidOptions(promise, label) {
			return promise
				.then(() => {
					throw new Error(label + ": expected rejection");
				})
				.catch((err) => {
					if (!err || err.name !== "InvalidOptionsError") {
						throw new Error(label + ": unexpected error class: " + String(err && err.name));
					}
				});
		}

		Promise.all([
			expectInvalidOptions(__kv.scan("bad"), "scan"),
			expectInvalidOptions(__kv.list("bad"), "list"),
			expectInvalidOptions(__kv.listKeys("bad"), "listKeys"),
			expectInvalidOptions(__kv.exportJSONL("bad"), "exportJSONL"),
			expectInvalidOptions(__kv.importJSONL("bad"), "importJSONL"),
			expectInvalidOptions(__kv.deleteByPrefix("bad"), "deleteByPrefix"),
			expectInvalidOptions(__kv.randomKey("bad"), "randomKey"),
			expectInvalidOptions(__kv.count("bad"), "count"),
			expectInvalidOptions(__kv.backup("bad"), "backup"),
			expectInvalidOptions(__kv.restore("bad"), "restore"),
			expectInvalidOptions(__kv.compareAndSwapDetailed("k", null, "v", "bad"), "compareAndSwapDetailed"),
			expectInvalidOptions(__kv.compareAndDeleteDetailed("k", "v", "bad"), "compareAndDeleteDetailed")
		]);
	`)
}

func TestKVAsync_AllOptionsMethods_InvalidOptionFieldType_RejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		function expectInvalidOptions(promise, label) {
			return promise
				.then(() => {
					throw new Error(label + ": expected rejection");
				})
				.catch((err) => {
					if (!err || err.name !== "InvalidOptionsError") {
						throw new Error(label + ": unexpected error class: " + String(err && err.name));
					}
				});
		}

		Promise.all([
			expectInvalidOptions(__kv.scan({ prefix: 123 }), "scan.prefix"),
			expectInvalidOptions(__kv.scan({ limit: "10" }), "scan.limit"),
			expectInvalidOptions(__kv.scan({ cursor: 99 }), "scan.cursor"),
			expectInvalidOptions(__kv.list({ prefix: true }), "list.prefix"),
			expectInvalidOptions(__kv.list({ limit: "10" }), "list.limit"),
			expectInvalidOptions(__kv.listKeys({ prefix: true }), "listKeys.prefix"),
			expectInvalidOptions(__kv.listKeys({ limit: "10" }), "listKeys.limit"),
			expectInvalidOptions(__kv.exportJSONL({ fileName: 100 }), "exportJSONL.fileName"),
			expectInvalidOptions(__kv.exportJSONL({ fileName: "./x.jsonl", prefix: true }), "exportJSONL.prefix"),
			expectInvalidOptions(__kv.exportJSONL({ fileName: "./x.jsonl", limit: "10" }), "exportJSONL.limit"),
			expectInvalidOptions(__kv.importJSONL({ fileName: 100 }), "importJSONL.fileName"),
			expectInvalidOptions(__kv.importJSONL({ fileName: "./x.jsonl", limit: "10" }), "importJSONL.limit"),
			expectInvalidOptions(__kv.importJSONL({ fileName: "./x.jsonl", batchSize: "10" }), "importJSONL.batchSize"),
			expectInvalidOptions(__kv.deleteByPrefix({ prefix: true, limit: 1 }), "deleteByPrefix.prefix"),
			expectInvalidOptions(__kv.deleteByPrefix({ prefix: "tmp:", limit: "10" }), "deleteByPrefix.limit"),
			expectInvalidOptions(__kv.randomKey({ prefix: 7 }), "randomKey.prefix"),
			expectInvalidOptions(__kv.count({ prefix: 7 }), "count.prefix"),
			expectInvalidOptions(__kv.backup({ fileName: 100 }), "backup.fileName"),
			expectInvalidOptions(__kv.backup({ allowConcurrentWrites: "true" }), "backup.allowConcurrentWrites"),
			expectInvalidOptions(__kv.restore({ fileName: 100 }), "restore.fileName"),
			expectInvalidOptions(__kv.restore({ maxEntries: "10" }), "restore.maxEntries"),
			expectInvalidOptions(__kv.restore({ maxBytes: "10" }), "restore.maxBytes"),
			expectInvalidOptions(
				__kv.compareAndSwapDetailed("k", null, "v", { includeCurrentOnMismatch: "true" }),
				"compareAndSwapDetailed.includeCurrentOnMismatch"
			),
			expectInvalidOptions(
				__kv.compareAndDeleteDetailed("k", "v", { includeCurrentOnMismatch: 1 }),
				"compareAndDeleteDetailed.includeCurrentOnMismatch"
			)
		]);
	`)
}

func TestKVAsync_KeyMethods_InvalidKeyType_RejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		function expectInvalidOptions(promise, label) {
			return promise
				.then(() => {
					throw new Error(label + ": expected rejection");
				})
				.catch((err) => {
					if (!err || err.name !== "InvalidOptionsError") {
						throw new Error(label + ": unexpected error class: " + String(err && err.name));
					}
				});
		}

		Promise.all([
			expectInvalidOptions(__kv.get(1), "get"),
			expectInvalidOptions(__kv.set(1, "v"), "set"),
			expectInvalidOptions(__kv.incrementBy(1, 1), "incrementBy"),
			expectInvalidOptions(__kv.getOrSet(1, "v"), "getOrSet"),
			expectInvalidOptions(__kv.swap(1, "v"), "swap"),
			expectInvalidOptions(__kv.delete(1), "delete"),
			expectInvalidOptions(__kv.exists(1), "exists"),
			expectInvalidOptions(__kv.deleteIfExists(1), "deleteIfExists"),
			expectInvalidOptions(__kv.compareAndSwap(1, null, "v"), "compareAndSwap"),
			expectInvalidOptions(__kv.compareAndSwapDetailed(1, null, "v", {}), "compareAndSwapDetailed"),
			expectInvalidOptions(__kv.compareAndDelete(1, "v"), "compareAndDelete"),
			expectInvalidOptions(__kv.compareAndDeleteDetailed(1, "v", {}), "compareAndDeleteDetailed"),
			expectInvalidOptions(__kv.setIfAbsent(1, "v"), "setIfAbsent")
		]);
	`)
}

func TestKVAsync_KeyMethods_EmptyKey_RejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		function expectInvalidOptionsWithNonEmptyHint(promise, label) {
			return promise
				.then(() => {
					throw new Error(label + ": expected rejection");
				})
				.catch((err) => {
					if (!err || err.name !== "InvalidOptionsError") {
						throw new Error(label + ": unexpected error class: " + String(err && err.name));
					}
					if (!String(err.message).includes("non-empty")) {
						throw new Error(label + ": expected non-empty validation hint");
					}
				});
		}

		Promise.all([
			expectInvalidOptionsWithNonEmptyHint(__kv.get(""), "get"),
			expectInvalidOptionsWithNonEmptyHint(__kv.set("", "v"), "set"),
			expectInvalidOptionsWithNonEmptyHint(__kv.incrementBy("", 1), "incrementBy"),
			expectInvalidOptionsWithNonEmptyHint(__kv.getOrSet("", "v"), "getOrSet"),
			expectInvalidOptionsWithNonEmptyHint(__kv.swap("", "v"), "swap"),
			expectInvalidOptionsWithNonEmptyHint(__kv.delete(""), "delete"),
			expectInvalidOptionsWithNonEmptyHint(__kv.exists(""), "exists"),
			expectInvalidOptionsWithNonEmptyHint(__kv.deleteIfExists(""), "deleteIfExists"),
			expectInvalidOptionsWithNonEmptyHint(__kv.compareAndSwap("", null, "v"), "compareAndSwap"),
			expectInvalidOptionsWithNonEmptyHint(__kv.compareAndSwapDetailed("", null, "v", {}), "compareAndSwapDetailed"),
			expectInvalidOptionsWithNonEmptyHint(__kv.compareAndDelete("", "v"), "compareAndDelete"),
			expectInvalidOptionsWithNonEmptyHint(__kv.compareAndDeleteDetailed("", "v", {}), "compareAndDeleteDetailed"),
			expectInvalidOptionsWithNonEmptyHint(__kv.setIfAbsent("", "v"), "setIfAbsent")
		]);
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

func TestKVAsync_PopRandom_ResolvesEntry(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.set("user:1", "Oleg")
			.then(() => __kv.popRandom({ prefix: "user:" }))
			.then((entry) => {
				if (!entry) {
					throw new Error("missing entry");
				}
				if (entry.key !== "user:1") {
					throw new Error("wrong key");
				}
				if (entry.value === undefined) {
					throw new Error("wrong value");
				}
			});
	`)
}

func TestKVAsync_PopRandom_EmptyResolvesNull(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.popRandom({ prefix: "missing:" })
			.then((entry) => {
				if (entry !== null) {
					throw new Error("expected null");
				}
			});
	`)
}

func TestKVAsync_ClaimRandom_ResolvesClaim(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.set("user:1", "Oleg")
			.then(() => __kv.claimRandom({ prefix: "user:", owner: "vu:1", ttl: 30000 }))
			.then((claim) => {
				if (!claim) {
					throw new Error("missing claim");
				}
				if (claim.id === "") {
					throw new Error("missing id");
				}
				if (claim.key !== "user:1") {
					throw new Error("wrong key");
				}
				if (typeof claim.token !== "number") {
					throw new Error("invalid token");
				}
				if (typeof claim.expiresAt !== "number") {
					throw new Error("invalid expiresAt");
				}
				if (!claim.entry || claim.entry.key !== "user:1" || claim.entry.value === undefined) {
					throw new Error("invalid entry payload");
				}
			});
	`)
}

func TestKVAsync_ClaimRandom_EmptyResolvesNull(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.claimRandom({ prefix: "missing:" })
			.then((claim) => {
				if (claim !== null) {
					throw new Error("expected null");
				}
			});
	`)
}

func TestKVAsync_ReleaseClaim_ReturnsTrue(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.set("user:1", "Oleg")
			.then(() => __kv.claimRandom({ prefix: "user:", ttl: 30000 }))
			.then((claim) => {
				if (!claim) {
					throw new Error("missing claim");
				}
				return __kv.releaseClaim(claim);
			})
			.then((released) => {
				if (released !== true) {
					throw new Error("expected true");
				}
			});
	`)
}

func TestKVAsync_CompleteClaim_ReturnsTrue(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.set("user:1", "Oleg")
			.then(() => __kv.claimRandom({ prefix: "user:", ttl: 30000 }))
			.then((claim) => {
				if (!claim) {
					throw new Error("missing claim");
				}
				return __kv.completeClaim(claim);
			})
			.then((completed) => {
				if (completed !== true) {
					throw new Error("expected true");
				}
				return __kv.exists("user:1");
			})
			.then((exists) => {
				if (exists !== false) {
					throw new Error("expected key deletion");
				}
			});
	`)
}

func TestKVAsync_Claim_InvalidOptions_RejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		function expectInvalidOptions(promise, label) {
			return promise
				.then(() => {
					throw new Error(label + ": expected rejection");
				})
				.catch((err) => {
					if (!err || err.name !== "InvalidOptionsError") {
						throw new Error(label + ": unexpected error class: " + String(err && err.name));
					}
				});
		}

		Promise.all([
			expectInvalidOptions(__kv.popRandom("bad"), "popRandom.options"),
			expectInvalidOptions(__kv.claimRandom({ ttl: 0 }), "claimRandom.ttl.positive"),
			expectInvalidOptions(__kv.claimRandom({ ttl: 1.5 }), "claimRandom.ttl.integer"),
			expectInvalidOptions(__kv.releaseClaim("bad"), "releaseClaim.claim"),
			expectInvalidOptions(__kv.completeClaim("bad"), "completeClaim.claim"),
			expectInvalidOptions(__kv.completeClaim({ id: "x", key: "k", token: 1 }, "bad"), "completeClaim.options")
		]);
	`)
}

func TestKVAsync_IncrementBy_FractionalDelta_RejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.incrementBy("counter", 1.5)
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

func TestKVAsync_Scan_FractionalLimit_RejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.scan({ limit: 1.2 })
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)
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
