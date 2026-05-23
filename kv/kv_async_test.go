package kv

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/grafana/sobek"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/js/modulestest"
	"go.k6.io/k6/lib"
	k6metrics "go.k6.io/k6/metrics"

	"github.com/oshokin/xk6-kv/kv/store"
)

// TestKVAsync_Set_ResolvesPromise verifies that async set resolves its promise.
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

// TestKVAsync_SetMany_ResolvesWrittenCount verifies that kv async set many resolves written count.
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

// TestKVAsync_StringSerialization_CoercesObjectToString verifies that kv async string serialization coerces object to string.
func TestKVAsync_StringSerialization_CoercesObjectToString(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(
		runtime.VU,
		store.NewSerializedStore(
			store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
			store.NewStringSerializer(),
		),
	)

	runKVScript(t, runtime, kv, `
		__kv.set("string:object", { a: 1 })
			.then(() => __kv.get("string:object"))
			.then((value) => {
				if (typeof value !== "string") {
					throw new Error("string serializer must return string");
				}
				if (!value.includes("map[") || !value.includes("a:1")) {
					throw new Error("unexpected string serializer value: " + value);
				}
			});
	`)
}

// TestKVAsync_SetMany_EmptyObjectResolvesZero verifies that kv async set many empty object resolves zero.
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

// TestKVAsync_SetMany_InvalidShapeRejectsPromise verifies that kv async set many invalid shape rejects its promise.
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

// TestKVAsync_Set_EmptyKeyRejectsPromise verifies that kv async set empty key rejects its promise.
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

// TestKVAsync_SetMany_EmptyKeyRejectsPromise verifies that kv async set many empty key rejects its promise.
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

// TestKVAsync_SetMany_SerializationFailureRejectsAndWritesNothing verifies that kv async set many serialization failure rejects and writes nothing.
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
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
				if (err.message !== "setMany validation failed: 1 invalid entry") {
					throw new Error("unexpected error message: " + String(err && err.message));
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

// TestKVAsync_CompareDetailed_SerializationFailureRejectsPromise verifies that kv async compare detailed serialization failure rejects its promise.
func TestKVAsync_CompareDetailed_SerializationFailureRejectsPromise(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		script string
	}{
		{
			name: "CompareAndSwapDetailed",
			script: `
				__kv.compareAndSwapDetailed("k", null, () => {}, { includeCurrentOnMismatch: true })
					.then(() => {
						throw new Error("expected rejection");
					})
					.catch((err) => {
						if (!err || err.name !== "SerializerError") {
							throw new Error("unexpected error class: " + String(err && err.name));
						}
					});
			`,
		},
		{
			name: "CompareAndDeleteDetailed",
			script: `
				__kv.compareAndDeleteDetailed("k", () => {}, { includeCurrentOnMismatch: true })
					.then(() => {
						throw new Error("expected rejection");
					})
					.catch((err) => {
						if (!err || err.name !== "SerializerError") {
							throw new Error("unexpected error class: " + String(err && err.name));
						}
					});
			`,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			runtime := modulestest.NewRuntime(t)
			kv := NewKV(
				runtime.VU,
				store.NewSerializedStore(
					store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
					store.NewJSONSerializer(),
				),
			)

			runKVScript(t, runtime, kv, testCase.script)
		})
	}
}

// TestKVAsync_GetMany_ResolvesItemsAndExistsFlags verifies that kv async get many resolves items and exists flags.
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

// TestKVAsync_GetMany_EmptyArrayResolvesEmptyArray verifies that kv async get many empty array resolves empty array.
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

// TestKVAsync_GetMany_EmptyStringKeyResolvesMissingItem verifies that kv async get many empty string key resolves missing item.
func TestKVAsync_GetMany_EmptyStringKeyResolvesMissingItem(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.getMany([""])
			.then((items) => {
				if (!Array.isArray(items) || items.length !== 1) {
					throw new Error("expected one result item");
				}
				if (items[0].key !== "" || items[0].exists !== false || items[0].value !== null) {
					throw new Error("empty string key must resolve as missing");
				}
			});
	`)
}

// TestKVAsync_GetMany_InvalidShapeRejectsPromise verifies that kv async get many invalid shape rejects its promise.
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

// TestKVAsync_GetMany_InvalidElementRejectsPromise verifies that kv async get many invalid element rejects its promise.
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

// TestKVAsync_GetMany_DuplicateKeys verifies that kv async get many duplicate keys.
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

// TestKVAsync_GetMany_DistinguishesMissingAndStoredJSONNull verifies that kv async get many distinguishes missing and stored json null.
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

// TestKVAsync_DeleteMany_ResolvesDeletedAndMissingCounts verifies that kv async delete many resolves deleted and missing counts.
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

// TestKVAsync_DeleteMany_RemovesKeys verifies that kv async delete many removes keys.
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

// TestKVAsync_DeleteMany_EmptyArrayResolvesZeroCounts verifies that kv async delete many empty array resolves zero counts.
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

// TestKVAsync_DeleteMany_DuplicateKeys verifies that kv async delete many duplicate keys.
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

// TestKVAsync_DeleteMany_InvalidShapeRejectsPromise verifies that kv async delete many invalid shape rejects its promise.
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

// TestKVAsync_DeleteMany_InvalidElementRejectsPromise verifies that kv async delete many invalid element rejects its promise.
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

// TestKVAsync_DeleteMany_EmptyKeyRejectsPromise verifies that kv async delete many empty key rejects its promise.
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

// TestKVAsync_GetMissingKey_RejectsPromise verifies that kv async get missing key kv async get missing key rejects its promise.
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

// TestKVAsync_Get_PanicInStore_RejectsPromise verifies that kv async get panic in store kv async get panic in store rejects its promise.
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

// TestKVAsync_CallbackEnqueue_ExactlyOnce verifies that kv async callback enqueue exactly once.
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

// TestKVAsync_ClosedDiskStore_RejectsWithoutHang verifies that kv async closed disk store rejects without hang.
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

// TestKVAsync_DiskReadOnlyMutationRejectsStableError verifies that kv async disk read only mutation rejects stable error.
func TestKVAsync_DiskReadOnlyMutationRejectsStableError(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	dbPath := filepath.Join(t.TempDir(), "async-readonly.db")

	writableStore, err := store.NewDiskStore(true, dbPath, nil)
	require.NoError(t, err)
	require.NoError(t, writableStore.Open())
	require.NoError(t, writableStore.Set("seed", "value"))
	require.NoError(t, writableStore.Close())

	readOnlyStore, err := store.NewDiskStore(
		true,
		dbPath,
		&store.DiskConfig{ReadOnly: store.GetComparablePointer(true)},
	)
	require.NoError(t, err)
	require.NoError(t, readOnlyStore.Open())

	kv := NewKV(runtime.VU, readOnlyStore)

	t.Cleanup(func() {
		_ = kv.Close()
	})

	runKVScript(t, runtime, kv, `
		__kv.set("blocked:key", "value")
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "StoreReadOnlyError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
				if (err.message !== "disk store was opened in read-only mode") {
					throw new Error("unexpected error message: " + String(err && err.message));
				}
			});
	`)
}

// TestKV_Close_IdempotentPerHandle verifies that kv close idempotent per handle.
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

// TestKV_Close_MemoryHandleRejectsAfterClose verifies that kv close memory handle rejects after close.
func TestKV_Close_MemoryHandleRejectsAfterClose(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.set("close:memory:key", "value")
			.then((value) => {
				if (value !== "value") {
					throw new Error("unexpected set result");
				}
			});
	`)

	require.NoError(t, kv.Close())

	runKVScript(t, runtime, kv, `
		__kv.get("close:memory:key")
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "StoreClosedError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
				if (err.message !== "kv store handle is closed") {
					throw new Error("unexpected error message: " + String(err && err.message));
				}
			});
	`)
}

// TestKV_Close_DiskHandleRejectsAfterClose verifies that kv close disk handle rejects after close.
func TestKV_Close_DiskHandleRejectsAfterClose(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	diskStore, err := store.NewDiskStore(
		true,
		filepath.Join(t.TempDir(), "close-handle-disk.db"),
		nil,
	)
	require.NoError(t, err)
	require.NoError(t, diskStore.Open())

	kv := NewKV(runtime.VU, diskStore)

	t.Cleanup(func() {
		_ = kv.Close()
	})

	runKVScript(t, runtime, kv, `
		__kv.set("close:disk:key", "value")
			.then(() => {
			});
	`)

	require.NoError(t, kv.Close())

	runKVScript(t, runtime, kv, `
		__kv.get("close:disk:key")
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "StoreClosedError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
				if (err.message !== "kv store handle is closed") {
					throw new Error("unexpected error message: " + String(err && err.message));
				}
			});
	`)
}

// TestValidateCSV_RejectsAfterClose verifies that validate csv rejects after close.
func TestValidateCSV_RejectsAfterClose(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	fileName := filepath.Join(t.TempDir(), "validate-after-close.csv")
	//nolint:forbidigo // test fixture creation requires file I/O.
	require.NoError(t, os.WriteFile(fileName, []byte("id,name\n1,Alice\n"), 0o644))
	require.NoError(t, runtime.VU.Runtime().Set("fileName", fileName))

	require.NoError(t, kv.Close())

	runKVScript(t, runtime, kv, `
		__kv.validateCSV({ fileName: fileName, keyColumn: "id", hasHeader: true })
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "StoreClosedError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)
}

// TestValidateJSONL_RejectsAfterClose verifies that validate jsonl rejects after close.
func TestValidateJSONL_RejectsAfterClose(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	fileName := filepath.Join(t.TempDir(), "validate-after-close.jsonl")
	//nolint:forbidigo // test fixture creation requires file I/O.
	require.NoError(t, os.WriteFile(fileName, []byte(`{"key":"u:1","value":{"name":"Alice"}}`+"\n"), 0o644))
	require.NoError(t, runtime.VU.Runtime().Set("fileName", fileName))

	require.NoError(t, kv.Close())

	runKVScript(t, runtime, kv, `
		__kv.validateJSONL({ fileName: fileName })
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "StoreClosedError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)
}

// TestKV_Close_DoesNotCloseOtherHandleUntilLastClose verifies that kv close does not close other handle until last close.
func TestKV_Close_DoesNotCloseOtherHandleUntilLastClose(t *testing.T) {
	t.Parallel()

	firstRuntime := modulestest.NewRuntime(t)
	secondRuntime := modulestest.NewRuntime(t)

	diskStore, err := store.NewDiskStore(
		true,
		filepath.Join(t.TempDir(), "close-handle-shared.db"),
		nil,
	)
	require.NoError(t, err)
	require.NoError(t, diskStore.Open())
	require.NoError(t, diskStore.Open())

	firstKV := NewKV(firstRuntime.VU, diskStore)
	secondKV := NewKV(secondRuntime.VU, diskStore)

	t.Cleanup(func() {
		_ = firstKV.Close()
		_ = secondKV.Close()
	})

	runKVScript(t, firstRuntime, firstKV, `
		__kv.set("close:shared:key", "shared-value")
			.then(() => {
			});
	`)

	require.NoError(t, firstKV.Close())

	runKVScript(t, firstRuntime, firstKV, `
		__kv.get("close:shared:key")
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "StoreClosedError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
				if (err.message !== "kv store handle is closed") {
					throw new Error("unexpected error message: " + String(err && err.message));
				}
			});
	`)

	runKVScript(t, secondRuntime, secondKV, `
		__kv.exists("close:shared:key")
			.then((exists) => {
				if (!exists) {
					throw new Error("key must remain accessible from still-open handle");
				}
			});
	`)

	require.NoError(t, secondKV.Close())

	err = diskStore.Set("close:shared:post-close", "value")
	require.Error(t, err, "store must reject operations after the last handle close")
}

// TestKV_Close_AllowsAlreadyStartedAsyncOperationToSettle verifies that kv close allows already started async operation to settle.
func TestKV_Close_AllowsAlreadyStartedAsyncOperationToSettle(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	blockingStore := &blockingGetStore{
		Store:   store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
		started: make(chan struct{}),
		unblock: make(chan struct{}),
	}
	kv := NewKV(runtime.VU, blockingStore)

	require.NoError(t, blockingStore.Set("close:in-flight:key", "value"))
	require.NoError(t, runtime.VU.Runtime().Set("__kv", kv))

	errCh := make(chan error, 1)

	go func() {
		_, err := runtime.RunOnEventLoop(`
			__kv.get("close:in-flight:key")
				.then(() => {
				})
				.catch((err) => {
					throw new Error("unexpected rejection: " + String(err && err.name));
				});
		`)
		errCh <- err
	}()

	select {
	case <-blockingStore.started:
	case err := <-errCh:
		require.NoError(t, err)
		t.Fatal("expected get operation to block before close")
	}

	require.NoError(t, kv.Close())
	close(blockingStore.unblock)

	require.NoError(t, <-errCh)
}

// TestKVAsync_IncrementBy_InvalidDelta_RejectsPromise verifies that kv async increment by invalid delta kv async increment by invalid delta rejects its promise.
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

// TestKVAsync_Count_ResolvesPromise verifies that async count resolves its promise.
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

// TestKVAsync_Count_InvalidOptions_RejectsPromise verifies that kv async count invalid options kv async count invalid options rejects its promise.
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

// TestKVAsync_ListKeys_ResolvesOrderedKeys verifies that kv async list keys resolves ordered keys.
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

// TestKVAsync_ListKeys_PrefixAndLimit verifies that kv async list keys prefix and limit.
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

// TestKVAsync_ListKeys_EmptyStore verifies that kv async list keys empty store.
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

// TestKVAsync_ListKeys_InvalidShapeRejectsPromise verifies that kv async list keys invalid shape rejects its promise.
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

// TestKVAsync_ListKeys_InvalidPrefixRejectsPromise verifies that kv async list keys invalid prefix rejects its promise.
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

// TestKVAsync_ListKeys_FractionalLimitRejectsPromise verifies that kv async list keys fractional limit rejects its promise.
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

// TestKVAsync_ScanKeys_ResolvesPagedKeys verifies that kv async scan keys resolves paged keys.
func TestKVAsync_ScanKeys_ResolvesPagedKeys(t *testing.T) {
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
			.then(() => __kv.scanKeys({ prefix: "user:", limit: 2 }))
			.then((page) => {
				const expected = ["user:1", "user:2"];
				if (JSON.stringify(page.keys) !== JSON.stringify(expected)) {
					throw new Error("unexpected first page: " + JSON.stringify(page));
				}
				if (!page.cursor || page.done) {
					throw new Error("expected continuation cursor");
				}

				return __kv.scanKeys({ prefix: "user:", cursor: page.cursor, limit: 2 });
			})
			.then((page) => {
				const expected = ["user:3"];
				if (JSON.stringify(page.keys) !== JSON.stringify(expected)) {
					throw new Error("unexpected second page: " + JSON.stringify(page));
				}
				if (page.cursor !== "" || !page.done) {
					throw new Error("expected done");
				}
			});
	`)
}

// TestKVAsync_ScanKeys_InvalidCursorRejectsPromise verifies that kv async scan keys invalid cursor rejects its promise.
func TestKVAsync_ScanKeys_InvalidCursorRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.scanKeys({ cursor: "bad" })
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidCursorError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)
}

// TestKVAsync_ScanKeys_InvalidPrefixRejectsPromise verifies that kv async scan keys invalid prefix rejects its promise.
func TestKVAsync_ScanKeys_InvalidPrefixRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.scanKeys({ prefix: 123 })
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

// TestKVAsync_ScanKeys_FractionalLimitRejectsPromise verifies that kv async scan keys fractional limit rejects its promise.
func TestKVAsync_ScanKeys_FractionalLimitRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.scanKeys({ limit: 1.5 })
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

// TestKVAsync_RandomKeys_ResolvesKeys verifies that kv async random keys resolves keys.
func TestKVAsync_RandomKeys_ResolvesKeys(t *testing.T) {
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
			.then(() => __kv.randomKeys({ prefix: "user:", count: 2 }))
			.then((keys) => {
				if (!Array.isArray(keys) || keys.length !== 2) {
					throw new Error("randomKeys must resolve an array with requested size");
				}

				const sorted = [...keys].sort();
				const expected = ["user:1", "user:2"];
				if (JSON.stringify(sorted) !== JSON.stringify(expected)) {
					throw new Error("unexpected randomKeys result: " + JSON.stringify(keys));
				}
			});
	`)
}

// TestKVAsync_RandomKeys_EmptyReturnsEmptyArray verifies that kv async random keys empty returns empty array.
func TestKVAsync_RandomKeys_EmptyReturnsEmptyArray(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.randomKeys({ prefix: "missing:", count: 3 })
			.then((keys) => {
				if (!Array.isArray(keys) || keys.length !== 0) {
					throw new Error("expected empty randomKeys result");
				}
			});
	`)
}

// TestKVAsync_RandomKeys_UniqueNoDuplicates verifies that kv async random keys unique no duplicates.
func TestKVAsync_RandomKeys_UniqueNoDuplicates(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.setMany({
			"user:1": "v1",
			"user:2": "v2",
			"user:3": "v3",
		})
			.then(() => __kv.randomKeys({ prefix: "user:", count: 2, unique: true }))
			.then((keys) => {
				if (!Array.isArray(keys) || keys.length !== 2) {
					throw new Error("unexpected keys length");
				}

				if (new Set(keys).size !== keys.length) {
					throw new Error("unique randomKeys must not contain duplicates");
				}
			});
	`)
}

// TestKVAsync_RandomKeys_CountLargerThanAvailableReturnsAvailable verifies that kv async random keys count larger than available returns available.
func TestKVAsync_RandomKeys_CountLargerThanAvailableReturnsAvailable(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.setMany({
			"user:1": "v1",
			"user:2": "v2",
		})
			.then(() => __kv.randomKeys({ prefix: "user:", count: 10, unique: true }))
			.then((keys) => {
				if (!Array.isArray(keys) || keys.length !== 2) {
					throw new Error("expected all available keys");
				}

				const sorted = [...keys].sort();
				const expected = ["user:1", "user:2"];
				if (JSON.stringify(sorted) !== JSON.stringify(expected)) {
					throw new Error("unexpected randomKeys result");
				}
			});
	`)
}

// TestKVAsync_RandomKeys_NonUniqueSingleCandidateRepeats verifies that kv async random keys non unique single candidate repeats.
func TestKVAsync_RandomKeys_NonUniqueSingleCandidateRepeats(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.set("user:1", "v1")
			.then(() => __kv.randomKeys({ prefix: "user:", count: 5, unique: false }))
			.then((keys) => {
				const expected = ["user:1", "user:1", "user:1", "user:1", "user:1"];
				if (JSON.stringify(keys) !== JSON.stringify(expected)) {
					throw new Error("non-unique sampling should repeat single candidate");
				}
			});
	`)
}

// TestKVAsync_RandomKeys_InvalidShapeRejectsPromise verifies that kv async random keys invalid shape rejects its promise.
func TestKVAsync_RandomKeys_InvalidShapeRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.randomKeys([])
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

// TestKVAsync_RandomKeys_MissingCountRejectsPromise verifies that kv async random keys missing count rejects its promise.
func TestKVAsync_RandomKeys_MissingCountRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.randomKeys({ prefix: "user:" })
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

// TestKVAsync_RandomKeys_NonPositiveCountRejectsPromise verifies that kv async random keys non positive count rejects its promise.
func TestKVAsync_RandomKeys_NonPositiveCountRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		function expectInvalidOptions(promise) {
			return promise
				.then(() => {
					throw new Error("expected rejection");
				})
				.catch((err) => {
					if (!err || err.name !== "InvalidOptionsError") {
						throw new Error("unexpected error class: " + String(err && err.name));
					}
				});
		}

		Promise.all([
			expectInvalidOptions(__kv.randomKeys({ count: 0 })),
			expectInvalidOptions(__kv.randomKeys({ count: -1 })),
		]);
	`)
}

// TestKVAsync_RandomKeys_FractionalCountRejectsPromise verifies that kv async random keys fractional count rejects its promise.
func TestKVAsync_RandomKeys_FractionalCountRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.randomKeys({ count: 1.5 })
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

// TestKVAsync_RandomKeys_TooLargeCountRejectsPromise verifies that kv async random keys too large count rejects its promise.
func TestKVAsync_RandomKeys_TooLargeCountRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.randomKeys({ count: Number.MAX_SAFE_INTEGER, unique: false })
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidOptionsError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
				if (!String(err.message).includes("less than or equal")) {
					throw new Error("unexpected error message: " + String(err.message));
				}
			});
	`)
}

// TestKVAsync_RandomKeys_InvalidUniqueRejectsPromise verifies that kv async random keys invalid unique rejects its promise.
func TestKVAsync_RandomKeys_InvalidUniqueRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.randomKeys({ count: 1, unique: "yes" })
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

// TestKVAsync_DeleteByPrefix_DeletesLimitedBatchAndReportsNotDone verifies that kv async delete by prefix deletes limited batch and reports not done.
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

// TestKVAsync_DeleteByPrefix_RepeatedCallsReportDone verifies that kv async delete by prefix repeated calls report done.
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

// TestKVAsync_DeleteByPrefix_InvalidShapeRejectsPromise verifies that kv async delete by prefix invalid shape rejects its promise.
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

// TestKVAsync_DeleteByPrefix_EmptyPrefixRejectsPromise verifies that kv async delete by prefix empty prefix rejects its promise.
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

// TestKVAsync_DeleteByPrefix_MissingLimitRejectsPromise verifies that kv async delete by prefix missing limit rejects its promise.
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

// TestKVAsync_DeleteByPrefix_InvalidLimitRejectsPromise verifies that kv async delete by prefix invalid limit rejects its promise.
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

// TestKVAsync_DeleteByPrefix_FractionalLimitRejectsPromise verifies that kv async delete by prefix fractional limit rejects its promise.
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

// TestKVAsync_Stats_ResolvesSnapshot verifies that kv async stats resolves snapshot.
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

// TestKVAsync_Stats_NullsUnavailableOptionalFields verifies that kv async stats nulls unavailable optional fields.
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

// TestKVAsync_ReportStats_ResolvesAndEmitsMetrics verifies that kv async report stats resolves and emits metrics.
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

// TestKVAsync_ReportStats_MetricsUnavailable_RejectsPromise verifies that kv async report stats metrics unavailable kv async report stats metrics unavailable rejects its promise.
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

// TestKVAsync_OperationMetrics_EmitsExpectedSamples verifies that kv async operation metrics emits expected samples.
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

// TestKVAsync_OperationMetrics_EmitsAsyncInFlight verifies that kv async operation metrics emits async in flight.
func TestKVAsync_OperationMetrics_EmitsAsyncInFlight(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	registry := runtime.VU.InitEnv().Registry
	rootTags := registry.RootTagSet()

	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	var err error

	kv.operationMetrics, err = newKVOperationMetrics(registry, Options{
		Backend:       BackendMemory,
		Serialization: SerializationJSON,
		TrackKeys:     true,
		Metrics:       &MetricsOptions{Operations: true},
	})
	require.NoError(t, err)

	samples := make(chan k6metrics.SampleContainer, 16)
	runtime.MoveToVUContext(&lib.State{
		BuiltinMetrics: runtime.BuiltinMetrics,
		Samples:        samples,
		Tags:           lib.NewVUStateTags(rootTags),
	})

	runKVScript(t, runtime, kv, `
		__kv.set("async:in-flight", "value")
			.then((value) => {
				if (value !== "value") {
					throw new Error("unexpected resolved value");
				}
			});
	`)

	var values []float64

	for _, container := range k6metrics.GetBufferedSamples(samples) {
		for _, sample := range container.GetSamples() {
			if sample.Metric.Name != metricKVAsyncInFlight {
				continue
			}

			values = append(values, sample.Value)

			_, hasOp := sample.Tags.Get(tagOp)
			assert.False(t, hasOp, "async saturation metric must not carry per-operation labels")

			backend, hasBackend := sample.Tags.Get(tagBackend)
			require.True(t, hasBackend)
			assert.Equal(t, BackendMemory, backend)

			trackKeys, hasTrackKeys := sample.Tags.Get(tagTrackKeys)
			require.True(t, hasTrackKeys)
			assert.Equal(t, "true", trackKeys)

			serialization, hasSerialization := sample.Tags.Get(tagSerialization)
			require.True(t, hasSerialization)
			assert.Equal(t, "json", serialization)
		}
	}

	require.Len(t, values, 2)
	assert.ElementsMatch(t, []float64{1, 0}, values)
}

// TestKVAsync_OperationMetrics_EmitsClosedHandlePreflightError verifies that kv async operation metrics emits closed handle preflight error.
func TestKVAsync_OperationMetrics_EmitsClosedHandlePreflightError(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))
	samples := attachOperationMetricsForTest(t, runtime, kv)

	require.NoError(t, kv.Close())

	runKVScript(t, runtime, kv, `
		__kv.get("closed:metrics")
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "StoreClosedError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)

	assertOperationMetricsError(t, samples, opGet, StoreClosedError)
}

// TestKVAsync_OperationMetrics_EmitsDatabaseNotOpenPreflightError verifies that kv async operation metrics emits database not open preflight error.
func TestKVAsync_OperationMetrics_EmitsDatabaseNotOpenPreflightError(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, nil)
	samples := attachOperationMetricsForTest(t, runtime, kv)

	runKVScript(t, runtime, kv, `
		__kv.get("not-open:metrics")
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "DatabaseNotOpenError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)

	assertOperationMetricsError(t, samples, opGet, DatabaseNotOpenError)
}

// TestKVAsync_ContextCanceledPreflightRejectsPromise verifies that kv async context canceled preflight rejects its promise.
func TestKVAsync_ContextCanceledPreflightRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	kv := NewKV(
		contextOverrideVU{
			VU:  runtime.VU,
			ctx: ctx,
		},
		store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
	)

	runKVScript(t, runtime, kv, `
		__kv.get("preflight:canceled")
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "OperationCanceledError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)
}

// TestAsyncOperation_ContextCanceledRejectsOnceAndDecrementsInFlight verifies that async operation context canceled rejects once and decrements in flight.
func TestAsyncOperation_ContextCanceledRejectsOnceAndDecrementsInFlight(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(
		runtime.VU,
		canceledGetStore{
			Store: store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
		},
	)

	samples := attachOperationMetricsForTest(t, runtime, kv)

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
		let rejectionCount = 0;

		__kv.get("cancelled:once")
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				rejectionCount += 1;
				if (rejectionCount !== 1) {
					throw new Error("promise rejected more than once");
				}
				if (!err || err.name !== "OperationCanceledError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)

	metricHits := make(map[string]int)

	var asyncInFlightValues []float64

	for _, container := range k6metrics.GetBufferedSamples(samples) {
		for _, sample := range container.GetSamples() {
			switch sample.Metric.Name {
			case metricKVAsyncInFlight:
				metricHits[metricKVAsyncInFlight]++

				asyncInFlightValues = append(asyncInFlightValues, sample.Value)
			case metricKVOperationsTotal:
				metricHits[metricKVOperationsTotal]++

				op, hasOp := sample.Tags.Get(tagOp)
				require.True(t, hasOp)
				assert.Equal(t, opGet, op)

				status, hasStatus := sample.Tags.Get(tagStatus)
				require.True(t, hasStatus)
				assert.Equal(t, statusError, status)
				assert.InDelta(t, 1.0, sample.Value, 1e-9)
			case metricKVOperationFailed:
				metricHits[metricKVOperationFailed]++

				op, hasOp := sample.Tags.Get(tagOp)
				require.True(t, hasOp)
				assert.Equal(t, opGet, op)
				assert.InDelta(t, 1.0, sample.Value, 1e-9)
			case metricKVErrorsTotal:
				metricHits[metricKVErrorsTotal]++

				errorType, hasErrorType := sample.Tags.Get(tagErrorType)
				require.True(t, hasErrorType)
				assert.Equal(t, string(OperationCanceledError), errorType)
				assert.InDelta(t, 1.0, sample.Value, 1e-9)
			}
		}
	}

	assert.EqualValues(t, 1, registerCount.Load(), "RegisterCallback must be called exactly once")
	assert.EqualValues(t, 1, enqueueCount.Load(), "enqueue callback must be called exactly once")
	assert.Equal(t, 1, metricHits[metricKVOperationsTotal])
	assert.Equal(t, 1, metricHits[metricKVOperationFailed])
	assert.Equal(t, 1, metricHits[metricKVErrorsTotal])
	require.Len(t, asyncInFlightValues, 2)
	assert.ElementsMatch(t, []float64{1, 0}, asyncInFlightValues)
}

// TestOperationMetrics_ContextCanceledUsesOperationCanceledError verifies that operation metrics context canceled uses operation canceled error.
func TestOperationMetrics_ContextCanceledUsesOperationCanceledError(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(
		runtime.VU,
		canceledGetStore{
			Store: store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
		},
	)

	samples := attachOperationMetricsForTest(t, runtime, kv)

	runKVScript(t, runtime, kv, `
		__kv.get("cancelled:operation")
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "OperationCanceledError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)

	metricHits := make(map[string]int)

	var asyncInFlightValues []float64

	for _, container := range k6metrics.GetBufferedSamples(samples) {
		for _, sample := range container.GetSamples() {
			switch sample.Metric.Name {
			case metricKVAsyncInFlight:
				metricHits[metricKVAsyncInFlight]++

				asyncInFlightValues = append(asyncInFlightValues, sample.Value)

				_, hasOp := sample.Tags.Get(tagOp)
				assert.False(t, hasOp, "async in-flight metric must not carry per-operation labels")
			case metricKVOperationsTotal:
				metricHits[metricKVOperationsTotal]++

				op, hasOp := sample.Tags.Get(tagOp)
				require.True(t, hasOp)
				assert.Equal(t, opGet, op)

				status, hasStatus := sample.Tags.Get(tagStatus)
				require.True(t, hasStatus)
				assert.Equal(t, statusError, status)
			case metricKVOperationDuration:
				metricHits[metricKVOperationDuration]++

				op, hasOp := sample.Tags.Get(tagOp)
				require.True(t, hasOp)
				assert.Equal(t, opGet, op)

				status, hasStatus := sample.Tags.Get(tagStatus)
				require.True(t, hasStatus)
				assert.Equal(t, statusError, status)
			case metricKVOperationFailed:
				metricHits[metricKVOperationFailed]++

				op, hasOp := sample.Tags.Get(tagOp)
				require.True(t, hasOp)
				assert.Equal(t, opGet, op)
				assert.InDelta(t, 1.0, sample.Value, 1e-9)
			case metricKVErrorsTotal:
				metricHits[metricKVErrorsTotal]++

				op, hasOp := sample.Tags.Get(tagOp)
				require.True(t, hasOp)
				assert.Equal(t, opGet, op)

				errorType, hasErrorType := sample.Tags.Get(tagErrorType)
				require.True(t, hasErrorType)
				assert.Equal(t, string(OperationCanceledError), errorType)
				assert.InDelta(t, 1.0, sample.Value, 1e-9)
			case metricKVEmptyResult:
				metricHits[metricKVEmptyResult]++
			}
		}
	}

	assert.Equal(t, 1, metricHits[metricKVOperationsTotal])
	assert.Equal(t, 1, metricHits[metricKVOperationDuration])
	assert.Equal(t, 1, metricHits[metricKVOperationFailed])
	assert.Equal(t, 1, metricHits[metricKVErrorsTotal])
	assert.Zero(t, metricHits[metricKVEmptyResult])
	require.Len(t, asyncInFlightValues, 2)
	assert.ElementsMatch(t, []float64{1, 0}, asyncInFlightValues)
}

// TestKVAsync_OperationMetrics_EmitsPanicAsUnknownError verifies that kv async operation metrics emits panic as unknown error.
func TestKVAsync_OperationMetrics_EmitsPanicAsUnknownError(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	registry := runtime.VU.InitEnv().Registry
	rootTags := registry.RootTagSet()

	panicStore := panicGetStore{
		Store: store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
	}
	kv := NewKV(runtime.VU, panicStore)

	var err error

	kv.operationMetrics, err = newKVOperationMetrics(registry, Options{
		Backend:       BackendMemory,
		Serialization: SerializationJSON,
		TrackKeys:     true,
		Metrics:       &MetricsOptions{Operations: true},
	})
	require.NoError(t, err)

	samples := make(chan k6metrics.SampleContainer, 16)
	runtime.MoveToVUContext(&lib.State{
		BuiltinMetrics: runtime.BuiltinMetrics,
		Samples:        samples,
		Tags:           lib.NewVUStateTags(rootTags),
	})

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

	metricHits := make(map[string]int)

	var seenUnknownError bool

	for _, container := range k6metrics.GetBufferedSamples(samples) {
		for _, sample := range container.GetSamples() {
			metricHits[sample.Metric.Name]++

			op, hasOp := sample.Tags.Get(tagOp)
			if sample.Metric.Name != metricKVAsyncInFlight {
				require.True(t, hasOp)
				assert.Equal(t, opGet, op)
			}

			switch sample.Metric.Name {
			case metricKVOperationsTotal:
				status, ok := sample.Tags.Get(tagStatus)
				require.True(t, ok)
				assert.Equal(t, statusError, status)
				assert.InDelta(t, 1.0, sample.Value, 1e-9)
			case metricKVOperationDuration:
				status, ok := sample.Tags.Get(tagStatus)
				require.True(t, ok)
				assert.Equal(t, statusError, status)
			case metricKVOperationFailed:
				assert.InDelta(t, 1.0, sample.Value, 1e-9)
			case metricKVErrorsTotal:
				errorType, ok := sample.Tags.Get(tagErrorType)
				require.True(t, ok)
				assert.Equal(t, string(UnknownError), errorType)
				assert.InDelta(t, 1.0, sample.Value, 1e-9)

				seenUnknownError = true
			}
		}
	}

	assert.Equal(t, 1, metricHits[metricKVOperationsTotal])
	assert.Equal(t, 1, metricHits[metricKVOperationDuration])
	assert.Equal(t, 1, metricHits[metricKVOperationFailed])
	assert.Equal(t, 1, metricHits[metricKVErrorsTotal])
	assert.Zero(t, metricHits[metricKVEmptyResult])
	assert.True(t, seenUnknownError)
}

// TestKVAsync_OperationMetrics_EmitsValidationError verifies that kv async operation metrics emits validation error.
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

// TestKVAsync_SetMany_OperationMetrics verifies that kv async set many operation metrics.
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

// TestKVAsync_GetMany_OperationMetrics verifies that kv async get many operation metrics.
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

// TestKVAsync_DeleteMany_OperationMetrics verifies that kv async delete many operation metrics.
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

// TestKVAsync_DeleteByPrefix_OperationMetrics verifies that kv async delete by prefix operation metrics.
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

// TestKVAsync_ListKeys_OperationMetrics verifies that kv async list keys operation metrics.
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

// TestKVAsync_ScanKeys_OperationMetrics verifies that kv async scan keys operation metrics.
func TestKVAsync_ScanKeys_OperationMetrics(t *testing.T) {
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
			.then(() => __kv.scanKeys({ prefix: "user:", limit: 1 }))
			.then((page) => {
				if (!page || !Array.isArray(page.keys) || page.keys.length !== 1) {
					throw new Error("unexpected scanKeys result");
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
			if !hasOp || op != opScanKeys {
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

// TestKVAsync_RandomKeys_OperationMetrics verifies that kv async random keys operation metrics.
func TestKVAsync_RandomKeys_OperationMetrics(t *testing.T) {
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
			.then(() => __kv.randomKeys({ prefix: "user:", count: 2 }))
			.then((keys) => {
				if (!Array.isArray(keys) || keys.length !== 2) {
					throw new Error("unexpected randomKeys result");
				}
			});
	`)

	var (
		seenTotal    bool
		seenDuration bool
		seenFailed   bool
		seenEmpty    bool
	)

	for _, container := range k6metrics.GetBufferedSamples(samples) {
		for _, sample := range container.GetSamples() {
			op, hasOp := sample.Tags.Get(tagOp)
			if !hasOp || op != opRandomKeys {
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
			case metricKVEmptyResult:
				assert.InDelta(t, 0.0, sample.Value, 1e-9)

				seenEmpty = true
			}
		}
	}

	assert.True(t, seenTotal)
	assert.True(t, seenDuration)
	assert.True(t, seenFailed)
	assert.True(t, seenEmpty, "randomKeys should emit empty-result metric samples")
}

// TestKVAsync_RandomKeys_EmptyResultMetrics verifies that kv async random keys empty result metrics.
func TestKVAsync_RandomKeys_EmptyResultMetrics(t *testing.T) {
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
		__kv.randomKeys({ prefix: "missing:", count: 5 })
			.then((keys) => {
				if (!Array.isArray(keys) || keys.length !== 0) {
					throw new Error("expected empty randomKeys result");
				}
			});
	`)

	var (
		seenTotal    bool
		seenDuration bool
		seenFailed   bool
		seenEmpty    bool
	)

	for _, container := range k6metrics.GetBufferedSamples(samples) {
		for _, sample := range container.GetSamples() {
			op, hasOp := sample.Tags.Get(tagOp)
			if !hasOp || op != opRandomKeys {
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
			case metricKVEmptyResult:
				assert.InDelta(t, 1.0, sample.Value, 1e-9)

				seenEmpty = true
			}
		}
	}

	assert.True(t, seenTotal)
	assert.True(t, seenDuration)
	assert.True(t, seenFailed)
	assert.True(t, seenEmpty, "empty randomKeys result should emit empty-result metrics")
}

// TestKVAsync_RandomKeys_InvalidOptionsMetrics verifies that kv async random keys invalid options metrics.
func TestKVAsync_RandomKeys_InvalidOptionsMetrics(t *testing.T) {
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
		__kv.randomKeys({ count: Number.MAX_SAFE_INTEGER, unique: false })
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
			if !ok || op != opRandomKeys {
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
	assert.False(t, seenEmpty, "failed randomKeys should not emit empty-result metrics")
	assert.False(t, seenFailedZero, "failed randomKeys should not emit failed=0 samples")
}

// TestKVAsync_DeleteByPrefix_InvalidOptionsMetrics verifies that kv async delete by prefix invalid options metrics.
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

// TestKVAsync_ListKeys_InvalidShapeMetrics verifies that kv async list keys invalid shape metrics.
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

// TestKVAsync_ScanKeys_InvalidShapeMetrics verifies that kv async scan keys invalid shape metrics.
func TestKVAsync_ScanKeys_InvalidShapeMetrics(t *testing.T) {
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
		__kv.scanKeys([])
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
			if !ok || op != opScanKeys {
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
	assert.False(t, seenEmpty, "scanKeys should not emit empty-result metrics")
	assert.False(t, seenFailedZero, "failed scanKeys should not emit failed=0 samples")
}

// TestKVAsync_ScanKeys_InvalidCursorMetrics verifies that kv async scan keys invalid cursor metrics.
func TestKVAsync_ScanKeys_InvalidCursorMetrics(t *testing.T) {
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
		__kv.scanKeys({ cursor: "bad" })
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "InvalidCursorError") {
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
			if !ok || op != opScanKeys {
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
				assert.Equal(t, string(InvalidCursorError), errorType)
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
	assert.False(t, seenEmpty, "scanKeys should not emit empty-result metrics")
	assert.False(t, seenFailedZero, "failed scanKeys should not emit failed=0 samples")
}

// TestKVAsync_DeleteMany_InvalidShapeMetrics verifies that kv async delete many invalid shape metrics.
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

// TestKVAsync_DeleteMany_EmptyKeyMetrics verifies that kv async delete many empty key metrics.
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

// TestKVAsync_GetMany_InvalidShapeMetrics verifies that kv async get many invalid shape metrics.
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

// TestKVAsync_SetMany_ValidationFailureMetrics verifies that kv async set many validation failure metrics.
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

// TestKVAsync_SetMany_SerializationFailureMetrics verifies that kv async set many serialization failure metrics.
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
				if (!err || err.name !== "InvalidOptionsError") {
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
}

// TestKVAsync_ExportJSONL_ResolvesSummary verifies that kv async export jsonl resolves summary.
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

// TestKVAsync_ExportJSONL_InvalidShapeRejectsPromise verifies that kv async export jsonl invalid shape rejects its promise.
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

// TestKVAsync_ExportJSONL_MissingFileNameRejectsPromise verifies that kv async export jsonl missing file name rejects its promise.
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

// TestKVAsync_ExportJSONL_FractionalLimitRejectsPromise verifies that kv async export jsonl fractional limit rejects its promise.
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

// TestKVAsync_ExportJSONL_OperationMetrics verifies that kv async export jsonl operation metrics.
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

// TestKVAsync_ExportJSONL_InvalidOptionsMetrics verifies that kv async export jsonl invalid options metrics.
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

// TestKVAsync_ImportJSONL_ResolvesSummary verifies that kv async import jsonl resolves summary.
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

// TestKVAsync_ImportJSONL_InvalidShapeRejectsPromise verifies that kv async import jsonl invalid shape rejects its promise.
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

// TestKVAsync_ImportJSONL_MissingFileNameRejectsPromise verifies that kv async import jsonl missing file name rejects its promise.
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

// TestKVAsync_ImportJSONL_FractionalLimitRejectsPromise verifies that kv async import jsonl fractional limit rejects its promise.
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

// TestKVAsync_ImportJSONL_FractionalBatchSizeRejectsPromise verifies that kv async import jsonl fractional batch size rejects its promise.
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

// TestKVAsync_ImportJSONL_FileNotFoundRejectsPromise verifies that kv async import jsonl file not found rejects its promise.
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

// TestKVAsync_ImportJSONL_MalformedJSONRejectsPromise verifies that kv async import jsonl malformed json rejects its promise.
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
				if (!String(err.message).includes("importJSONL failed after 1 records")) {
					throw new Error("missing import progress: " + String(err.message));
				}
				if (!String(err.message).includes("previous batches may already be committed")) {
					throw new Error("missing partial commit warning: " + String(err.message));
				}
				if (!String(err.message).includes("line 2")) {
					throw new Error("missing failing line: " + String(err.message));
				}
				return __kv.getMany(["user:1", "user:2"]);
			})
			.then((items) => {
				if (!Array.isArray(items) || items.length !== 2) {
					throw new Error("unexpected getMany shape");
				}
				if (items[0].exists !== true) {
					throw new Error("committed batch was not preserved");
				}
				if (items[1].exists !== false) {
					throw new Error("failed record must not be imported");
				}
			});
	`)
}

// TestKVAsync_ImportJSONL_BlankLineRejectsPromise verifies that kv async import jsonl blank line rejects its promise.
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

// TestKVAsync_ImportJSONL_ContextCanceledDuringOperationRejectsPromise verifies that kv async import jsonl context canceled during operation rejects its promise.
func TestKVAsync_ImportJSONL_ContextCanceledDuringOperationRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)

	ctx, cancel := context.WithCancel(context.Background())
	backing := store.NewSerializedStore(
		store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
		store.NewJSONSerializer(),
	)

	kv := NewKV(
		contextOverrideVU{
			VU:  runtime.VU,
			ctx: ctx,
		},
		&cancelOnFirstSetManyStore{
			Store:  backing,
			cancel: cancel,
		},
	)

	fileName := filepath.Join(t.TempDir(), "cancel-import.jsonl")
	//nolint:forbidigo // test fixture creation requires file I/O.
	require.NoError(t, os.WriteFile(fileName, []byte(
		`{"key":"user:1","value":{"name":"Alice"}}`+"\n"+
			`{"key":"user:2","value":{"name":"Bob"}}`+"\n",
	), 0o644))
	require.NoError(t, runtime.VU.Runtime().Set("fileName", fileName))

	runKVScript(t, runtime, kv, `
		__kv.importJSONL({ fileName: fileName, batchSize: 1 })
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "OperationCanceledError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)
}

// TestKVAsync_ImportCSV_ContextCanceledDuringOperationRejectsPromise verifies that kv async import csv context canceled during operation rejects its promise.
func TestKVAsync_ImportCSV_ContextCanceledDuringOperationRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)

	ctx, cancel := context.WithCancel(context.Background())
	backing := store.NewSerializedStore(
		store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
		store.NewJSONSerializer(),
	)

	kv := NewKV(
		contextOverrideVU{
			VU:  runtime.VU,
			ctx: ctx,
		},
		&cancelOnFirstSetManyStore{
			Store:  backing,
			cancel: cancel,
		},
	)

	fileName := filepath.Join(t.TempDir(), "cancel-import.csv")
	//nolint:forbidigo // test fixture creation requires file I/O.
	require.NoError(t, os.WriteFile(fileName, []byte(
		"id,name\n"+
			"user:1,Alice\n"+
			"user:2,Bob\n",
	), 0o644))
	require.NoError(t, runtime.VU.Runtime().Set("fileName", fileName))

	runKVScript(t, runtime, kv, `
		__kv.importCSV({ fileName: fileName, keyColumn: "id", batchSize: 1 })
			.then(() => {
				throw new Error("expected rejection");
			})
			.catch((err) => {
				if (!err || err.name !== "OperationCanceledError") {
					throw new Error("unexpected error class: " + String(err && err.name));
				}
			});
	`)
}

// TestKVAsync_ImportJSONL_OperationMetrics verifies that kv async import jsonl operation metrics.
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

// TestKVAsync_ImportJSONL_InvalidOptionsMetrics verifies that kv async import jsonl invalid options metrics.
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

// TestKVAsync_AllOptionsMethods_InvalidOptionsType_RejectsPromise verifies that kv async all options methods invalid options type kv async all options methods invalid options type rejects its promise.
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
			expectInvalidOptions(__kv.scanKeys("bad"), "scanKeys"),
			expectInvalidOptions(__kv.list("bad"), "list"),
			expectInvalidOptions(__kv.listKeys("bad"), "listKeys"),
			expectInvalidOptions(__kv.randomKeys("bad"), "randomKeys"),
			expectInvalidOptions(__kv.exportJSONL("bad"), "exportJSONL"),
			expectInvalidOptions(__kv.exportCSV("bad"), "exportCSV"),
			expectInvalidOptions(__kv.importJSONL("bad"), "importJSONL"),
			expectInvalidOptions(__kv.validateJSONL("bad"), "validateJSONL"),
			expectInvalidOptions(__kv.validateCSV("bad"), "validateCSV"),
			expectInvalidOptions(__kv.deleteByPrefix("bad"), "deleteByPrefix"),
			expectInvalidOptions(__kv.randomKey("bad"), "randomKey"),
			expectInvalidOptions(__kv.count("bad"), "count"),
			expectInvalidOptions(__kv.allocationStats("bad"), "allocationStats"),
			expectInvalidOptions(__kv.backup("bad"), "backup"),
			expectInvalidOptions(__kv.restore("bad"), "restore"),
			expectInvalidOptions(__kv.compareAndSwapDetailed("k", null, "v", "bad"), "compareAndSwapDetailed"),
			expectInvalidOptions(__kv.compareAndDeleteDetailed("k", "v", "bad"), "compareAndDeleteDetailed")
		]);
	`)
}

// TestKVAsync_AllOptionsMethods_InvalidOptionFieldType_RejectsPromise verifies that kv async all options methods invalid option field type kv async all options methods invalid option field type rejects its promise.
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
			expectInvalidOptions(__kv.scanKeys({ prefix: 123 }), "scanKeys.prefix"),
			expectInvalidOptions(__kv.scanKeys({ limit: "10" }), "scanKeys.limit"),
			expectInvalidOptions(__kv.scanKeys({ cursor: 99 }), "scanKeys.cursor"),
			expectInvalidOptions(__kv.list({ prefix: true }), "list.prefix"),
			expectInvalidOptions(__kv.list({ limit: "10" }), "list.limit"),
			expectInvalidOptions(__kv.listKeys({ prefix: true }), "listKeys.prefix"),
			expectInvalidOptions(__kv.listKeys({ limit: "10" }), "listKeys.limit"),
			expectInvalidOptions(__kv.randomKeys({ prefix: 7, count: 1 }), "randomKeys.prefix"),
			expectInvalidOptions(__kv.randomKeys({ count: "10" }), "randomKeys.count"),
			expectInvalidOptions(__kv.randomKeys({ count: 1, unique: "yes" }), "randomKeys.unique"),
			expectInvalidOptions(__kv.exportJSONL({ fileName: 100 }), "exportJSONL.fileName"),
			expectInvalidOptions(__kv.exportJSONL({ fileName: "./x.jsonl", prefix: true }), "exportJSONL.prefix"),
			expectInvalidOptions(__kv.exportJSONL({ fileName: "./x.jsonl", limit: "10" }), "exportJSONL.limit"),
			expectInvalidOptions(__kv.exportCSV({ fileName: 100, columns: ["status"] }), "exportCSV.fileName"),
			expectInvalidOptions(__kv.exportCSV({ fileName: "./x.csv", columns: "bad" }), "exportCSV.columns"),
			expectInvalidOptions(
				__kv.exportCSV({ fileName: "./x.csv", columns: ["status"], delimiter: 1 }),
				"exportCSV.delimiter"
			),
			expectInvalidOptions(__kv.importJSONL({ fileName: 100 }), "importJSONL.fileName"),
			expectInvalidOptions(__kv.importJSONL({ fileName: "./x.jsonl", limit: "10" }), "importJSONL.limit"),
			expectInvalidOptions(__kv.importJSONL({ fileName: "./x.jsonl", batchSize: "10" }), "importJSONL.batchSize"),
			expectInvalidOptions(__kv.validateJSONL({ fileName: 100 }), "validateJSONL.fileName"),
			expectInvalidOptions(__kv.validateCSV({ fileName: 100 }), "validateCSV.fileName"),
			expectInvalidOptions(__kv.validateCSV({ fileName: "./x.csv", delimiter: 1 }), "validateCSV.delimiter"),
			expectInvalidOptions(__kv.deleteByPrefix({ prefix: true, limit: 1 }), "deleteByPrefix.prefix"),
			expectInvalidOptions(__kv.deleteByPrefix({ prefix: "tmp:", limit: "10" }), "deleteByPrefix.limit"),
			expectInvalidOptions(__kv.randomKey({ prefix: 7 }), "randomKey.prefix"),
			expectInvalidOptions(__kv.count({ prefix: 7 }), "count.prefix"),
			expectInvalidOptions(__kv.allocationStats({ prefix: 7 }), "allocationStats.prefix"),
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

// TestKVAsync_KeyMethods_InvalidKeyType_RejectsPromise verifies that kv async key methods invalid key type kv async key methods invalid key type rejects its promise.
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

// TestKVAsync_KeyMethods_EmptyKey_RejectsPromise verifies that kv async key methods empty key kv async key methods empty key rejects its promise.
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

// TestKVAsync_Set_Concurrent_NoRaceOrPanic verifies that kv async set concurrent no race or panic.
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

// TestKVAsync_PopRandom_ResolvesEntry verifies that kv async pop random resolves entry.
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

// TestKVAsync_PopRandom_EmptyResolvesNull verifies that kv async pop random empty resolves null.
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

// TestKVAsync_ClaimRandom_ResolvesClaim verifies that kv async claim random resolves claim.
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

// TestKVAsync_ClaimRandom_EmptyResolvesNull verifies that kv async claim random empty resolves null.
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

// TestKVAsync_ReleaseClaim_ReturnsTrue verifies that kv async release claim returns true.
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

// TestKVAsync_CompleteClaim_ReturnsTrue verifies that kv async complete claim returns true.
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

// TestKVAsync_ClaimKey_ResolvesClaim verifies that kv async claim key resolves claim.
func TestKVAsync_ClaimKey_ResolvesClaim(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.set("user:1", "Oleg")
			.then(() => __kv.claimKey("user:1", { owner: "vu:1", ttl: 30000 }))
			.then((claim) => {
				if (!claim) {
					throw new Error("missing claim");
				}
				if (claim.key !== "user:1") {
					throw new Error("wrong key");
				}
			});
	`)
}

// TestKVAsync_ClaimRandomMany_ResolvesClaims verifies that kv async claim random many resolves claims.
func TestKVAsync_ClaimRandomMany_ResolvesClaims(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.setMany({
			"user:1": "Alice",
			"user:2": "Bob",
			"user:3": "Carol"
		})
			.then(() => __kv.claimRandomMany({ prefix: "user:", count: 2, ttl: 30000 }))
			.then((claims) => {
				if (!Array.isArray(claims)) {
					throw new Error("claims must be an array");
				}
				if (claims.length !== 2) {
					throw new Error("wrong claims count");
				}
			});
	`)
}

// TestKVAsync_PopRandomMany_ResolvesEntries verifies that kv async pop random many resolves entries.
func TestKVAsync_PopRandomMany_ResolvesEntries(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.setMany({
			"user:1": "Alice",
			"user:2": "Bob",
			"user:3": "Carol"
		})
			.then(() => __kv.popRandomMany({ prefix: "user:", count: 2 }))
			.then((entries) => {
				if (!Array.isArray(entries)) {
					throw new Error("entries must be an array");
				}
				if (entries.length !== 2) {
					throw new Error("wrong entries count");
				}
			});
	`)
}

// TestKVAsync_RenewClaim_ReturnsTrue verifies that kv async renew claim returns true.
func TestKVAsync_RenewClaim_ReturnsTrue(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		let initialClaim;
		__kv.set("user:1", "Oleg")
			.then(() => __kv.claimRandom({ prefix: "user:", ttl: 200 }))
			.then((claim) => {
				if (!claim) {
					throw new Error("missing claim");
				}
				initialClaim = claim;
				return __kv.renewClaim(claim, { ttl: 30000 });
			})
			.then((renewed) => {
				if (renewed !== true) {
					throw new Error("expected true");
				}
				return __kv.releaseClaim(initialClaim);
			})
			.then((released) => {
				if (released !== true) {
					throw new Error("expected release=true");
				}
			});
	`)
}

// TestKVAsync_ReleaseClaims_ReturnsPartialResult verifies that kv async release claims returns partial result.
func TestKVAsync_ReleaseClaims_ReturnsPartialResult(t *testing.T) {
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
				return __kv.releaseClaims([claim, claim]);
			})
			.then((result) => {
				if (result.attempted !== 2) {
					throw new Error("wrong attempted");
				}
				if (result.released !== 1) {
					throw new Error("wrong released");
				}
				if (!Array.isArray(result.failed) || result.failed.length !== 1) {
					throw new Error("wrong failed length");
				}
				if (result.failed[0].name !== "ClaimNotUpdated") {
					throw new Error("wrong failure name");
				}
				if (result.failed[0].index !== 1) {
					throw new Error("wrong failure index");
				}
			});
	`)
}

// TestKVAsync_CompleteClaims_ReturnsPartialResult verifies that kv async complete claims returns partial result.
func TestKVAsync_CompleteClaims_ReturnsPartialResult(t *testing.T) {
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
				return __kv.completeClaims([claim, claim], { deleteKey: true });
			})
			.then((result) => {
				if (result.attempted !== 2) {
					throw new Error("wrong attempted");
				}
				if (result.completed !== 1) {
					throw new Error("wrong completed");
				}
				if (!Array.isArray(result.failed) || result.failed.length !== 1) {
					throw new Error("wrong failed length");
				}
				if (result.failed[0].name !== "ClaimNotUpdated") {
					throw new Error("wrong failure name");
				}
				if (result.failed[0].index !== 1) {
					throw new Error("wrong failure index");
				}
			});
	`)
}

// TestKVAsync_RenewClaims_ReturnsPartialResult verifies that kv async renew claims returns partial result.
func TestKVAsync_RenewClaims_ReturnsPartialResult(t *testing.T) {
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
				const stale = {
					id: claim.id,
					key: claim.key,
					token: claim.token + 1
				};
				return __kv.renewClaims([claim, stale], { ttl: 30000 });
			})
			.then((result) => {
				if (result.attempted !== 2) {
					throw new Error("wrong attempted");
				}
				if (result.renewed !== 1) {
					throw new Error("wrong renewed");
				}
				if (!Array.isArray(result.failed) || result.failed.length !== 1) {
					throw new Error("wrong failed length");
				}
				if (result.failed[0].name !== "ClaimNotUpdated") {
					throw new Error("wrong failure name");
				}
				if (result.failed[0].index !== 1) {
					throw new Error("wrong failure index");
				}
			});
	`)
}

// TestKVAsync_ClaimKeys_ReportsClaimedBusyMissing verifies that kv async claim keys reports claimed busy missing.
func TestKVAsync_ClaimKeys_ReportsClaimedBusyMissing(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		let busyClaim;
		let claimKeysResult;

		__kv.setMany({
			"users:1": "u1",
			"users:2": "u2"
		})
			.then(() => __kv.claimKey("users:2", { ttl: 30000 }))
			.then((claim) => {
				if (!claim) {
					throw new Error("missing busy claim");
				}
				busyClaim = claim;
				return __kv.claimKeys(["users:1", "users:2", "users:3"], { ttl: 30000 });
			})
			.then((result) => {
				claimKeysResult = result;
				if (!Array.isArray(result.claimed) || result.claimed.length !== 1) {
					throw new Error("wrong claimed length");
				}
				if (!Array.isArray(result.busy) || result.busy.length !== 1 || result.busy[0] !== "users:2") {
					throw new Error("wrong busy payload");
				}
				if (!Array.isArray(result.missing) || result.missing.length !== 1 || result.missing[0] !== "users:3") {
					throw new Error("wrong missing payload");
				}
				return __kv.releaseClaim(busyClaim);
			})
			.then((releasedBusy) => {
				if (releasedBusy !== true) {
					throw new Error("busy claim must be released");
				}
				return Promise.all(claimKeysResult.claimed.map((claim) => __kv.releaseClaim(claim)));
			});
	`)
}

// TestKVAsync_ClaimKeys_AllOrNothingReleasesPartialClaims verifies that kv async claim keys all or nothing releases partial claims.
func TestKVAsync_ClaimKeys_AllOrNothingReleasesPartialClaims(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		let followUpClaim;
		let untouchedClaim;

		__kv.setMany({
			"users:1": "u1",
			"users:2": "u2"
		})
			.then(() => __kv.claimKeys(["users:1", "users:missing", "users:2"], {
				ttl: 30000,
				allOrNothing: true
			}))
			.then((result) => {
				if (!Array.isArray(result.claimed) || result.claimed.length !== 0) {
					throw new Error("claimed must be empty when allOrNothing fails");
				}
				if (!Array.isArray(result.missing) || result.missing.length !== 1) {
					throw new Error("missing must include unmatched key");
				}
				if (result.missing[0] !== "users:missing") {
					throw new Error("missing must preserve first failing key");
				}
				return __kv.claimKey("users:2", { ttl: 30000 });
			})
			.then((claim) => {
				if (!claim) {
					throw new Error("post-failure keys must remain claimable");
				}
				if (claim.token !== 2) {
					throw new Error("allOrNothing must stop after first missing/busy key");
				}
				untouchedClaim = claim;
				return __kv.claimKey("users:1", { ttl: 30000 });
			})
			.then((claim) => {
				if (!claim) {
					throw new Error("allOrNothing rollback must release claimed keys");
				}
				followUpClaim = claim;
				return Promise.all([
					__kv.releaseClaim(untouchedClaim),
					__kv.releaseClaim(followUpClaim)
				]);
			})
			.then((released) => {
				if (!Array.isArray(released) || released.length !== 2) {
					throw new Error("release results must contain both follow-up claims");
				}
				if (released[0] !== true || released[1] !== true) {
					throw new Error("follow-up claim must be releasable");
				}
			});
	`)
}

// TestKVAsync_BatchClaimLifecycle_InvalidPayloadRejectsPromise verifies that kv async batch claim lifecycle invalid payload rejects its promise.
func TestKVAsync_BatchClaimLifecycle_InvalidPayloadRejectsPromise(t *testing.T) {
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
			expectInvalidOptions(__kv.releaseClaims(null), "releaseClaims.claims"),
			expectInvalidOptions(__kv.releaseClaims("bad"), "releaseClaims.claims.type"),
			expectInvalidOptions(__kv.releaseClaims([{}]), "releaseClaims.claims.item"),
			expectInvalidOptions(__kv.completeClaims("bad"), "completeClaims.claims"),
			expectInvalidOptions(
				__kv.completeClaims([{ id: "c", key: "k", token: 1 }], "bad"),
				"completeClaims.options"
			),
			expectInvalidOptions(__kv.renewClaims([{ id: "c", key: "k", token: 1 }], {}), "renewClaims.options.ttl"),
			expectInvalidOptions(__kv.claimKeys(null), "claimKeys.keys"),
			expectInvalidOptions(__kv.claimKeys("bad"), "claimKeys.keys.type"),
			expectInvalidOptions(__kv.claimKeys([""], {}), "claimKeys.keys.empty"),
			expectInvalidOptions(__kv.claimKeys(["users:1", "users:1"], {}), "claimKeys.keys.duplicate"),
			expectInvalidOptions(__kv.claimKeys(["users:1"], { ttl: 0 }), "claimKeys.options.ttl"),
			expectInvalidOptions(__kv.claimKeys(["users:1"], "bad"), "claimKeys.options")
		]);
	`)
}

// TestKVAsync_Claim_InvalidOptions_RejectsPromise verifies that kv async claim invalid options kv async claim invalid options rejects its promise.
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
			expectInvalidOptions(__kv.popRandomMany("bad"), "popRandomMany.options"),
			expectInvalidOptions(__kv.popRandomMany({ count: 0 }), "popRandomMany.count.positive"),
			expectInvalidOptions(__kv.claimRandom({ ttl: 0 }), "claimRandom.ttl.positive"),
			expectInvalidOptions(__kv.claimRandom({ ttl: 1.5 }), "claimRandom.ttl.integer"),
			expectInvalidOptions(__kv.claimRandom({ ttl: Number.MAX_SAFE_INTEGER }), "claimRandom.ttl.max"),
			expectInvalidOptions(__kv.claimRandom({ owner: "o".repeat(257) }), "claimRandom.owner.max"),
			expectInvalidOptions(__kv.claimKey("", { ttl: 1000 }), "claimKey.key.empty"),
			expectInvalidOptions(__kv.claimKey("user:1", { ttl: 0 }), "claimKey.ttl.positive"),
			expectInvalidOptions(__kv.claimRandomMany(), "claimRandomMany.count.required"),
			expectInvalidOptions(__kv.claimRandomMany({ count: 0 }), "claimRandomMany.count.positive"),
			expectInvalidOptions(__kv.claimRandomMany({ count: 1, ttl: 0 }), "claimRandomMany.ttl.positive"),
			expectInvalidOptions(__kv.releaseClaim("bad"), "releaseClaim.claim"),
			expectInvalidOptions(__kv.releaseClaim({ id: "", key: "k", token: 1 }), "releaseClaim.claim.id.empty"),
			expectInvalidOptions(__kv.releaseClaim({ id: "c", key: "", token: 1 }), "releaseClaim.claim.key.empty"),
			expectInvalidOptions(__kv.completeClaim("bad"), "completeClaim.claim"),
			expectInvalidOptions(__kv.completeClaim({ id: "", key: "k", token: 1 }), "completeClaim.claim.id.empty"),
			expectInvalidOptions(__kv.completeClaim({ id: "c", key: "", token: 1 }), "completeClaim.claim.key.empty"),
			expectInvalidOptions(__kv.completeClaim({ id: "x", key: "k", token: 1 }, "bad"), "completeClaim.options"),
			expectInvalidOptions(__kv.renewClaim("bad", { ttl: 1000 }), "renewClaim.claim"),
			expectInvalidOptions(__kv.renewClaim({ id: "x", key: "k", token: 1 }), "renewClaim.options.required"),
			expectInvalidOptions(__kv.renewClaim({ id: "x", key: "k", token: 1 }, { ttl: 0 }), "renewClaim.ttl.positive")
		]);
	`)
}

// TestKVAsync_IncrementBy_FractionalDelta_RejectsPromise verifies that kv async increment by fractional delta kv async increment by fractional delta rejects its promise.
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

// TestKVAsync_Scan_FractionalLimit_RejectsPromise verifies that kv async scan fractional limit kv async scan fractional limit rejects its promise.
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

// runKVScript runs kv script.
func runKVScript(t *testing.T, runtime *modulestest.Runtime, kv *KV, script string) {
	t.Helper()

	require.NoError(t, runtime.VU.Runtime().Set("__kv", kv))

	_, err := runtime.RunOnEventLoop(script)
	require.NoError(t, err)
}

// attachOperationMetricsForTest attaches operation metrics for test for tests.
func attachOperationMetricsForTest(t *testing.T, runtime *modulestest.Runtime, kv *KV) chan k6metrics.SampleContainer {
	t.Helper()

	registry := runtime.VU.InitEnv().Registry
	rootTags := registry.RootTagSet()

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

	return samples
}

// assertOperationMetricsError asserts operation metrics error.
func assertOperationMetricsError(
	t *testing.T,
	samples chan k6metrics.SampleContainer,
	expectedOp string,
	expectedError ErrorName,
) {
	t.Helper()

	metricHits := make(map[string]int)

	for _, container := range k6metrics.GetBufferedSamples(samples) {
		for _, sample := range container.GetSamples() {
			switch sample.Metric.Name {
			case metricKVOperationsTotal:
				metricHits[metricKVOperationsTotal]++

				status, ok := sample.Tags.Get(tagStatus)
				require.True(t, ok)
				assert.Equal(t, statusError, status)

				op, ok := sample.Tags.Get(tagOp)
				require.True(t, ok)
				assert.Equal(t, expectedOp, op)
				assert.InDelta(t, 1.0, sample.Value, 1e-9)
			case metricKVOperationDuration:
				metricHits[metricKVOperationDuration]++

				status, ok := sample.Tags.Get(tagStatus)
				require.True(t, ok)
				assert.Equal(t, statusError, status)

				op, ok := sample.Tags.Get(tagOp)
				require.True(t, ok)
				assert.Equal(t, expectedOp, op)
			case metricKVOperationFailed:
				metricHits[metricKVOperationFailed]++

				op, ok := sample.Tags.Get(tagOp)
				require.True(t, ok)
				assert.Equal(t, expectedOp, op)
				assert.InDelta(t, 1.0, sample.Value, 1e-9)
			case metricKVErrorsTotal:
				metricHits[metricKVErrorsTotal]++

				op, ok := sample.Tags.Get(tagOp)
				require.True(t, ok)
				assert.Equal(t, expectedOp, op)

				errorType, ok := sample.Tags.Get(tagErrorType)
				require.True(t, ok)
				assert.Equal(t, string(expectedError), errorType)
				assert.InDelta(t, 1.0, sample.Value, 1e-9)
			case metricKVEmptyResult:
				metricHits[metricKVEmptyResult]++
			}
		}
	}

	assert.Equal(t, 1, metricHits[metricKVOperationsTotal])
	assert.Equal(t, 1, metricHits[metricKVOperationDuration])
	assert.Equal(t, 1, metricHits[metricKVOperationFailed])
	assert.Equal(t, 1, metricHits[metricKVErrorsTotal])
	assert.Zero(t, metricHits[metricKVEmptyResult])
}

// panicGetStore is a test double that stubs panic get store behavior.
type panicGetStore struct {
	store.Store
}

// panicGetStore implements Get for panic get store test scenarios.
func (s panicGetStore) Get(_ string) (any, error) {
	panic("boom")
}

// canceledGetStore is a test double that stubs canceled get store behavior.
type canceledGetStore struct {
	store.Store
}

// canceledGetStore implements Get for canceled get store test scenarios.
func (s canceledGetStore) Get(_ string) (any, error) {
	return nil, context.Canceled
}

// contextOverrideVU is a test VU wrapper for context override vu tests.
type contextOverrideVU struct {
	modules.VU
	// ctx overrides the context returned by contextOverrideVU.
	ctx context.Context
}

// contextOverrideVU returns the overridden context for context override vu.
func (v contextOverrideVU) Context() context.Context {
	return v.ctx
}

// cancelOnFirstSetManyStore is a test double that stubs cancel on first set many store behavior.
type cancelOnFirstSetManyStore struct {
	store.Store
	// cancel cancels the test context from cancelOnFirstSetManyStore.
	cancel context.CancelFunc
	// alreadyFired tracks whether cancelOnFirstSetManyStore already triggered cancellation.
	alreadyFired atomic.Bool
}

// cancelOnFirstSetManyStore implements SetMany for cancel on first set many store test scenarios.
func (s *cancelOnFirstSetManyStore) SetMany(entries []store.Entry) (int64, error) {
	if s.cancel != nil && s.alreadyFired.CompareAndSwap(false, true) {
		s.cancel()
	}

	return s.Store.SetMany(entries)
}

// closeCountingStore is a test double that stubs close counting store behavior.
type closeCountingStore struct {
	store.Store
	// closeCalls records close invocations for close counting store.
	closeCalls atomic.Int64
}

// closeCountingStore implements Close for close counting store test scenarios.
func (s *closeCountingStore) Close() error {
	s.closeCalls.Add(1)

	return s.Store.Close()
}

// blockingGetStore is a test double that stubs blocking get store behavior.
type blockingGetStore struct {
	store.Store
	// started signals when blockingGetStore begins waiting in Get.
	started chan struct{}
	// unblock unblocks blockingGetStore when closed.
	unblock chan struct{}
}

// blockingGetStore implements Get for blocking get store test scenarios.
func (s *blockingGetStore) Get(key string) (any, error) {
	if s.started != nil {
		select {
		case <-s.started:
		default:
			close(s.started)
		}
	}

	if s.unblock != nil {
		<-s.unblock
	}

	return s.Store.Get(key)
}
