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
