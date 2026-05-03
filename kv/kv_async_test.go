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

func TestKVAsync_SetMany_EmptyKeyMatchesSetBehavior(t *testing.T) {
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
			.then((result) => {
				if (!result || result.written !== 1) {
					throw new Error("unexpected written count");
				}
				return __kv.get("");
			})
			.then((value) => {
				if (value !== "batch-value") {
					throw new Error("unexpected stored value for empty key");
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
