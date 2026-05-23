package kv

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	k6common "go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/js/modulestest"
	k6metrics "go.k6.io/k6/metrics"
)

// initlessVU is a test VU wrapper for initless vu tests.
type initlessVU struct {
	modules.VU
}

// initlessVU.InitEnv implements InitEnv for initless vu test scenarios.
func (v initlessVU) InitEnv() *k6common.InitEnvironment {
	return nil
}

// TestOpenKV_ConcurrentInitializationSharesStore tests that
// concurrent openKv calls share the same store.
func TestOpenKV_ConcurrentInitializationSharesStore(t *testing.T) {
	t.Parallel()

	rootModule := newTestRootModule(t)

	primaryRuntime := modulestest.NewRuntime(t)
	secondaryRuntime := modulestest.NewRuntime(t)

	primaryModuleInstance := rootModule.NewModuleInstance(primaryRuntime.VU).(*ModuleInstance)
	secondaryModuleInstance := rootModule.NewModuleInstance(secondaryRuntime.VU).(*ModuleInstance)

	primaryOptions := primaryRuntime.VU.Runtime().ToValue(map[string]any{
		"backend":       BackendMemory,
		"serialization": SerializationJSON,
	})
	secondaryOptions := secondaryRuntime.VU.Runtime().ToValue(map[string]any{
		"backend":       BackendMemory,
		"serialization": SerializationJSON,
	})

	var (
		enterCount   atomic.Uint32
		firstEntered = make(chan struct{})
		firstRelease = make(chan struct{})
	)

	testOpenKVStoreBarrierMu.Lock()

	testOpenKVStoreBarrier = func() {
		if enterCount.Add(1) != 1 {
			return
		}

		close(firstEntered)
		<-firstRelease
	}

	testOpenKVStoreBarrierMu.Unlock()

	defer func() {
		testOpenKVStoreBarrierMu.Lock()

		testOpenKVStoreBarrier = nil

		testOpenKVStoreBarrierMu.Unlock()
	}()

	results := make(chan *ModuleInstance, 2)

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()

		primaryModuleInstance.OpenKv(primaryOptions)

		results <- primaryModuleInstance
	}()

	go func() {
		defer wg.Done()

		secondaryModuleInstance.OpenKv(secondaryOptions)

		results <- secondaryModuleInstance
	}()

	<-firstEntered
	close(firstRelease)

	firstDone := <-results
	secondDone := <-results

	wg.Wait()

	firstStore := firstDone.kv.store
	secondStore := secondDone.kv.store

	require.NotNil(t, firstStore, "first KV instance should have a store")
	require.NotNil(t, secondStore, "second KV instance should have a store")

	require.Same(t, firstStore, secondStore, "concurrent OpenKv calls must receive the same backing store instance")
	require.Same(t, firstStore, rootModule.store, "root module store must be shared across VUs")

	t.Cleanup(func() {
		if firstStore != nil {
			_ = firstStore.Close()
		}
	})
}

// TestOpenKV_RejectsOutsideInitContext verifies that open kv rejects outside init context.
func TestOpenKV_RejectsOutsideInitContext(t *testing.T) {
	t.Parallel()

	rootModule := newTestRootModule(t)
	runtime := modulestest.NewRuntime(t)
	moduleInstance := rootModule.NewModuleInstance(initlessVU{VU: runtime.VU}).(*ModuleInstance)

	options := runtime.VU.Runtime().ToValue(map[string]any{
		"backend":       BackendMemory,
		"serialization": SerializationJSON,
	})

	var recovered any

	func() {
		defer func() {
			recovered = recover()
		}()

		moduleInstance.OpenKv(options)
	}()

	require.NotNil(t, recovered)
	require.Contains(t, fmt.Sprint(recovered), InvalidOptionsError)
	require.Nil(t, rootModule.store)
}

// TestOpenKV_RejectsConflictingOptions tests that openKv
// rejects conflicting options by panicking.
func TestOpenKV_RejectsConflictingOptions(t *testing.T) {
	t.Parallel()

	rootModule := newTestRootModule(t)

	runtime := modulestest.NewRuntime(t)
	moduleInstance := rootModule.NewModuleInstance(runtime.VU).(*ModuleInstance)

	memoryBackendOptions := runtime.VU.Runtime().ToValue(map[string]any{
		"backend":       BackendMemory,
		"serialization": SerializationJSON,
	})

	diskBackendOptions := runtime.VU.Runtime().ToValue(map[string]any{
		"backend": BackendDisk,
		"path":    "/tmp/kv",
	})

	require.NotPanics(t, func() {
		moduleInstance.OpenKv(memoryBackendOptions)
	})

	require.Panics(t, func() {
		moduleInstance.OpenKv(diskBackendOptions)
	})

	t.Cleanup(func() {
		if moduleInstance.kv != nil {
			_ = moduleInstance.kv.Close()
		}
	})
}

// TestOpenKV_AllowsEquivalentDiskPaths tests that openKv allows
// equivalent disk paths by not panicking.
func TestOpenKV_AllowsEquivalentDiskPaths(t *testing.T) {
	t.Parallel()

	rootModule := newTestRootModule(t)
	runtime := modulestest.NewRuntime(t)
	moduleInstance := rootModule.NewModuleInstance(runtime.VU).(*ModuleInstance)

	tempDir := t.TempDir()

	// Ensure root module store closes before temp dir cleanup.
	t.Cleanup(func() {
		cleanupRootModule(t, rootModule)

		if moduleInstance.kv != nil {
			_ = moduleInstance.kv.Close()
		}
	})

	absolutePath := filepath.Join(tempDir, "kv.db")
	extraSegmentsPath := filepath.Join(absolutePath, "..", filepath.Base(absolutePath))

	absolutePathOptions := runtime.VU.Runtime().ToValue(map[string]any{
		"backend": BackendDisk,
		"path":    absolutePath,
	})

	relativePathOptions := runtime.VU.Runtime().ToValue(map[string]any{
		"backend": BackendDisk,
		"path":    extraSegmentsPath,
	})

	require.NotPanics(t, func() {
		moduleInstance.OpenKv(absolutePathOptions)
	})

	require.NotPanics(t, func() {
		moduleInstance.OpenKv(relativePathOptions)
	})
}

// TestOpenKV_MemoryPathDoesNotCauseConflict verifies that open kv memory path does not cause conflict.
func TestOpenKV_MemoryPathDoesNotCauseConflict(t *testing.T) {
	t.Parallel()

	rootModule := newTestRootModule(t)
	runtime := modulestest.NewRuntime(t)
	moduleInstance := rootModule.NewModuleInstance(runtime.VU).(*ModuleInstance)

	first := runtime.VU.Runtime().ToValue(map[string]any{
		"backend": BackendMemory,
		"path":    "/tmp/ignored-a.db",
	})
	second := runtime.VU.Runtime().ToValue(map[string]any{
		"backend": BackendMemory,
	})

	require.NotPanics(t, func() {
		moduleInstance.OpenKv(first)
	})
	require.NotPanics(t, func() {
		moduleInstance.OpenKv(second)
	})

	t.Cleanup(func() {
		if moduleInstance.kv != nil {
			_ = moduleInstance.kv.Close()
		}
	})
}

// TestOpenKV_DiskMemoryOptionsDoNotCauseConflict verifies that open kv disk memory options do not cause conflict.
func TestOpenKV_DiskMemoryOptionsDoNotCauseConflict(t *testing.T) {
	t.Parallel()

	rootModule := newTestRootModule(t)
	runtime := modulestest.NewRuntime(t)
	moduleInstance := rootModule.NewModuleInstance(runtime.VU).(*ModuleInstance)

	dbPath := filepath.Join(t.TempDir(), "kv.db")

	// Close store before TempDir cleanup removes db files.
	t.Cleanup(func() {
		cleanupRootModule(t, rootModule)

		if moduleInstance.kv != nil {
			_ = moduleInstance.kv.Close()
		}
	})

	first := runtime.VU.Runtime().ToValue(map[string]any{
		"backend": BackendDisk,
		"path":    dbPath,
		"memory": map[string]any{
			"shardCount": 64,
		},
	})
	second := runtime.VU.Runtime().ToValue(map[string]any{
		"backend": BackendDisk,
		"path":    dbPath,
	})

	require.NotPanics(t, func() {
		moduleInstance.OpenKv(first)
	})
	require.NotPanics(t, func() {
		moduleInstance.OpenKv(second)
	})
}

// TestOpenKV_InitializesReportStatsMetrics verifies that openKv wires reportStats metric emitters.
func TestOpenKV_InitializesReportStatsMetrics(t *testing.T) {
	t.Parallel()

	rootModule := newTestRootModule(t)
	runtime := modulestest.NewRuntime(t)
	moduleInstance := rootModule.NewModuleInstance(runtime.VU).(*ModuleInstance)

	options := runtime.VU.Runtime().ToValue(map[string]any{
		"backend":       BackendMemory,
		"serialization": SerializationJSON,
		"trackKeys":     true,
	})

	require.NotPanics(t, func() {
		moduleInstance.OpenKv(options)
	})

	require.NotNil(t, moduleInstance.kv)
	require.NotNil(t, moduleInstance.kv.stateMetrics, "kv handle must have reportStats metrics emitter")
	require.NotNil(t, rootModule.stateMetrics, "root module must cache reportStats metrics emitter")
	require.Nil(t, moduleInstance.kv.operationMetrics, "operation metrics must be disabled by default")
}

// TestOpenKV_ClearsStoreWhenStateMetricsInitFails verifies partially initialized store cleanup
// when reportStats metric registration fails after store creation.
func TestOpenKV_ClearsStoreWhenStateMetricsInitFails(t *testing.T) {
	t.Parallel()

	rootModule := newTestRootModule(t)
	runtime := modulestest.NewRuntime(t)
	moduleInstance := rootModule.NewModuleInstance(runtime.VU).(*ModuleInstance)

	_, err := runtime.VU.InitEnv().Registry.NewMetric(metricKVKeys, k6metrics.Counter, k6metrics.Default)
	require.NoError(t, err)

	options := runtime.VU.Runtime().ToValue(map[string]any{
		"backend":       BackendMemory,
		"serialization": SerializationJSON,
		"trackKeys":     true,
	})

	require.Panics(t, func() {
		moduleInstance.OpenKv(options)
	})

	require.Nil(t, rootModule.store, "store must be cleared on metric init failure")
	require.Nil(t, rootModule.stateMetrics, "state metrics must not be cached on failure")
	require.Nil(t, moduleInstance.kv, "KV handle must not be created on metric init failure")
}

// TestOpenKV_InitializesOperationMetricsWhenEnabled verifies that open kv initializes operation metrics when enabled.
func TestOpenKV_InitializesOperationMetricsWhenEnabled(t *testing.T) {
	t.Parallel()

	rootModule := newTestRootModule(t)
	runtime := modulestest.NewRuntime(t)
	moduleInstance := rootModule.NewModuleInstance(runtime.VU).(*ModuleInstance)

	options := runtime.VU.Runtime().ToValue(map[string]any{
		"backend":       BackendMemory,
		"serialization": SerializationJSON,
		"trackKeys":     true,
		"metrics": map[string]any{
			"operations": true,
		},
	})

	require.NotPanics(t, func() {
		moduleInstance.OpenKv(options)
	})

	require.NotNil(t, moduleInstance.kv)
	require.NotNil(t, moduleInstance.kv.stateMetrics, "state metrics must be initialized")
	require.NotNil(t, moduleInstance.kv.operationMetrics, "operation metrics must be initialized")
	require.NotNil(t, rootModule.operationMetrics, "root module must cache operation metrics emitter")
}

// TestOpenKV_RejectsConflictingMetricsOperationsOption verifies that open kv rejects conflicting metrics operations option.
func TestOpenKV_RejectsConflictingMetricsOperationsOption(t *testing.T) {
	t.Parallel()

	rootModule := newTestRootModule(t)
	runtime := modulestest.NewRuntime(t)
	moduleInstance := rootModule.NewModuleInstance(runtime.VU).(*ModuleInstance)

	enabledOptions := runtime.VU.Runtime().ToValue(map[string]any{
		"backend":       BackendMemory,
		"serialization": SerializationJSON,
		"metrics": map[string]any{
			"operations": true,
		},
	})
	disabledOptions := runtime.VU.Runtime().ToValue(map[string]any{
		"backend":       BackendMemory,
		"serialization": SerializationJSON,
		"metrics": map[string]any{
			"operations": false,
		},
	})

	require.NotPanics(t, func() {
		moduleInstance.OpenKv(enabledOptions)
	})

	require.Panics(t, func() {
		moduleInstance.OpenKv(disabledOptions)
	})
}

// TestOpenKV_ClearsStoreWhenOperationMetricsInitFails verifies that open kv clears store when operation metrics init fails.
func TestOpenKV_ClearsStoreWhenOperationMetricsInitFails(t *testing.T) {
	t.Parallel()

	rootModule := newTestRootModule(t)
	runtime := modulestest.NewRuntime(t)
	moduleInstance := rootModule.NewModuleInstance(runtime.VU).(*ModuleInstance)

	_, err := runtime.VU.InitEnv().Registry.NewMetric(metricKVOperationsTotal, k6metrics.Gauge, k6metrics.Default)
	require.NoError(t, err)

	options := runtime.VU.Runtime().ToValue(map[string]any{
		"backend":       BackendMemory,
		"serialization": SerializationJSON,
		"metrics": map[string]any{
			"operations": true,
		},
	})

	require.Panics(t, func() {
		moduleInstance.OpenKv(options)
	})

	require.Nil(t, rootModule.store, "store must be cleared on metric init failure")
	require.Nil(t, rootModule.stateMetrics, "state metrics must be cleared on operation metric init failure")
	require.Nil(t, rootModule.operationMetrics, "operation metrics must not be cached on failure")
	require.Nil(t, moduleInstance.kv, "KV handle must not be created on metric init failure")
}

// TestOpenKV_RejectsMetricsBooleanShortcut verifies that open kv rejects metrics boolean shortcut.
func TestOpenKV_RejectsMetricsBooleanShortcut(t *testing.T) {
	t.Parallel()

	rootModule := newTestRootModule(t)
	runtime := modulestest.NewRuntime(t)
	moduleInstance := rootModule.NewModuleInstance(runtime.VU).(*ModuleInstance)

	options := runtime.VU.Runtime().ToValue(map[string]any{
		"backend":       BackendMemory,
		"serialization": SerializationJSON,
		"metrics":       true,
	})

	require.Panics(t, func() {
		moduleInstance.OpenKv(options)
	})

	require.Nil(t, rootModule.store)
	require.Nil(t, rootModule.operationMetrics)
}

// newTestRootModule creates a new test root module.
func newTestRootModule(t *testing.T) *RootModule {
	t.Helper()

	rm := New()

	t.Cleanup(func() {
		cleanupRootModule(t, rm)
	})

	return rm
}

// cleanupRootModule cleans up the root module.
func cleanupRootModule(t *testing.T, rm *RootModule) {
	t.Helper()

	rm.mu.Lock()
	store := rm.store
	rm.store = nil
	rm.mu.Unlock()

	if store != nil {
		require.NoError(t, store.Close())
	}
}
