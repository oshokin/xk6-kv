package kv

import (
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.k6.io/k6/js/modulestest"
)

// TestOpenKvConcurrentInitializationSharesStore tests that
// concurrent openKv calls share the same store.
func TestOpenKvConcurrentInitializationSharesStore(t *testing.T) {
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

// TestOpenKvRejectsConflictingOptions tests that openKv
// rejects conflicting options by panicking.
func TestOpenKvRejectsConflictingOptions(t *testing.T) {
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

// TestOpenKvAllowsEquivalentDiskPaths tests that openKv allows
// equivalent disk paths by not panicking.
func TestOpenKvAllowsEquivalentDiskPaths(t *testing.T) {
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

	absPath := filepath.Join(tempDir, "kv.db")
	extraSegmentsPath := filepath.Join(absPath, "..", filepath.Base(absPath))

	absOptions := runtime.VU.Runtime().ToValue(map[string]any{
		"backend": BackendDisk,
		"path":    absPath,
	})

	relOptions := runtime.VU.Runtime().ToValue(map[string]any{
		"backend": BackendDisk,
		"path":    extraSegmentsPath,
	})

	require.NotPanics(t, func() {
		moduleInstance.OpenKv(absOptions)
	})

	require.NotPanics(t, func() {
		moduleInstance.OpenKv(relOptions)
	})
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
