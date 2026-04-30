package kv

import (
	"fmt"
	"sync"

	"go.k6.io/k6/js/modules"

	"github.com/oshokin/xk6-kv/kv/store"
)

//nolint:gochecknoglobals // this is a test hook.
var (
	// testOpenKVStoreBarrier runs when a goroutine enters store initialization.
	// Tests use it to synchronize concurrent OpenKv calls; production keeps it nil.
	testOpenKVStoreBarrier func()
	// testOpenKVStoreBarrierMu guards reads/writes of testOpenKVStoreBarrier.
	testOpenKVStoreBarrierMu sync.RWMutex
)

// getOrCreateStore creates a new store if it doesn't exist,
// or returns the existing store if the options are the same.
// Returns (store, isNewlyCreated, error).
// The isNewlyCreated flag helps callers know whether to clean up on failure.
func (rm *RootModule) getOrCreateStore(options Options) (store.Store, bool, error) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.store != nil {
		if rm.opts.Equal(options) {
			return rm.store, false, nil
		}

		// Reject re-configuration attempts to prevent confusion and data loss.
		// Users must use consistent options across all VUs in a test.
		return nil, false, fmt.Errorf(
			"%w: backend=%q path=%q serialization=%q trackKeys=%t metrics.operations=%t",
			store.ErrKVOptionsConflict,
			rm.opts.Backend, rm.opts.Path, rm.opts.Serialization, rm.opts.TrackKeys,
			rm.opts.Metrics.operationsEnabled(),
		)
	}

	// Test hook: allows test code to synchronize concurrent OpenKv calls.
	// Production code sees nil and skips this entirely.
	testOpenKVStoreBarrierMu.RLock()

	barrier := testOpenKVStoreBarrier

	testOpenKVStoreBarrierMu.RUnlock()

	if barrier != nil {
		barrier()
	}

	baseStore, err := rm.createBaseStore(options)
	if err != nil {
		return nil, false, err
	}

	serializer, err := rm.createSerializer(options)
	if err != nil {
		return nil, false, err
	}

	rm.store = store.NewSerializedStore(baseStore, serializer)
	rm.opts = options

	return rm.store, true, nil
}

// createBaseStore creates a new base store based on the options.
func (rm *RootModule) createBaseStore(options Options) (store.Store, error) {
	switch options.Backend {
	case BackendMemory:
		memoryCfg, err := options.MemoryOptions.ToMemoryConfig()
		if err != nil {
			return nil, err
		}

		if memoryCfg == nil {
			memoryCfg = new(store.MemoryConfig)
		}

		memoryCfg.TrackKeys = options.TrackKeys

		return store.NewMemoryStore(memoryCfg), nil
	case BackendDisk:
		diskCfg, err := options.DiskOptions.ToDiskConfig()
		if err != nil {
			return nil, err
		}

		diskStore, err := store.NewDiskStore(options.TrackKeys, options.Path, diskCfg)
		if err != nil {
			return nil, err
		}

		return diskStore, nil
	default:
		// Unreachable: backend is validated in NewOptionsFrom before this is called.
		return nil, fmt.Errorf("%w: %s", store.ErrInvalidBackend, options.Backend)
	}
}

// createSerializer creates a new serializer based on the options.
func (rm *RootModule) createSerializer(options Options) (store.Serializer, error) {
	switch options.Serialization {
	case SerializationJSON:
		return store.NewJSONSerializer(), nil
	case SerializationString:
		return store.NewStringSerializer(), nil
	default:
		// Unreachable: serialization is validated in NewOptionsFrom before this is called.
		return nil, fmt.Errorf("%w: %s", store.ErrInvalidSerialization, options.Serialization)
	}
}

// ensureStateMetrics initializes reportStats() metrics during openKv()
// when k6 registry access is available.
func (rm *RootModule) ensureStateMetrics(vu modules.VU) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.stateMetrics != nil {
		return nil
	}

	initEnv := vu.InitEnv()
	if initEnv == nil || initEnv.Registry == nil {
		// openKv() is expected in init context; keep behavior non-breaking and
		// let reportStats() return a clear runtime error when metrics are unavailable.
		return nil
	}

	stateMetrics, err := newKVStateMetrics(initEnv.Registry)
	if err != nil {
		return err
	}

	rm.stateMetrics = stateMetrics

	return nil
}

// ensureOperationMetrics initializes optional automatic operation metrics during openKv().
// When metrics.operations is enabled, metric registry access must be available.
func (rm *RootModule) ensureOperationMetrics(vu modules.VU, options Options) error {
	if !options.Metrics.operationsEnabled() {
		return nil
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.operationMetrics != nil {
		return nil
	}

	initEnv := vu.InitEnv()
	if initEnv == nil || initEnv.Registry == nil {
		return fmt.Errorf(
			"%w: metrics.operations=true requires openKv() in init context with registry access",
			store.ErrKVOptionsInvalid,
		)
	}

	operationMetrics, err := newKVOperationMetrics(initEnv.Registry, options)
	if err != nil {
		return err
	}

	rm.operationMetrics = operationMetrics

	return nil
}

// clearStoreOnFailure resets the store reference if initialization failed.
// This prevents partially-initialized stores from being reused by subsequent calls.
// Only applicable when this goroutine created the store (isNewlyCreated == true).
//
// Critical safety mechanism: if Open() fails after we set rm.store but before
// the store is fully usable, we must nil it out so the next OpenKv() call
// doesn't return a broken store to another VU.
func (rm *RootModule) clearStoreOnFailure(candidate store.Store, isNewlyCreated bool) {
	if !isNewlyCreated {
		// Another goroutine created the store: don't touch it.
		return
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Double-check we're clearing the right store (defensive against races).
	// Only nil it out if it still points to our candidate.
	if rm.store == candidate {
		rm.store = nil
		rm.stateMetrics = nil
		rm.operationMetrics = nil
	}
}
