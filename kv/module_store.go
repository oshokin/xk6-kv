package kv

import (
	"fmt"
	"sync"

	"github.com/oshokin/xk6-kv/kv/store"
)

// testOpenKVStoreBarrier is a test hook invoked the moment a goroutine enters the
// store-initialization path. It lets tests synchronize concurrent calls to OpenKv
// without impacting production behavior (nil in non-test builds).
//
//nolint:gochecknoglobals // this is a test hook.
var (
	testOpenKVStoreBarrier   func()
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
			"%w: backend=%q path=%q serialization=%q trackKeys=%t",
			store.ErrKVOptionsConflict,
			rm.opts.Backend, rm.opts.Path, rm.opts.Serialization, rm.opts.TrackKeys,
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
	}
}
