package kv

import (
	"sync"

	"github.com/grafana/sobek"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"

	"github.com/oshokin/xk6-kv/kv/store"
)

type (
	// RootModule is a module singleton created once per test process.
	// It owns the shared Store used by all VUs.
	RootModule struct {
		// store is shared store instance, created on first openKv().
		store store.Store

		// opts holds the options used when the store was created.
		opts Options

		// mu protects store creation and configuration.
		mu sync.Mutex
	}

	// ModuleInstance is created per VU.
	// It holds the per-VU JS bindings and a pointer
	// to the RootModule to access the shared store.
	ModuleInstance struct {
		vu modules.VU
		rm *RootModule
		// kv provides a key-value database that can be used to store and retrieve data.
		// The database is opened when the first KV instance is created, and closed when the last KV
		// instance is closed.
		kv *KV
	}
)

const (
	// BackendDisk is the persistent store backed by the filesystem.
	BackendDisk = "disk"
	// BackendMemory is an in-memory, process-local store (fast, ephemeral).
	BackendMemory = "memory"
	// SerializationJSON encodes/decodes values using JSON.
	SerializationJSON = "json"
	// SerializationString encodes string values directly as bytes (and vice versa).
	SerializationString = "string"

	// DefaultBackend is used when the user does not specify a backend.
	DefaultBackend = BackendDisk
	// DefaultSerialization is used when the user does not specify a serialization format.
	DefaultSerialization = SerializationJSON
)

// Compile-time interface assertions (defensive).
var (
	_ modules.Instance = new(ModuleInstance)
	_ modules.Module   = new(RootModule)
)

// New returns a pointer to a new RootModule instance.
func New() *RootModule {
	return &RootModule{
		// As default, the store is nil, we expect the user to call openKv()
		// which should set the store shared between all VUs.
		store: nil,
	}
}

// NewModuleInstance implements modules.Module.
// It creates a per-VU instance wired to the RootModule (which owns the shared store).
func (rm *RootModule) NewModuleInstance(vu modules.VU) modules.Instance {
	// configureRuntime(vu.Runtime())
	return &ModuleInstance{
		vu: vu,
		rm: rm,
	}
}

// Exports implements modules.Instance and exposes
// the JavaScript API surface for this module.
// Currently, only openKv() is exported.
func (mi *ModuleInstance) Exports() modules.Exports {
	return modules.Exports{
		Named: map[string]any{
			"openKv": mi.OpenKv,
		},
	}
}

// NewKV returns the per-VU KV object as a Sobek object.
// Typically created via OpenKv(); exposed for completeness/testing.
func (mi *ModuleInstance) NewKV(_ sobek.ConstructorCall) *sobek.Object {
	return mi.vu.Runtime().ToValue(mi.kv).ToObject(mi.vu.Runtime())
}

// OpenKv parses user options, initializes the shared store (once),
// and returns the per-VU KV object bound to that shared store.
//
// Concurrency & visibility guarantees:
//   - The first successful call to OpenKv "wins" and decides the backend + serialization.
//   - Later calls reuse the established store and ignore differing options.
func (mi *ModuleInstance) OpenKv(opts sobek.Value) *sobek.Object {
	options, err := NewOptionsFrom(mi.vu, opts)
	if err != nil {
		common.Throw(mi.vu.Runtime(), err)
		return nil
	}

	backingStore, isNewlyCreated, err := mi.rm.getOrCreateStore(options)
	if err != nil {
		common.Throw(mi.vu.Runtime(), err)
		return nil
	}

	// Each VU invocation calls Open to bump the shared reference counter.
	if err := backingStore.Open(); err != nil {
		mi.rm.clearStoreOnFailure(backingStore, isNewlyCreated)
		common.Throw(mi.vu.Runtime(), err)

		return nil
	}

	kv := NewKV(mi.vu, backingStore)
	mi.kv = kv

	return mi.vu.Runtime().ToValue(mi.kv).ToObject(mi.vu.Runtime())
}
