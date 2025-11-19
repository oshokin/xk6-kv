package kv

import (
	"fmt"
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

// testOpenKVStoreBarrier is a test hook invoked the moment a goroutine enters the
// store-initialization path. It lets tests synchronize concurrent calls to OpenKv
// without impacting production behavior (nil in non-test builds).
//
//nolint:gochecknoglobals // this is a test hook.
var testOpenKVStoreBarrier func()

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
	return &ModuleInstance{
		vu: vu,
		rm: rm,
	}
}

// getOrCreateStore creates a new store if it doesn't exist,
// or returns the existing store if the options are the same.
func (rm *RootModule) getOrCreateStore(options Options) (store.Store, bool, error) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.store != nil {
		if rm.opts.Equal(options) {
			return rm.store, false, nil
		}

		return nil, false, fmt.Errorf(
			"openKv already initialized with backend=%q serialization=%q trackKeys=%t path=%q",
			rm.opts.Backend, rm.opts.Serialization, rm.opts.TrackKeys, rm.opts.Path,
		)
	}

	if testOpenKVStoreBarrier != nil {
		testOpenKVStoreBarrier()
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
		return store.NewMemoryStore(options.TrackKeys), nil
	case BackendDisk:
		diskStore, err := store.NewDiskStore(options.TrackKeys, options.Path)
		if err != nil {
			return nil, err
		}

		return diskStore, nil
	default:
		return nil, fmt.Errorf("unsupported backend: %s", options.Backend)
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
		return nil, fmt.Errorf("unsupported serialization: %s", options.Serialization)
	}
}

// clearStoreOnFailure clears the store if it was newly created and failed to initialize.
func (rm *RootModule) clearStoreOnFailure(candidate store.Store, isNewlyCreated bool) {
	if !isNewlyCreated {
		return
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.store == candidate {
		rm.store = nil
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

// Options controls how the shared store is created on the first call to openKv().
type Options struct {
	// Backend selects the storage engine backing the KV store.
	// Valid values: "memory" (ephemeral), "disk" (persistent).
	Backend string `json:"backend"`

	// Path points to the BoltDB file when using the disk backend.
	// When empty or invalid the default path is used.
	// Ignored by the memory backend.
	Path string `json:"path"`

	// Serialization selects how values are encoded/decoded when stored.
	// Valid values: "json" (structured), "string" (raw string to []byte).
	Serialization string `json:"serialization"`

	// TrackKeys enables in-memory key indexing for faster List/RandomKey/prefix ops.
	// This consumes additional memory proportional to the number of keys.
	TrackKeys bool `json:"trackKeys"`
}

// NewOptionsFrom converts a Sobek (JS) value into an Options instance, applying defaults
// and validating user input. It's intentionally strict to fail fast on invalid configs.
func NewOptionsFrom(vu modules.VU, options sobek.Value) (Options, error) {
	// Defaults keep backward compatibility and sensible behavior out of the box.
	opts := Options{
		Backend:       DefaultBackend,
		Serialization: DefaultSerialization,
	}

	if common.IsNullish(options) {
		return opts, nil
	}

	if err := vu.Runtime().ExportTo(options, &opts); err != nil {
		return opts, fmt.Errorf("unable to parse kv options: %w", err)
	}

	// Validate backend.
	if opts.Backend != BackendMemory && opts.Backend != BackendDisk {
		return opts, fmt.Errorf(
			"invalid backend: %q; valid values are: %q, %q",
			opts.Backend, BackendMemory, BackendDisk,
		)
	}

	// Validate serialization.
	if opts.Serialization != SerializationJSON && opts.Serialization != SerializationString {
		return opts, fmt.Errorf(
			"invalid serialization: %q; valid values are: %q, %q",
			opts.Serialization, SerializationJSON, SerializationString,
		)
	}

	if opts.Backend == BackendDisk {
		canonicalPath, err := store.ResolveDiskPath(opts.Path)
		if err != nil {
			return opts, err
		}

		opts.Path = canonicalPath
	}

	return opts, nil
}

// Equal checks if two Options are equal.
func (o Options) Equal(other Options) bool {
	return o.Backend == other.Backend &&
		o.Serialization == other.Serialization &&
		o.TrackKeys == other.TrackKeys &&
		o.Path == other.Path
}
