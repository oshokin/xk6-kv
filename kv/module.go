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
var (
	testOpenKVStoreBarrier   func()
	testOpenKVStoreBarrierMu sync.RWMutex
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
			"%w: backend=%q path=%q serialization=%q trackKeys=%t shardCount=%d",
			store.ErrKVOptionsConflict,
			rm.opts.Backend, rm.opts.Path, rm.opts.Serialization, rm.opts.TrackKeys, rm.opts.ShardCount,
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
		return store.NewMemoryStore(options.TrackKeys, options.ShardCount), nil
	case BackendDisk:
		diskStore, err := store.NewDiskStore(options.TrackKeys, options.Path)
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
	Backend string `js:"backend"`

	// Path points to the BoltDB file when using the disk backend.
	// When empty or invalid the default path is used.
	// Ignored by the memory backend.
	Path string `js:"path"`

	// Serialization selects how values are encoded/decoded when stored.
	// Valid values: "json" (structured), "string" (raw string to []byte).
	Serialization string `js:"serialization"`

	// TrackKeys enables in-memory key indexing for faster List/RandomKey/prefix ops.
	// This consumes additional memory proportional to the number of keys.
	TrackKeys bool `js:"trackKeys"`

	// ShardCount sets the number of shards for the memory backend.
	// If <= 0, defaults to runtime.NumCPU() (automatic).
	// If > store.MaxShardCount, capped at store.MaxShardCount.
	// Ignored by the disk backend.
	ShardCount int `js:"shardCount"`
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
		return opts, fmt.Errorf("%w: %w", store.ErrKVOptionsInvalid, err)
	}

	// Validate backend.
	if opts.Backend != BackendMemory && opts.Backend != BackendDisk {
		return opts, fmt.Errorf(
			"%w: %q; valid values are: %q, %q",
			store.ErrInvalidBackend, opts.Backend, BackendMemory, BackendDisk,
		)
	}

	if opts.Backend == BackendDisk {
		canonicalPath, err := store.ResolveDiskPath(opts.Path)
		if err != nil {
			return opts, err
		}

		opts.Path = canonicalPath
	}

	// Validate serialization.
	if opts.Serialization != SerializationJSON && opts.Serialization != SerializationString {
		return opts, fmt.Errorf(
			"%w: %q; valid values are: %q, %q",
			store.ErrInvalidSerialization, opts.Serialization, SerializationJSON, SerializationString,
		)
	}

	// Validate shardCount for memory backend (ignored for disk backend).
	if opts.Backend == BackendMemory {
		opts.ShardCount = store.MaxShardCount
	}

	return opts, nil
}

// Equal checks if two Options are equal.
func (o Options) Equal(other Options) bool {
	return o.Backend == other.Backend &&
		o.Path == other.Path &&
		o.Serialization == other.Serialization &&
		o.TrackKeys == other.TrackKeys &&
		o.ShardCount == other.ShardCount
}
