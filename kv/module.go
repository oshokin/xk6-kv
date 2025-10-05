package kv

import (
	"fmt"

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
		Named: map[string]interface{}{
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

	if mi.rm.store == nil {
		// Create the base store based on the backend option.
		var baseStore store.Store

		switch options.Backend {
		case BackendMemory:
			baseStore = store.NewMemoryStore(options.TrackKeys)
		case BackendDisk:
			baseStore = store.NewDiskStore(options.TrackKeys, options.Path)
		}

		// Create the serializer based on the serialization option.
		var serializer store.Serializer

		switch options.Serialization {
		case SerializationJSON:
			serializer = store.NewJSONSerializer()
		case SerializationString:
			serializer = store.NewStringSerializer()
		default:
			// Defensive default: JSON is a safe, structured default.
			serializer = store.NewJSONSerializer()
		}

		// Create a serialized store with the chosen store and serializer.
		serializedStore := store.NewSerializedStore(baseStore, serializer)
		mi.rm.store = serializedStore
	}

	kv := NewKV(mi.vu, mi.rm.store)
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

	return opts, nil
}
