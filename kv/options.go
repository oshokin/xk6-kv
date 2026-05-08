package kv

import (
	"fmt"

	"github.com/grafana/sobek"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"

	"github.com/oshokin/xk6-kv/kv/store"
)

// Options controls how the shared store is created on the first call to openKv().
type Options struct {
	// Backend selects the storage engine backing the KV store.
	// Valid values: "memory" (ephemeral), "disk" (persistent).
	Backend string `js:"backend"`

	// Path points to the bbolt file when using the disk backend.
	// When empty, the default path is used.
	// Invalid paths return an option error.
	// Ignored by the memory backend.
	Path string `js:"path"`

	// Serialization selects how values are encoded/decoded when stored.
	// Valid values: "json" (structured), "string" (raw string to []byte).
	Serialization string `js:"serialization"`

	// TrackKeys enables an in-memory key index.
	// It accelerates randomKey/randomKeys/count paths where supported.
	// Cursor scans still read from the backend source of truth.
	// This consumes additional memory proportional to the number of keys.
	TrackKeys bool `js:"trackKeys"`

	// MemoryOptions contains memory-backend specific configuration.
	// Ignored by the "disk" backend.
	MemoryOptions *MemoryOptions `js:"memory"`

	// DiskOptions contains bbolt-specific configuration for the "disk" backend.
	// Ignored by the "memory" backend.
	DiskOptions *DiskOptions `js:"disk"`

	// Metrics configures optional k6 custom metric emission.
	Metrics *MetricsOptions `js:"metrics"`
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

	err := vu.Runtime().ExportTo(options, &opts)
	if err != nil {
		return opts, fmt.Errorf("%w: %w", store.ErrKVOptionsInvalid, err)
	}

	switch opts.Backend {
	case BackendMemory:
		// Memory backend doesn't need path validation.
	case BackendDisk:
		canonicalPath, err := store.ResolveDiskPath(opts.Path)
		if err != nil {
			return opts, err
		}

		opts.Path = canonicalPath
		if opts.DiskOptions != nil {
			err := opts.DiskOptions.Validate()
			if err != nil {
				return opts, err
			}
		}
	default:
		return opts, fmt.Errorf(
			"%w: %q; valid values are: %q, %q",
			store.ErrInvalidBackend, opts.Backend, BackendMemory, BackendDisk,
		)
	}

	// Validate serialization.
	if opts.Serialization != SerializationJSON && opts.Serialization != SerializationString {
		return opts, fmt.Errorf(
			"%w: %q; valid values are: %q, %q",
			store.ErrInvalidSerialization, opts.Serialization, SerializationJSON, SerializationString,
		)
	}

	return opts.Canonical(), nil
}

// Canonical normalizes options so backend-ignored fields cannot trigger false
// equivalence mismatches.
func (o Options) Canonical() Options {
	switch o.Backend {
	case BackendMemory:
		o.Path = ""
		o.DiskOptions = nil
	case BackendDisk:
		o.MemoryOptions = nil
	}

	return o
}

// Equal checks if two Options are equal.
func (o Options) Equal(other Options) bool {
	o = o.Canonical()
	other = other.Canonical()

	return o.Backend == other.Backend &&
		o.Path == other.Path &&
		o.Serialization == other.Serialization &&
		o.TrackKeys == other.TrackKeys &&
		o.MemoryOptions.Equal(other.MemoryOptions) &&
		o.DiskOptions.Equal(other.DiskOptions) &&
		o.Metrics.Equal(other.Metrics)
}
