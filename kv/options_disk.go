package kv

import (
	"fmt"
	"math"
	"strings"

	"github.com/oshokin/xk6-kv/kv/store"
)

// DiskOptions exposes a curated subset of bbolt.Options at the k6 JS layer.
//
// Goals:
//   - Let users tune the big, understandable trade-offs:
//   - lock timeout,
//   - durability vs throughput,
//   - freelist behavior for large DBs,
//   - read-only vs read-write.
//   - Keep low-level OS-specific knobs (OpenFile, Logger, MmapFlags, PageSize)
//     internal so we don't drown users in rarely-used settings.
type DiskOptions struct {
	// Timeout controls how long bbolt waits to acquire the file lock.
	//
	// When zero, bbolt waits indefinitely. In k6 tests this can cause hangs
	// if another process holds the DB lock. Setting a finite timeout makes
	// misconfiguration fail fast instead.
	//
	// Accepted types:
	//   - number: milliseconds.
	//   - string: Go duration, e.g. "1s", "500ms", "1.5s".
	Timeout any `js:"timeout"`

	// NoSync maps to Options.NoSync and DB.NoSync.
	//
	// When true, bbolt will not fsync on each commit. This can significantly
	// increase write throughput, but recent commits can be lost if the process
	// or machine crashes.
	//
	// This is useful for ephemeral test data or bulk loads, but should not be
	// used where durability matters.
	NoSync *bool `js:"noSync"`

	// NoGrowSync maps to Options.NoGrowSync.
	//
	// When true, bbolt skips the fsync that normally happens when the DB file
	// grows. This improves performance on some filesystems at the cost of extra
	// risk during crashes (particularly on older ext3/ext4 setups).
	NoGrowSync *bool `js:"noGrowSync"`

	// NoFreelistSync maps to Options.NoFreelistSync.
	//
	// When true, the freelist is not synced to disk and is instead rebuilt at
	// open time. This speeds up normal writes and reduces file churn at the
	// expense of longer recovery time on crash.
	NoFreelistSync *bool `js:"noFreelistSync"`

	// PreLoadFreelist maps to Options.PreLoadFreelist.
	//
	// When true, bbolt loads the freelist into memory on open. For large DBs
	// this can reduce latency for subsequent writes (but makes open a bit
	// slower and uses more RAM).
	PreLoadFreelist *bool `js:"preLoadFreelist"`

	// FreelistType selects the internal freelist representation.
	//
	// Valid values:
	//   - "": use bbolt default ("array").
	//   - "array": simple slice-based freelist (default; fine for small DBs).
	//   - "map": hashmap-based freelist; usually faster on large, fragmented DBs.
	FreelistType *string `js:"freelistType"`

	// ReadOnly maps to Options.ReadOnly and opens the database in read-only mode.
	//
	// Useful for tests that only read from a pre-populated DB and want to avoid
	// accidental writes or compactions.
	ReadOnly *bool `js:"readOnly"`

	// InitialMmapSize maps to Options.InitialMmapSize (bytes).
	//
	// Larger values can reduce remapping as the DB grows and avoid blocking
	// writers when readers hold long-lived transactions, at the cost of
	// reserving more virtual address space (and on Windows, growing the DB file
	// to this size immediately).
	//
	// Accepted types:
	//   - number: bytes.
	//   - string: size, e.g. "64MB", "1GiB".
	InitialMmapSize any `js:"initialMmapSize"`

	// Mlock maps to Options.Mlock (UNIX only).
	//
	// When true, bbolt will attempt to lock the database file pages in memory,
	// preventing them from being paged out. This can avoid page faults during
	// heavy tests at the cost of memory not being reclaimable by the OS.
	Mlock *bool `js:"mlock"`
}

// Validate validates DiskOptions and returns an error if invalid.
func (do *DiskOptions) Validate() error {
	if do == nil {
		return nil
	}

	if do.Timeout != nil {
		if _, err := parseDurationValue(do.Timeout); err != nil {
			return fmt.Errorf("%w: timeout: %w", store.ErrKVOptionsInvalid, err)
		}
	}

	if do.InitialMmapSize != nil {
		if _, err := parseSizeValue(do.InitialMmapSize); err != nil {
			return fmt.Errorf("%w: initialMmapSize: %w", store.ErrKVOptionsInvalid, err)
		}
	}

	if do.FreelistType != nil {
		switch strings.ToLower(*do.FreelistType) {
		case "", "array", "map":
		default:
			return fmt.Errorf("%w: freelistType: %q", store.ErrKVOptionsInvalid, *do.FreelistType)
		}
	}

	return nil
}

// Equal checks if two DiskOptions are equal.
func (do *DiskOptions) Equal(other *DiskOptions) bool {
	if do.areDefault() && other.areDefault() {
		return true
	}

	if do == nil || other == nil {
		return false
	}

	return durationValuesEqual(do.Timeout, other.Timeout) &&
		comparablePointersEqual(do.NoSync, other.NoSync) &&
		comparablePointersEqual(do.NoGrowSync, other.NoGrowSync) &&
		comparablePointersEqual(do.NoFreelistSync, other.NoFreelistSync) &&
		comparablePointersEqual(do.PreLoadFreelist, other.PreLoadFreelist) &&
		comparablePointersEqual(normalizeStringPointer(do.FreelistType), normalizeStringPointer(other.FreelistType)) &&
		comparablePointersEqual(do.ReadOnly, other.ReadOnly) &&
		sizeValuesEqual(do.InitialMmapSize, other.InitialMmapSize) &&
		comparablePointersEqual(do.Mlock, other.Mlock)
}

// ToDiskConfig converts DiskOptions into a store-level DiskConfig with parsed values.
func (do *DiskOptions) ToDiskConfig() (*store.DiskConfig, error) {
	if do == nil {
		//nolint:nilnil // nil disk options are valid and result in a default disk configuration.
		return nil, nil
	}

	cfg := new(store.DiskConfig)

	if do.Timeout != nil {
		duration, err := parseDurationValue(do.Timeout)
		if err != nil {
			return nil, fmt.Errorf("%w: timeout: %w", store.ErrKVOptionsInvalid, err)
		}

		cfg.Timeout = &duration
	}

	if do.NoSync != nil {
		value := *do.NoSync
		cfg.NoSync = &value
	}

	if do.NoGrowSync != nil {
		value := *do.NoGrowSync
		cfg.NoGrowSync = &value
	}

	if do.NoFreelistSync != nil {
		value := *do.NoFreelistSync
		cfg.NoFreelistSync = &value
	}

	if do.PreLoadFreelist != nil {
		value := *do.PreLoadFreelist
		cfg.PreLoadFreelist = &value
	}

	if do.FreelistType != nil {
		value := strings.ToLower(*do.FreelistType)
		cfg.FreelistType = &value
	}

	if do.ReadOnly != nil {
		value := *do.ReadOnly
		cfg.ReadOnly = &value
	}

	if do.InitialMmapSize != nil {
		size, err := parseSizeValue(do.InitialMmapSize)
		if err != nil {
			return nil, fmt.Errorf("%w: initialMmapSize: %w", store.ErrKVOptionsInvalid, err)
		}

		if size > math.MaxInt {
			return nil, fmt.Errorf("%w: initialMmapSize too large: %d", store.ErrKVOptionsInvalid, size)
		}

		intSize := int(size)
		cfg.InitialMmapSize = &intSize
	}

	if do.Mlock != nil {
		value := *do.Mlock
		cfg.Mlock = &value
	}

	return cfg, nil
}

// areDefault returns true when no disk-specific knobs are provided.
func (do *DiskOptions) areDefault() bool {
	if do == nil {
		return true
	}

	return do.Timeout == nil &&
		do.NoSync == nil &&
		do.NoGrowSync == nil &&
		do.NoFreelistSync == nil &&
		do.PreLoadFreelist == nil &&
		do.FreelistType == nil &&
		do.ReadOnly == nil &&
		do.InitialMmapSize == nil &&
		do.Mlock == nil
}
