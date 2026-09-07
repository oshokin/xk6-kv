package store

import (
	"fmt"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

const (
	// DefaultDiskStoreOpenTimeout is the maximum amount of time xk6-kv waits
	// for the bbolt file lock when disk.timeout is omitted.
	//
	// bbolt itself defaults to Timeout == 0, which means waiting indefinitely.
	// A finite extension-level default prevents a k6 process from hanging when
	// another process already holds the database file lock.
	//
	// Users can explicitly set disk.timeout to 0 to opt back into bbolt's
	// indefinite-wait behavior.
	DefaultDiskStoreOpenTimeout = 5 * time.Second
)

// DiskConfig holds validated bbolt-specific tuning knobs parsed at the JS layer.
type DiskConfig struct {
	// Timeout is the amount of time to wait to obtain a file lock.
	//
	// A nil Timeout uses DefaultDiskStoreOpenTimeout.
	// An explicitly configured zero duration waits indefinitely, matching
	// bbolt semantics.
	Timeout *time.Duration
	// NoSync sets the initial value of DB.NoSync. Normally this can just be
	// set directly on the DB itself when returned from Open(), but this option
	// is useful in APIs which expose Options but not the underlying DB.
	NoSync *bool
	// NoGrowSync sets the DB.NoGrowSync flag before memory mapping the file.
	NoGrowSync *bool
	// NoFreelistSync disables syncing freelist to disk. This improves the database write performance
	// under normal operation, but requires a full database re-sync during recovery.
	NoFreelistSync *bool
	// PreLoadFreelist sets whether to load the free pages when opening
	// the db file. Note when opening db in write mode, bbolt will always
	// load the free pages.
	PreLoadFreelist *bool
	// FreelistType sets the backend freelist type. There are two options. Array which is simple but endures
	// dramatic performance degradation if database is large and fragmentation in freelist is common.
	// The alternative one is using hashmap, it is faster in almost all circumstances
	// but it doesn't guarantee that it offers the smallest page id available. In normal case it is safe.
	// The default type is array. Valid values: "array" or "map".
	FreelistType *string
	// ReadOnly opens database in read-only mode. Uses flock(..., LOCK_SH |LOCK_NB) to
	// grab a shared lock (UNIX).
	ReadOnly *bool
	// InitialMmapSize is the initial mmap size of the database
	// in bytes. Read transactions won't block write transaction
	// if the InitialMmapSize is large enough to hold database mmap
	// size. (See DB.Begin for more information)
	//
	// If <= 0, the initial map size is 0.
	// If initialMmapSize is smaller than the previous database size,
	// it takes no effect.
	//
	// Note: On Windows, due to platform limitations, the database file size
	// will be immediately resized to match `InitialMmapSize` (aligned to page size)
	// when the DB is opened. On non-Windows platforms, the file size will grow
	// dynamically based on the actual amount of written data, regardless of `InitialMmapSize`.
	InitialMmapSize *int
	// Mlock locks database file in memory when set to true.
	// It prevents potential page faults, however
	// used memory can't be reclaimed. (UNIX only)
	Mlock *bool
}

// validate validates the DiskConfig.
func (cfg *DiskConfig) validate() error {
	if cfg == nil {
		return nil
	}

	if cfg.InitialMmapSize != nil && *cfg.InitialMmapSize < 0 {
		return fmt.Errorf("%w: initialMmapSize must be non-negative", ErrKVOptionsInvalid)
	}

	return nil
}

func buildBBoltOptions(cfg *DiskConfig) (*bolt.Options, error) {
	// Start from bbolt defaults so fields that xk6-kv does not expose continue
	// to follow the dependency defaults.
	opts := *bolt.DefaultOptions

	// bbolt's default Timeout is zero, which means waiting indefinitely for
	// the database file lock. xk6-kv intentionally overrides only this one
	// default so a second k6 process fails fast instead of hanging forever.
	opts.Timeout = DefaultDiskStoreOpenTimeout

	if cfg == nil {
		return &opts, nil
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	// An explicitly provided timeout wins over the xk6-kv default.
	// In particular, an explicit zero restores bbolt's indefinite wait.
	if cfg.Timeout != nil {
		opts.Timeout = *cfg.Timeout
	}

	if cfg.NoSync != nil {
		opts.NoSync = *cfg.NoSync
	}

	if cfg.NoGrowSync != nil {
		opts.NoGrowSync = *cfg.NoGrowSync
	}

	if cfg.NoFreelistSync != nil {
		opts.NoFreelistSync = *cfg.NoFreelistSync
	}

	if cfg.PreLoadFreelist != nil {
		opts.PreLoadFreelist = *cfg.PreLoadFreelist
	}

	if cfg.FreelistType != nil {
		switch strings.ToLower(*cfg.FreelistType) {
		case "", "array":
			opts.FreelistType = bolt.FreelistArrayType
		case "map":
			opts.FreelistType = bolt.FreelistMapType
		default:
			return nil, fmt.Errorf("%w: freelistType: %q", ErrKVOptionsInvalid, *cfg.FreelistType)
		}
	}

	if cfg.ReadOnly != nil {
		opts.ReadOnly = *cfg.ReadOnly
	}

	if cfg.InitialMmapSize != nil {
		opts.InitialMmapSize = *cfg.InitialMmapSize
	}

	if cfg.Mlock != nil {
		opts.Mlock = *cfg.Mlock
	}

	return &opts, nil
}
