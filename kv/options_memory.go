package kv

import (
	"github.com/oshokin/xk6-kv/kv/store"
)

// MemoryOptions exposes memory backend tuning knobs.
type MemoryOptions struct {
	// ShardCount sets the number of shards for the memory backend.
	// If <= 0, defaults to runtime.NumCPU() (automatic).
	// If > store.MaxShardCount, capped at store.MaxShardCount.
	ShardCount int `js:"shardCount"`
}

// Equal checks if two MemoryOptions are equal.
func (mo *MemoryOptions) Equal(other *MemoryOptions) bool {
	if mo == nil && other == nil {
		return true
	}

	if mo == nil || other == nil {
		return false
	}

	return mo.ShardCount == other.ShardCount
}

// ToMemoryConfig converts MemoryOptions into a store-level MemoryConfig.
func (mo *MemoryOptions) ToMemoryConfig() (*store.MemoryConfig, error) {
	if mo == nil {
		//nolint:nilnil // nil memory options are valid and result in a default memory configuration.
		return nil, nil
	}

	return &store.MemoryConfig{
		ShardCount: mo.ShardCount,
	}, nil
}
