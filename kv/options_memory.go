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
	return mo.getShardCount() == other.getShardCount()
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

// getShardCount collapses automatic shard selection into a single comparable value.
func (mo *MemoryOptions) getShardCount() int {
	if mo == nil || mo.ShardCount <= 0 {
		return 0
	}

	return mo.ShardCount
}
