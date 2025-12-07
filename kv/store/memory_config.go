package store

import "runtime"

// MemoryConfig holds memory-store-specific configuration.
type MemoryConfig struct {
	// TrackKeys enables per-shard key tracking for deterministic scans/random selection.
	TrackKeys bool
	// ShardCount sets the number of shards for the memory backend.
	// If <= 0, defaults to runtime.NumCPU().
	// If > MaxShardCount, capped at MaxShardCount.
	ShardCount int
}

// GetShardCount returns the shard count for the memory store.
// If the shard count is not set, it defaults to runtime.NumCPU().
// If the shard count is greater than MaxShardCount, it is capped at MaxShardCount.
func (cfg *MemoryConfig) GetShardCount() int {
	var shards int
	if cfg != nil {
		shards = cfg.ShardCount
	}

	if shards <= 0 {
		shards = max(1, runtime.NumCPU())
	}

	if shards > MaxShardCount {
		shards = MaxShardCount
	}

	return shards
}
