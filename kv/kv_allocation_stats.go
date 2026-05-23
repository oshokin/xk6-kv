package kv

import (
	"github.com/grafana/sobek"

	"github.com/oshokin/xk6-kv/kv/store"
)

// AllocationStats returns a prefix-scoped allocation snapshot.
func (k *KV) AllocationStats(options sobek.Value) *sobek.Promise {
	statsOptions, err := importAllocationStatsOptions(k.vu.Runtime(), options)
	if err != nil {
		return k.rejectedPromiseObserved(opAllocationStats, err)
	}

	return k.runAsyncWithStoreObserved(
		opAllocationStats,
		func(s store.Store) (any, error) {
			snapshot, statsErr := s.AllocationStats(statsOptions.Prefix)
			if statsErr != nil {
				return nil, statsErr
			}

			if snapshot == nil {
				return nil, unexpectedStoreOutput("store.AllocationStats")
			}

			return snapshot, nil
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}
