package kv

import (
	"github.com/grafana/sobek"

	"github.com/oshokin/xk6-kv/kv/store"
)

// Stats returns a Promise that resolves to a diagnostic snapshot of store state.
func (k *KV) Stats() *sobek.Promise {
	return k.runAsyncWithStore(
		func(s store.Store) (any, error) {
			snapshot, err := s.Stats()
			if err != nil {
				return nil, err
			}

			if snapshot == nil {
				return nil, unexpectedStoreOutput("store.Stats")
			}

			return snapshot, nil
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}
