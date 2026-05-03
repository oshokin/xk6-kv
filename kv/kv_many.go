package kv

import (
	"github.com/grafana/sobek"

	"github.com/oshokin/xk6-kv/kv/store"
)

type (
	setManyResult struct {
		Written int64 `js:"written"`
	}
)

// SetMany validates input entries and writes them in one logical batch.
func (k *KV) SetMany(entriesValue sobek.Value) *sobek.Promise {
	entries, err := importSetManyEntries(entriesValue)
	if err != nil {
		return k.rejectedPromiseObserved(opSetMany, err)
	}

	return k.runAsyncWithStoreObserved(
		opSetMany,
		func(s store.Store) (any, error) {
			written, setErr := s.SetMany(entries)
			if setErr != nil {
				return nil, setErr
			}

			return &setManyResult{
				Written: written,
			}, nil
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}
