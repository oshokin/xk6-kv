package kv

import (
	"github.com/grafana/sobek"

	"github.com/oshokin/xk6-kv/kv/store"
)

type (
	getManyItem struct {
		Key    string `js:"key"`
		Exists bool   `js:"exists"`
		Value  any    `js:"value"`
	}

	setManyResult struct {
		Written int64 `js:"written"`
	}
)

// GetMany reads multiple keys and resolves per-key items in the same order as input keys.
// Missing keys produce {exists:false, value:null} items.
func (k *KV) GetMany(keysValue sobek.Value) *sobek.Promise {
	keys, err := importGetManyKeys(keysValue)
	if err != nil {
		return k.rejectedPromiseObserved(opGetMany, err)
	}

	return k.runAsyncWithStoreObserved(
		opGetMany,
		func(s store.Store) (any, error) {
			entries, err := s.GetMany(keys)
			if err != nil {
				return nil, err
			}

			if len(entries) != len(keys) {
				return nil, unexpectedStoreOutput("store.GetMany")
			}

			items := make([]getManyItem, len(entries))
			for i, entry := range entries {
				if entry == nil {
					items[i] = getManyItem{
						Key:    keys[i],
						Exists: false,
						Value:  nil,
					}

					continue
				}

				items[i] = getManyItem{
					Key:    keys[i],
					Exists: true,
					Value:  entry.Value,
				}
			}

			return items, nil
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

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

// DeleteMany deletes explicit non-empty keys and resolves delete/missing counts.
// Missing keys are counted in the result and are not errors.
func (k *KV) DeleteMany(keysValue sobek.Value) *sobek.Promise {
	keys, err := importDeleteManyKeys(keysValue)
	if err != nil {
		return k.rejectedPromiseObserved(opDeleteMany, err)
	}

	return k.runAsyncWithStoreObserved(
		opDeleteMany,
		func(s store.Store) (any, error) {
			return s.DeleteMany(keys)
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}
