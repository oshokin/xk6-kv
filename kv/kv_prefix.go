package kv

import (
	"github.com/grafana/sobek"

	"github.com/oshokin/xk6-kv/kv/store"
)

// DeleteByPrefix deletes up to limit keys with the given non-empty prefix.
// It resolves { deleted, done }.
func (k *KV) DeleteByPrefix(options sobek.Value) *sobek.Promise {
	deleteOptions, err := importDeleteByPrefixOptions(k.vu.Runtime(), options)
	if err != nil {
		return k.rejectedPromiseObserved(opDeleteByPrefix, err)
	}

	return k.runAsyncWithStoreObserved(
		opDeleteByPrefix,
		func(s store.Store) (any, error) {
			return s.DeleteByPrefix(deleteOptions.Prefix, deleteOptions.Limit)
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}
