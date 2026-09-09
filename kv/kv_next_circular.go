package kv

import (
	"github.com/grafana/sobek"

	"github.com/oshokin/xk6-kv/kv/store"
)

// NextCircular returns the next reusable matching entry in circular
// lexicographic order.
func (k *KV) NextCircular(options sobek.Value) *sobek.Promise {
	circularOptions, err := importNextCircularOptions(k.vu.Runtime(), options)
	if err != nil {
		return k.rejectedPromiseObserved(opNextCircular, err)
	}

	return k.runAsyncWithStoreObserved(
		opNextCircular,
		func(s store.Store) (any, error) {
			return s.NextCircular(circularOptions.Prefix)
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			if result == nil {
				return sobek.Null()
			}

			return rt.ToValue(result)
		},
	)
}
