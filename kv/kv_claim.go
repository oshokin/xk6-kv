package kv

import (
	"github.com/grafana/sobek"

	"github.com/oshokin/xk6-kv/kv/store"
)

// PopRandom atomically selects and removes a random matching entry.
// Resolves to null when no matching entry exists.
func (k *KV) PopRandom(options sobek.Value) *sobek.Promise {
	popOptions, err := importPopRandomOptions(k.vu.Runtime(), options)
	if err != nil {
		return k.rejectedPromise(err)
	}

	return k.runAsyncWithStore(
		func(s store.Store) (any, error) {
			return s.PopRandom(popOptions.Prefix)
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			if result == nil {
				return sobek.Null()
			}

			return rt.ToValue(result)
		},
	)
}

// ClaimRandom atomically leases a random matching entry.
// Resolves to null when no free entry exists.
func (k *KV) ClaimRandom(options sobek.Value) *sobek.Promise {
	claimOptions, err := importClaimRandomOptions(k.vu.Runtime(), options)
	if err != nil {
		return k.rejectedPromise(err)
	}

	return k.runAsyncWithStore(
		func(s store.Store) (any, error) {
			return s.ClaimRandom(&store.ClaimOptions{
				Prefix: claimOptions.Prefix,
				Owner:  claimOptions.Owner,
				TTLMs:  claimOptions.TTLMs,
			})
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			if result == nil {
				return sobek.Null()
			}

			return rt.ToValue(result)
		},
	)
}

// ReleaseClaim releases a live claim.
func (k *KV) ReleaseClaim(claim sobek.Value) *sobek.Promise {
	claimRef, err := importClaimRefPayload(k.vu.Runtime(), "releaseClaim", claim)
	if err != nil {
		return k.rejectedPromise(err)
	}

	return k.runAsyncWithStore(
		func(s store.Store) (any, error) {
			return s.ReleaseClaim(&store.ClaimRef{
				ID:    claimRef.ID,
				Key:   claimRef.Key,
				Token: claimRef.Token,
			})
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

// CompleteClaim completes a live claim.
func (k *KV) CompleteClaim(claim sobek.Value, options sobek.Value) *sobek.Promise {
	claimRef, err := importClaimRefPayload(k.vu.Runtime(), "completeClaim", claim)
	if err != nil {
		return k.rejectedPromise(err)
	}

	completeOptions, err := importCompleteClaimOptions(k.vu.Runtime(), options)
	if err != nil {
		return k.rejectedPromise(err)
	}

	return k.runAsyncWithStore(
		func(s store.Store) (any, error) {
			return s.CompleteClaim(
				&store.ClaimRef{
					ID:    claimRef.ID,
					Key:   claimRef.Key,
					Token: claimRef.Token,
				},
				&store.CompleteClaimOptions{
					DeleteKey: completeOptions.DeleteKey,
				},
			)
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}
