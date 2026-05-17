package kv

import (
	"github.com/grafana/sobek"

	"github.com/oshokin/xk6-kv/kv/store"
)

// PopRandom claims one random free matching entry and removes it.
// Resolves to null when no matching entry exists.
func (k *KV) PopRandom(options sobek.Value) *sobek.Promise {
	popOptions, err := importPopRandomOptions(k.vu.Runtime(), options)
	if err != nil {
		return k.rejectedPromiseObserved(opPopRandom, err)
	}

	return k.runAsyncWithStoreObserved(
		opPopRandom,
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

// PopRandomMany claims up to count random free matching entries and removes each claimed key.
// Completed deletes are not rolled back if a later completion fails.
// Remaining live claims are released best-effort.
// Resolves to an empty slice when no matching entries exist.
func (k *KV) PopRandomMany(options sobek.Value) *sobek.Promise {
	popOptions, err := importPopRandomManyOptions(k.vu.Runtime(), options)
	if err != nil {
		return k.rejectedPromiseObserved(opPopRandomMany, err)
	}

	return k.runAsyncWithStoreObserved(
		opPopRandomMany,
		func(s store.Store) (any, error) {
			return s.PopRandomMany(popOptions.Prefix, popOptions.Count)
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

// ClaimRandom leases a random matching free entry.
// Resolves to null when no free entry exists.
func (k *KV) ClaimRandom(options sobek.Value) *sobek.Promise {
	claimOptions, err := importClaimRandomOptions(k.vu.Runtime(), options)
	if err != nil {
		return k.rejectedPromiseObserved(opClaimRandom, err)
	}

	return k.runAsyncWithStoreObserved(
		opClaimRandom,
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

// ClaimKey leases a specific key.
// Resolves to null when the key is missing or already live-claimed.
func (k *KV) ClaimKey(key sobek.Value, options sobek.Value) *sobek.Promise {
	keyString, err := parseRequiredNonEmptyStringArg("claimKey", "key", key)
	if err != nil {
		return k.rejectedPromiseObserved(opClaimKey, err)
	}

	claimOptions, err := importClaimKeyOptions(k.vu.Runtime(), options)
	if err != nil {
		return k.rejectedPromiseObserved(opClaimKey, err)
	}

	return k.runAsyncWithStoreObserved(
		opClaimKey,
		func(s store.Store) (any, error) {
			return s.ClaimKey(keyString, &store.ClaimOptions{
				Owner: claimOptions.Owner,
				TTLMs: claimOptions.TTLMs,
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

// ClaimRandomMany leases up to count random matching free entries.
// Resolves to an empty slice when no free entries exist.
func (k *KV) ClaimRandomMany(options sobek.Value) *sobek.Promise {
	claimOptions, err := importClaimRandomManyOptions(k.vu.Runtime(), options)
	if err != nil {
		return k.rejectedPromiseObserved(opClaimRandomMany, err)
	}

	return k.runAsyncWithStoreObserved(
		opClaimRandomMany,
		func(s store.Store) (any, error) {
			return s.ClaimRandomMany(&store.ClaimManyOptions{
				Prefix: claimOptions.Prefix,
				Count:  claimOptions.Count,
				Owner:  claimOptions.Owner,
				TTLMs:  claimOptions.TTLMs,
			})
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

// ReleaseClaim releases a live claim.
// Resolves to false when the claim is missing, expired, or already released.
func (k *KV) ReleaseClaim(claim sobek.Value) *sobek.Promise {
	return k.runClaimMutationObserved(
		opReleaseClaim,
		"releaseClaim",
		claim,
		func(s store.Store, claimRef *store.ClaimRef) (any, error) {
			return s.ReleaseClaim(claimRef)
		},
	)
}

// CompleteClaim completes a live claim.
// Resolves to false when the claim is missing, expired, or already completed.
func (k *KV) CompleteClaim(claim sobek.Value, options sobek.Value) *sobek.Promise {
	completeOptions, err := importCompleteClaimOptions(k.vu.Runtime(), options)
	if err != nil {
		return k.rejectedPromiseObserved(opCompleteClaim, err)
	}

	return k.runClaimMutationObserved(
		opCompleteClaim,
		"completeClaim",
		claim,
		func(s store.Store, claimRef *store.ClaimRef) (any, error) {
			return s.CompleteClaim(
				claimRef,
				&store.CompleteClaimOptions{
					DeleteKey: completeOptions.DeleteKey,
				},
			)
		},
	)
}

// RenewClaim extends a live claim lease without changing the claim token.
// Resolves to false when the claim is missing, expired, stale, or no longer owns the key.
func (k *KV) RenewClaim(claim sobek.Value, options sobek.Value) *sobek.Promise {
	renewOptions, err := importRenewClaimOptions(k.vu.Runtime(), options)
	if err != nil {
		return k.rejectedPromiseObserved(opRenewClaim, err)
	}

	return k.runClaimMutationObserved(
		opRenewClaim,
		"renewClaim",
		claim,
		func(s store.Store, claimRef *store.ClaimRef) (any, error) {
			return s.RenewClaim(
				claimRef,
				&store.RenewClaimOptions{
					TTLMs: renewOptions.TTLMs,
				},
			)
		},
	)
}

// runClaimMutationObserved runs a claim mutation operation and observes the result.
// Resolves to the result of the mutation operation.
func (k *KV) runClaimMutationObserved(
	op string,
	method string,
	claim sobek.Value,
	call func(store.Store, *store.ClaimRef) (any, error),
) *sobek.Promise {
	claimRef, err := importClaimRefPayload(k.vu.Runtime(), method, claim)
	if err != nil {
		return k.rejectedPromiseObserved(op, err)
	}

	storeClaimRef := &store.ClaimRef{
		ID:    claimRef.ID,
		Key:   claimRef.Key,
		Token: claimRef.Token,
	}

	return k.runAsyncWithStoreObserved(
		op,
		func(s store.Store) (any, error) {
			return call(s, storeClaimRef)
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}
