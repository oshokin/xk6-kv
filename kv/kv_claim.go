package kv

import (
	"errors"
	"fmt"

	"github.com/grafana/sobek"

	"github.com/oshokin/xk6-kv/kv/store"
)

// claimNotUpdatedFailureName is the claim not updated failure name const.
const claimNotUpdatedFailureName = "ClaimNotUpdated"

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

// ClaimKeys leases explicit keys and reports claimed, busy, and missing sets.
// With allOrNothing=true, processing stops on first busy/missing and acquired claims are rolled back
// best-effort (not a transaction).
func (k *KV) ClaimKeys(keys sobek.Value, options sobek.Value) *sobek.Promise {
	claimKeys, err := importClaimKeysPayload(k.vu.Runtime(), keys)
	if err != nil {
		return k.rejectedPromiseObserved(opClaimKeys, err)
	}

	claimOptions, err := importClaimKeysOptions(k.vu.Runtime(), options)
	if err != nil {
		return k.rejectedPromiseObserved(opClaimKeys, err)
	}

	return k.runAsyncWithStoreObserved(
		opClaimKeys,
		func(s store.Store) (any, error) {
			return claimKeysBatch(s, claimKeys, claimOptions)
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
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

// ReleaseClaims is a sequential lifecycle helper over releaseClaim semantics.
// It returns partial success details and is not a cross-claim transaction.
func (k *KV) ReleaseClaims(claims sobek.Value) *sobek.Promise {
	claimRefs, err := importClaimRefsPayload(k.vu.Runtime(), "releaseClaims", claims)
	if err != nil {
		return k.rejectedPromiseObserved(opReleaseClaims, err)
	}

	return k.runAsyncWithStoreObserved(
		opReleaseClaims,
		func(s store.Store) (any, error) {
			return releaseClaimsBatch(s, claimRefs)
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
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

// CompleteClaims is a sequential lifecycle helper over completeClaim semantics.
// It returns partial success details and is not a cross-claim transaction.
func (k *KV) CompleteClaims(claims sobek.Value, options sobek.Value) *sobek.Promise {
	claimRefs, err := importClaimRefsPayload(k.vu.Runtime(), "completeClaims", claims)
	if err != nil {
		return k.rejectedPromiseObserved(opCompleteClaims, err)
	}

	completeOptions, err := importCompleteClaimsOptions(k.vu.Runtime(), options)
	if err != nil {
		return k.rejectedPromiseObserved(opCompleteClaims, err)
	}

	return k.runAsyncWithStoreObserved(
		opCompleteClaims,
		func(s store.Store) (any, error) {
			return completeClaimsBatch(s, claimRefs, &store.CompleteClaimOptions{
				DeleteKey: completeOptions.DeleteKey,
			})
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
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

// RenewClaims is a sequential lifecycle helper over renewClaim semantics.
// It returns partial success details and is not a cross-claim transaction.
func (k *KV) RenewClaims(claims sobek.Value, options sobek.Value) *sobek.Promise {
	claimRefs, err := importClaimRefsPayload(k.vu.Runtime(), "renewClaims", claims)
	if err != nil {
		return k.rejectedPromiseObserved(opRenewClaims, err)
	}

	renewOptions, err := importRenewClaimsOptions(k.vu.Runtime(), options)
	if err != nil {
		return k.rejectedPromiseObserved(opRenewClaims, err)
	}

	return k.runAsyncWithStoreObserved(
		opRenewClaims,
		func(s store.Store) (any, error) {
			return renewClaimsBatch(s, claimRefs, &store.RenewClaimOptions{
				TTLMs: renewOptions.TTLMs,
			})
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
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

// releaseClaimsBatch processes a batch of claims against the store.
func releaseClaimsBatch(s store.Store, refs []*store.ClaimRef) (*releaseClaimsResult, error) {
	result := &releaseClaimsResult{
		Attempted: int64(len(refs)),
		Failed:    make([]claimBatchFailure, 0),
	}

	for idx, ref := range refs {
		released, err := s.ReleaseClaim(ref)
		if err != nil {
			return nil, fmt.Errorf(
				"%w: releaseClaims failed at index %d after releasing %d of %d claims; previous successful operations were not rolled back",
				err,
				idx,
				result.Released,
				len(refs),
			)
		}

		if released {
			result.Released++
			continue
		}

		result.Failed = append(result.Failed, claimNotUpdatedFailure("releaseClaims", idx, ref))
	}

	return result, nil
}

// completeClaimsBatch processes a batch of claims against the store.
func completeClaimsBatch(
	s store.Store,
	refs []*store.ClaimRef,
	options *store.CompleteClaimOptions,
) (*completeClaimsResult, error) {
	result := &completeClaimsResult{
		Attempted: int64(len(refs)),
		Failed:    make([]claimBatchFailure, 0),
	}

	for idx, ref := range refs {
		completed, err := s.CompleteClaim(ref, options)
		if err != nil {
			return nil, fmt.Errorf(
				"%w: completeClaims failed at index %d after completing %d of %d claims; previous successful operations were not rolled back",
				err,
				idx,
				result.Completed,
				len(refs),
			)
		}

		if completed {
			result.Completed++
			continue
		}

		result.Failed = append(result.Failed, claimNotUpdatedFailure("completeClaims", idx, ref))
	}

	return result, nil
}

// renewClaimsBatch processes a batch of claims against the store.
func renewClaimsBatch(
	s store.Store,
	refs []*store.ClaimRef,
	options *store.RenewClaimOptions,
) (*renewClaimsResult, error) {
	result := &renewClaimsResult{
		Attempted: int64(len(refs)),
		Failed:    make([]claimBatchFailure, 0),
	}

	for idx, ref := range refs {
		renewed, err := s.RenewClaim(ref, options)
		if err != nil {
			return nil, fmt.Errorf(
				"%w: renewClaims failed at index %d after renewing %d of %d claims; previous successful operations were not rolled back",
				err,
				idx,
				result.Renewed,
				len(refs),
			)
		}

		if renewed {
			result.Renewed++
			continue
		}

		result.Failed = append(result.Failed, claimNotUpdatedFailure("renewClaims", idx, ref))
	}

	return result, nil
}

// claimNotUpdatedFailure builds a batch failure for a stale or missing claim.
func claimNotUpdatedFailure(method string, index int, ref *store.ClaimRef) claimBatchFailure {
	failure := claimBatchFailure{
		Index:   int64(index),
		Name:    claimNotUpdatedFailureName,
		Message: method + ": claim was missing, expired, stale, or no longer owns the key",
	}

	if ref != nil {
		failure.ID = ref.ID
		failure.Key = ref.Key
	}

	return failure
}

// claimKeysBatch processes a batch of claims against the store.
func claimKeysBatch(
	s store.Store,
	keys []string,
	options *claimKeysOptions,
) (*claimKeysResult, error) {
	result := &claimKeysResult{
		Claimed: make([]*store.EntryClaim, 0, len(keys)),
		Busy:    make([]string, 0),
		Missing: make([]string, 0),
	}

	if len(keys) == 0 {
		return result, nil
	}

	claimOptions := &store.ClaimOptions{
		Owner: options.Owner,
		TTLMs: options.TTLMs,
	}

	acquiredClaims := make([]*store.EntryClaim, 0, len(keys))

	releaseAcquiredOnError := func(opErr error) (*claimKeysResult, error) {
		releaseErr := releaseEntryClaimsBestEffort(s, acquiredClaims)
		if releaseErr != nil {
			return nil, errors.Join(opErr, releaseErr)
		}

		return nil, opErr
	}

	rollbackAndReturn := func() (*claimKeysResult, error) {
		if err := releaseEntryClaimsBestEffort(s, acquiredClaims); err != nil {
			return nil, err
		}

		result.Claimed = result.Claimed[:0]

		return result, nil
	}

	for _, key := range keys {
		claim, err := s.ClaimKey(key, claimOptions)
		if err != nil {
			return releaseAcquiredOnError(err)
		}

		if claim != nil {
			acquiredClaims = append(acquiredClaims, claim)
			result.Claimed = append(result.Claimed, claim)

			continue
		}

		exists, err := s.Exists(key)
		if err != nil {
			return releaseAcquiredOnError(err)
		}

		if !exists {
			result.Missing = append(result.Missing, key)
		} else {
			result.Busy = append(result.Busy, key)
		}

		if options.AllOrNothing {
			// allOrNothing avoids intentionally keeping partial claims from this call.
			// Rollback is best-effort and does not provide transaction isolation.
			return rollbackAndReturn()
		}
	}

	return result, nil
}

// releaseEntryClaimsBestEffort releases acquired claims for public allOrNothing rollback.
// Rollback is intentionally best-effort: claims may have already expired or been mutated
// by concurrent operations. Only technical storage errors are treated as rollback failures.
func releaseEntryClaimsBestEffort(s store.Store, claims []*store.EntryClaim) error {
	for _, claim := range claims {
		if claim == nil {
			continue
		}

		_, err := s.ReleaseClaim(claim.Ref())
		if err != nil {
			return err
		}
	}

	return nil
}
