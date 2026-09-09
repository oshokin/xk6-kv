package store

import "sync"

type (
	// claimOwnerBindingKey identifies one sticky owner allocation scope.
	//
	// Prefix is intentionally part of the identity so one logical owner can
	// independently hold claims from different key pools.
	claimOwnerBindingKey struct {
		Prefix string
		Owner  string
	}

	// claimOwnerBindingSlot serializes sticky allocation for exactly one
	// (prefix, owner) pair.
	//
	// ref may become stale after release, completion, expiry, delete, clear,
	// restore, close/reopen, or other claim lifecycle operations. Staleness is
	// validated lazily on the next ClaimForOwner call.
	claimOwnerBindingSlot struct {
		mu  sync.Mutex
		ref *ClaimRef
	}

	// claimOwnerBindingRegistry stores process-local sticky owner bindings.
	//
	// The registry itself is not persisted and is not a security or
	// distributed-lock boundary.
	claimOwnerBindingRegistry struct {
		mu    sync.Mutex
		slots map[claimOwnerBindingKey]*claimOwnerBindingSlot
	}
)

// slot returns the sticky binding slot for one (prefix, owner) pair.
//
// Lock ordering:
//   - registry lock is held only while looking up/creating the slot
//   - caller then uses slot.mu
//   - store-specific locks are acquired under slot.mu
func (r *claimOwnerBindingRegistry) slot(prefix string, owner string) *claimOwnerBindingSlot {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.slots == nil {
		r.slots = make(map[claimOwnerBindingKey]*claimOwnerBindingSlot)
	}

	key := claimOwnerBindingKey{
		Prefix: prefix,
		Owner:  owner,
	}

	slot := r.slots[key]
	if slot != nil {
		return slot
	}

	slot = new(claimOwnerBindingSlot)
	r.slots[key] = slot

	return slot
}

// claimForOwnerWithRegistry reuses or allocates one sticky claim for (prefix, owner).
func claimForOwnerWithRegistry(
	registry *claimOwnerBindingRegistry,
	opts *ClaimOptions,
	lookup func(ref *ClaimRef, opts *ClaimOptions) (*EntryClaim, bool, error),
	allocate func(opts *ClaimOptions) (*EntryClaim, error),
) (*EntryClaim, error) {
	slot := registry.slot(opts.Prefix, opts.Owner)

	slot.mu.Lock()
	defer slot.mu.Unlock()

	if slot.ref != nil {
		claim, live, err := lookup(slot.ref, opts)
		if err != nil {
			// Keep the reference on technical lookup errors so retries can validate it.
			return nil, err
		}

		if live {
			return claim, nil
		}

		// Drop only stale binding reference; claim lifecycle cleanup stays in store paths.
		slot.ref = nil
	}

	claim, err := allocate(opts)
	if err != nil {
		return nil, err
	}

	if claim == nil {
		//nolint:nilnil // exhausted pool is a normal allocation result.
		return nil, nil
	}

	slot.ref = claim.Ref()

	return claim, nil
}
