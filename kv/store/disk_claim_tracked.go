package store

import (
	"container/heap"
	"fmt"
	"slices"
	"time"

	bolt "go.etcd.io/bbolt"
)

type (
	// trackedDiskClaimRecord is the internal representation of a tracked disk claim record.
	trackedDiskClaimRecord struct {
		// ID is the unique claim identifier.
		ID string
		// Key is the claimed user key.
		Key string
		// Owner is an optional logical owner identifier for diagnostics.
		Owner string
		// Token is a monotonically increasing fence token for stale-holder protection.
		Token int64
		// ExpiresAt is Unix epoch milliseconds when the lease expires.
		ExpiresAt int64
		// Version is the version of the claim record.
		Version uint64
	}

	// diskClaimExpiryItem is the internal representation of a disk claim expiry item.
	diskClaimExpiryItem struct {
		// Key is the claimed user key.
		Key string
		// ID is the unique claim identifier.
		ID string
		// Version is the version of the claim record.
		Version uint64
		// ExpiresAt is Unix epoch milliseconds when the lease expires.
		ExpiresAt int64
	}

	// diskClaimExpiryHeap is a min-heap of disk claim expiry items.
	diskClaimExpiryHeap []diskClaimExpiryItem
)

// Len returns the length of the heap.
func (h *diskClaimExpiryHeap) Len() int { return len(*h) }

// Less returns true if the item at index i is less than the item at index j.
func (h *diskClaimExpiryHeap) Less(i, j int) bool {
	return (*h)[i].ExpiresAt < (*h)[j].ExpiresAt
}

// Swap swaps the items at indices i and j.
func (h *diskClaimExpiryHeap) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
}

// Push pushes a new item onto the heap.
func (h *diskClaimExpiryHeap) Push(x any) {
	item, ok := x.(diskClaimExpiryItem)
	if !ok {
		panic(fmt.Sprintf("diskClaimExpiryHeap expected diskClaimExpiryItem, got %T", x))
	}

	*h = append(*h, item)
}

// Pop pops the last item from the heap.
func (h *diskClaimExpiryHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]

	return item
}

// reapExpiredTrackedClaimsLocked reaps expired tracked claims.
func (s *DiskStore) reapExpiredTrackedClaimsLocked(now int64) {
	if s.ost == nil {
		return
	}

	for len(s.claimExpiry) > 0 {
		item := s.claimExpiry[0]
		if item.ExpiresAt > now {
			return
		}

		heap.Pop(&s.claimExpiry)

		record, ok := s.ost.Meta(item.Key)
		if !ok || record == nil {
			continue
		}

		if record.ID != item.ID || record.Version != item.Version {
			continue
		}

		if record.ExpiresAt > now {
			continue
		}

		s.ost.UpdateMeta(item.Key, func(_ *trackedDiskClaimRecord) (*trackedDiskClaimRecord, bool) {
			return nil, true
		})
	}
}

// loadTrackedDiskValueLocked loads the value of a tracked disk key.
func (s *DiskStore) loadTrackedDiskValueLocked(key string) ([]byte, bool, error) {
	var (
		out    []byte
		exists bool
	)

	err := s.handle.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		raw := bucket.Get([]byte(key))
		if raw == nil {
			return nil
		}

		out = slices.Clone(raw)
		exists = true

		return nil
	})
	if err != nil {
		return nil, false, fmt.Errorf("%w: %w", ErrDiskStoreReadFailed, err)
	}

	return out, exists, nil
}

// deleteTrackedClaimedKeyFromDiskLocked deletes a tracked claimed key from the disk.
func (s *DiskStore) deleteTrackedClaimedKeyFromDiskLocked(key string) error {
	err := s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		return bucket.Delete([]byte(key))
	})
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDiskStoreWriteFailed, err)
	}

	return nil
}

// claimRandomTracked claims a random key from the tracked index.
func (s *DiskStore) claimRandomTracked(opts *ClaimOptions) (*EntryClaim, error) {
	release, err := s.beginOperation()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	if err := s.ensureWritable(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreWriteFailed, err)
	}

	normalized := normalizeClaimOptions(opts)
	if err := validateClaimOptions(normalized); err != nil {
		return nil, err
	}

	now := time.Now().UnixMilli()

	s.keysLock.Lock()
	defer s.keysLock.Unlock()

	s.reapExpiredTrackedClaimsLocked(now)

	key, ok := s.randomSelectableKeyWithTrackingLocked(normalized.Prefix)
	if !ok {
		return nil, nil //nolint:nilnil // no free key is a normal result.
	}

	value, exists, err := s.loadTrackedDiskValueLocked(key)
	if err != nil {
		return nil, err
	}

	if !exists {
		s.removeKeyIndexLocked(key)
		return nil, nil //nolint:nilnil // key disappeared, nothing to claim.
	}

	claim := s.applyTrackedClaimLocked(key, value, normalized.Owner, now+normalized.TTLMs)
	if claim == nil {
		return nil, nil //nolint:nilnil // key disappeared concurrently from tracked index.
	}

	return claim, nil
}

// isTrackedKeyClaimableLocked checks if a tracked key is claimable.
func (s *DiskStore) isTrackedKeyClaimableLocked(key string, now int64) bool {
	existing, ok := s.ost.Meta(key)
	if !ok {
		return false
	}

	if existing == nil {
		return true
	}

	if existing.ExpiresAt > now {
		return false
	}

	s.ost.UpdateMeta(key, func(_ *trackedDiskClaimRecord) (*trackedDiskClaimRecord, bool) {
		return nil, true
	})

	return true
}

// applyTrackedClaimLocked applies a tracked claim.
func (s *DiskStore) applyTrackedClaimLocked(key string, value []byte, owner string, expiresAt int64) *EntryClaim {
	token := s.claimToken.Add(1)
	record := &trackedDiskClaimRecord{
		ID:        claimIDFromToken(token),
		Key:       key,
		Owner:     owner,
		Token:     token,
		ExpiresAt: expiresAt,
		Version:   1,
	}

	var applied bool

	updated := s.ost.UpdateMeta(key, func(old *trackedDiskClaimRecord) (*trackedDiskClaimRecord, bool) {
		if old != nil {
			return old, false
		}

		applied = true

		return record, false
	})
	if !updated || !applied {
		return nil
	}

	heap.Push(&s.claimExpiry, diskClaimExpiryItem{
		Key:       key,
		ID:        record.ID,
		Version:   record.Version,
		ExpiresAt: record.ExpiresAt,
	})

	return &EntryClaim{
		ID:        record.ID,
		Key:       key,
		Owner:     record.Owner,
		Token:     record.Token,
		ExpiresAt: record.ExpiresAt,
		Entry: Entry{
			Key:   key,
			Value: value,
		},
	}
}

// releaseTrackedClaim releases a tracked claim.
func (s *DiskStore) releaseTrackedClaim(ref *ClaimRef) (bool, error) {
	release, err := s.beginOperation()
	if err != nil {
		return false, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	if !isValidClaimRef(ref) {
		return false, nil
	}

	if err := s.ensureWritable(); err != nil {
		return false, fmt.Errorf("%w: %w", ErrDiskStoreWriteFailed, err)
	}

	now := time.Now().UnixMilli()

	s.keysLock.Lock()
	defer s.keysLock.Unlock()

	record, ok := s.ost.Meta(ref.Key)
	if !ok || record == nil {
		return false, nil
	}

	if record.ExpiresAt <= now {
		s.ost.UpdateMeta(ref.Key, func(_ *trackedDiskClaimRecord) (*trackedDiskClaimRecord, bool) {
			return nil, true
		})

		return false, nil
	}

	if record.ID != ref.ID || record.Token != ref.Token {
		return false, nil
	}

	s.ost.UpdateMeta(ref.Key, func(_ *trackedDiskClaimRecord) (*trackedDiskClaimRecord, bool) {
		return nil, true
	})

	return true, nil
}

// completeTrackedClaim completes a tracked claim.
func (s *DiskStore) completeTrackedClaim(ref *ClaimRef, opts *CompleteClaimOptions) (bool, error) {
	release, err := s.beginOperation()
	if err != nil {
		return false, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	if !isValidClaimRef(ref) {
		return false, nil
	}

	if err := s.ensureWritable(); err != nil {
		return false, fmt.Errorf("%w: %w", ErrDiskStoreWriteFailed, err)
	}

	normalized := normalizeCompleteClaimOptions(opts)
	now := time.Now().UnixMilli()

	s.keysLock.Lock()
	defer s.keysLock.Unlock()

	record, ok := s.ost.Meta(ref.Key)
	if !ok || record == nil {
		return false, nil
	}

	if record.ExpiresAt <= now {
		s.ost.UpdateMeta(ref.Key, func(_ *trackedDiskClaimRecord) (*trackedDiskClaimRecord, bool) {
			return nil, true
		})

		return false, nil
	}

	if record.ID != ref.ID || record.Token != ref.Token {
		return false, nil
	}

	if !normalized.DeleteKey {
		s.ost.UpdateMeta(ref.Key, func(_ *trackedDiskClaimRecord) (*trackedDiskClaimRecord, bool) {
			return nil, true
		})

		return true, nil
	}

	if err := s.deleteTrackedClaimedKeyFromDiskLocked(ref.Key); err != nil {
		return false, err
	}

	s.removeKeyIndexLocked(ref.Key)

	return true, nil
}

// claimKeyTracked claims a key from the tracked index.
func (s *DiskStore) claimKeyTracked(key string, opts *ClaimOptions) (*EntryClaim, error) {
	release, err := s.beginOperation()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	if err := s.ensureWritable(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreWriteFailed, err)
	}

	if err := validateNonEmptyKey(key); err != nil {
		return nil, err
	}

	normalized := normalizeClaimOptions(opts)
	if err := validateClaimOptions(normalized); err != nil {
		return nil, err
	}

	now := time.Now().UnixMilli()

	s.keysLock.Lock()
	defer s.keysLock.Unlock()

	s.reapExpiredTrackedClaimsLocked(now)

	if !s.isTrackedKeyClaimableLocked(key, now) {
		return nil, nil //nolint:nilnil // key absent or currently claimed.
	}

	value, exists, err := s.loadTrackedDiskValueLocked(key)
	if err != nil {
		return nil, err
	}

	if !exists {
		s.removeKeyIndexLocked(key)
		return nil, nil //nolint:nilnil // key disappeared from durable store.
	}

	claim := s.applyTrackedClaimLocked(key, value, normalized.Owner, now+normalized.TTLMs)
	if claim == nil {
		return nil, nil //nolint:nilnil // key disappeared concurrently from index.
	}

	return claim, nil
}

// claimRandomManyTracked claims up to Count random keys from tracked index.
func (s *DiskStore) claimRandomManyTracked(opts *ClaimManyOptions) ([]*EntryClaim, error) {
	release, err := s.beginOperation()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	if err := s.ensureWritable(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreWriteFailed, err)
	}

	normalized := normalizeClaimManyOptions(opts)
	if err := validateClaimManyOptions(normalized); err != nil {
		return nil, err
	}

	now := time.Now().UnixMilli()

	s.keysLock.Lock()
	defer s.keysLock.Unlock()

	s.reapExpiredTrackedClaimsLocked(now)

	target := int(normalized.Count)
	claims := make([]*EntryClaim, 0, target)
	leaseExpiresAt := now + normalized.TTLMs

	err = s.handle.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		for len(claims) < target {
			key, ok := s.randomSelectableKeyWithTrackingLocked(normalized.Prefix)
			if !ok {
				break
			}

			value := bucket.Get([]byte(key))
			if value == nil {
				s.removeKeyIndexLocked(key)
				continue
			}

			claim := s.applyTrackedClaimLocked(key, slices.Clone(value), normalized.Owner, leaseExpiresAt)
			if claim == nil {
				continue
			}

			claims = append(claims, claim)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreReadFailed, err)
	}

	return claims, nil
}

// renewClaimTracked renews a tracked claim.
func (s *DiskStore) renewClaimTracked(ref *ClaimRef, opts *RenewClaimOptions) (bool, error) {
	release, err := s.beginOperation()
	if err != nil {
		return false, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	if !isValidClaimRef(ref) {
		return false, nil
	}

	if err := s.ensureWritable(); err != nil {
		return false, fmt.Errorf("%w: %w", ErrDiskStoreWriteFailed, err)
	}

	if err := validateRenewClaimOptions(opts); err != nil {
		return false, err
	}

	now := time.Now().UnixMilli()

	s.keysLock.Lock()
	defer s.keysLock.Unlock()

	record, ok := s.ost.Meta(ref.Key)
	if !ok || record == nil {
		return false, nil
	}

	if record.ExpiresAt <= now {
		s.ost.UpdateMeta(ref.Key, func(_ *trackedDiskClaimRecord) (*trackedDiskClaimRecord, bool) {
			return nil, true
		})

		return false, nil
	}

	if record.ID != ref.ID || record.Token != ref.Token {
		return false, nil
	}

	var renewed *trackedDiskClaimRecord

	updated := s.ost.UpdateMeta(ref.Key, func(old *trackedDiskClaimRecord) (*trackedDiskClaimRecord, bool) {
		if old == nil || old.ID != ref.ID || old.Token != ref.Token {
			return old, old == nil
		}

		next := *old
		next.ExpiresAt = now + opts.TTLMs
		next.Version++
		renewed = &next

		return renewed, false
	})
	if !updated || renewed == nil {
		return false, nil
	}

	heap.Push(&s.claimExpiry, diskClaimExpiryItem{
		Key:       ref.Key,
		ID:        renewed.ID,
		Version:   renewed.Version,
		ExpiresAt: renewed.ExpiresAt,
	})

	return true, nil
}
