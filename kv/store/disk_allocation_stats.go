package store

import (
	"bytes"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

// boltAllocationClaimState classifies a durable key's claim lease for allocation stats.
type boltAllocationClaimState int

const (
	// boltAllocationClaimStateNone means the key has no live or expired claim record.
	boltAllocationClaimStateNone boltAllocationClaimState = iota
	// boltAllocationClaimStateLive means the key is blocked by an unexpired claim.
	boltAllocationClaimStateLive
	// boltAllocationClaimStateExpired means the key has an expired claim still present.
	boltAllocationClaimStateExpired
)

// AllocationStats returns a prefix-scoped allocation snapshot for disk backend.
// For trackKeys=true on writable stores, it reads the in-memory operational index.
// For read-only stores (or trackKeys=false), it scans durable keys and claims in bbolt.
func (s *DiskStore) AllocationStats(prefix string) (*AllocationStats, error) {
	release, err := s.beginOperation()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	now := time.Now().UnixMilli()

	if s.trackedClaimsEnabled() && !s.diskReadOnly() {
		return s.allocationStatsFromOperationalIndex(prefix, now), nil
	}

	return s.allocationStatsFromBolt(prefix, now)
}

// allocationStatsFromOperationalIndex builds allocation stats from the in-memory tracked index.
func (s *DiskStore) allocationStatsFromOperationalIndex(prefix string, now int64) *AllocationStats {
	unlockKeys := s.lockStatsIndexReader()
	defer unlockKeys()

	snapshot := &AllocationStats{
		Prefix:    prefix,
		Backend:   backendDiskName,
		TrackKeys: s.trackKeys,
	}

	if s.ost == nil {
		return snapshot
	}

	left, right := s.ost.RangeBounds(prefix)
	snapshot.Total = int64(right - left)

	if snapshot.Total == 0 {
		return snapshot
	}

	s.ost.WalkMetaPrefix(prefix, func(_ string, record *trackedDiskClaimRecord) bool {
		if record == nil {
			return true
		}

		if record.ExpiresAt <= now {
			snapshot.ClaimedExpired++
			return true
		}

		snapshot.ClaimedLive++

		return true
	})

	// Defensive clamp for potential index corruption.
	snapshot.Claimable = max(snapshot.Total-snapshot.ClaimedLive, 0)

	return snapshot
}

// allocationStatsFromBolt scans durable keys and claims to build allocation stats.
func (s *DiskStore) allocationStatsFromBolt(prefix string, now int64) (*AllocationStats, error) {
	snapshot := &AllocationStats{
		Prefix:    prefix,
		Backend:   backendDiskName,
		TrackKeys: s.trackKeys,
	}

	err := s.handle.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		claimsBucket := tx.Bucket(diskClaimsBucket)

		prefixBytes := []byte(prefix)
		cursor := bucket.Cursor()

		var (
			keyBytes []byte
			value    []byte
		)

		if prefix == "" {
			keyBytes, value = cursor.First()
		} else {
			keyBytes, value = cursor.Seek(prefixBytes)
		}

		for ; keyBytes != nil; keyBytes, value = cursor.Next() {
			if prefix != "" && !bytes.HasPrefix(keyBytes, prefixBytes) {
				break
			}

			// Defensive skip for non-value entries.
			if value == nil {
				continue
			}

			key := string(keyBytes)
			snapshot.Total++

			claimState, claimErr := s.classifyBoltAllocationClaimStateTx(claimsBucket, key, now)
			if claimErr != nil {
				return claimErr
			}

			switch claimState {
			case boltAllocationClaimStateNone:
				snapshot.Claimable++
			case boltAllocationClaimStateLive:
				snapshot.ClaimedLive++
			case boltAllocationClaimStateExpired:
				snapshot.ClaimedExpired++
				snapshot.Claimable++
			}
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreReadFailed, err)
	}

	return snapshot, nil
}

// classifyBoltAllocationClaimStateTx resolves claim state for key within a read transaction.
func (s *DiskStore) classifyBoltAllocationClaimStateTx(
	claimsBucket *bolt.Bucket,
	key string,
	now int64,
) (boltAllocationClaimState, error) {
	if claimsBucket == nil {
		return boltAllocationClaimStateNone, nil
	}

	claimIDRaw := claimsBucket.Get(s.claimKeyIndexKey(key))
	if claimIDRaw == nil {
		return boltAllocationClaimStateNone, nil
	}

	claimID := string(claimIDRaw)

	recordRaw := claimsBucket.Get(s.claimIDKey(claimID))
	if recordRaw == nil {
		return boltAllocationClaimStateNone, nil
	}

	record, err := s.decodeDiskClaim(recordRaw)
	if err != nil {
		return boltAllocationClaimStateNone, err
	}

	if record.Key != key || record.ID != claimID {
		return boltAllocationClaimStateNone, nil
	}

	if record.ExpiresAt <= now {
		return boltAllocationClaimStateExpired, nil
	}

	return boltAllocationClaimStateLive, nil
}
