package store

import (
	"bytes"
	"fmt"
	"os"
	"time"

	bolt "go.etcd.io/bbolt"
)

// diskStatsCounts is a helper struct to count the number of keys and claims.
type diskStatsCounts struct {
	// KeyCount is the number of keys in the store.
	KeyCount int64
	// Claims is the number of claims in the store.
	Claims ClaimStats
}

// Stats returns a diagnostic snapshot of the disk store.
func (s *DiskStore) Stats() (*StatsSnapshot, error) {
	release, err := s.beginOperation()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	unlockKeys := s.lockStatsIndexReader()
	defer unlockKeys()

	now := time.Now().UnixMilli()

	counts, err := s.readStatsCounts(now)
	if err != nil {
		return nil, err
	}

	indexStats := s.buildIndexStats(counts.KeyCount)

	// Release the read lock before filesystem stat syscall.
	unlockKeys()

	info, err := os.Stat(s.path)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreStatFailed, err)
	}

	return &StatsSnapshot{
		Backend:   backendDiskName,
		TrackKeys: s.trackKeys,
		Count:     counts.KeyCount,
		Claims:    counts.Claims,
		Index:     indexStats,
		Disk: &DiskStats{
			Path:      s.path,
			SizeBytes: info.Size(),
			ReadOnly:  s.diskReadOnly(),
		},
	}, nil
}

// lockStatsIndexReader locks the index reader.
func (s *DiskStore) lockStatsIndexReader() func() {
	if !s.trackKeys {
		return func() {}
	}

	s.keysLock.RLock()

	var unlocked bool

	return func() {
		if unlocked {
			return
		}

		s.keysLock.RUnlock()

		unlocked = true
	}
}

// readStatsCounts reads the stats counts.
func (s *DiskStore) readStatsCounts(now int64) (*diskStatsCounts, error) {
	counts := &diskStatsCounts{}
	useTrackedClaims := s.trackedClaimsEnabled() && !s.diskReadOnly()

	err := s.handle.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}

		counts.KeyCount = int64(bucket.Stats().KeyN)

		if useTrackedClaims {
			return nil
		}

		claimsBucket := tx.Bucket(diskClaimsBucket)

		claims, statsErr := s.countDiskClaimsStats(claimsBucket, now)
		if statsErr != nil {
			return statsErr
		}

		counts.Claims = *claims

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreReadFailed, err)
	}

	if useTrackedClaims {
		counts.Claims = s.countTrackedClaimsLocked(now)
	}

	return counts, nil
}

// buildIndexStats builds the index stats.
func (s *DiskStore) buildIndexStats(keyCount int64) *IndexStats {
	if !s.trackKeys {
		return nil
	}

	indexStats := &IndexStats{
		Enabled:  true,
		KeysList: int64(len(s.keysList)),
		KeysMap:  int64(len(s.keysMap)),
		OST:      0,
	}

	if s.ost != nil {
		indexStats.OST = int64(s.ost.Len())
	}

	indexStats.Consistent = indexStats.KeysList == indexStats.KeysMap &&
		indexStats.KeysMap == indexStats.OST &&
		indexStats.OST == keyCount

	return indexStats
}

// countDiskClaimsStats counts the disk claims stats.
func (s *DiskStore) countDiskClaimsStats(claims *bolt.Bucket, now int64) (*ClaimStats, error) {
	stats := &ClaimStats{}

	if claims == nil {
		return stats, nil
	}

	cursor := claims.Cursor()

	for k, v := cursor.Seek(claimIDPrefix); k != nil && bytes.HasPrefix(k, claimIDPrefix); k, v = cursor.Next() {
		record, decodeErr := s.decodeDiskClaim(v)
		if decodeErr != nil {
			return nil, decodeErr
		}

		if record.ExpiresAt <= now {
			stats.Expired++
		} else {
			stats.Live++
		}
	}

	return stats, nil
}

// countTrackedClaimsLocked counts the tracked claims.
func (s *DiskStore) countTrackedClaimsLocked(now int64) ClaimStats {
	var stats ClaimStats

	if s.ost == nil {
		return stats
	}

	s.ost.WalkMeta(func(_ string, record *trackedDiskClaimRecord) {
		if record == nil {
			return
		}

		if record.ExpiresAt <= now {
			stats.Expired++
			return
		}

		stats.Live++
	})

	return stats
}
