package store

import (
	"bytes"
	"fmt"
	"slices"

	bolt "go.etcd.io/bbolt"
)

// CompareAndSwap replaces value only if current equals 'old'. Returns true if swapped.
func (s *DiskStore) CompareAndSwap(key string, oldValue any, newValue any) (bool, error) {
	result, err := s.CompareAndSwapDetailed(key, oldValue, newValue, false)
	if err != nil {
		return false, err
	}

	return result.Swapped, nil
}

// CompareAndSwapDetailed performs compare-and-swap and returns structured
// mismatch metadata from the same bbolt transaction.
func (s *DiskStore) CompareAndSwapDetailed(
	key string,
	oldValue any,
	newValue any,
	includeCurrentOnMismatch bool,
) (*CompareAndSwapDetailedResult, error) {
	if err := validateNonEmptyKey(key); err != nil {
		return nil, err
	}

	release, err := s.beginOperation()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	if err := s.ensureWritable(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreCompareSwapFailed, err)
	}

	if s.trackKeys {
		// Keep bbolt mutation and index update as one logical operation.
		s.keysLock.Lock()
		defer s.keysLock.Unlock()
	}

	expectAbsent, oldBytes, err := s.prepareCompareAndSwapOldBytes(oldValue)
	if err != nil {
		return nil, err
	}

	// Convert new value to bytes if it's not already.
	newBytes, err := normalizeToBytes(newValue)
	if err != nil {
		return nil, err
	}

	var (
		result = &CompareAndSwapDetailedResult{
			Swapped: false,
			Reason:  CompareReasonMismatch,
			Existed: false,
		}
		inserted bool
	)

	err = s.handle.Update(func(tx *bolt.Tx) error {
		txResult, txInserted, txErr := s.compareAndSwapDetailedInTx(
			tx,
			key,
			expectAbsent,
			oldBytes,
			newBytes,
			includeCurrentOnMismatch,
		)
		if txErr != nil {
			return txErr
		}

		result = txResult
		inserted = txInserted

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreCompareSwapFailed, err)
	}

	// Indexes only change when a new key is inserted via expectAbsent semantics.
	if result.Swapped && inserted && s.trackKeys {
		s.addKeyIndexLocked(key)
	}

	return result, nil
}

// CompareAndDelete deletes key only if current equals "oldValue".
// Returns true if deleted.
func (s *DiskStore) CompareAndDelete(key string, oldValue any) (bool, error) {
	result, err := s.CompareAndDeleteDetailed(key, oldValue, false)
	if err != nil {
		return false, err
	}

	return result.Deleted, nil
}

// CompareAndDeleteDetailed deletes key only if current equals oldValue and
// returns structured mismatch metadata from the same bbolt transaction.
func (s *DiskStore) CompareAndDeleteDetailed(
	key string,
	oldValue any,
	includeCurrentOnMismatch bool,
) (*CompareAndDeleteDetailedResult, error) {
	if err := validateNonEmptyKey(key); err != nil {
		return nil, err
	}

	release, err := s.beginOperation()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreOpenFailed, err)
	}
	defer release()

	if err := s.ensureWritable(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreCompareDeleteFailed, err)
	}

	if s.trackKeys {
		// Keep bbolt mutation and index update as one logical operation.
		s.keysLock.Lock()
		defer s.keysLock.Unlock()
	}

	oldBytes, err := normalizeToBytes(oldValue)
	if err != nil {
		return nil, err
	}

	result := &CompareAndDeleteDetailedResult{
		Deleted: false,
		Reason:  CompareReasonMismatch,
		Existed: false,
	}

	err = s.handle.Update(func(tx *bolt.Tx) error {
		txResult, txErr := s.compareAndDeleteDetailedInTx(
			tx,
			key,
			oldBytes,
			includeCurrentOnMismatch,
		)
		if txErr != nil {
			return txErr
		}

		result = txResult

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDiskStoreCompareDeleteFailed, err)
	}

	// Update tracking structures if deleted.
	if result.Deleted && s.trackKeys {
		s.removeKeyIndexLocked(key)
	}

	return result, nil
}

// prepareCompareAndSwapOldBytes prepares CAS compare inputs:
// nil oldValue means "expect key absent"; non-nil values are normalized to bytes.
func (s *DiskStore) prepareCompareAndSwapOldBytes(oldValue any) (bool, []byte, error) {
	expectAbsent := oldValue == nil
	if expectAbsent {
		return true, nil, nil
	}

	oldBytes, err := normalizeToBytes(oldValue)
	if err != nil {
		return false, nil, err
	}

	return false, oldBytes, nil
}

// compareAndSwapDetailedInTx executes CAS decision + write inside one bbolt tx.
// It returns (result, insertedNewKey, error).
func (s *DiskStore) compareAndSwapDetailedInTx(
	tx *bolt.Tx,
	key string,
	expectAbsent bool,
	oldBytes []byte,
	newBytes []byte,
	includeCurrentOnMismatch bool,
) (*CompareAndSwapDetailedResult, bool, error) {
	bucket, err := s.compareBucket(tx)
	if err != nil {
		return nil, false, err
	}

	current := bucket.Get([]byte(key))
	exists := current != nil

	if expectAbsent {
		if exists {
			return s.newCompareAndSwapMismatchResult(true, current, includeCurrentOnMismatch), false, nil
		}
	} else if !exists || !bytes.Equal(current, oldBytes) {
		return s.newCompareAndSwapMismatchResult(exists, current, includeCurrentOnMismatch), false, nil
	}

	if err := bucket.Put([]byte(key), newBytes); err != nil {
		return nil, false, err
	}

	return &CompareAndSwapDetailedResult{
		Swapped: true,
		Reason:  CompareAndSwapReasonSwapped,
		Existed: exists,
	}, expectAbsent && !exists, nil
}

// compareAndDeleteDetailedInTx executes CAD decision + delete inside one bbolt tx.
func (s *DiskStore) compareAndDeleteDetailedInTx(
	tx *bolt.Tx,
	key string,
	oldBytes []byte,
	includeCurrentOnMismatch bool,
) (*CompareAndDeleteDetailedResult, error) {
	bucket, err := s.compareBucket(tx)
	if err != nil {
		return nil, err
	}

	current := bucket.Get([]byte(key))

	exists := current != nil
	if !exists || !bytes.Equal(current, oldBytes) {
		return s.newCompareAndDeleteMismatchResult(exists, current, includeCurrentOnMismatch), nil
	}

	if err := bucket.Delete([]byte(key)); err != nil {
		return nil, err
	}

	claimsBucket, err := ensureClaimsBucket(tx)
	if err != nil {
		return nil, err
	}

	if err := deleteClaimForKeyTx(claimsBucket, key); err != nil {
		return nil, err
	}

	return &CompareAndDeleteDetailedResult{
		Deleted: true,
		Reason:  CompareAndDeleteReasonDeleted,
		Existed: true,
	}, nil
}

// compareBucket fetches the configured bucket or returns ErrBucketNotFound.
func (s *DiskStore) compareBucket(tx *bolt.Tx) (*bolt.Bucket, error) {
	bucket := tx.Bucket(s.bucket)
	if bucket == nil {
		return nil, fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
	}

	return bucket, nil
}

// newCompareAndSwapMismatchResult builds a CAS mismatch payload with optional current.
func (s *DiskStore) newCompareAndSwapMismatchResult(
	existed bool,
	currentValue []byte,
	includeCurrentOnMismatch bool,
) *CompareAndSwapDetailedResult {
	result := &CompareAndSwapDetailedResult{
		Swapped: false,
		Reason:  CompareReasonMismatch,
		Existed: existed,
	}

	if includeCurrentOnMismatch && existed {
		result.Current = slices.Clone(currentValue)
		result.HasCurrent = true
	}

	return result
}

// newCompareAndDeleteMismatchResult builds a CAD mismatch payload with optional current.
func (s *DiskStore) newCompareAndDeleteMismatchResult(
	existed bool,
	currentValue []byte,
	includeCurrentOnMismatch bool,
) *CompareAndDeleteDetailedResult {
	result := &CompareAndDeleteDetailedResult{
		Deleted: false,
		Reason:  CompareReasonMismatch,
		Existed: existed,
	}

	if includeCurrentOnMismatch && existed {
		result.Current = slices.Clone(currentValue)
		result.HasCurrent = true
	}

	return result
}
