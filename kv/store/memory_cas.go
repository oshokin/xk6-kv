package store

import (
	"bytes"
	"slices"
)

// CompareAndSwap atomically replaces the current value with newValue only if the
// current value equals oldValue (or the key is absent when oldValue is nil).
func (s *MemoryStore) CompareAndSwap(key string, oldValue any, newValue any) (bool, error) {
	result, err := s.CompareAndSwapDetailed(key, oldValue, newValue, false)
	if err != nil {
		return false, err
	}

	return result.Swapped, nil
}

// CompareAndSwapDetailed atomically replaces the current value with newValue
// only if the compare condition matches, returning structured mismatch metadata.
func (s *MemoryStore) CompareAndSwapDetailed(
	key string,
	oldValue any,
	newValue any,
	includeCurrentOnMismatch bool,
) (*CompareAndSwapDetailedResult, error) {
	if err := validateNonEmptyKey(key); err != nil {
		return nil, err
	}

	release, err := s.guardMutation()
	if err != nil {
		return nil, err
	}
	defer release()

	expectAbsent := oldValue == nil

	var oldBytes []byte

	if !expectAbsent {
		oldBytes, err = normalizeToBytes(oldValue)
		if err != nil {
			return nil, err
		}
	}

	// Convert new value to bytes.
	newBytes, err := normalizeToBytes(newValue)
	if err != nil {
		return nil, err
	}

	shard := s.getShardByKey(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	current, exists := shard.container[key]

	switch {
	case expectAbsent:
		// CAS with nil oldValue: only succeed if key doesn't exist.
		if exists {
			return s.newCompareAndSwapMismatchResult(true, current, includeCurrentOnMismatch), nil
		}

		shard.container[key] = newBytes
		shard.addKeyTrackingLocked(key)

		return &CompareAndSwapDetailedResult{
			Swapped: true,
			Reason:  CompareAndSwapReasonSwapped,
			Existed: false,
		}, nil
	default:
		// CAS with non-nil oldValue: compare current value byte-for-byte.
		if !exists || !bytes.Equal(current, oldBytes) {
			return s.newCompareAndSwapMismatchResult(exists, current, includeCurrentOnMismatch), nil
		}

		shard.container[key] = newBytes

		// Indexes do not change because the key already existed.
		// No need to update keysList/keysMap/OSTree for existing keys.
		return &CompareAndSwapDetailedResult{
			Swapped: true,
			Reason:  CompareAndSwapReasonSwapped,
			Existed: true,
		}, nil
	}
}

// CompareAndDelete deletes key only if the current value equals oldValue.
func (s *MemoryStore) CompareAndDelete(key string, oldValue any) (bool, error) {
	result, err := s.CompareAndDeleteDetailed(key, oldValue, false)
	if err != nil {
		return false, err
	}

	return result.Deleted, nil
}

// CompareAndDeleteDetailed deletes key only if the current value equals oldValue
// and returns structured mismatch metadata.
func (s *MemoryStore) CompareAndDeleteDetailed(
	key string,
	oldValue any,
	includeCurrentOnMismatch bool,
) (*CompareAndDeleteDetailedResult, error) {
	if err := validateNonEmptyKey(key); err != nil {
		return nil, err
	}

	release, err := s.guardMutation()
	if err != nil {
		return nil, err
	}
	defer release()

	oldBytes, err := normalizeToBytes(oldValue)
	if err != nil {
		return nil, err
	}

	shard := s.getShardByKey(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	current, exists := shard.container[key]
	if !exists || !bytes.Equal(current, oldBytes) {
		return s.newCompareAndDeleteMismatchResult(exists, current, includeCurrentOnMismatch), nil
	}

	delete(shard.container, key)
	shard.removeKeyTrackingLocked(key)
	delete(shard.claims, key)

	return &CompareAndDeleteDetailedResult{
		Deleted: true,
		Reason:  CompareAndDeleteReasonDeleted,
		Existed: true,
	}, nil
}

// newCompareAndSwapMismatchResult builds a mismatch result for compare-and-swap.
func (s *MemoryStore) newCompareAndSwapMismatchResult(
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

// newCompareAndDeleteMismatchResult builds a mismatch result for compare-and-delete.
func (s *MemoryStore) newCompareAndDeleteMismatchResult(
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
