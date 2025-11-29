package store

// blockMutations prevents future mutating operations from succeeding until unblocked.
func (s *MemoryStore) blockMutations(reason error) error {
	if reason == nil {
		reason = ErrMutationBlocked
	}

	s.mutationGate.Lock()
	defer s.mutationGate.Unlock()

	// Check if already blocked.
	if s.isMutationBlocked {
		if s.mutationBlockReason != nil {
			return s.mutationBlockReason
		}

		return ErrMutationBlocked
	}

	// Set the block flag and reason.
	s.isMutationBlocked = true
	s.mutationBlockReason = reason

	return nil
}

// unblockMutations clears any outstanding mutation blocks.
func (s *MemoryStore) unblockMutations() {
	s.mutationGate.Lock()
	defer s.mutationGate.Unlock()

	s.mutationBlockReason = nil
	s.isMutationBlocked = false
}

// guardMutation acquires the shared mutation gate and verifies mutations are allowed.
// It returns a release function that must be called (typically via defer) once the mutation ends.
func (s *MemoryStore) guardMutation() (func(), error) {
	s.mutationGate.RLock()

	// Check block status while holding the lock.
	if s.isMutationBlocked {
		// We need to extract the reason while holding the lock to avoid race conditions.
		reason := s.mutationBlockReason
		s.mutationGate.RUnlock()

		if reason != nil {
			return nil, reason
		}

		return nil, ErrMutationBlocked
	}

	return s.finishWriter, nil
}

// finishWriter releases the shared mutation gate acquired in guardMutation.
func (s *MemoryStore) finishWriter() {
	s.mutationGate.RUnlock()
}
