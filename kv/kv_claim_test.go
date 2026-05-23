package kv

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oshokin/xk6-kv/kv/store"
)

// claimKeysBatchStore is a test double that stubs claim keys batch store behavior.
type claimKeysBatchStore struct {
	store.Store

	// existsByKey maps keys to configured exists for claim keys batch store.
	existsByKey map[string]bool
	// existsErrByKey maps keys to configured exists err for claim keys batch store.
	existsErrByKey map[string]error

	// claimByKey maps keys to configured claim for claim keys batch store.
	claimByKey map[string]*store.EntryClaim
	// claimErrByKey maps keys to configured claim err for claim keys batch store.
	claimErrByKey map[string]error
	// claimCalls records claim invocations for claim keys batch store.
	claimCalls []string

	// releaseByKey maps keys to configured release for claim keys batch store.
	releaseByKey map[string]bool
	// releaseErrByKey maps keys to configured release err for claim keys batch store.
	releaseErrByKey map[string]error
	// releaseCalls records release invocations for claim keys batch store.
	releaseCalls []string
	// existsCalls records exists invocations for claim keys batch store.
	existsCalls []string
}

// claimLifecycleBatchStore is a test double that stubs claim lifecycle batch store behavior.
type claimLifecycleBatchStore struct {
	store.Store

	// releaseResults holds configured release outcomes for claim lifecycle batch store.
	releaseResults map[string]bool
	// releaseErrByID maps claim IDs to configured release err for claim lifecycle batch store.
	releaseErrByID map[string]error

	// completeResults holds configured complete outcomes for claim lifecycle batch store.
	completeResults map[string]bool
	// completeErrByID maps claim IDs to configured complete err for claim lifecycle batch store.
	completeErrByID map[string]error

	// renewResults holds configured renew outcomes for claim lifecycle batch store.
	renewResults map[string]bool
	// renewErrByID maps claim IDs to configured renew err for claim lifecycle batch store.
	renewErrByID map[string]error
}

// claimLifecycleBatchStore.ReleaseClaim implements ReleaseClaim for claim lifecycle batch store test scenarios.
func (s *claimLifecycleBatchStore) ReleaseClaim(ref *store.ClaimRef) (bool, error) {
	if ref == nil {
		return false, nil
	}

	if err, ok := s.releaseErrByID[ref.ID]; ok && err != nil {
		return false, err
	}

	if released, ok := s.releaseResults[ref.ID]; ok {
		return released, nil
	}

	return true, nil
}

// claimLifecycleBatchStore.CompleteClaim implements CompleteClaim for claim lifecycle batch store test scenarios.
func (s *claimLifecycleBatchStore) CompleteClaim(ref *store.ClaimRef, _ *store.CompleteClaimOptions) (bool, error) {
	if ref == nil {
		return false, nil
	}

	if err, ok := s.completeErrByID[ref.ID]; ok && err != nil {
		return false, err
	}

	if completed, ok := s.completeResults[ref.ID]; ok {
		return completed, nil
	}

	return true, nil
}

// claimLifecycleBatchStore.RenewClaim implements RenewClaim for claim lifecycle batch store test scenarios.
func (s *claimLifecycleBatchStore) RenewClaim(ref *store.ClaimRef, _ *store.RenewClaimOptions) (bool, error) {
	if ref == nil {
		return false, nil
	}

	if err, ok := s.renewErrByID[ref.ID]; ok && err != nil {
		return false, err
	}

	if renewed, ok := s.renewResults[ref.ID]; ok {
		return renewed, nil
	}

	return true, nil
}

// claimKeysBatchStore.Exists implements Exists for claim keys batch store test scenarios.
func (s *claimKeysBatchStore) Exists(key string) (bool, error) {
	s.existsCalls = append(s.existsCalls, key)

	if err, ok := s.existsErrByKey[key]; ok && err != nil {
		return false, err
	}

	if exists, ok := s.existsByKey[key]; ok {
		return exists, nil
	}

	return false, nil
}

// claimKeysBatchStore.ClaimKey implements ClaimKey for claim keys batch store test scenarios.
func (s *claimKeysBatchStore) ClaimKey(key string, _ *store.ClaimOptions) (*store.EntryClaim, error) {
	s.claimCalls = append(s.claimCalls, key)

	if err, ok := s.claimErrByKey[key]; ok && err != nil {
		return nil, err
	}

	if claim, ok := s.claimByKey[key]; ok {
		return claim, nil
	}

	return nil, nil //nolint:nilnil // nil claim is the busy-key contract for ClaimKey.
}

// TestClaimKeys_DoesNotCallExistsForSuccessfullyClaimedKey verifies that claim keys does not call exists for successfully claimed key.
func TestClaimKeys_DoesNotCallExistsForSuccessfullyClaimedKey(t *testing.T) {
	t.Parallel()

	stub := &claimKeysBatchStore{
		claimByKey: map[string]*store.EntryClaim{
			"users:1": newTestEntryClaim(),
		},
	}

	result, err := claimKeysBatch(
		stub,
		[]string{"users:1"},
		&claimKeysOptions{TTLMs: 30_000},
	)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Claimed, 1)
	assert.Empty(t, result.Busy)
	assert.Empty(t, result.Missing)
	assert.Equal(t, []string{"users:1"}, stub.claimCalls)
	assert.Empty(t, stub.existsCalls)
}

// claimKeysBatchStore.ReleaseClaim implements ReleaseClaim for claim keys batch store test scenarios.
func (s *claimKeysBatchStore) ReleaseClaim(ref *store.ClaimRef) (bool, error) {
	if ref == nil {
		return false, nil
	}

	s.releaseCalls = append(s.releaseCalls, ref.Key)

	if err, ok := s.releaseErrByKey[ref.Key]; ok && err != nil {
		return false, err
	}

	if released, ok := s.releaseByKey[ref.Key]; ok {
		return released, nil
	}

	return true, nil
}

// TestClaimKeys_AllOrNothingReleasesAcquiredClaimsOnMissingKey verifies that claim keys all or nothing releases acquired claims on missing key.
func TestClaimKeys_AllOrNothingReleasesAcquiredClaimsOnMissingKey(t *testing.T) {
	t.Parallel()

	stub := &claimKeysBatchStore{
		existsByKey: map[string]bool{
			"users:1":       true,
			"users:missing": false,
		},
		claimByKey: map[string]*store.EntryClaim{
			"users:1": newTestEntryClaim(),
		},
		releaseByKey: map[string]bool{
			"users:1": true,
		},
	}

	result, err := claimKeysBatch(
		stub,
		[]string{"users:1", "users:missing"},
		&claimKeysOptions{TTLMs: 30_000, AllOrNothing: true},
	)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Empty(t, result.Claimed)
	assert.Empty(t, result.Busy)
	assert.Equal(t, []string{"users:missing"}, result.Missing)
	assert.Equal(t, []string{"users:1"}, stub.releaseCalls)
}

// TestClaimKeys_AllOrNothingReleasesAcquiredClaimsOnBusyKey verifies that claim keys all or nothing releases acquired claims on busy key.
func TestClaimKeys_AllOrNothingReleasesAcquiredClaimsOnBusyKey(t *testing.T) {
	t.Parallel()

	stub := &claimKeysBatchStore{
		existsByKey: map[string]bool{
			"users:1":    true,
			"users:busy": true,
		},
		claimByKey: map[string]*store.EntryClaim{
			"users:1": newTestEntryClaim(),
			// users:busy intentionally absent -> busy (claim=nil)
		},
		releaseByKey: map[string]bool{
			"users:1": true,
		},
	}

	result, err := claimKeysBatch(
		stub,
		[]string{"users:1", "users:busy"},
		&claimKeysOptions{TTLMs: 30_000, AllOrNothing: true},
	)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Empty(t, result.Claimed)
	assert.Equal(t, []string{"users:busy"}, result.Busy)
	assert.Empty(t, result.Missing)
	assert.Equal(t, []string{"users:1"}, stub.releaseCalls)
}

// TestClaimKeys_AllOrNothing_RollbackExpiredClaimDoesNotReject verifies that claim keys all or nothing rollback expired claim does not reject.
func TestClaimKeys_AllOrNothing_RollbackExpiredClaimDoesNotReject(t *testing.T) {
	t.Parallel()

	stub := &claimKeysBatchStore{
		existsByKey: map[string]bool{
			"users:1":       true,
			"users:missing": false,
		},
		claimByKey: map[string]*store.EntryClaim{
			"users:1": newTestEntryClaimWithExpiresAt(time.Now().Add(-time.Minute).UnixMilli()),
		},
		// Rollback release=false simulates claim already expired/raced.
		releaseByKey: map[string]bool{
			"users:1": false,
		},
	}

	result, err := claimKeysBatch(
		stub,
		[]string{"users:1", "users:missing"},
		&claimKeysOptions{TTLMs: 1, AllOrNothing: true},
	)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Empty(t, result.Claimed)
	assert.Equal(t, []string{"users:missing"}, result.Missing)
	assert.Equal(t, []string{"users:1"}, stub.releaseCalls)
}

// allOrNothing is not transactional; rollback is intentionally best-effort.
func TestClaimKeys_AllOrNothing_DocsContract_BestEffortRollback(t *testing.T) {
	t.Parallel()

	stub := &claimKeysBatchStore{
		existsByKey: map[string]bool{
			"users:1":       true,
			"users:missing": false,
		},
		claimByKey: map[string]*store.EntryClaim{
			"users:1": newTestEntryClaimWithExpiresAt(time.Now().Add(time.Minute).UnixMilli()),
		},
		releaseByKey: map[string]bool{
			"users:1": false,
		},
	}

	result, err := claimKeysBatch(
		stub,
		[]string{"users:1", "users:missing"},
		&claimKeysOptions{TTLMs: 30_000, AllOrNothing: true},
	)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Empty(t, result.Claimed)
	assert.Equal(t, []string{"users:missing"}, result.Missing)
	assert.Equal(t, []string{"users:1"}, stub.releaseCalls)
}

// TestClaimKeys_AllOrNothing_RollbackTechnicalErrorRejects verifies that claim keys all or nothing rollback technical error rejects.
func TestClaimKeys_AllOrNothing_RollbackTechnicalErrorRejects(t *testing.T) {
	t.Parallel()

	stub := &claimKeysBatchStore{
		existsByKey: map[string]bool{
			"users:1":       true,
			"users:missing": false,
		},
		claimByKey: map[string]*store.EntryClaim{
			"users:1": newTestEntryClaim(),
		},
		releaseErrByKey: map[string]error{
			"users:1": errors.New("release failed"),
		},
	}

	result, err := claimKeysBatch(
		stub,
		[]string{"users:1", "users:missing"},
		&claimKeysOptions{TTLMs: 30_000, AllOrNothing: true},
	)
	require.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorContains(t, err, "release failed")
}

// TestReleaseClaims_TechnicalErrorMentionsProgress verifies that release claims technical error mentions progress.
func TestReleaseClaims_TechnicalErrorMentionsProgress(t *testing.T) {
	t.Parallel()

	failErr := errors.New("backend release failure")
	stub := &claimLifecycleBatchStore{
		releaseErrByID: map[string]error{
			"claim-3": failErr,
		},
	}

	refs := []*store.ClaimRef{
		{ID: "claim-1", Key: "users:1", Token: 1},
		{ID: "claim-2", Key: "users:2", Token: 2},
		{ID: "claim-3", Key: "users:3", Token: 3},
	}

	result, err := releaseClaimsBatch(stub, refs)
	require.Error(t, err)
	assert.Nil(t, result)
	require.ErrorIs(t, err, failErr)
	require.ErrorContains(t, err, "releaseClaims failed at index 2 after releasing 2 of 3 claims")
	require.ErrorContains(t, err, "previous successful operations were not rolled back")
}

// TestCompleteClaims_TechnicalErrorMentionsProgress verifies that complete claims technical error mentions progress.
func TestCompleteClaims_TechnicalErrorMentionsProgress(t *testing.T) {
	t.Parallel()

	failErr := errors.New("backend complete failure")
	stub := &claimLifecycleBatchStore{
		completeErrByID: map[string]error{
			"claim-3": failErr,
		},
	}

	refs := []*store.ClaimRef{
		{ID: "claim-1", Key: "users:1", Token: 1},
		{ID: "claim-2", Key: "users:2", Token: 2},
		{ID: "claim-3", Key: "users:3", Token: 3},
	}

	result, err := completeClaimsBatch(stub, refs, &store.CompleteClaimOptions{DeleteKey: true})
	require.Error(t, err)
	assert.Nil(t, result)
	require.ErrorIs(t, err, failErr)
	require.ErrorContains(t, err, "completeClaims failed at index 2 after completing 2 of 3 claims")
	require.ErrorContains(t, err, "previous successful operations were not rolled back")
}

// TestRenewClaims_TechnicalErrorMentionsProgress verifies that renew claims technical error mentions progress.
func TestRenewClaims_TechnicalErrorMentionsProgress(t *testing.T) {
	t.Parallel()

	failErr := errors.New("backend renew failure")
	stub := &claimLifecycleBatchStore{
		renewErrByID: map[string]error{
			"claim-3": failErr,
		},
	}

	refs := []*store.ClaimRef{
		{ID: "claim-1", Key: "users:1", Token: 1},
		{ID: "claim-2", Key: "users:2", Token: 2},
		{ID: "claim-3", Key: "users:3", Token: 3},
	}

	result, err := renewClaimsBatch(stub, refs, &store.RenewClaimOptions{TTLMs: 30_000})
	require.Error(t, err)
	assert.Nil(t, result)
	require.ErrorIs(t, err, failErr)
	require.ErrorContains(t, err, "renewClaims failed at index 2 after renewing 2 of 3 claims")
	require.ErrorContains(t, err, "previous successful operations were not rolled back")
}

// newTestEntryClaim creates test entry claim for tests.
func newTestEntryClaim() *store.EntryClaim {
	return newTestEntryClaimWithExpiresAt(time.Now().Add(time.Minute).UnixMilli())
}

// newTestEntryClaimWithExpiresAt creates test entry claim with expires at for tests.
func newTestEntryClaimWithExpiresAt(expiresAt int64) *store.EntryClaim {
	const key = "users:1"

	return &store.EntryClaim{
		ID:    key + ":claim",
		Key:   key,
		Token: 1,
		Owner: "test-vu",
		// Explicit expiry enables deterministic rollback behavior tests.
		ExpiresAt: expiresAt,
		Entry: store.Entry{
			Key:   key,
			Value: "value",
		},
	}
}
