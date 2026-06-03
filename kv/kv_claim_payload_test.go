package kv

import (
	"strings"
	"testing"

	"github.com/grafana/sobek"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/v2/js/modulestest"

	"github.com/oshokin/xk6-kv/kv/store"
)

// TestReleaseClaimsPayload_NullRejects verifies that release claims payload null rejects.
func TestReleaseClaimsPayload_NullRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importClaimRefsPayload(rt, "releaseClaims", sobek.Null())
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
}

// TestReleaseClaimsPayload_NonArrayRejects verifies that release claims payload non array rejects.
func TestReleaseClaimsPayload_NonArrayRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importClaimRefsPayload(rt, "releaseClaims", rt.ToValue(map[string]any{
		"id": "c1",
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
}

// TestReleaseClaimsPayload_EmptyArrayAccepted verifies that release claims payload empty array accepted.
func TestReleaseClaimsPayload_EmptyArrayAccepted(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	refs, err := importClaimRefsPayload(rt, "releaseClaims", rt.ToValue([]any{}))
	require.NoError(t, err)
	require.NotNil(t, refs)
	assert.Empty(t, refs)
}

// TestReleaseClaimsPayload_AcceptsEntryClaimSlice verifies that release claims payload accepts entry claim slice.
func TestReleaseClaimsPayload_AcceptsEntryClaimSlice(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	refs, err := importClaimRefsPayload(rt, "renewClaims", rt.ToValue([]*store.EntryClaim{
		{
			ID:    "claim-1",
			Key:   "users:1",
			Token: 1,
		},
	}))
	require.NoError(t, err)
	require.Len(t, refs, 1)
	assert.Equal(t, "claim-1", refs[0].ID)
	assert.Equal(t, "users:1", refs[0].Key)
	assert.EqualValues(t, 1, refs[0].Token)
}

// TestReleaseClaimsPayload_AcceptsEntryClaimValueSlice verifies that release claims payload accepts entry claim value slice.
func TestReleaseClaimsPayload_AcceptsEntryClaimValueSlice(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	refs, err := importClaimRefsPayload(rt, "releaseClaims", rt.ToValue([]store.EntryClaim{
		{
			ID:    "claim-1",
			Key:   "users:1",
			Token: 1,
		},
	}))
	require.NoError(t, err)
	require.Len(t, refs, 1)
	assert.Equal(t, "claim-1", refs[0].ID)
	assert.Equal(t, "users:1", refs[0].Key)
	assert.EqualValues(t, 1, refs[0].Token)
}

// TestReleaseClaimsPayload_AcceptsClaimRefSlice verifies that release claims payload accepts claim ref slice.
func TestReleaseClaimsPayload_AcceptsClaimRefSlice(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	refs, err := importClaimRefsPayload(rt, "releaseClaims", rt.ToValue([]store.ClaimRef{
		{
			ID:    "claim-1",
			Key:   "users:1",
			Token: 1,
		},
	}))
	require.NoError(t, err)
	require.Len(t, refs, 1)
	assert.Equal(t, "claim-1", refs[0].ID)
	assert.Equal(t, "users:1", refs[0].Key)
	assert.EqualValues(t, 1, refs[0].Token)
}

// TestReleaseClaimsPayload_AcceptsClaimRefPointerSlice verifies that release claims payload accepts claim ref pointer slice.
func TestReleaseClaimsPayload_AcceptsClaimRefPointerSlice(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	refs, err := importClaimRefsPayload(rt, "releaseClaims", rt.ToValue([]*store.ClaimRef{
		{
			ID:    "claim-1",
			Key:   "users:1",
			Token: 1,
		},
	}))
	require.NoError(t, err)
	require.Len(t, refs, 1)
	assert.Equal(t, "claim-1", refs[0].ID)
	assert.Equal(t, "users:1", refs[0].Key)
	assert.EqualValues(t, 1, refs[0].Token)
}

// TestReleaseClaimsPayload_RejectsNilClaimRefPointer verifies that release claims payload rejects nil claim ref pointer.
func TestReleaseClaimsPayload_RejectsNilClaimRefPointer(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importClaimRefsPayload(rt, "releaseClaims", rt.ToValue([]*store.ClaimRef{nil}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "claims[0] claim must be an object")
}

// TestReleaseClaimsPayload_RejectsNilEntryClaimPointer verifies that release claims payload rejects nil entry claim pointer.
func TestReleaseClaimsPayload_RejectsNilEntryClaimPointer(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importClaimRefsPayload(rt, "releaseClaims", rt.ToValue([]*store.EntryClaim{nil}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "claims[0] claim must be an object")
}

// TestReleaseClaimsPayload_RejectsInvalidNativeClaimRef verifies that release claims payload rejects invalid native claim ref.
func TestReleaseClaimsPayload_RejectsInvalidNativeClaimRef(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importClaimRefsPayload(rt, "releaseClaims", rt.ToValue([]store.ClaimRef{
		{
			ID:    "",
			Key:   "users:1",
			Token: 1,
		},
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "claim.id must be a non-empty string")
}

// TestReleaseClaimsPayload_InvalidItemRejectsWholeCall verifies that release claims payload invalid item rejects whole call.
func TestReleaseClaimsPayload_InvalidItemRejectsWholeCall(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importClaimRefsPayload(rt, "releaseClaims", rt.ToValue([]any{
		map[string]any{
			"id":    "c1",
			"key":   "users:1",
			"token": 1,
		},
		map[string]any{
			"id":    "c2",
			"key":   "",
			"token": 1,
		},
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
}

// TestRenewClaimsOptions_MissingTTLRejects verifies that renew claims options missing ttl rejects.
func TestRenewClaimsOptions_MissingTTLRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importRenewClaimsOptions(rt, rt.ToValue(map[string]any{}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "options.ttl is required")
}

// TestRenewClaimsOptions_InvalidTTLRejects verifies that renew claims options invalid ttl rejects.
func TestRenewClaimsOptions_InvalidTTLRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importRenewClaimsOptions(rt, rt.ToValue(map[string]any{
		"ttl": int64(0),
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "must be a positive integer")
}

// TestCompleteClaimsOptions_DefaultDeleteKeyTrue verifies that complete claims options default delete key true.
func TestCompleteClaimsOptions_DefaultDeleteKeyTrue(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	options, err := importCompleteClaimsOptions(rt, sobek.Undefined())
	require.NoError(t, err)
	require.NotNil(t, options)
	assert.True(t, options.DeleteKey)
}

// TestClaimKeysPayload_NullRejects verifies that claim keys payload null rejects.
func TestClaimKeysPayload_NullRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importClaimKeysPayload(rt, sobek.Null())
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
}

// TestClaimKeysPayload_NonArrayRejects verifies that claim keys payload non array rejects.
func TestClaimKeysPayload_NonArrayRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importClaimKeysPayload(rt, rt.ToValue("users:1"))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
}

// TestClaimKeysPayload_EmptyArrayAccepted verifies that claim keys payload empty array accepted.
func TestClaimKeysPayload_EmptyArrayAccepted(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	keys, err := importClaimKeysPayload(rt, rt.ToValue([]any{}))
	require.NoError(t, err)
	assert.Empty(t, keys)
}

// TestClaimKeysPayload_EmptyKeyRejects verifies that claim keys payload empty key rejects.
func TestClaimKeysPayload_EmptyKeyRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importClaimKeysPayload(rt, rt.ToValue([]any{"users:1", ""}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
}

// TestClaimKeysPayload_DuplicateKeyRejects verifies that claim keys payload duplicate key rejects.
func TestClaimKeysPayload_DuplicateKeyRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importClaimKeysPayload(rt, rt.ToValue([]any{"users:1", "users:1"}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
}

// TestClaimKeysOptions_OwnerMaxRejects verifies that claim keys options owner max rejects.
func TestClaimKeysOptions_OwnerMaxRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importClaimKeysOptions(rt, rt.ToValue(map[string]any{
		"owner": strings.Repeat("o", store.MaxClaimOwnerBytes+1),
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
}

// TestClaimKeysOptions_TTLRejectsNonPositive verifies that claim keys options ttl rejects non positive.
func TestClaimKeysOptions_TTLRejectsNonPositive(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importClaimKeysOptions(rt, rt.ToValue(map[string]any{
		"ttl": int64(0),
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
}

// TestClaimKeysOptions_AllOrNothingDefaultsFalse verifies that claim keys options all or nothing defaults false.
func TestClaimKeysOptions_AllOrNothingDefaultsFalse(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	options, err := importClaimKeysOptions(rt, sobek.Undefined())
	require.NoError(t, err)
	require.NotNil(t, options)
	assert.False(t, options.AllOrNothing)
	assert.Equal(t, store.DefaultClaimTTLMs, options.TTLMs)
}
