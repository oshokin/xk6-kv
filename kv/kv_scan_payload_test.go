package kv

import (
	"testing"

	"github.com/grafana/sobek"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/js/modulestest"

	"github.com/oshokin/xk6-kv/kv/store"
)

// TestImportListKeysOptions_Nullish verifies that import list keys options nullish.
func TestImportListKeysOptions_Nullish(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	for _, value := range []sobek.Value{sobek.Undefined(), sobek.Null()} {
		opts, err := importListKeysOptions(rt, value)
		require.NoError(t, err)
		assert.Empty(t, opts.Prefix)
		assert.EqualValues(t, 0, opts.Limit)
	}
}

// TestImportListKeysOptions_NonObjectRejects verifies that import list keys options non object rejects.
func TestImportListKeysOptions_NonObjectRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importListKeysOptions(rt, rt.ToValue([]any{}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
}

// TestImportListKeysOptions_ValidOptions verifies that import list keys options valid options.
func TestImportListKeysOptions_ValidOptions(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	value := rt.ToValue(map[string]any{
		"prefix": "user:",
		"limit":  int64(10),
	})

	opts, err := importListKeysOptions(rt, value)
	require.NoError(t, err)
	assert.Equal(t, "user:", opts.Prefix)
	assert.EqualValues(t, 10, opts.Limit)
}

// TestImportListKeysOptions_InvalidPrefixRejects verifies that import list keys options invalid prefix rejects.
func TestImportListKeysOptions_InvalidPrefixRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importListKeysOptions(rt, rt.ToValue(map[string]any{
		"prefix": 123,
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
}

// TestImportListKeysOptions_FractionalLimitRejects verifies that import list keys options fractional limit rejects.
func TestImportListKeysOptions_FractionalLimitRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importListKeysOptions(rt, rt.ToValue(map[string]any{
		"limit": 1.5,
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
}

// TestImportListKeysOptions_ZeroAndNegativeLimitAllowed verifies that import list keys options zero and negative limit allowed.
func TestImportListKeysOptions_ZeroAndNegativeLimitAllowed(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	for _, limit := range []int64{0, -1} {
		opts, err := importListKeysOptions(rt, rt.ToValue(map[string]any{
			"limit": limit,
		}))
		require.NoError(t, err)
		assert.Equal(t, limit, opts.Limit)
	}
}

// TestImportListKeysOptions_LimitAboveMaxRejects verifies that import list keys options limit above max rejects.
func TestImportListKeysOptions_LimitAboveMaxRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importListKeysOptions(rt, rt.ToValue(map[string]any{
		"limit": store.MaxListLimit + 1,
	}))
	requireInvalidOptionsError(t, err, "listKeys options.limit")
}

// TestImportScanKeysOptions_Nullish verifies that import scan keys options nullish.
func TestImportScanKeysOptions_Nullish(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	for _, value := range []sobek.Value{sobek.Undefined(), sobek.Null()} {
		opts, err := importScanKeysOptions(rt, value)
		require.NoError(t, err)
		assert.Empty(t, opts.Prefix)
		assert.Empty(t, opts.Cursor)
		assert.EqualValues(t, 0, opts.Limit)
	}
}

// TestImportScanKeysOptions_NonObjectRejects verifies that import scan keys options non object rejects.
func TestImportScanKeysOptions_NonObjectRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importScanKeysOptions(rt, rt.ToValue([]any{}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
}

// TestImportScanKeysOptions_ValidOptions verifies that import scan keys options valid options.
func TestImportScanKeysOptions_ValidOptions(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	value := rt.ToValue(map[string]any{
		"prefix": "user:",
		"limit":  int64(10),
		"cursor": "YQ==",
	})

	opts, err := importScanKeysOptions(rt, value)
	require.NoError(t, err)
	assert.Equal(t, "user:", opts.Prefix)
	assert.EqualValues(t, 10, opts.Limit)
	assert.Equal(t, "YQ==", opts.Cursor)
}

// TestImportScanKeysOptions_InvalidPrefixRejects verifies that import scan keys options invalid prefix rejects.
func TestImportScanKeysOptions_InvalidPrefixRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importScanKeysOptions(rt, rt.ToValue(map[string]any{
		"prefix": 123,
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
}

// TestImportScanKeysOptions_InvalidCursorRejects verifies that import scan keys options invalid cursor rejects.
func TestImportScanKeysOptions_InvalidCursorRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importScanKeysOptions(rt, rt.ToValue(map[string]any{
		"cursor": 123,
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
}

// TestImportScanKeysOptions_FractionalLimitRejects verifies that import scan keys options fractional limit rejects.
func TestImportScanKeysOptions_FractionalLimitRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importScanKeysOptions(rt, rt.ToValue(map[string]any{
		"limit": 1.5,
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
}

// TestImportScanKeysOptions_ZeroAndNegativeLimitAllowed verifies that import scan keys options zero and negative limit allowed.
func TestImportScanKeysOptions_ZeroAndNegativeLimitAllowed(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	for _, limit := range []int64{0, -1} {
		opts, err := importScanKeysOptions(rt, rt.ToValue(map[string]any{
			"limit": limit,
		}))
		require.NoError(t, err)
		assert.Equal(t, limit, opts.Limit)
	}
}

// TestImportScanKeysOptions_LimitAboveMaxRejects verifies that import scan keys options limit above max rejects.
func TestImportScanKeysOptions_LimitAboveMaxRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importScanKeysOptions(rt, rt.ToValue(map[string]any{
		"limit": store.MaxScanLimit + 1,
	}))
	requireInvalidOptionsError(t, err, "scanKeys options.limit")
}

// TestImportScanOptions_LimitAboveMaxRejects verifies that import scan options limit above max rejects.
func TestImportScanOptions_LimitAboveMaxRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importScanOptions(rt, rt.ToValue(map[string]any{
		"limit": store.MaxScanLimit + 1,
	}))
	requireInvalidOptionsError(t, err, "scan options.limit")
}

// TestImportListOptions_LimitAboveMaxRejects verifies that import list options limit above max rejects.
func TestImportListOptions_LimitAboveMaxRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importListOptions(rt, rt.ToValue(map[string]any{
		"limit": store.MaxListLimit + 1,
	}))
	requireInvalidOptionsError(t, err, "list options.limit")
}

// requireInvalidOptionsError is a test helper for require invalid options error.
func requireInvalidOptionsError(t *testing.T, err error, messagePart string) {
	t.Helper()

	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, messagePart)
}
