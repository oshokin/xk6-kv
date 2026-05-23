package kv

import (
	"testing"

	"github.com/grafana/sobek"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/js/modulestest"

	"github.com/oshokin/xk6-kv/kv/store"
)

// TestImportDeleteByPrefixOptions_NullRejects verifies that import delete by prefix options null rejects.
func TestImportDeleteByPrefixOptions_NullRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	for _, value := range []sobek.Value{sobek.Undefined(), sobek.Null()} {
		_, err := importDeleteByPrefixOptions(rt, value)
		require.Error(t, err)

		var kvErr *Error
		require.ErrorAs(t, err, &kvErr)
		assert.Equal(t, InvalidOptionsError, kvErr.Name)
	}
}

// TestImportDeleteByPrefixOptions_NonObjectRejects verifies that import delete by prefix options non object rejects.
func TestImportDeleteByPrefixOptions_NonObjectRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importDeleteByPrefixOptions(rt, rt.ToValue([]any{}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
}

// TestImportDeleteByPrefixOptions_MissingPrefixRejects verifies that import delete by prefix options missing prefix rejects.
func TestImportDeleteByPrefixOptions_MissingPrefixRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importDeleteByPrefixOptions(rt, rt.ToValue(map[string]any{
		"limit": int64(1),
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "prefix is required")
}

// TestImportDeleteByPrefixOptions_EmptyPrefixRejects verifies that import delete by prefix options empty prefix rejects.
func TestImportDeleteByPrefixOptions_EmptyPrefixRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importDeleteByPrefixOptions(rt, rt.ToValue(map[string]any{
		"prefix": "",
		"limit":  int64(1),
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "non-empty")
}

// TestImportDeleteByPrefixOptions_InvalidPrefixRejects verifies that import delete by prefix options invalid prefix rejects.
func TestImportDeleteByPrefixOptions_InvalidPrefixRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importDeleteByPrefixOptions(rt, rt.ToValue(map[string]any{
		"prefix": 123,
		"limit":  int64(1),
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
}

// TestImportDeleteByPrefixOptions_MissingLimitRejects verifies that import delete by prefix options missing limit rejects.
func TestImportDeleteByPrefixOptions_MissingLimitRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importDeleteByPrefixOptions(rt, rt.ToValue(map[string]any{
		"prefix": "tmp:",
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "limit is required")
}

// TestImportDeleteByPrefixOptions_ZeroLimitRejects verifies that import delete by prefix options zero limit rejects.
func TestImportDeleteByPrefixOptions_ZeroLimitRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importDeleteByPrefixOptions(rt, rt.ToValue(map[string]any{
		"prefix": "tmp:",
		"limit":  int64(0),
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "positive integer")
}

// TestImportDeleteByPrefixOptions_NegativeLimitRejects verifies that import delete by prefix options negative limit rejects.
func TestImportDeleteByPrefixOptions_NegativeLimitRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importDeleteByPrefixOptions(rt, rt.ToValue(map[string]any{
		"prefix": "tmp:",
		"limit":  int64(-1),
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "positive integer")
}

// TestImportDeleteByPrefixOptions_FractionalLimitRejects verifies that import delete by prefix options fractional limit rejects.
func TestImportDeleteByPrefixOptions_FractionalLimitRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importDeleteByPrefixOptions(rt, rt.ToValue(map[string]any{
		"prefix": "tmp:",
		"limit":  1.5,
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
}

// TestImportDeleteByPrefixOptions_LimitAboveMaxRejects verifies that import delete by prefix options limit above max rejects.
func TestImportDeleteByPrefixOptions_LimitAboveMaxRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importDeleteByPrefixOptions(rt, rt.ToValue(map[string]any{
		"prefix": "tmp:",
		"limit":  store.MaxDeleteByPrefixLimit + 1,
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "deleteByPrefix options.limit")
}

// TestImportDeleteByPrefixOptions_ValidOptions verifies that import delete by prefix options valid options.
func TestImportDeleteByPrefixOptions_ValidOptions(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	options, err := importDeleteByPrefixOptions(rt, rt.ToValue(map[string]any{
		"prefix": "tmp:",
		"limit":  int64(100),
	}))
	require.NoError(t, err)
	assert.Equal(t, "tmp:", options.Prefix)
	assert.EqualValues(t, 100, options.Limit)
}
