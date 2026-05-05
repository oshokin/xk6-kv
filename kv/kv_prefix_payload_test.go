package kv

import (
	"testing"

	"github.com/grafana/sobek"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/js/modulestest"
)

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

func TestImportDeleteByPrefixOptions_NonObjectRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importDeleteByPrefixOptions(rt, rt.ToValue([]any{}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
}

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
