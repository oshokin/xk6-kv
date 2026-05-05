package kv

import (
	"path/filepath"
	"testing"

	"github.com/grafana/sobek"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/js/modulestest"
)

func TestImportExportJSONLOptions_NullRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	for _, value := range []sobek.Value{sobek.Undefined(), sobek.Null()} {
		_, err := importExportJSONLOptions(rt, value)
		require.Error(t, err)

		var kvErr *Error
		require.ErrorAs(t, err, &kvErr)
		assert.Equal(t, InvalidOptionsError, kvErr.Name)
	}
}

func TestImportExportJSONLOptions_NonObjectRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importExportJSONLOptions(rt, rt.ToValue([]any{}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
}

func TestImportExportJSONLOptions_MissingFileNameRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importExportJSONLOptions(rt, rt.ToValue(map[string]any{
		"prefix": "user:",
		"limit":  int64(10),
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "fileName is required")
}

func TestImportExportJSONLOptions_EmptyFileNameRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importExportJSONLOptions(rt, rt.ToValue(map[string]any{
		"fileName": "   ",
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "fileName is required")
}

func TestImportExportJSONLOptions_InvalidPrefixRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importExportJSONLOptions(rt, rt.ToValue(map[string]any{
		"fileName": "./exports/users.jsonl",
		"prefix":   true,
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
}

func TestImportExportJSONLOptions_FractionalLimitRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importExportJSONLOptions(rt, rt.ToValue(map[string]any{
		"fileName": "./exports/users.jsonl",
		"limit":    1.5,
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
}

func TestImportExportJSONLOptions_ZeroAndNegativeLimitAllowed(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	for _, limit := range []int64{0, -1} {
		opts, err := importExportJSONLOptions(rt, rt.ToValue(map[string]any{
			"fileName": "./exports/users.jsonl",
			"limit":    limit,
		}))
		require.NoError(t, err)
		assert.Equal(t, limit, opts.Limit)
	}
}

func TestImportExportJSONLOptions_ValidOptions(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	opts, err := importExportJSONLOptions(rt, rt.ToValue(map[string]any{
		"fileName": "./exports/users.jsonl",
		"prefix":   "user:",
		"limit":    int64(10),
	}))
	require.NoError(t, err)
	assert.Equal(t, filepath.Clean("./exports/users.jsonl"), opts.FileName)
	assert.Equal(t, "user:", opts.Prefix)
	assert.EqualValues(t, 10, opts.Limit)
}
