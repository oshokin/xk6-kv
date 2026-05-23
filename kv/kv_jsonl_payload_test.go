package kv

import (
	"testing"

	"github.com/grafana/sobek"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/js/modulestest"
)

// TestImportExportJSONLOptions_NullRejects verifies that import export jsonl options null rejects.
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

// TestImportExportJSONLOptions_NonObjectRejects verifies that import export jsonl options non object rejects.
func TestImportExportJSONLOptions_NonObjectRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importExportJSONLOptions(rt, rt.ToValue([]any{}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
}

// TestImportExportJSONLOptions_MissingFileNameRejects verifies that import export jsonl options missing file name rejects.
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

// TestImportExportJSONLOptions_EmptyFileNameRejects verifies that import export jsonl options empty file name rejects.
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

// TestImportExportJSONLOptions_InvalidPrefixRejects verifies that import export jsonl options invalid prefix rejects.
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

// TestImportExportJSONLOptions_FractionalLimitRejects verifies that import export jsonl options fractional limit rejects.
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

// TestImportExportJSONLOptions_LimitAboveMaxRejects verifies that import export jsonl options limit above max rejects.
func TestImportExportJSONLOptions_LimitAboveMaxRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importExportJSONLOptions(rt, rt.ToValue(map[string]any{
		"fileName": "./exports/users.jsonl",
		"limit":    MaxExportJSONLLimit + 1,
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "exportJSONL options.limit")
}

// TestImportExportJSONLOptions_ZeroAndNegativeLimitAllowed verifies that import export jsonl options zero and negative limit allowed.
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

// TestImportExportJSONLOptions_ValidOptions verifies that import export jsonl options valid options.
func TestImportExportJSONLOptions_ValidOptions(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	opts, err := importExportJSONLOptions(rt, rt.ToValue(map[string]any{
		"fileName": "./exports/users.jsonl",
		"prefix":   "user:",
		"limit":    int64(10),
	}))
	require.NoError(t, err)
	assert.Equal(t, "./exports/users.jsonl", opts.FileName)
	assert.Equal(t, "user:", opts.Prefix)
	assert.EqualValues(t, 10, opts.Limit)
}

// TestImportImportJSONLOptions_NullRejects verifies that import import jsonl options null rejects.
func TestImportImportJSONLOptions_NullRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	for _, value := range []sobek.Value{sobek.Undefined(), sobek.Null()} {
		_, err := importImportJSONLOptions(rt, value)
		require.Error(t, err)

		var kvErr *Error
		require.ErrorAs(t, err, &kvErr)
		assert.Equal(t, InvalidOptionsError, kvErr.Name)
	}
}

// TestImportImportJSONLOptions_NonObjectRejects verifies that import import jsonl options non object rejects.
func TestImportImportJSONLOptions_NonObjectRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importImportJSONLOptions(rt, rt.ToValue([]any{}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
}

// TestImportImportJSONLOptions_MissingFileNameRejects verifies that import import jsonl options missing file name rejects.
func TestImportImportJSONLOptions_MissingFileNameRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importImportJSONLOptions(rt, rt.ToValue(map[string]any{
		"limit":     int64(10),
		"batchSize": int64(100),
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "fileName is required")
}

// TestImportImportJSONLOptions_EmptyFileNameRejects verifies that import import jsonl options empty file name rejects.
func TestImportImportJSONLOptions_EmptyFileNameRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importImportJSONLOptions(rt, rt.ToValue(map[string]any{
		"fileName": "   ",
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "fileName is required")
}

// TestImportImportJSONLOptions_InvalidFileNameRejects verifies that import import jsonl options invalid file name rejects.
func TestImportImportJSONLOptions_InvalidFileNameRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importImportJSONLOptions(rt, rt.ToValue(map[string]any{
		"fileName": true,
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "options.fileName")
}

// TestImportImportJSONLOptions_FractionalLimitRejects verifies that import import jsonl options fractional limit rejects.
func TestImportImportJSONLOptions_FractionalLimitRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importImportJSONLOptions(rt, rt.ToValue(map[string]any{
		"fileName": "./imports/users.jsonl",
		"limit":    1.5,
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "options.limit")
}

// TestImportImportJSONLOptions_FractionalBatchSizeRejects verifies that import import jsonl options fractional batch size rejects.
func TestImportImportJSONLOptions_FractionalBatchSizeRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importImportJSONLOptions(rt, rt.ToValue(map[string]any{
		"fileName":  "./imports/users.jsonl",
		"batchSize": 1.5,
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "options.batchSize")
}

// TestImportImportJSONLOptions_LimitAboveMaxRejects verifies that import import jsonl options limit above max rejects.
func TestImportImportJSONLOptions_LimitAboveMaxRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importImportJSONLOptions(rt, rt.ToValue(map[string]any{
		"fileName": "./imports/users.jsonl",
		"limit":    MaxImportJSONLLimit + 1,
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "importJSONL options.limit")
}

// TestImportImportJSONLOptions_BatchSizeAboveMaxRejects verifies that import import jsonl options batch size above max rejects.
func TestImportImportJSONLOptions_BatchSizeAboveMaxRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importImportJSONLOptions(rt, rt.ToValue(map[string]any{
		"fileName":  "./imports/users.jsonl",
		"batchSize": MaxJSONLBatchSize + 1,
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "importJSONL options.batchSize")
}

// TestImportImportJSONLOptions_ZeroAndNegativeLimitAllowed verifies that import import jsonl options zero and negative limit allowed.
func TestImportImportJSONLOptions_ZeroAndNegativeLimitAllowed(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	for _, limit := range []int64{0, -1} {
		opts, err := importImportJSONLOptions(rt, rt.ToValue(map[string]any{
			"fileName": "./imports/users.jsonl",
			"limit":    limit,
		}))
		require.NoError(t, err)
		assert.Equal(t, limit, opts.Limit)
	}
}

// TestImportImportJSONLOptions_ZeroAndNegativeBatchSizeAllowed verifies that import import jsonl options zero and negative batch size allowed.
func TestImportImportJSONLOptions_ZeroAndNegativeBatchSizeAllowed(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	for _, batchSize := range []int64{0, -1} {
		opts, err := importImportJSONLOptions(rt, rt.ToValue(map[string]any{
			"fileName":  "./imports/users.jsonl",
			"batchSize": batchSize,
		}))
		require.NoError(t, err)
		assert.Equal(t, batchSize, opts.BatchSize)
	}
}

// TestImportImportJSONLOptions_ValidMinimalOptions verifies that import import jsonl options valid minimal options.
func TestImportImportJSONLOptions_ValidMinimalOptions(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	opts, err := importImportJSONLOptions(rt, rt.ToValue(map[string]any{
		"fileName": "./imports/users.jsonl",
	}))
	require.NoError(t, err)
	assert.Equal(t, "./imports/users.jsonl", opts.FileName)
	assert.EqualValues(t, 0, opts.Limit)
	assert.EqualValues(t, 0, opts.BatchSize)
}

// TestImportImportJSONLOptions_ValidFullOptions verifies that import import jsonl options valid full options.
func TestImportImportJSONLOptions_ValidFullOptions(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	opts, err := importImportJSONLOptions(rt, rt.ToValue(map[string]any{
		"fileName":  "./imports/users.jsonl",
		"limit":     int64(50),
		"batchSize": int64(10),
	}))
	require.NoError(t, err)
	assert.Equal(t, "./imports/users.jsonl", opts.FileName)
	assert.EqualValues(t, 50, opts.Limit)
	assert.EqualValues(t, 10, opts.BatchSize)
}

// TestValidateJSONLOptions_NullRejects verifies that validate jsonl options null rejects.
func TestValidateJSONLOptions_NullRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	for _, value := range []sobek.Value{sobek.Undefined(), sobek.Null()} {
		_, err := importValidateJSONLOptions(rt, value)
		require.Error(t, err)

		var kvErr *Error
		require.ErrorAs(t, err, &kvErr)
		assert.Equal(t, InvalidOptionsError, kvErr.Name)
	}
}

// TestValidateJSONLOptions_MissingFileNameRejects verifies that validate jsonl options missing file name rejects.
func TestValidateJSONLOptions_MissingFileNameRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importValidateJSONLOptions(rt, rt.ToValue(map[string]any{
		"limit": int64(10),
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "fileName is required")
}

// TestValidateJSONLOptions_ZeroAndNegativeLimitAllowed verifies that validate jsonl options zero and negative limit allowed.
func TestValidateJSONLOptions_ZeroAndNegativeLimitAllowed(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	for _, limit := range []int64{0, -1} {
		opts, err := importValidateJSONLOptions(rt, rt.ToValue(map[string]any{
			"fileName": "./imports/users.jsonl",
			"limit":    limit,
		}))
		require.NoError(t, err)
		assert.Equal(t, "./imports/users.jsonl", opts.FileName)
		assert.Equal(t, limit, opts.Limit)
	}
}
