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

func TestImportImportJSONLOptions_NonObjectRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importImportJSONLOptions(rt, rt.ToValue([]any{}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
}

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

func TestImportImportJSONLOptions_ValidMinimalOptions(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	opts, err := importImportJSONLOptions(rt, rt.ToValue(map[string]any{
		"fileName": "./imports/users.jsonl",
	}))
	require.NoError(t, err)
	assert.Equal(t, filepath.Clean("./imports/users.jsonl"), opts.FileName)
	assert.EqualValues(t, 0, opts.Limit)
	assert.EqualValues(t, 0, opts.BatchSize)
}

func TestImportImportJSONLOptions_ValidFullOptions(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	opts, err := importImportJSONLOptions(rt, rt.ToValue(map[string]any{
		"fileName":  "./imports/users.jsonl",
		"limit":     int64(50),
		"batchSize": int64(10),
	}))
	require.NoError(t, err)
	assert.Equal(t, filepath.Clean("./imports/users.jsonl"), opts.FileName)
	assert.EqualValues(t, 50, opts.Limit)
	assert.EqualValues(t, 10, opts.BatchSize)
}
