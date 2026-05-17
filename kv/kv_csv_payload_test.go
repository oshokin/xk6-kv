package kv

import (
	"testing"

	"github.com/grafana/sobek"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/js/modulestest"
)

func TestImportCSVOptions_NullRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	for _, value := range []sobek.Value{sobek.Undefined(), sobek.Null()} {
		_, err := importImportCSVOptions(rt, value)
		require.Error(t, err)

		var kvErr *Error
		require.ErrorAs(t, err, &kvErr)
		assert.Equal(t, InvalidOptionsError, kvErr.Name)
	}
}

func TestImportCSVOptions_MissingRequiredFieldsRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importImportCSVOptions(rt, rt.ToValue(map[string]any{
		"fileName": "./fixtures/users.csv",
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "keyColumn is required")
}

func TestImportCSVOptions_InvalidDelimiterRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importImportCSVOptions(rt, rt.ToValue(map[string]any{
		"fileName":  "./fixtures/users.csv",
		"keyColumn": "id",
		"delimiter": ";;",
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "delimiter")
}

func TestImportCSVOptions_HasHeaderFalseRequiresNumericKeyColumn(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importImportCSVOptions(rt, rt.ToValue(map[string]any{
		"fileName":  "./fixtures/users.csv",
		"keyColumn": "id",
		"hasHeader": false,
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "non-negative integer")
}

func TestImportCSVOptions_ValidOptions(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	opts, err := importImportCSVOptions(rt, rt.ToValue(map[string]any{
		"fileName":  "./fixtures/users.csv",
		"keyColumn": "id",
		"delimiter": ";",
		"hasHeader": true,
		"limit":     int64(25),
		"batchSize": int64(10),
	}))
	require.NoError(t, err)

	assert.Equal(t, "./fixtures/users.csv", opts.FileName)
	assert.Equal(t, "id", opts.KeyColumn)
	assert.Equal(t, ';', opts.Delimiter)
	assert.True(t, opts.HasHeader)
	assert.EqualValues(t, 25, opts.Limit)
	assert.EqualValues(t, 10, opts.BatchSize)
}

func TestImportCSVOptions_NegativeLimitRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importImportCSVOptions(rt, rt.ToValue(map[string]any{
		"fileName":  "./fixtures/users.csv",
		"keyColumn": "id",
		"limit":     int64(-1),
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "limit must be non-negative")
}

func TestImportCSVOptions_NegativeBatchSizeRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importImportCSVOptions(rt, rt.ToValue(map[string]any{
		"fileName":  "./fixtures/users.csv",
		"keyColumn": "id",
		"batchSize": int64(-1),
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "batchSize must be non-negative")
}
