package kv

import (
	"testing"

	"github.com/grafana/sobek"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/v2/js/modulestest"
)

// csvDelimiterParseCase is a test type used by csv delimiter parse case tests.
type csvDelimiterParseCase struct {
	// name identifies the name case under test.
	name string
	// parse holds test state for csv delimiter parse case.
	parse func(rt *sobek.Runtime, delimiter string) error
}

// csvDelimiterParseCases is a test helper for csv delimiter parse cases.
func csvDelimiterParseCases() []*csvDelimiterParseCase {
	return []*csvDelimiterParseCase{
		{
			name: "importCSV",
			parse: func(rt *sobek.Runtime, delimiter string) error {
				_, err := importImportCSVOptions(rt, rt.ToValue(map[string]any{
					"fileName":  "./fixtures/users.csv",
					"keyColumn": "id",
					"delimiter": delimiter,
				}))

				return err
			},
		},
		{
			name: "exportCSV",
			parse: func(rt *sobek.Runtime, delimiter string) error {
				_, err := importExportCSVOptions(rt, rt.ToValue(map[string]any{
					"fileName":  "./exports/users.csv",
					"columns":   []string{"status"},
					"delimiter": delimiter,
				}))

				return err
			},
		},
		{
			name: "validateCSV",
			parse: func(rt *sobek.Runtime, delimiter string) error {
				_, err := importValidateCSVOptions(rt, rt.ToValue(map[string]any{
					"fileName":  "./fixtures/users.csv",
					"delimiter": delimiter,
				}))

				return err
			},
		},
	}
}

// TestImportCSVOptions_NullRejects verifies that import csv options null rejects.
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

// TestImportCSVOptions_MissingRequiredFieldsRejects verifies that import csv options missing required fields rejects.
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

// TestImportCSVOptions_InvalidDelimiterRejects verifies that import csv options invalid delimiter rejects.
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

// TestCSVDelimiterRejectsNewline verifies that csv delimiter rejects newline.
func TestCSVDelimiterRejectsNewline(t *testing.T) {
	t.Parallel()

	for _, tc := range csvDelimiterParseCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rt := modulestest.NewRuntime(t).VU.Runtime()

			err := tc.parse(rt, "\n")
			require.Error(t, err)

			var kvErr *Error
			require.ErrorAs(t, err, &kvErr)
			assert.Equal(t, InvalidOptionsError, kvErr.Name)
			assert.Contains(t, kvErr.Message, "must be a valid CSV delimiter")
		})
	}
}

// TestCSVDelimiterRejectsCarriageReturn verifies that csv delimiter rejects carriage return.
func TestCSVDelimiterRejectsCarriageReturn(t *testing.T) {
	t.Parallel()

	for _, tc := range csvDelimiterParseCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rt := modulestest.NewRuntime(t).VU.Runtime()

			err := tc.parse(rt, "\r")
			require.Error(t, err)

			var kvErr *Error
			require.ErrorAs(t, err, &kvErr)
			assert.Equal(t, InvalidOptionsError, kvErr.Name)
			assert.Contains(t, kvErr.Message, "must be a valid CSV delimiter")
		})
	}
}

// TestCSVDelimiterRejectsQuote verifies that csv delimiter rejects quote.
func TestCSVDelimiterRejectsQuote(t *testing.T) {
	t.Parallel()

	for _, tc := range csvDelimiterParseCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rt := modulestest.NewRuntime(t).VU.Runtime()

			err := tc.parse(rt, `"`)
			require.Error(t, err)

			var kvErr *Error
			require.ErrorAs(t, err, &kvErr)
			assert.Equal(t, InvalidOptionsError, kvErr.Name)
			assert.Contains(t, kvErr.Message, "must be a valid CSV delimiter")
		})
	}
}

// TestCSVDelimiterRejectsReplacementRune verifies that csv delimiter rejects replacement rune.
func TestCSVDelimiterRejectsReplacementRune(t *testing.T) {
	t.Parallel()

	for _, tc := range csvDelimiterParseCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rt := modulestest.NewRuntime(t).VU.Runtime()

			err := tc.parse(rt, "\uFFFD")
			require.Error(t, err)

			var kvErr *Error
			require.ErrorAs(t, err, &kvErr)
			assert.Equal(t, InvalidOptionsError, kvErr.Name)
			assert.Contains(t, kvErr.Message, "must be a valid CSV delimiter")
		})
	}
}

// TestCSVDelimiterAcceptsSemicolon verifies that csv delimiter accepts semicolon.
func TestCSVDelimiterAcceptsSemicolon(t *testing.T) {
	t.Parallel()

	for _, tc := range csvDelimiterParseCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rt := modulestest.NewRuntime(t).VU.Runtime()
			require.NoError(t, tc.parse(rt, ";"))
		})
	}
}

// TestCSVDelimiterAcceptsTab verifies that csv delimiter accepts tab.
func TestCSVDelimiterAcceptsTab(t *testing.T) {
	t.Parallel()

	for _, tc := range csvDelimiterParseCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rt := modulestest.NewRuntime(t).VU.Runtime()
			require.NoError(t, tc.parse(rt, "\t"))
		})
	}
}

// TestImportCSVOptions_HasHeaderFalseRequiresNumericKeyColumn verifies that import csv options has header false requires numeric key column.
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

// TestImportCSVOptions_ValidOptions verifies that import csv options valid options.
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

// TestImportCSVOptions_NegativeLimitRejects verifies that import csv options negative limit rejects.
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

// TestImportCSVOptions_NegativeBatchSizeRejects verifies that import csv options negative batch size rejects.
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

// TestExportCSVOptions_NullRejects verifies that export csv options null rejects.
func TestExportCSVOptions_NullRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	for _, value := range []sobek.Value{sobek.Undefined(), sobek.Null()} {
		_, err := importExportCSVOptions(rt, value)
		require.Error(t, err)

		var kvErr *Error
		require.ErrorAs(t, err, &kvErr)
		assert.Equal(t, InvalidOptionsError, kvErr.Name)
	}
}

// TestExportCSVOptions_MissingFileNameRejects verifies that export csv options missing file name rejects.
func TestExportCSVOptions_MissingFileNameRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importExportCSVOptions(rt, rt.ToValue(map[string]any{
		"columns": []string{"status"},
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "fileName is required")
}

// TestExportCSVOptions_MissingColumnsRejects verifies that export csv options missing columns rejects.
func TestExportCSVOptions_MissingColumnsRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importExportCSVOptions(rt, rt.ToValue(map[string]any{
		"fileName": "./exports/users.csv",
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "columns is required")
}

// TestExportCSVOptions_EmptyColumnRejects verifies that export csv options empty column rejects.
func TestExportCSVOptions_EmptyColumnRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importExportCSVOptions(rt, rt.ToValue(map[string]any{
		"fileName": "./exports/users.csv",
		"columns":  []string{"status", " "},
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "non-empty string")
}

// TestExportCSVOptions_DuplicateColumnRejects verifies that export csv options duplicate column rejects.
func TestExportCSVOptions_DuplicateColumnRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importExportCSVOptions(rt, rt.ToValue(map[string]any{
		"fileName": "./exports/users.csv",
		"columns":  []string{"status", "status"},
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "duplicate")
}

// TestExportCSVOptions_ColumnWhitespaceIsSignificant verifies that export csv options column whitespace is significant.
func TestExportCSVOptions_ColumnWhitespaceIsSignificant(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	opts, err := importExportCSVOptions(rt, rt.ToValue(map[string]any{
		"fileName": "./exports/users.csv",
		"columns":  []string{"userId", " userId "},
	}))
	require.NoError(t, err)
	assert.Equal(t, []string{"userId", " userId "}, opts.Columns)
}

// TestExportCSVOptions_InvalidDelimiterRejects verifies that export csv options invalid delimiter rejects.
func TestExportCSVOptions_InvalidDelimiterRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importExportCSVOptions(rt, rt.ToValue(map[string]any{
		"fileName":  "./exports/users.csv",
		"columns":   []string{"status"},
		"delimiter": ";;",
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "delimiter")
}

// TestExportCSVOptions_IncludeKeyDefaultsTrue verifies that export csv options include key defaults true.
func TestExportCSVOptions_IncludeKeyDefaultsTrue(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	opts, err := importExportCSVOptions(rt, rt.ToValue(map[string]any{
		"fileName": "./exports/users.csv",
		"columns":  []string{"status"},
	}))
	require.NoError(t, err)
	assert.True(t, opts.IncludeKey)
}

// TestExportCSVOptions_RejectsImplicitKeyCollision verifies that export csv options rejects implicit key collision.
func TestExportCSVOptions_RejectsImplicitKeyCollision(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importExportCSVOptions(rt, rt.ToValue(map[string]any{
		"fileName": "./exports/users.csv",
		"columns":  []string{"key", "status"},
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, `must not contain "key" when includeKey=true`)
}

// TestExportCSVOptions_AllowsValueKeyColumnWhenIncludeKeyFalse verifies that export csv options allows value key column when include key false.
func TestExportCSVOptions_AllowsValueKeyColumnWhenIncludeKeyFalse(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	opts, err := importExportCSVOptions(rt, rt.ToValue(map[string]any{
		"fileName":   "./exports/users.csv",
		"columns":    []string{"key", "status"},
		"includeKey": false,
	}))
	require.NoError(t, err)
	assert.False(t, opts.IncludeKey)
	assert.Equal(t, []string{"key", "status"}, opts.Columns)
}

// TestExportCSVOptions_ZeroAndNegativeLimitAllowed verifies that export csv options zero and negative limit allowed.
func TestExportCSVOptions_ZeroAndNegativeLimitAllowed(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	for _, limit := range []int64{0, -1} {
		opts, err := importExportCSVOptions(rt, rt.ToValue(map[string]any{
			"fileName": "./exports/users.csv",
			"columns":  []string{"status"},
			"limit":    limit,
		}))
		require.NoError(t, err)
		assert.Equal(t, limit, opts.Limit)
	}
}

// TestExportCSVOptions_ValidOptions verifies that export csv options valid options.
func TestExportCSVOptions_ValidOptions(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	opts, err := importExportCSVOptions(rt, rt.ToValue(map[string]any{
		"fileName":   "./exports/users.csv",
		"prefix":     "users:",
		"limit":      int64(5),
		"delimiter":  ";",
		"columns":    []string{"status", "requestId"},
		"includeKey": false,
	}))
	require.NoError(t, err)
	assert.Equal(t, "./exports/users.csv", opts.FileName)
	assert.Equal(t, "users:", opts.Prefix)
	assert.EqualValues(t, 5, opts.Limit)
	assert.Equal(t, ';', opts.Delimiter)
	assert.Equal(t, []string{"status", "requestId"}, opts.Columns)
	assert.False(t, opts.IncludeKey)
}

// TestValidateCSVOptions_NullRejects verifies that validate csv options null rejects.
func TestValidateCSVOptions_NullRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	for _, value := range []sobek.Value{sobek.Undefined(), sobek.Null()} {
		_, err := importValidateCSVOptions(rt, value)
		require.Error(t, err)

		var kvErr *Error
		require.ErrorAs(t, err, &kvErr)
		assert.Equal(t, InvalidOptionsError, kvErr.Name)
	}
}

// TestValidateCSVOptions_MissingFileNameRejects verifies that validate csv options missing file name rejects.
func TestValidateCSVOptions_MissingFileNameRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importValidateCSVOptions(rt, rt.ToValue(map[string]any{
		"keyColumn": "id",
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "fileName is required")
}

// TestValidateCSVOptions_InvalidDelimiterRejects verifies that validate csv options invalid delimiter rejects.
func TestValidateCSVOptions_InvalidDelimiterRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importValidateCSVOptions(rt, rt.ToValue(map[string]any{
		"fileName":  "./fixtures/users.csv",
		"delimiter": ";;",
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "delimiter")
}

// TestValidateCSVOptions_HasHeaderFalseRequiresNumericKeyColumn verifies that validate csv options has header false requires numeric key column.
func TestValidateCSVOptions_HasHeaderFalseRequiresNumericKeyColumn(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importValidateCSVOptions(rt, rt.ToValue(map[string]any{
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

// TestValidateCSVOptions_NegativeLimitAcceptedForFullScan verifies that validate csv options negative limit accepted for full scan.
func TestValidateCSVOptions_NegativeLimitAcceptedForFullScan(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	opts, err := importValidateCSVOptions(rt, rt.ToValue(map[string]any{
		"fileName": "./fixtures/users.csv",
		"limit":    int64(-1),
	}))
	require.NoError(t, err)
	assert.EqualValues(t, -1, opts.Limit)
}

// TestValidateCSVOptions_ZeroLimitAccepted verifies that validate csv options zero limit accepted.
func TestValidateCSVOptions_ZeroLimitAccepted(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	opts, err := importValidateCSVOptions(rt, rt.ToValue(map[string]any{
		"fileName": "./fixtures/users.csv",
		"limit":    int64(0),
	}))
	require.NoError(t, err)
	assert.Zero(t, opts.Limit)
}

// TestValidateCSVOptions_PositiveLimitAccepted verifies that validate csv options positive limit accepted.
func TestValidateCSVOptions_PositiveLimitAccepted(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	opts, err := importValidateCSVOptions(rt, rt.ToValue(map[string]any{
		"fileName": "./fixtures/users.csv",
		"limit":    int64(50),
	}))
	require.NoError(t, err)
	assert.EqualValues(t, 50, opts.Limit)
}

// TestValidateCSVOptions_LimitAboveMaxRejects verifies that validate csv options limit above max rejects.
func TestValidateCSVOptions_LimitAboveMaxRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importValidateCSVOptions(rt, rt.ToValue(map[string]any{
		"fileName": "./fixtures/users.csv",
		"limit":    MaxImportCSVLimit + 1,
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
	assert.Contains(t, kvErr.Message, "must be less than or equal to 1000000")
}
