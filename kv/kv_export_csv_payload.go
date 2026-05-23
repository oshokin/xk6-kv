package kv

import (
	"fmt"
	"slices"
	"strings"

	"github.com/grafana/sobek"
	"go.k6.io/k6/js/common"
)

// exportCSVOptions holds parsed options for the corresponding KV method.
type exportCSVOptions struct {
	// FileName is the path of the CSV or JSONL file.
	FileName string
	// Prefix selects only keys that start with the given string.
	Prefix string
	// Limit caps how many rows or entries are processed.
	Limit int64
	// Delimiter is the CSV field separator rune.
	Delimiter rune
	// Columns lists CSV column names to export.
	Columns []string
	// IncludeKey adds the store key as its own CSV column.
	IncludeKey bool
}

// importExportCSVOptions parses exportCSV() options.
// exportCSV() is intentionally a flat object-table export: columns target
// top-level object fields only and no schema inference/transforms are applied.
func importExportCSVOptions(rt *sobek.Runtime, options sobek.Value) (exportCSVOptions, error) {
	parsed := exportCSVOptions{
		Delimiter:  ',',
		IncludeKey: true,
	}

	if common.IsNullish(options) {
		return parsed, NewError(
			InvalidOptionsError,
			"exportCSV options must be an object with non-empty fileName and columns",
		)
	}

	if err := ensureOptionalObjectOptions("exportCSV", options); err != nil {
		return parsed, err
	}

	optionsObj := options.ToObject(rt)

	if err := parseExportCSVRequiredFields(&parsed, optionsObj); err != nil {
		return parsed, err
	}

	if err := parseExportCSVOptionalDelimiter(&parsed, optionsObj); err != nil {
		return parsed, err
	}

	if err := parseExportCSVOptionalLimit(&parsed, optionsObj); err != nil {
		return parsed, err
	}

	if err := parseExportCSVOptionalIncludeKey(&parsed, optionsObj); err != nil {
		return parsed, err
	}

	if err := validateExportCSVImplicitKeyCollision(parsed); err != nil {
		return parsed, err
	}

	prefix, isSet, err := parseOptionalStringOption("exportCSV", "prefix", optionsObj.Get("prefix"))
	if err != nil {
		return parsed, err
	}

	if isSet {
		parsed.Prefix = prefix
	}

	return parsed, nil
}

// parseExportCSVRequiredFields parses and validates a single options field.
func parseExportCSVRequiredFields(parsed *exportCSVOptions, optionsObj *sobek.Object) error {
	fileName, fileNameSet, err := parseOptionalStringOption("exportCSV", "fileName", optionsObj.Get("fileName"))
	if err != nil {
		return err
	}

	if !fileNameSet || strings.TrimSpace(fileName) == "" {
		return NewError(InvalidOptionsError, "exportCSV fileName is required")
	}

	parsed.FileName = fileName

	columnsValue := optionsObj.Get("columns")
	if common.IsNullish(columnsValue) {
		return NewError(InvalidOptionsError, "exportCSV columns is required for flat object-table export")
	}

	columns, err := parseExportCSVColumns(columnsValue.Export())
	if err != nil {
		return err
	}

	if len(columns) == 0 {
		return NewError(InvalidOptionsError, "exportCSV columns must be a non-empty array")
	}

	if err := validateExportCSVColumns(columns); err != nil {
		return err
	}

	parsed.Columns = columns

	return nil
}

// parseExportCSVColumns parses and validates a single options field.
func parseExportCSVColumns(exported any) ([]string, error) {
	switch columns := exported.(type) {
	case []string:
		return append([]string(nil), columns...), nil
	case []any:
		parsed := make([]string, 0, len(columns))
		for i, item := range columns {
			column, ok := item.(string)
			if !ok {
				return nil, NewError(
					InvalidOptionsError,
					fmt.Sprintf("exportCSV options.columns[%d] must be a string; got %T", i, item),
				)
			}

			parsed = append(parsed, column)
		}

		return parsed, nil
	default:
		return nil, NewError(
			InvalidOptionsError,
			fmt.Sprintf("exportCSV options.columns must be an array of strings; got %T", exported),
		)
	}
}

// validateExportCSVColumns validates user-supplied input.
func validateExportCSVColumns(columns []string) error {
	seen := make(map[string]struct{}, len(columns))

	for i, column := range columns {
		if strings.TrimSpace(column) == "" {
			return NewError(
				InvalidOptionsError,
				fmt.Sprintf("exportCSV options.columns[%d] must be a non-empty string", i),
			)
		}

		if _, ok := seen[column]; ok {
			return NewError(
				InvalidOptionsError,
				fmt.Sprintf("exportCSV options.columns contains duplicate column %q", column),
			)
		}

		seen[column] = struct{}{}
	}

	return nil
}

// parseExportCSVOptionalDelimiter parses and validates a single options field.
func parseExportCSVOptionalDelimiter(parsed *exportCSVOptions, optionsObj *sobek.Object) error {
	delimiter, err := parseOptionalCSVDelimiterOrDefault("exportCSV", optionsObj, parsed.Delimiter)
	if err != nil {
		return err
	}

	parsed.Delimiter = delimiter

	return nil
}

// parseExportCSVOptionalLimit parses and validates a single options field.
func parseExportCSVOptionalLimit(parsed *exportCSVOptions, optionsObj *sobek.Object) error {
	limit, limitSet, err := parseOptionalInt64Option("exportCSV", "limit", optionsObj.Get("limit"))
	if err != nil {
		return err
	}

	if !limitSet {
		return nil
	}

	if err := rejectIfAbove("exportCSV", "limit", limit, MaxExportCSVLimit); err != nil {
		return err
	}

	parsed.Limit = limit

	return nil
}

// parseExportCSVOptionalIncludeKey parses and validates a single options field.
func parseExportCSVOptionalIncludeKey(parsed *exportCSVOptions, optionsObj *sobek.Object) error {
	includeKey, isSet, err := parseOptionalBoolOption("exportCSV", "includeKey", optionsObj.Get("includeKey"))
	if err != nil {
		return err
	}

	if isSet {
		parsed.IncludeKey = includeKey
	}

	return nil
}

// validateExportCSVImplicitKeyCollision validates user-supplied input.
func validateExportCSVImplicitKeyCollision(opts exportCSVOptions) error {
	if !opts.IncludeKey {
		return nil
	}

	if slices.Contains(opts.Columns, "key") {
		return NewError(
			InvalidOptionsError,
			`exportCSV options.columns must not contain "key" when includeKey=true`,
		)
	}

	return nil
}
