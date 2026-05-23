package kv

import (
	"strconv"
	"strings"

	"github.com/grafana/sobek"
	"go.k6.io/k6/js/common"
)

// validateCSVOptions holds parsed options for the corresponding KV method.
type validateCSVOptions struct {
	// FileName is the path of the CSV or JSONL file.
	FileName string
	// KeyColumn names the CSV column used as the store key.
	KeyColumn string
	// KeyColumnSet holds the key column set value.
	KeyColumnSet bool
	// Delimiter is the CSV field separator rune.
	Delimiter rune
	// HasHeader indicates whether the CSV file includes a header row.
	HasHeader bool
	// Limit caps how many rows or entries are processed.
	Limit int64
}

//nolint:funlen // Option parsing is kept linear and explicit for clearer validation errors.
func importValidateCSVOptions(rt *sobek.Runtime, options sobek.Value) (validateCSVOptions, error) {
	parsed := validateCSVOptions{
		Delimiter: ',',
		HasHeader: true,
	}

	if common.IsNullish(options) {
		return parsed, NewError(
			InvalidOptionsError,
			"validateCSV options must be an object with non-empty fileName",
		)
	}

	if err := ensureOptionalObjectOptions("validateCSV", options); err != nil {
		return parsed, err
	}

	optionsObj := options.ToObject(rt)

	fileName, fileNameSet, err := parseOptionalStringOption("validateCSV", "fileName", optionsObj.Get("fileName"))
	if err != nil {
		return parsed, err
	}

	if !fileNameSet || strings.TrimSpace(fileName) == "" {
		return parsed, NewError(InvalidOptionsError, "validateCSV fileName is required")
	}

	parsed.FileName = fileName

	keyColumn, keyColumnSet, err := parseOptionalStringOption("validateCSV", "keyColumn", optionsObj.Get("keyColumn"))
	if err != nil {
		return parsed, err
	}

	if keyColumnSet {
		if strings.TrimSpace(keyColumn) == "" {
			return parsed, NewError(
				InvalidOptionsError,
				"validateCSV keyColumn must be a non-empty string when provided",
			)
		}

		parsed.KeyColumn = keyColumn
		parsed.KeyColumnSet = true
	}

	delimiter, err := parseOptionalCSVDelimiterOrDefault("validateCSV", optionsObj, parsed.Delimiter)
	if err != nil {
		return parsed, err
	}

	parsed.Delimiter = delimiter

	hasHeader, hasHeaderSet, err := parseOptionalBoolOption("validateCSV", "hasHeader", optionsObj.Get("hasHeader"))
	if err != nil {
		return parsed, err
	}

	if hasHeaderSet {
		parsed.HasHeader = hasHeader
	}

	if parsed.KeyColumnSet && !parsed.HasHeader {
		keyIndex, parseErr := strconv.Atoi(parsed.KeyColumn)
		if parseErr != nil || keyIndex < 0 {
			return parsed, NewError(
				InvalidOptionsError,
				"validateCSV keyColumn must be a non-negative integer when hasHeader=false",
			)
		}
	}

	limit, limitSet, err := parseOptionalInt64Option("validateCSV", "limit", optionsObj.Get("limit"))
	if err != nil {
		return parsed, err
	}

	if limitSet {
		if err := rejectIfAbove("validateCSV", "limit", limit, MaxImportCSVLimit); err != nil {
			return parsed, err
		}

		parsed.Limit = limit
	}

	return parsed, nil
}
