package kv

import (
	"strings"

	"github.com/grafana/sobek"
	"go.k6.io/k6/js/common"
)

// exportJSONLOptions holds parsed options for the corresponding KV method.
type exportJSONLOptions struct {
	// FileName is the path of the CSV or JSONL file.
	FileName string
	// Prefix selects only keys that start with the given string.
	Prefix string
	// Limit caps how many rows or entries are processed.
	Limit int64
}

// importJSONLOptions holds parsed options for the corresponding KV method.
type importJSONLOptions struct {
	// FileName is the path of the CSV or JSONL file.
	FileName string
	// Limit caps how many rows or entries are processed.
	Limit int64
	// BatchSize is the number of rows written per store batch.
	BatchSize int64
}

// validateJSONLOptions holds parsed options for the corresponding KV method.
type validateJSONLOptions struct {
	// FileName is the path of the CSV or JSONL file.
	FileName string
	// Limit caps how many rows or entries are processed.
	Limit int64
}

// importExportJSONLOptions parses Sobek options for the corresponding KV method.
func importExportJSONLOptions(rt *sobek.Runtime, options sobek.Value) (exportJSONLOptions, error) {
	parsed := exportJSONLOptions{}

	optionsObj, fileName, limit, err := parseRequiredJSONLFileNameAndLimit(rt, "exportJSONL", options)
	if err != nil {
		return parsed, err
	}

	parsed.FileName = fileName
	parsed.Limit = limit

	prefix, prefixSet, err := parseOptionalStringOption(
		"exportJSONL",
		"prefix",
		optionsObj.Get("prefix"),
	)
	if err != nil {
		return parsed, err
	}

	if prefixSet {
		parsed.Prefix = prefix
	}

	return parsed, nil
}

// importImportJSONLOptions parses Sobek options for the corresponding KV method.
func importImportJSONLOptions(rt *sobek.Runtime, options sobek.Value) (importJSONLOptions, error) {
	parsed := importJSONLOptions{}

	optionsObj, fileName, limit, err := parseRequiredJSONLFileNameAndLimit(rt, "importJSONL", options)
	if err != nil {
		return parsed, err
	}

	parsed.FileName = fileName
	parsed.Limit = limit

	batchSize, batchSizeSet, err := parseOptionalInt64Option(
		"importJSONL",
		"batchSize",
		optionsObj.Get("batchSize"),
	)
	if err != nil {
		return parsed, err
	}

	if batchSizeSet {
		if err := rejectIfAbove("importJSONL", "batchSize", batchSize, MaxJSONLBatchSize); err != nil {
			return parsed, err
		}

		parsed.BatchSize = batchSize
	}

	return parsed, nil
}

// importValidateJSONLOptions parses Sobek options for the corresponding KV method.
func importValidateJSONLOptions(rt *sobek.Runtime, options sobek.Value) (validateJSONLOptions, error) {
	parsed := validateJSONLOptions{}

	_, fileName, limit, err := parseRequiredJSONLFileNameAndLimitWithMax(
		rt,
		"validateJSONL",
		options,
		MaxImportJSONLLimit,
	)
	if err != nil {
		return parsed, err
	}

	parsed.FileName = fileName
	parsed.Limit = limit

	return parsed, nil
}

// parseRequiredJSONLFileNameAndLimit parses and validates a single options field.
func parseRequiredJSONLFileNameAndLimit(
	rt *sobek.Runtime,
	method string,
	options sobek.Value,
) (*sobek.Object, string, int64, error) {
	maxLimit := MaxExportJSONLLimit
	if method == "importJSONL" {
		maxLimit = MaxImportJSONLLimit
	}

	return parseRequiredJSONLFileNameAndLimitWithMax(rt, method, options, maxLimit)
}

// parseRequiredJSONLFileNameAndLimitWithMax parses and validates a single options field.
func parseRequiredJSONLFileNameAndLimitWithMax(
	rt *sobek.Runtime,
	method string,
	options sobek.Value,
	maxLimit int64,
) (*sobek.Object, string, int64, error) {
	if common.IsNullish(options) {
		return nil, "", 0, NewError(
			InvalidOptionsError,
			method+" options must be an object with non-empty fileName",
		)
	}

	if err := ensureOptionalObjectOptions(method, options); err != nil {
		return nil, "", 0, err
	}

	optionsObj := options.ToObject(rt)

	fileName, fileNameSet, err := parseOptionalStringOption(
		method,
		"fileName",
		optionsObj.Get("fileName"),
	)
	if err != nil {
		return nil, "", 0, err
	}

	if !fileNameSet || strings.TrimSpace(fileName) == "" {
		return nil, "", 0, NewError(
			InvalidOptionsError,
			method+" fileName is required",
		)
	}

	limit, limitSet, err := parseOptionalInt64Option(
		method,
		"limit",
		optionsObj.Get("limit"),
	)
	if err != nil {
		return nil, "", 0, err
	}

	parsedLimit := int64(0)

	if limitSet {
		if err := rejectIfAbove(method, "limit", limit, maxLimit); err != nil {
			return nil, "", 0, err
		}

		parsedLimit = limit
	}

	return optionsObj, fileName, parsedLimit, nil
}
