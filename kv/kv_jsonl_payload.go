package kv

import (
	"path/filepath"
	"strings"

	"github.com/grafana/sobek"
	"go.k6.io/k6/js/common"
)

type exportJSONLOptions struct {
	FileName string
	Prefix   string
	Limit    int64
}

type importJSONLOptions struct {
	FileName  string
	Limit     int64
	BatchSize int64
}

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

func parseRequiredJSONLFileNameAndLimit(
	rt *sobek.Runtime,
	method string,
	options sobek.Value,
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
		maxLimit := MaxExportJSONLLimit
		if method == "importJSONL" {
			maxLimit = MaxImportJSONLLimit
		}

		if err := rejectIfAbove(method, "limit", limit, maxLimit); err != nil {
			return nil, "", 0, err
		}

		parsedLimit = limit
	}

	return optionsObj, filepath.Clean(fileName), parsedLimit, nil
}
