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

func importExportJSONLOptions(rt *sobek.Runtime, options sobek.Value) (exportJSONLOptions, error) {
	parsed := exportJSONLOptions{}

	if common.IsNullish(options) {
		return parsed, NewError(
			InvalidOptionsError,
			"exportJSONL options must be an object with non-empty fileName",
		)
	}

	if err := ensureOptionalObjectOptions("exportJSONL", options); err != nil {
		return parsed, err
	}

	optionsObj := options.ToObject(rt)

	fileName, fileNameSet, err := parseOptionalStringOption(
		"exportJSONL",
		"fileName",
		optionsObj.Get("fileName"),
	)
	if err != nil {
		return parsed, err
	}

	if !fileNameSet || strings.TrimSpace(fileName) == "" {
		return parsed, NewError(
			InvalidOptionsError,
			"exportJSONL fileName is required",
		)
	}

	parsed.FileName = filepath.Clean(fileName)

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

	limit, limitSet, err := parseOptionalInt64Option(
		"exportJSONL",
		"limit",
		optionsObj.Get("limit"),
	)
	if err != nil {
		return parsed, err
	}

	if limitSet {
		parsed.Limit = limit
	}

	return parsed, nil
}
