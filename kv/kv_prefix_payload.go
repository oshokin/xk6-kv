package kv

import (
	"fmt"

	"github.com/grafana/sobek"
	"go.k6.io/k6/v2/js/common"

	"github.com/oshokin/xk6-kv/kv/store"
)

// deleteByPrefixOptions holds parsed options for the corresponding KV method.
type deleteByPrefixOptions struct {
	// Prefix selects only keys that start with the given string.
	Prefix string
	// Limit caps how many rows or entries are processed.
	Limit int64
}

// importDeleteByPrefixOptions parses Sobek options for the corresponding KV method.
func importDeleteByPrefixOptions(rt *sobek.Runtime, options sobek.Value) (deleteByPrefixOptions, error) {
	parsed := deleteByPrefixOptions{}

	if common.IsNullish(options) {
		return parsed, NewError(
			InvalidOptionsError,
			"deleteByPrefix options must be an object with non-empty prefix and positive limit",
		)
	}

	if err := ensureOptionalObjectOptions("deleteByPrefix", options); err != nil {
		return parsed, err
	}

	optionsObj := options.ToObject(rt)

	prefix, prefixSet, err := parseOptionalStringOption(
		"deleteByPrefix",
		"prefix",
		optionsObj.Get("prefix"),
	)
	if err != nil {
		return parsed, err
	}

	if !prefixSet {
		return parsed, NewError(
			InvalidOptionsError,
			"deleteByPrefix prefix is required",
		)
	}

	if prefix == "" {
		return parsed, NewError(
			InvalidOptionsError,
			"deleteByPrefix prefix must be a non-empty string",
		)
	}

	limit, limitSet, err := parseOptionalInt64Option(
		"deleteByPrefix",
		"limit",
		optionsObj.Get("limit"),
	)
	if err != nil {
		return parsed, err
	}

	if !limitSet {
		return parsed, NewError(
			InvalidOptionsError,
			"deleteByPrefix limit is required",
		)
	}

	if limit <= 0 {
		return parsed, NewError(
			InvalidOptionsError,
			fmt.Sprintf("deleteByPrefix limit must be a positive integer; got %d", limit),
		)
	}

	if err := rejectIfAbove("deleteByPrefix", "limit", limit, store.MaxDeleteByPrefixLimit); err != nil {
		return parsed, err
	}

	parsed.Prefix = prefix
	parsed.Limit = limit

	return parsed, nil
}
