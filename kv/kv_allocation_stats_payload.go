package kv

import (
	"github.com/grafana/sobek"
	"go.k6.io/k6/v2/js/common"
)

// allocationStatsOptions holds parsed options for the corresponding KV method.
type allocationStatsOptions struct {
	// Prefix selects only keys that start with the given string.
	Prefix string `js:"prefix"`
}

// importAllocationStatsOptions parses Sobek options for the corresponding KV method.
func importAllocationStatsOptions(rt *sobek.Runtime, options sobek.Value) (allocationStatsOptions, error) {
	parsed := allocationStatsOptions{}

	if err := ensureOptionalObjectOptions("allocationStats", options); err != nil {
		return parsed, err
	}

	if common.IsNullish(options) {
		return parsed, nil
	}

	optionsObj := options.ToObject(rt)

	prefix, isSet, err := parseOptionalStringOption("allocationStats", "prefix", optionsObj.Get("prefix"))
	if err != nil {
		return parsed, err
	}

	if isSet {
		parsed.Prefix = prefix
	}

	return parsed, nil
}
