package kv

import (
	"github.com/grafana/sobek"
	"go.k6.io/k6/v2/js/common"
)

type nextCircularOptions struct {
	Prefix string `js:"prefix"`
}

func importNextCircularOptions(rt *sobek.Runtime, options sobek.Value) (nextCircularOptions, error) {
	parsed := nextCircularOptions{}

	if err := ensureOptionalObjectOptions("nextCircular", options); err != nil {
		return parsed, err
	}

	if common.IsNullish(options) {
		return parsed, nil
	}

	optionsObj := options.ToObject(rt)

	prefix, isSet, err := parseOptionalStringOption("nextCircular", "prefix", optionsObj.Get("prefix"))
	if err != nil {
		return parsed, err
	}

	if isSet {
		parsed.Prefix = prefix
	}

	return parsed, nil
}
