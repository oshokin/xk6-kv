package kv

import (
	"github.com/grafana/sobek"
	"go.k6.io/k6/js/common"

	"github.com/oshokin/xk6-kv/kv/store"
)

type (
	// scanOptions holds optional filters for scan().
	scanOptions struct {
		// Prefix selects only keys that start with the given string.
		Prefix string `js:"prefix"`

		// Limit is the maximum number of entries to return in a single page;
		// <= 0 means "no limit" (effectively behaves like list()).
		Limit int64 `js:"limit"`

		// Cursor is an opaque continuation token previously returned from Scan().
		// It is a base64-encoded representation of the last key in the previous page.
		Cursor string `js:"cursor"`

		// isLimitSet indicates whether Limit was explicitly provided from JS.
		isLimitSet bool
	}

	// scanResult is the JS-facing result of scan().
	scanResult struct {
		// Entries holds the page of key/value pairs (using store.Entry directly).
		Entries []store.Entry `js:"entries"`

		// Cursor is an opaque continuation token. The first call should pass an
		// empty cursor (or omit it); subsequent calls should pass the cursor from
		// the previous result. When Cursor is empty, the scan is complete.
		Cursor string `js:"cursor"`

		// Done is true when the scan is complete (i.e., Cursor == "").
		Done bool `js:"done"`
	}

	// listOptions describes filters for list() operations, all fields are optional.
	listOptions struct {
		// Prefix selects only keys that start with the given string.
		Prefix string `js:"prefix"`

		// Limit is the maximum number of entries to return; <= 0 means "no limit".
		Limit int64 `js:"limit"`

		// limitSet indicates whether Limit was explicitly provided from JS.
		limitSet bool
	}

	// randomKeyOptions holds the optional prefix filter for randomKey().
	randomKeyOptions struct {
		// Prefix restricts random selection to keys beginning with this string.
		Prefix string `js:"prefix"`
	}
)

// importScanOptions converts a Sobek value into ScanOptions.
// Accepts null/undefined and partial objects; unknown fields are ignored.
func importScanOptions(rt *sobek.Runtime, options sobek.Value) scanOptions {
	scanOptions := scanOptions{}

	if common.IsNullish(options) {
		return scanOptions
	}

	optionsObj := options.ToObject(rt)

	prefixValue := optionsObj.Get("prefix")
	if !common.IsNullish(prefixValue) {
		scanOptions.Prefix = prefixValue.String()
	}

	cursorValue := optionsObj.Get("cursor")
	if !common.IsNullish(cursorValue) {
		scanOptions.Cursor = cursorValue.String()
	}

	limitValue := optionsObj.Get("limit")
	if common.IsNullish(limitValue) {
		return scanOptions
	}

	var (
		parsedLimit int64
		err         = rt.ExportTo(limitValue, &parsedLimit)
	)
	if err == nil {
		scanOptions.Limit = parsedLimit
		scanOptions.isLimitSet = true
	}

	return scanOptions
}

// importListOptions converts a Sobek value into ListOptions, accepting null/undefined
// and partial objects. Unknown fields are ignored.
func importListOptions(rt *sobek.Runtime, options sobek.Value) listOptions {
	listOptions := listOptions{}

	// If no options are passed, return the default options.
	if common.IsNullish(options) {
		return listOptions
	}

	// Interpret the options as a plain object from JS.
	optionsObj := options.ToObject(rt)

	prefixValue := optionsObj.Get("prefix")
	if !common.IsNullish(prefixValue) {
		listOptions.Prefix = prefixValue.String()
	}

	limitValue := optionsObj.Get("limit")
	if common.IsNullish(limitValue) {
		return listOptions
	}

	var (
		parsedLimit int64
		err         = rt.ExportTo(limitValue, &parsedLimit)
	)
	if err == nil {
		listOptions.Limit = parsedLimit
		listOptions.limitSet = true
	}

	return listOptions
}

// importRandomKeyOptions converts a Sobek value into RandomKeyOptions.
// Accepts null/undefined and partial objects; missing fields default to zero-values.
func importRandomKeyOptions(rt *sobek.Runtime, options sobek.Value) randomKeyOptions {
	randomKeyOptions := randomKeyOptions{}
	if common.IsNullish(options) {
		return randomKeyOptions
	}

	optionsObj := options.ToObject(rt)

	prefixValue := optionsObj.Get("prefix")
	if !common.IsNullish(prefixValue) {
		randomKeyOptions.Prefix = prefixValue.String()
	}

	return randomKeyOptions
}
