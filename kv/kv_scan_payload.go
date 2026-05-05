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

	// listKeysOptions describes filters for listKeys() operations.
	listKeysOptions struct {
		// Prefix selects only keys that start with the given string.
		Prefix string `js:"prefix"`

		// Limit is the maximum number of keys to return; <= 0 means "no limit".
		Limit int64 `js:"limit"`
	}

	// randomKeyOptions holds the optional prefix filter for randomKey().
	randomKeyOptions struct {
		// Prefix restricts random selection to keys beginning with this string.
		Prefix string `js:"prefix"`
	}

	// countOptions holds the optional prefix filter for count().
	countOptions struct {
		// Prefix restricts counting to keys beginning with this string.
		Prefix string `js:"prefix"`
	}
)

// importScanOptions converts a Sobek value into ScanOptions.
// Accepts null/undefined and partial objects.
func importScanOptions(rt *sobek.Runtime, options sobek.Value) (scanOptions, error) {
	scanOptions := scanOptions{}

	err := ensureOptionalObjectOptions("scan", options)
	if err != nil {
		return scanOptions, err
	}

	if common.IsNullish(options) {
		return scanOptions, nil
	}

	optionsObj := options.ToObject(rt)

	prefixValue := optionsObj.Get("prefix")

	prefix, isSet, err := parseOptionalStringOption("scan", "prefix", prefixValue)
	if err != nil {
		return scanOptions, err
	}

	if isSet {
		scanOptions.Prefix = prefix
	}

	cursorValue := optionsObj.Get("cursor")

	cursor, isSet, err := parseOptionalStringOption("scan", "cursor", cursorValue)
	if err != nil {
		return scanOptions, err
	}

	if isSet {
		scanOptions.Cursor = cursor
	}

	limitValue := optionsObj.Get("limit")

	parsedLimit, isSet, err := parseOptionalInt64Option("scan", "limit", limitValue)
	if err != nil {
		return scanOptions, err
	}

	if isSet {
		scanOptions.Limit = parsedLimit
		scanOptions.isLimitSet = true
	}

	return scanOptions, nil
}

// importListOptions converts a Sobek value into ListOptions, accepting null/undefined
// and partial objects.
func importListOptions(rt *sobek.Runtime, options sobek.Value) (listOptions, error) {
	listOptions := listOptions{}

	err := ensureOptionalObjectOptions("list", options)
	if err != nil {
		return listOptions, err
	}

	if common.IsNullish(options) {
		return listOptions, nil
	}

	optionsObj := options.ToObject(rt)

	prefixValue := optionsObj.Get("prefix")

	prefix, isSet, err := parseOptionalStringOption("list", "prefix", prefixValue)
	if err != nil {
		return listOptions, err
	}

	if isSet {
		listOptions.Prefix = prefix
	}

	limitValue := optionsObj.Get("limit")

	parsedLimit, isSet, err := parseOptionalInt64Option("list", "limit", limitValue)
	if err != nil {
		return listOptions, err
	}

	if isSet {
		listOptions.Limit = parsedLimit
		listOptions.limitSet = true
	}

	return listOptions, nil
}

// importListKeysOptions converts a Sobek value into listKeysOptions, accepting
// null/undefined and partial objects.
func importListKeysOptions(rt *sobek.Runtime, options sobek.Value) (listKeysOptions, error) {
	parsedOptions := listKeysOptions{}

	err := ensureOptionalObjectOptions("listKeys", options)
	if err != nil {
		return parsedOptions, err
	}

	if common.IsNullish(options) {
		return parsedOptions, nil
	}

	optionsObj := options.ToObject(rt)

	prefix, prefixSet, err := parseOptionalStringOption("listKeys", "prefix", optionsObj.Get("prefix"))
	if err != nil {
		return parsedOptions, err
	}

	if prefixSet {
		parsedOptions.Prefix = prefix
	}

	limit, limitSet, err := parseOptionalInt64Option("listKeys", "limit", optionsObj.Get("limit"))
	if err != nil {
		return parsedOptions, err
	}

	if limitSet {
		parsedOptions.Limit = limit
	}

	return parsedOptions, nil
}

// importRandomKeyOptions converts a Sobek value into RandomKeyOptions.
// Accepts null/undefined and partial objects.
func importRandomKeyOptions(rt *sobek.Runtime, options sobek.Value) (randomKeyOptions, error) {
	randomKeyOptions := randomKeyOptions{}

	err := ensureOptionalObjectOptions("randomKey", options)
	if err != nil {
		return randomKeyOptions, err
	}

	if common.IsNullish(options) {
		return randomKeyOptions, nil
	}

	optionsObj := options.ToObject(rt)

	prefixValue := optionsObj.Get("prefix")

	prefix, isSet, err := parseOptionalStringOption("randomKey", "prefix", prefixValue)
	if err != nil {
		return randomKeyOptions, err
	}

	if isSet {
		randomKeyOptions.Prefix = prefix
	}

	return randomKeyOptions, nil
}

// importCountOptions converts a Sobek value into CountOptions.
// Accepts null/undefined and plain objects; other input types are rejected.
func importCountOptions(rt *sobek.Runtime, options sobek.Value) (countOptions, error) {
	countOptions := countOptions{}

	err := ensureOptionalObjectOptions("count", options)
	if err != nil {
		return countOptions, err
	}

	if common.IsNullish(options) {
		return countOptions, nil
	}

	optionsObj := options.ToObject(rt)

	prefixValue := optionsObj.Get("prefix")

	prefix, isSet, err := parseOptionalStringOption("count", "prefix", prefixValue)
	if err != nil {
		return countOptions, err
	}

	if isSet {
		countOptions.Prefix = prefix
	}

	return countOptions, nil
}
