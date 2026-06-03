package kv

import (
	"fmt"

	"github.com/grafana/sobek"
	"go.k6.io/k6/v2/js/common"

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
		// Treat it as opaque; callers must not parse or construct it manually.
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

	// scanKeysOptions holds optional filters for scanKeys().
	scanKeysOptions struct {
		// Prefix selects only keys that start with the given string.
		Prefix string `js:"prefix"`

		// Limit is the maximum number of keys to return in a single page;
		// <= 0 means "no limit".
		Limit int64 `js:"limit"`

		// Cursor is an opaque continuation token previously returned from ScanKeys().
		// Treat it as opaque; callers must not parse or construct it manually.
		Cursor string `js:"cursor"`

		// isLimitSet indicates whether Limit was explicitly provided from JS.
		isLimitSet bool
	}

	// scanKeysResult is the JS-facing result of scanKeys().
	scanKeysResult struct {
		// Keys holds the page of key names.
		Keys []string `js:"keys"`

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

	// randomKeysOptions holds filters for randomKeys().
	randomKeysOptions struct {
		// Prefix restricts random selection to keys beginning with this string.
		Prefix string `js:"prefix"`
		// Count controls how many keys should be returned.
		Count int64 `js:"count"`
		// Unique controls whether returned keys must be distinct.
		Unique bool `js:"unique"`
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
	parsed := scanOptions{}

	prefix, cursor, limit, limitSet, err := importScanLikeOptions("scan", rt, options)
	if err != nil {
		return parsed, err
	}

	parsed.Prefix = prefix
	parsed.Cursor = cursor

	if limitSet {
		parsed.Limit = limit
		parsed.isLimitSet = true
	}

	return parsed, nil
}

// importScanKeysOptions converts a Sobek value into scanKeysOptions.
// Accepts null/undefined and partial objects.
func importScanKeysOptions(rt *sobek.Runtime, options sobek.Value) (scanKeysOptions, error) {
	parsed := scanKeysOptions{}

	prefix, cursor, limit, limitSet, err := importScanLikeOptions("scanKeys", rt, options)
	if err != nil {
		return parsed, err
	}

	parsed.Prefix = prefix
	parsed.Cursor = cursor

	if limitSet {
		parsed.Limit = limit
		parsed.isLimitSet = true
	}

	return parsed, nil
}

// importScanLikeOptions parses shared option shape for scan-style methods.
func importScanLikeOptions(
	method string,
	rt *sobek.Runtime,
	options sobek.Value,
) (prefix string, cursor string, limit int64, limitSet bool, err error) {
	if err = ensureOptionalObjectOptions(method, options); err != nil {
		return "", "", 0, false, err
	}

	if common.IsNullish(options) {
		return "", "", 0, false, nil
	}

	optionsObj := options.ToObject(rt)

	parsedPrefix, prefixSet, parseErr := parseOptionalStringOption(method, "prefix", optionsObj.Get("prefix"))
	if parseErr != nil {
		return "", "", 0, false, parseErr
	}

	if prefixSet {
		prefix = parsedPrefix
	}

	parsedCursor, cursorSet, parseErr := parseOptionalStringOption(method, "cursor", optionsObj.Get("cursor"))
	if parseErr != nil {
		return "", "", 0, false, parseErr
	}

	if cursorSet {
		cursor = parsedCursor
	}

	parsedLimit, parsedLimitSet, parseErr := parseOptionalInt64Option(method, "limit", optionsObj.Get("limit"))
	if parseErr != nil {
		return "", "", 0, false, parseErr
	}

	if parsedLimitSet {
		if parseErr = rejectIfAbove(method, "limit", parsedLimit, store.MaxScanLimit); parseErr != nil {
			return "", "", 0, false, parseErr
		}

		limit = parsedLimit
		limitSet = true
	}

	return prefix, cursor, limit, limitSet, nil
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
		if err := rejectIfAbove("list", "limit", parsedLimit, store.MaxListLimit); err != nil {
			return listOptions, err
		}

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
		if err := rejectIfAbove("listKeys", "limit", limit, store.MaxListLimit); err != nil {
			return parsedOptions, err
		}

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

// importRandomKeysOptions converts a Sobek value into randomKeysOptions.
// randomKeys requires a non-null options object because count is required.
func importRandomKeysOptions(rt *sobek.Runtime, options sobek.Value) (randomKeysOptions, error) {
	parsed := randomKeysOptions{
		Unique: true,
	}

	if err := ensureOptionalObjectOptions("randomKeys", options); err != nil {
		return parsed, err
	}

	if common.IsNullish(options) {
		return parsed, NewError(
			InvalidOptionsError,
			"randomKeys count is required",
		)
	}

	optionsObj := options.ToObject(rt)

	prefix, prefixSet, err := parseOptionalStringOption("randomKeys", "prefix", optionsObj.Get("prefix"))
	if err != nil {
		return parsed, err
	}

	if prefixSet {
		parsed.Prefix = prefix
	}

	count, countSet, err := parseOptionalInt64Option("randomKeys", "count", optionsObj.Get("count"))
	if err != nil {
		return parsed, err
	}

	if !countSet {
		return parsed, NewError(
			InvalidOptionsError,
			"randomKeys count is required",
		)
	}

	if count <= 0 {
		return parsed, NewError(
			InvalidOptionsError,
			"randomKeys count must be a positive integer",
		)
	}

	if count > store.MaxRandomKeysCount {
		return parsed, NewError(
			InvalidOptionsError,
			fmt.Sprintf("randomKeys count must be less than or equal to %d", store.MaxRandomKeysCount),
		)
	}

	parsed.Count = count

	unique, uniqueSet, err := parseOptionalBoolOption("randomKeys", "unique", optionsObj.Get("unique"))
	if err != nil {
		return parsed, err
	}

	if uniqueSet {
		parsed.Unique = unique
	}

	return parsed, nil
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
