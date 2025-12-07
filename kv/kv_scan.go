package kv

import (
	"encoding/base64"
	"fmt"

	"github.com/grafana/sobek"
	"go.k6.io/k6/js/common"

	"github.com/oshokin/xk6-kv/kv/store"
)

type (
	// ScanOptions holds optional filters for scan().
	ScanOptions struct {
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

	// ScanResult is the JS-facing result of scan().
	ScanResult struct {
		// Entries holds the page of key/value pairs (using store.Entry directly).
		Entries []store.Entry `js:"entries"`

		// Cursor is an opaque continuation token. The first call should pass an
		// empty cursor (or omit it); subsequent calls should pass the cursor from
		// the previous result. When Cursor is empty, the scan is complete.
		Cursor string `js:"cursor"`

		// Done is true when the scan is complete (i.e., Cursor == "").
		Done bool `js:"done"`
	}

	// ListOptions describes filters for list() operations, all fields are optional.
	ListOptions struct {
		// Prefix selects only keys that start with the given string.
		Prefix string `js:"prefix"`

		// Limit is the maximum number of entries to return; <= 0 means "no limit".
		Limit int64 `js:"limit"`

		// limitSet indicates whether Limit was explicitly provided from JS.
		limitSet bool
	}

	// RandomKeyOptions holds the optional prefix filter for randomKey().
	RandomKeyOptions struct {
		// Prefix restricts random selection to keys beginning with this string.
		Prefix string `js:"prefix"`
	}
)

// Scan returns a Promise that resolves to { entries: [], cursor: string, done: bool }.
// It supports cursor-based pagination over keys, ordered lexicographically.
// Pass the cursor from a previous result to continue; omit it (or pass "") to start fresh.
func (k *KV) Scan(options sobek.Value) *sobek.Promise {
	scanOptions := ImportScanOptions(k.vu.Runtime(), options)

	return k.runAsyncWithStore(
		func(s store.Store) (any, error) {
			var afterKey string

			if scanOptions.Cursor != "" {
				raw, err := base64.StdEncoding.DecodeString(scanOptions.Cursor)
				if err != nil {
					return nil, fmt.Errorf("%w: %w", store.ErrInvalidCursor, err)
				}

				afterKey = string(raw)
			}

			page, err := s.Scan(scanOptions.Prefix, afterKey, scanOptions.Limit)
			if err != nil {
				return nil, err
			}

			if page == nil {
				return nil, unexpectedStoreOutput("store.Scan")
			}

			var (
				cursor string
				done   bool
			)

			if page.NextKey != "" {
				cursor = base64.StdEncoding.EncodeToString([]byte(page.NextKey))
				done = false
			} else {
				cursor = ""
				done = true
			}

			return &ScanResult{
				Entries: page.Entries,
				Cursor:  cursor,
				Done:    done,
			}, nil
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

// List returns a Promise that resolves to an array of { key, value } objects,
// ordered lexicographically by key. Options support prefix and limit.
func (k *KV) List(options sobek.Value) *sobek.Promise {
	// Import list options from JavaScript (safe on the VU thread now).
	listOptions := ImportListOptions(k.vu.Runtime(), options)

	return k.runAsyncWithStore(
		func(s store.Store) (any, error) {
			entries, err := s.List(listOptions.Prefix, listOptions.Limit)
			if err != nil {
				return nil, err
			}

			if entries == nil {
				return nil, unexpectedStoreOutput("store.List")
			}

			return entries, nil
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

// RandomKey returns a Promise that resolves to a random key as a string.
// If the store is empty or no keys match the optional prefix, resolves to "" (empty string).
func (k *KV) RandomKey(options sobek.Value) *sobek.Promise {
	randomKeyOptions := ImportRandomKeyOptions(k.vu.Runtime(), options)

	return k.runAsyncWithStore(
		func(s store.Store) (any, error) {
			return s.RandomKey(randomKeyOptions.Prefix)
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

// ImportScanOptions converts a Sobek value into ScanOptions.
// Accepts null/undefined and partial objects; unknown fields are ignored.
func ImportScanOptions(rt *sobek.Runtime, options sobek.Value) ScanOptions {
	scanOptions := ScanOptions{}

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

// ImportListOptions converts a Sobek value into ListOptions, accepting null/undefined
// and partial objects. Unknown fields are ignored.
func ImportListOptions(rt *sobek.Runtime, options sobek.Value) ListOptions {
	listOptions := ListOptions{}

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

// ImportRandomKeyOptions converts a Sobek value into RandomKeyOptions.
// Accepts null/undefined and partial objects; missing fields default to zero-values.
func ImportRandomKeyOptions(rt *sobek.Runtime, options sobek.Value) RandomKeyOptions {
	randomKeyOptions := RandomKeyOptions{}
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
