package kv

import (
	"encoding/base64"
	"fmt"

	"github.com/grafana/sobek"

	"github.com/oshokin/xk6-kv/kv/store"
)

// Scan returns a Promise that resolves to { entries: [], cursor: string, done: bool }.
// It supports cursor-based pagination over keys, ordered lexicographically.
// Pass the cursor from a previous result to continue; omit it (or pass "") to start fresh.
func (k *KV) Scan(options sobek.Value) *sobek.Promise {
	scanOptions, err := importScanOptions(k.vu.Runtime(), options)
	if err != nil {
		return k.rejectedPromise(err)
	}

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

			return &scanResult{
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
	listOptions, err := importListOptions(k.vu.Runtime(), options)
	if err != nil {
		return k.rejectedPromise(err)
	}

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

// Count returns a Promise that resolves to the number of keys matching an optional prefix.
// Pass null/undefined (or omit options) to count all keys.
func (k *KV) Count(options sobek.Value) *sobek.Promise {
	countOptions, err := importCountOptions(k.vu.Runtime(), options)
	if err != nil {
		return k.rejectedPromise(err)
	}

	return k.runAsyncWithStore(
		func(s store.Store) (any, error) {
			return s.Count(countOptions.Prefix)
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

// RandomKey returns a Promise that resolves to a random key as a string.
// If the store is empty or no keys match the optional prefix, resolves to "" (empty string).
func (k *KV) RandomKey(options sobek.Value) *sobek.Promise {
	randomKeyOptions, err := importRandomKeyOptions(k.vu.Runtime(), options)
	if err != nil {
		return k.rejectedPromise(err)
	}

	return k.runAsyncWithStore(
		func(s store.Store) (any, error) {
			return s.RandomKey(randomKeyOptions.Prefix)
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}
