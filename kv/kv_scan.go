package kv

import (
	"encoding/base64"
	"fmt"

	"github.com/grafana/sobek"

	"github.com/oshokin/xk6-kv/kv/store"
)

func decodeOpaqueCursor(cursor string) (string, error) {
	if cursor == "" {
		return "", nil
	}

	raw, err := base64.StdEncoding.DecodeString(cursor)
	if err != nil {
		return "", fmt.Errorf("%w: %w", store.ErrInvalidCursor, err)
	}

	return string(raw), nil
}

func encodeOpaqueCursor(nextKey string) string {
	if nextKey == "" {
		return ""
	}

	return base64.StdEncoding.EncodeToString([]byte(nextKey))
}

// Scan returns a Promise that resolves to { entries: [], cursor: string, done: bool }.
// It supports cursor-based pagination over keys, ordered lexicographically.
// Pass the cursor from a previous result to continue; omit it (or pass "") to start fresh.
func (k *KV) Scan(options sobek.Value) *sobek.Promise {
	scanOptions, err := importScanOptions(k.vu.Runtime(), options)
	if err != nil {
		return k.rejectedPromiseObserved(opScan, err)
	}

	return k.runAsyncWithStoreObserved(
		opScan,
		func(s store.Store) (any, error) {
			afterKey, err := decodeOpaqueCursor(scanOptions.Cursor)
			if err != nil {
				return nil, err
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
				cursor = encodeOpaqueCursor(page.NextKey)
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

// ScanKeys returns a Promise that resolves to { keys: [], cursor: string, done: bool }.
// It supports cursor-based pagination over keys, ordered lexicographically.
// Pass the cursor from a previous result to continue; omit it (or pass "") to start fresh.
func (k *KV) ScanKeys(options sobek.Value) *sobek.Promise {
	scanOptions, err := importScanKeysOptions(k.vu.Runtime(), options)
	if err != nil {
		return k.rejectedPromiseObserved(opScanKeys, err)
	}

	return k.runAsyncWithStoreObserved(
		opScanKeys,
		func(s store.Store) (any, error) {
			afterKey, err := decodeOpaqueCursor(scanOptions.Cursor)
			if err != nil {
				return nil, err
			}

			page, err := s.ScanKeys(scanOptions.Prefix, afterKey, scanOptions.Limit)
			if err != nil {
				return nil, err
			}

			if page == nil {
				return nil, unexpectedStoreOutput("store.ScanKeys")
			}

			cursor := encodeOpaqueCursor(page.NextKey)

			return &scanKeysResult{
				Keys:   page.Keys,
				Cursor: cursor,
				Done:   cursor == "",
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
		return k.rejectedPromiseObserved(opList, err)
	}

	return k.runAsyncWithStoreObserved(
		opList,
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

// ListKeys returns a Promise that resolves to an array of key strings,
// ordered lexicographically by key. Options support prefix and limit.
func (k *KV) ListKeys(options sobek.Value) *sobek.Promise {
	listOptions, err := importListKeysOptions(k.vu.Runtime(), options)
	if err != nil {
		return k.rejectedPromiseObserved(opListKeys, err)
	}

	return k.runAsyncWithStoreObserved(
		opListKeys,
		func(s store.Store) (any, error) {
			keys, err := s.ListKeys(listOptions.Prefix, listOptions.Limit)
			if err != nil {
				return nil, err
			}

			if keys == nil {
				return []string{}, nil
			}

			return keys, nil
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
		return k.rejectedPromiseObserved(opCount, err)
	}

	return k.runAsyncWithStoreObserved(
		opCount,
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
		return k.rejectedPromiseObserved(opRandomKey, err)
	}

	return k.runAsyncWithStoreObserved(
		opRandomKey,
		func(s store.Store) (any, error) {
			return s.RandomKey(randomKeyOptions.Prefix)
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

// RandomKeys returns a Promise that resolves to random key names.
func (k *KV) RandomKeys(options sobek.Value) *sobek.Promise {
	randomKeysOptions, err := importRandomKeysOptions(k.vu.Runtime(), options)
	if err != nil {
		return k.rejectedPromiseObserved(opRandomKeys, err)
	}

	return k.runAsyncWithStoreObserved(
		opRandomKeys,
		func(s store.Store) (any, error) {
			return s.RandomKeys(randomKeysOptions.Prefix, randomKeysOptions.Count, randomKeysOptions.Unique)
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}
