package kv

import (
	"fmt"
	"sort"

	"github.com/grafana/sobek"
	"go.k6.io/k6/v2/js/common"

	"github.com/oshokin/xk6-kv/kv/store"
)

const (
	// setManyErrorNameInvalidEntries is the set many error name invalid entries const.
	setManyErrorNameInvalidEntries = "InvalidEntries"
	// setManyErrorNameEmptyKey is the set many error name empty key const.
	setManyErrorNameEmptyKey = "EmptyKey"
)

// importGetManyKeys is an internal helper.
func importGetManyKeys(value sobek.Value) ([]string, error) {
	if common.IsNullish(value) {
		return nil, NewError(
			InvalidOptionsError,
			"getMany keys must be an array of strings; got null or undefined",
		)
	}

	exported := value.Export()

	switch rawItems := exported.(type) {
	case []any:
		keys := make([]string, len(rawItems))

		for i, raw := range rawItems {
			key, ok := raw.(string)
			if !ok {
				return nil, NewError(
					InvalidOptionsError,
					fmt.Sprintf("getMany keys[%d] must be a string; got %T", i, raw),
				)
			}

			keys[i] = key
		}

		return keys, nil
	case []string:
		keys := make([]string, len(rawItems))
		copy(keys, rawItems)

		return keys, nil
	default:
		return nil, NewError(
			InvalidOptionsError,
			fmt.Sprintf("getMany keys must be an array of strings; got %T", exported),
		)
	}
}

// importDeleteManyKeys is an internal helper.
func importDeleteManyKeys(value sobek.Value) ([]string, error) {
	if common.IsNullish(value) {
		return nil, NewError(
			InvalidOptionsError,
			"deleteMany keys must be an array of non-empty strings; got null or undefined",
		)
	}

	exported := value.Export()

	switch rawItems := exported.(type) {
	case []any:
		keys := make([]string, len(rawItems))

		for i, raw := range rawItems {
			key, ok := raw.(string)
			if !ok {
				return nil, NewError(
					InvalidOptionsError,
					fmt.Sprintf("deleteMany keys[%d] must be a string; got %T", i, raw),
				)
			}

			if key == "" {
				return nil, NewError(
					InvalidOptionsError,
					fmt.Sprintf("deleteMany keys[%d] must be a non-empty string", i),
				)
			}

			keys[i] = key
		}

		return keys, nil
	case []string:
		keys := make([]string, len(rawItems))

		for i, key := range rawItems {
			if key == "" {
				return nil, NewError(
					InvalidOptionsError,
					fmt.Sprintf("deleteMany keys[%d] must be a non-empty string", i),
				)
			}

			keys[i] = key
		}

		return keys, nil
	default:
		return nil, NewError(
			InvalidOptionsError,
			fmt.Sprintf("deleteMany keys must be an array of non-empty strings; got %T", exported),
		)
	}
}

// importSetManyEntries is an internal helper.
func importSetManyEntries(entriesValue sobek.Value) ([]store.Entry, error) {
	if common.IsNullish(entriesValue) {
		return nil, NewErrorWithDetails(
			InvalidOptionsError,
			"setMany entries must be an object; got null or undefined",
			[]ErrorDetail{{
				Name:    setManyErrorNameInvalidEntries,
				Message: "entries must be an object",
			}},
		)
	}

	exported := entriesValue.Export()

	rawEntries, ok := exported.(map[string]any)
	if !ok {
		return nil, NewErrorWithDetails(
			InvalidOptionsError,
			fmt.Sprintf("setMany entries must be an object; got %T", exported),
			[]ErrorDetail{{
				Name:    setManyErrorNameInvalidEntries,
				Message: "entries must be an object",
			}},
		)
	}

	if len(rawEntries) == 0 {
		return []store.Entry{}, nil
	}

	// Sort keys for deterministic diagnostics and stable test expectations.
	keys := make([]string, 0, len(rawEntries))
	for key := range rawEntries {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	entries := make([]store.Entry, 0, len(keys))
	details := make([]ErrorDetail, 0)

	for _, key := range keys {
		if key == "" {
			details = append(details, ErrorDetail{
				Key:     key,
				Name:    setManyErrorNameEmptyKey,
				Message: "key must be a non-empty string",
			})

			continue
		}

		entries = append(entries, store.Entry{
			Key:   key,
			Value: rawEntries[key],
		})
	}

	if len(details) > 0 {
		message := fmt.Sprintf("setMany validation failed: %d invalid entries", len(details))
		if len(details) == 1 {
			message = "setMany validation failed: 1 invalid entry"
		}

		return nil, NewErrorWithDetails(
			InvalidOptionsError,
			message,
			details,
		)
	}

	return entries, nil
}
