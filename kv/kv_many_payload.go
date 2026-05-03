package kv

import (
	"fmt"
	"sort"

	"github.com/grafana/sobek"
	"go.k6.io/k6/js/common"

	"github.com/oshokin/xk6-kv/kv/store"
)

const (
	setManyErrorNameInvalidEntries = "InvalidEntries"
)

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

	for _, key := range keys {
		entries = append(entries, store.Entry{
			Key:   key,
			Value: rawEntries[key],
		})
	}

	return entries, nil
}
