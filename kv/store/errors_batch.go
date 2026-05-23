package store

import "fmt"

const (
	// EntryErrorNameSerializer marks a per-entry serialization failure.
	EntryErrorNameSerializer = "SerializerError"
)

type (
	// EntryListErrorKind groups batch failures by top-level recovery strategy.
	EntryListErrorKind string

	// EntryError carries per-entry failure details for batch operations.
	//
	// Keep this as a value type to avoid per-item heap objects. See
	// setmany_slice_shape_benchmark_test.go for allocation benchmarks.
	EntryError struct {
		// Key is the entry key that failed validation or serialization.
		Key string
		// Name is a stable machine-readable error category.
		Name string
		// Message is a human-readable failure description.
		Message string
	}

	// EntryListError represents a batch operation failure with per-entry details.
	EntryListError struct {
		// Operation is the batch API name (for example "setMany").
		Operation string
		// Kind groups failures by recovery strategy.
		Kind EntryListErrorKind
		// Message is the top-level batch failure summary.
		Message string
		// Errors lists per-entry failures.
		Errors []EntryError
	}
)

const (
	// EntryListErrorKindSerialization marks value serialization failures.
	EntryListErrorKindSerialization EntryListErrorKind = "serialization"
)

// NewEntryListError creates a structured batch error.
func NewEntryListError(operation string, kind EntryListErrorKind, errors []EntryError) *EntryListError {
	invalidCount := len(errors)

	entryWord := "entries"
	if invalidCount == 1 {
		entryWord = "entry"
	}

	return &EntryListError{
		Operation: operation,
		Kind:      kind,
		Message:   fmt.Sprintf("%s validation failed: %d invalid %s", operation, invalidCount, entryWord),
		Errors:    errors,
	}
}

// Error implements the error interface.
func (e *EntryListError) Error() string {
	if e == nil {
		return ""
	}

	return e.Message
}
