package kv

import (
	"fmt"

	"github.com/grafana/sobek"
	"go.k6.io/k6/js/common"
)

// ensureOptionalObjectOptions validates that options is either nullish or a plain object.
func ensureOptionalObjectOptions(method string, options sobek.Value) error {
	if common.IsNullish(options) {
		return nil
	}

	exported := options.Export()
	if _, ok := exported.(map[string]any); !ok {
		return NewError(
			InvalidOptionsError,
			fmt.Sprintf("%s options must be an object, null, or undefined; got %T", method, exported),
		)
	}

	return nil
}

// parseOptionalStringOption reads an optional string field from options.
func parseOptionalStringOption(method, field string, value sobek.Value) (string, bool, error) {
	if common.IsNullish(value) {
		return "", false, nil
	}

	exported := value.Export()

	parsedValue, ok := exported.(string)
	if !ok {
		return "", false, NewError(
			InvalidOptionsError,
			fmt.Sprintf("%s options.%s must be a string; got %T", method, field, exported),
		)
	}

	return parsedValue, true, nil
}

// parseRequiredStringArg reads a required string argument (e.g., key).
func parseRequiredStringArg(method, arg string, value sobek.Value) (string, error) {
	if common.IsNullish(value) {
		return "", NewError(
			InvalidOptionsError,
			fmt.Sprintf("%s %s must be a string; got null or undefined", method, arg),
		)
	}

	exported := value.Export()

	parsedValue, ok := exported.(string)
	if !ok {
		return "", NewError(
			InvalidOptionsError,
			fmt.Sprintf("%s %s must be a string; got %T", method, arg, exported),
		)
	}

	return parsedValue, nil
}

// parseRequiredNonEmptyStringArg reads a required non-empty string argument.
//
//nolint:unparam // method and arg are used for error messages.
func parseRequiredNonEmptyStringArg(method, arg string, value sobek.Value) (string, error) {
	parsedValue, err := parseRequiredStringArg(method, arg, value)
	if err != nil {
		return "", err
	}

	if parsedValue == "" {
		return "", NewError(
			InvalidOptionsError,
			fmt.Sprintf("%s %s must be a non-empty string", method, arg),
		)
	}

	return parsedValue, nil
}

// parseOptionalBoolOption reads an optional boolean field from options.
func parseOptionalBoolOption(method, field string, value sobek.Value) (bool, bool, error) {
	if common.IsNullish(value) {
		return false, false, nil
	}

	exported := value.Export()

	parsedValue, ok := exported.(bool)
	if !ok {
		return false, false, NewError(
			InvalidOptionsError,
			fmt.Sprintf("%s options.%s must be a boolean; got %T", method, field, exported),
		)
	}

	return parsedValue, true, nil
}

// parseOptionalInt64Option reads an optional numeric field from options.
func parseOptionalInt64Option(method, field string, value sobek.Value) (int64, bool, error) {
	if common.IsNullish(value) {
		return 0, false, nil
	}

	parsedValue, err := exportToInt64(value)
	if err != nil {
		return 0, false, NewError(
			InvalidOptionsError,
			fmt.Sprintf("%s options.%s must be a number: %v", method, field, err),
		)
	}

	return parsedValue, true, nil
}

func rejectIfAbove(method, field string, value, maximum int64) error {
	if value <= maximum {
		return nil
	}

	return NewError(
		InvalidOptionsError,
		fmt.Sprintf("%s options.%s must be less than or equal to %d; got %d", method, field, maximum, value),
	)
}
