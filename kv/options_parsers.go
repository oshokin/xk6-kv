package kv

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
)

// comparablePointersEqual checks if two comparable pointers are equal.
func comparablePointersEqual[T comparable](a, b *T) bool {
	if a == nil && b == nil {
		return true
	}

	if a == nil || b == nil {
		return false
	}

	return *a == *b
}

// normalizeStringPointer normalizes a string pointer to lowercase and returns a new pointer.
func normalizeStringPointer(value *string) *string {
	if value == nil {
		return nil
	}

	lowered := strings.ToLower(*value)

	return &lowered
}

// parseSizeValue parses a size value that can be either a number (bytes) or a string like "64mb".
// Caller is responsible for wrapping the error, if it's used in JS code.
func parseSizeValue(v any) (uint64, error) {
	switch x := v.(type) {
	case int64:
		if x < 0 {
			return 0, fmt.Errorf("negative size: %d", x)
		}

		return uint64(x), nil
	case int:
		if x < 0 {
			return 0, fmt.Errorf("negative size: %d", x)
		}

		return uint64(x), nil
	case int32:
		if x < 0 {
			return 0, fmt.Errorf("negative size: %d", x)
		}

		return uint64(x), nil
	case uint64:
		return x, nil
	case uint:
		return uint64(x), nil
	case uint32:
		return uint64(x), nil
	case float64:
		if err := rejectNonFiniteFloat("size", x); err != nil {
			return 0, err
		}

		if x < 0 {
			return 0, fmt.Errorf("negative size: %f", x)
		}

		if x > float64(math.MaxUint64) {
			return 0, fmt.Errorf("size too large: %f", x)
		}

		if math.Trunc(x) != x {
			return 0, fmt.Errorf("size must be a whole number of bytes: %f", x)
		}

		return uint64(x), nil
	case string:
		size, err := humanize.ParseBytes(x)
		if err != nil {
			return 0, fmt.Errorf("invalid size string %q: %w", x, err)
		}

		return size, nil
	default:
		return 0, fmt.Errorf("unsupported size type: %T", x)
	}
}

// parseDurationValue parses a duration value that can be
// either a number (milliseconds) or a string like "1s".
// Caller is responsible for wrapping the error, if it's used in JS code.
func parseDurationValue(v any) (time.Duration, error) {
	switch x := v.(type) {
	case int:
		return durationFromMillis(int64(x))
	case int32:
		return durationFromMillis(int64(x))
	case int64:
		return durationFromMillis(x)
	case uint:
		if x > math.MaxInt64 {
			return 0, fmt.Errorf("duration too large: %d", x)
		}

		return durationFromMillis(int64(x))
	case uint32:
		return durationFromMillis(int64(x))
	case uint64:
		if x > math.MaxInt64 {
			return 0, fmt.Errorf("duration too large: %d", x)
		}

		return durationFromMillis(int64(x))
	case float64:
		if err := rejectNonFiniteFloat("duration", x); err != nil {
			return 0, err
		}

		if x > float64(math.MaxInt64) || x < float64(math.MinInt64) {
			return 0, fmt.Errorf("duration too large: %f", x)
		}

		if math.Trunc(x) != x {
			return 0, fmt.Errorf("duration must be whole milliseconds: %f", x)
		}

		return durationFromMillis(int64(x))
	case string:
		duration, err := time.ParseDuration(x)
		if err != nil {
			return 0, fmt.Errorf("invalid duration string %q: %w", x, err)
		}

		return duration, nil
	default:
		return 0, fmt.Errorf("unsupported duration type: %T", x)
	}
}

// rejectNonFiniteFloat rejects JavaScript numeric sentinels that cannot be
// safely converted into Go integer-backed sizes or durations.
func rejectNonFiniteFloat(kind string, value float64) error {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return fmt.Errorf("%s must be finite: %f", kind, value)
	}

	return nil
}

// durationFromMillis converts milliseconds to a time.Duration and returns an error if invalid.
func durationFromMillis(ms int64) (time.Duration, error) {
	if ms < 0 {
		return 0, fmt.Errorf("negative duration: %d", ms)
	}

	maxMillis := math.MaxInt64 / int64(time.Millisecond)
	if ms > maxMillis {
		return 0, fmt.Errorf("duration too large: %dms", ms)
	}

	return time.Duration(ms) * time.Millisecond, nil
}

// sizeValuesEqual checks if two size values are equal.
func sizeValuesEqual(a, b any) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	}

	left, err := parseSizeValue(a)
	if err != nil {
		return false
	}

	right, err := parseSizeValue(b)
	if err != nil {
		return false
	}

	return left == right
}

// durationValuesEqual checks if two duration values are equal.
func durationValuesEqual(a, b any) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	}

	left, err := parseDurationValue(a)
	if err != nil {
		return false
	}

	right, err := parseDurationValue(b)
	if err != nil {
		return false
	}

	return left == right
}
