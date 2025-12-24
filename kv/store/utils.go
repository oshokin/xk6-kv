package store

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// GetComparablePointer returns an addressable copy of the value so callers can
// obtain a stable pointer even for non-addressable operands (e.g. literals).
func GetComparablePointer[T any](v T) *T {
	return &v
}

// ResolveDiskPath normalizes user-provided paths and applies fast-fail defaults.
// Empty strings revert to the default DB file path.
func ResolveDiskPath(dbPath string) (string, error) {
	trimmedPath := strings.TrimSpace(dbPath)
	if trimmedPath == "" {
		defaultPath, err := filepath.Abs(DefaultDiskStorePath)
		if err != nil {
			return "", fmt.Errorf("%w: default path %q: %w", ErrDiskPathResolveFailed, DefaultDiskStorePath, err)
		}

		return defaultPath, nil
	}

	cleanedPath := filepath.Clean(trimmedPath)

	result, err := filepath.Abs(cleanedPath)
	if err != nil {
		return "", fmt.Errorf("%w: path %q: %w", ErrDiskPathResolveFailed, cleanedPath, err)
	}

	info, err := os.Stat(result)
	switch {
	case err == nil:
		if info.IsDir() {
			return result, fmt.Errorf("%w: %q", ErrDiskPathIsDirectory, result)
		}

		return result, nil
	case errors.Is(err, os.ErrNotExist):
		return result, nil
	default:
		return result, fmt.Errorf("%w: %q: %w", ErrDiskPathResolveFailed, result, err)
	}
}

// normalizeToBytes converts value to an owned []byte copy.
func normalizeToBytes(value any) ([]byte, error) {
	switch v := value.(type) {
	case []byte:
		// Make a copy to avoid external aliasing after Set/Swap/GetOrSet.
		return slices.Clone(v), nil
	case string:
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("%w: %T", ErrUnsupportedValueType, value)
	}
}

// clamp constrains a value to lie within [low, high] bounds.
// Used to cap preallocation sizes to prevent OOM while avoiding tiny allocations.
func clamp[T cmp.Ordered](value, low, high T) T {
	return max(low, min(value, high))
}

// parseCounterValue converts raw bytes persisted by the store into an int64
// suitable for IncrementBy.
// IncrementBy bypasses the serializer entirely (it operates on already-stored bytes),
// so we need to understand every encoding the serializer might have produced earlier.
// This helper accepts the formats emitted by the built-in JSON and string serializers:
// plain decimal ASCII, JSON numbers, and JSON strings containing decimal ASCII.
func parseCounterValue(raw []byte) (int64, error) {
	if len(raw) == 0 {
		return 0, errors.New("empty value")
	}

	if value, err := strconv.ParseInt(string(raw), 10, 64); err == nil {
		return value, nil
	}

	var jsonNumber json.Number
	if err := json.Unmarshal(raw, &jsonNumber); err == nil {
		if value, convErr := jsonNumber.Int64(); convErr == nil {
			return value, nil
		}
	}

	var quoted string
	if err := json.Unmarshal(raw, &quoted); err == nil {
		return strconv.ParseInt(quoted, 10, 64)
	}

	return 0, errors.New("unsupported counter encoding")
}
