package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

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

	absPath, err := filepath.Abs(cleanedPath)
	if err != nil {
		return "", fmt.Errorf("%w: path %q: %w", ErrDiskPathResolveFailed, cleanedPath, err)
	}

	info, err := os.Stat(absPath)
	switch {
	case err == nil:
		if info.IsDir() {
			return absPath, fmt.Errorf("%w: %q", ErrDiskPathIsDirectory, absPath)
		}

		return absPath, nil
	case errors.Is(err, os.ErrNotExist):
		return absPath, nil
	default:
		return absPath, fmt.Errorf("%w: %q: %w", ErrDiskPathResolveFailed, absPath, err)
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
func clamp(value, low, high int) int {
	return max(low, min(value, high))
}
