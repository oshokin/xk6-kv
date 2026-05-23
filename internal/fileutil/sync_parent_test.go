// Package fileutil provides utility functions for file operations.
package fileutil

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSyncParentDir verifies that sync parent dir.
func TestSyncParentDir(t *testing.T) {
	t.Parallel()

	target := filepath.Join(t.TempDir(), "artifact.jsonl")
	require.NoError(t, SyncParentDir(target))
}
