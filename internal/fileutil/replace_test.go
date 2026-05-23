package fileutil

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestReplaceFile_ReplacesExistingTarget verifies that replace file replaces existing target.
func TestReplaceFile_ReplacesExistingTarget(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	targetPath := filepath.Join(dir, "target.jsonl")
	tempPath := filepath.Join(dir, "target.jsonl.tmp")

	//nolint:forbidigo // file I/O is required for replacement tests.
	require.NoError(t, os.WriteFile(targetPath, []byte("old"), 0o644))
	//nolint:forbidigo // file I/O is required for replacement tests.
	require.NoError(t, os.WriteFile(tempPath, []byte("new"), 0o644))

	require.NoError(t, ReplaceFile(tempPath, targetPath))

	//nolint:forbidigo // file I/O is required for replacement tests.
	content, err := os.ReadFile(targetPath)
	require.NoError(t, err)
	require.Equal(t, "new", string(content))

	_, err = os.Stat(tempPath)
	require.Error(t, err)

	//nolint:forbidigo // file I/O is required for replacement tests.
	require.True(t, os.IsNotExist(err))
}

// TestReplaceFile_MissingTempReturnsError verifies that replace file missing temp returns error.
func TestReplaceFile_MissingTempReturnsError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	targetPath := filepath.Join(dir, "target.csv")
	missingTempPath := filepath.Join(dir, "missing.tmp")

	err := ReplaceFile(missingTempPath, targetPath)
	require.Error(t, err)
}

// TestReplaceFile_TargetDirectoryReturnsError verifies that replace file rejects directory targets.
func TestReplaceFile_TargetDirectoryReturnsError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	targetPath := filepath.Join(dir, "target.csv")
	tempPath := filepath.Join(dir, "target.csv.tmp")

	require.NoError(t, os.MkdirAll(targetPath, 0o755))
	//nolint:forbidigo // file I/O is required for replacement tests.
	require.NoError(t, os.WriteFile(tempPath, []byte("new"), 0o644))

	err := ReplaceFile(tempPath, targetPath)
	require.Error(t, err)
	require.Contains(t, err.Error(), "directory")

	_, statErr := os.Stat(tempPath)
	require.NoError(t, statErr)
}
