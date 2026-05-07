//go:build !windows

package fileutil

import (
	"os"
	"path/filepath"
)

// SyncParentDir fsyncs the parent directory entry for path.
// On Unix-like systems this hardens rename durability after crashes.
func SyncParentDir(path string) error {
	//nolint:forbidigo // directory fsync requires opening the parent directory.
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer func() {
		_ = dir.Close()
	}()

	return dir.Sync()
}
