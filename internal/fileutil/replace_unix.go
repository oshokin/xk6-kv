//go:build !windows

package fileutil

import "os"

// ReplaceFile atomically replaces targetPath with tempPath when both files are
// in the same directory on Unix-like systems.
func ReplaceFile(tempPath, targetPath string) error {
	//nolint:forbidigo // file I/O is required for replacement.
	return os.Rename(tempPath, targetPath)
}
