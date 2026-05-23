//go:build windows

package fileutil

import (
	"errors"
	"io/fs"
	"os"
)

// ReplaceFile replaces targetPath with tempPath on Windows.
//
// Windows replacement is best-effort and not atomic in the same sense as POSIX
// rename. Callers should treat this as a crash-safer strategy than direct
// overwrite, not as a portable transactional primitive.
func ReplaceFile(tempPath, targetPath string) error {
	//nolint:forbidigo // file I/O is required for replacement.
	if err := os.Rename(tempPath, targetPath); err == nil {
		return nil
	}

	//nolint:forbidigo // file I/O is required for replacement.
	if err := os.Remove(targetPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}

	//nolint:forbidigo // file I/O is required for replacement.
	return os.Rename(tempPath, targetPath)
}
