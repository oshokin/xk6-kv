package fileutil

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
)

// validateReplaceTarget rejects directory targets so file replacement cannot
// silently replace a directory path on platforms that allow remove+rename.
func validateReplaceTarget(targetPath string) error {
	info, err := os.Stat(targetPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}

		return err
	}

	if info.IsDir() {
		return fmt.Errorf("replace target is a directory: %s", targetPath)
	}

	return nil
}
