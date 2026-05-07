//go:build windows

package fileutil

// SyncParentDir is a best-effort no-op on Windows.
// Directory fsync semantics differ across platforms and are not portable.
func SyncParentDir(_ string) error {
	return nil
}
