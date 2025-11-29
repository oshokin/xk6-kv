package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMemoryStore_BackupOptions_DefaultFileName verifies
// that BackupOptions defaults to the default disk path when FileName is empty.
func TestMemoryStore_BackupOptions_DefaultFileName(t *testing.T) {
	t.Parallel()

	opts := new(BackupOptions)

	require.NoError(t, opts.normalize())

	expected, err := ResolveDiskPath("")
	require.NoError(t, err)

	assert.Equal(t, expected, opts.FileName)
}

// TestMemoryStore_RestoreOptions_DefaultFileName verifies that RestoreOptions
// defaults to the default disk path when FileName is empty.
func TestMemoryStore_RestoreOptions_DefaultFileName(t *testing.T) {
	t.Parallel()

	opts := new(RestoreOptions)

	require.NoError(t, opts.normalize())

	expected, err := ResolveDiskPath("")
	require.NoError(t, err)

	assert.Equal(t, expected, opts.FileName)
}
