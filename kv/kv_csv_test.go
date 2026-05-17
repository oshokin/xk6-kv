package kv

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oshokin/xk6-kv/kv/store"
)

func TestImportCSV_ImportsRowsAndSupportsClaiming(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	target := filepath.Join(t.TempDir(), "users.csv")
	writeCSVFileForTest(
		t,
		target,
		"id,name,role",
		"user:1,Alice,admin",
		"user:2,Bob,reader",
	)

	result, err := importCSV(serialized, importCSVOptions{
		FileName:  target,
		KeyColumn: "id",
		HasHeader: true,
		BatchSize: 1,
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 2, result.Imported)
	assert.Equal(t, target, result.FileName)
	assert.Positive(t, result.BytesRead)

	item, err := serialized.Get("user:1")
	require.NoError(t, err)

	row, ok := item.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Alice", row["name"])
	assert.Equal(t, "admin", row["role"])

	claims, err := serialized.ClaimRandomMany(&store.ClaimManyOptions{
		Prefix: "user:",
		Count:  2,
		TTLMs:  30_000,
	})
	require.NoError(t, err)
	require.Len(t, claims, 2)
}

func TestImportCSV_KeyColumnMissingInHeaderFails(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	target := filepath.Join(t.TempDir(), "users.csv")
	writeCSVFileForTest(
		t,
		target,
		"id,name",
		"user:1,Alice",
	)

	result, err := importCSV(serialized, importCSVOptions{
		FileName:  target,
		KeyColumn: "missing",
		HasHeader: true,
	})
	require.Nil(t, result)
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrValueParseFailed)
}

func TestImportCSV_DuplicateHeaderFails(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	target := filepath.Join(t.TempDir(), "users.csv")
	writeCSVFileForTest(
		t,
		target,
		"id,name,id",
		"user:1,Alice,user:1",
	)

	result, err := importCSV(serialized, importCSVOptions{
		FileName:  target,
		KeyColumn: "id",
		HasHeader: true,
	})
	require.Nil(t, result)
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrValueParseFailed)
}

func TestImportCSV_EmptyHeaderFails(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	target := filepath.Join(t.TempDir(), "users.csv")
	writeCSVFileForTest(
		t,
		target,
		"id,,email",
		"user:1,Alice,a@example.com",
	)

	result, err := importCSV(serialized, importCSVOptions{
		FileName:  target,
		KeyColumn: "id",
		HasHeader: true,
	})
	require.Nil(t, result)
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrValueParseFailed)
	require.ErrorContains(t, err, "header column 1 is empty")
}

func TestImportCSV_EmptyKeyFails(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	target := filepath.Join(t.TempDir(), "users.csv")
	writeCSVFileForTest(
		t,
		target,
		"id,name",
		",Alice",
	)

	result, err := importCSV(serialized, importCSVOptions{
		FileName:  target,
		KeyColumn: "id",
		HasHeader: true,
	})
	require.Nil(t, result)
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrValueParseFailed)
}

func TestImportCSV_SemicolonDelimiterAndLimit(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	target := filepath.Join(t.TempDir(), "users.csv")
	writeCSVFileForTest(
		t,
		target,
		"id;name",
		"user:1;Alice",
		"user:2;Bob",
		"user:3;Carol",
	)

	result, err := importCSV(serialized, importCSVOptions{
		FileName:  target,
		KeyColumn: "id",
		Delimiter: ';',
		HasHeader: true,
		Limit:     2,
		BatchSize: 1,
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 2, result.Imported)

	exists, err := serialized.Exists("user:3")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestImportCSV_NoHeaderUsesNumericKeyColumn(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	target := filepath.Join(t.TempDir(), "users.csv")
	writeCSVFileForTest(
		t,
		target,
		"user:1,Alice",
		"user:2,Bob",
	)

	result, err := importCSV(serialized, importCSVOptions{
		FileName:  target,
		KeyColumn: "0",
		HasHeader: false,
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 2, result.Imported)

	item, err := serialized.Get("user:1")
	require.NoError(t, err)

	row, ok := item.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "user:1", row["column_0"])
	assert.Equal(t, "Alice", row["column_1"])
}

func writeCSVFileForTest(t *testing.T, path string, lines ...string) {
	t.Helper()

	content := strings.Join(lines, "\n") + "\n"
	//nolint:forbidigo // file I/O is required for CSV importer tests.
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
}
