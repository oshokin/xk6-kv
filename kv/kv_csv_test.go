package kv

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oshokin/xk6-kv/kv/store"
)

// typedMapCSVSerializer is a test serializer used by typed map csv serializer tests.
type typedMapCSVSerializer struct{}

// typedMapCSVSerializer implements serializer serialize for typed map csv serializer.
func (typedMapCSVSerializer) Serialize(_ any) ([]byte, error) {
	return []byte("raw"), nil
}

// typedMapCSVSerializer implements serializer deserialize for typed map csv serializer.
func (typedMapCSVSerializer) Deserialize(_ []byte) (any, error) {
	return map[string]string{
		"name": "Alice",
	}, nil
}

// typedMapCSVSerializer implements serializer type for typed map csv serializer.
func (typedMapCSVSerializer) Type() string {
	return "typed-map-csv-test"
}

// unsupportedScalarCSVSerializer is a test serializer used by unsupported scalar csv serializer tests.
type unsupportedScalarCSVSerializer struct{}

// unsupportedScalarCSVSerializer implements serializer serialize for unsupported scalar csv serializer.
func (unsupportedScalarCSVSerializer) Serialize(_ any) ([]byte, error) {
	return []byte("raw"), nil
}

// unsupportedScalarCSVSerializer implements serializer deserialize for unsupported scalar csv serializer.
func (unsupportedScalarCSVSerializer) Deserialize(_ []byte) (any, error) {
	return map[string]any{
		"name":      "Alice",
		"createdAt": time.Unix(1, 0).UTC(),
	}, nil
}

// unsupportedScalarCSVSerializer implements serializer type for unsupported scalar csv serializer.
func (unsupportedScalarCSVSerializer) Type() string {
	return "unsupported-scalar-csv-test"
}

// TestImportCSV_ImportsRowsAndSupportsClaiming verifies that import csv imports rows and supports claiming.
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

	result, err := importCSV(context.Background(), serialized, importCSVOptions{
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

// TestImportCSV_ContextCanceledBeforeStart verifies that import csv context canceled before start.
func TestImportCSV_ContextCanceledBeforeStart(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := importCSV(ctx, mem, importCSVOptions{
		FileName:  filepath.Join(t.TempDir(), "ignored.csv"),
		KeyColumn: "id",
		HasHeader: true,
	})
	require.Nil(t, result)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}

// TestImportCSV_ContextCanceledDuringFlush verifies that import csv context canceled during flush.
func TestImportCSV_ContextCanceledDuringFlush(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	target := filepath.Join(t.TempDir(), "cancel-during-flush.csv")
	writeCSVFileForTest(
		t,
		target,
		"id,name",
		"user:1,Alice",
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancelingStore := jsonlCancelingSetManyStore{
		Store:  serialized,
		cancel: cancel,
	}

	result, err := importCSV(ctx, cancelingStore, importCSVOptions{
		FileName:  target,
		KeyColumn: "id",
		HasHeader: true,
		BatchSize: 1,
	})
	require.Nil(t, result)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)

	exists, existsErr := serialized.Exists("user:1")
	require.NoError(t, existsErr)
	assert.False(t, exists, "canceled CSV flush must not commit partial rows")
}

// TestImportCSV_KeyColumnMissingInHeaderFails verifies that import csv key column missing in header fails.
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

	result, err := importCSV(context.Background(), serialized, importCSVOptions{
		FileName:  target,
		KeyColumn: "missing",
		HasHeader: true,
	})
	require.Nil(t, result)
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrValueParseFailed)
}

// TestImportCSV_DuplicateHeaderFails verifies that import csv duplicate header fails.
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

	result, err := importCSV(context.Background(), serialized, importCSVOptions{
		FileName:  target,
		KeyColumn: "id",
		HasHeader: true,
	})
	require.Nil(t, result)
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrValueParseFailed)
}

// TestImportCSV_EmptyHeaderFails verifies that import csv empty header fails.
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

	result, err := importCSV(context.Background(), serialized, importCSVOptions{
		FileName:  target,
		KeyColumn: "id",
		HasHeader: true,
	})
	require.Nil(t, result)
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrValueParseFailed)
	require.ErrorContains(t, err, "header column 1 is empty")
}

// TestImportCSV_EmptyKeyFails verifies that import csv empty key fails.
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

	result, err := importCSV(context.Background(), serialized, importCSVOptions{
		FileName:  target,
		KeyColumn: "id",
		HasHeader: true,
	})
	require.Nil(t, result)
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrValueParseFailed)
}

// TestImportCSV_SemicolonDelimiterAndLimit verifies that import csv semicolon delimiter and limit.
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

	result, err := importCSV(context.Background(), serialized, importCSVOptions{
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

// TestImportCSV_NoHeaderUsesNumericKeyColumn verifies that import csv no header uses numeric key column.
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

	result, err := importCSV(context.Background(), serialized, importCSVOptions{
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

// TestImportCSV_MissingColumnsAreEmpty verifies that import csv missing columns are empty.
func TestImportCSV_MissingColumnsAreEmpty(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	target := filepath.Join(t.TempDir(), "users-ragged-missing.csv")
	writeCSVFileForTest(
		t,
		target,
		"id,name,role",
		"user:1,Alice,admin",
		"user:2,Bob",
	)

	result, err := importCSV(context.Background(), serialized, importCSVOptions{
		FileName:  target,
		KeyColumn: "id",
		HasHeader: true,
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 2, result.Imported)

	item, err := serialized.Get("user:2")
	require.NoError(t, err)

	row, ok := item.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Bob", row["name"])
	assert.Empty(t, row["role"])
}

// TestImportCSV_ExtraColumnsUseGeneratedColumnNames verifies that import csv extra columns use generated column names.
func TestImportCSV_ExtraColumnsUseGeneratedColumnNames(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	target := filepath.Join(t.TempDir(), "users-ragged-extra.csv")
	writeCSVFileForTest(
		t,
		target,
		"username,password,role",
		"u3,p3,user,extra",
	)

	result, err := importCSV(context.Background(), serialized, importCSVOptions{
		FileName:  target,
		KeyColumn: "username",
		HasHeader: true,
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 1, result.Imported)

	item, err := serialized.Get("u3")
	require.NoError(t, err)

	row, ok := item.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "user", row["role"])
	assert.Equal(t, "extra", row["column_3"])
}

// TestExportCSV_ExportsFlatObjectsWithHeader verifies that export csv exports flat objects with header.
func TestExportCSV_ExportsFlatObjectsWithHeader(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	require.NoError(t, serialized.Set("users:1", map[string]any{"name": "Alice", "age": 30}))
	require.NoError(t, serialized.Set("users:2", map[string]any{"name": "Bob", "age": 31}))

	target := filepath.Join(t.TempDir(), "users.csv")
	result, err := exportCSV(context.Background(), serialized, exportCSVOptions{
		FileName:   target,
		Prefix:     "users:",
		Columns:    []string{"name", "age"},
		IncludeKey: true,
		Delimiter:  ',',
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 2, result.Exported)
	assert.Equal(t, target, result.FileName)
	assert.Positive(t, result.BytesWritten)

	records := readCSVRecordsForTest(t, target, ',')
	require.Len(t, records, 3)
	assert.Equal(t, []string{"key", "name", "age"}, records[0])
	assert.Equal(t, []string{"users:1", "Alice", "30"}, records[1])
	assert.Equal(t, []string{"users:2", "Bob", "31"}, records[2])
}

// TestExportCSV_PrefixAndLimit verifies that export csv prefix and limit.
func TestExportCSV_PrefixAndLimit(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	require.NoError(t, serialized.Set("users:1", map[string]any{"status": 200}))
	require.NoError(t, serialized.Set("users:2", map[string]any{"status": 201}))
	require.NoError(t, serialized.Set("orders:1", map[string]any{"status": 500}))

	target := filepath.Join(t.TempDir(), "limited.csv")
	result, err := exportCSV(context.Background(), serialized, exportCSVOptions{
		FileName:   target,
		Prefix:     "users:",
		Limit:      1,
		Columns:    []string{"status"},
		IncludeKey: true,
		Delimiter:  ',',
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 1, result.Exported)

	records := readCSVRecordsForTest(t, target, ',')
	require.Len(t, records, 2)
	assert.Equal(t, []string{"key", "status"}, records[0])
	assert.True(t, strings.HasPrefix(records[1][0], "users:"))
}

// TestExportCSV_IncludeKeyFalse verifies that export csv include key false.
func TestExportCSV_IncludeKeyFalse(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	require.NoError(t, serialized.Set("users:1", map[string]any{"name": "Alice"}))

	target := filepath.Join(t.TempDir(), "without-key.csv")
	result, err := exportCSV(context.Background(), serialized, exportCSVOptions{
		FileName:   target,
		Columns:    []string{"name"},
		IncludeKey: false,
		Delimiter:  ',',
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 1, result.Exported)

	records := readCSVRecordsForTest(t, target, ',')
	require.Len(t, records, 2)
	assert.Equal(t, []string{"name"}, records[0])
	assert.Equal(t, []string{"Alice"}, records[1])
}

// TestExportCSV_SemicolonDelimiter verifies that export csv semicolon delimiter.
func TestExportCSV_SemicolonDelimiter(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	require.NoError(t, serialized.Set("users:1", map[string]any{"status": 200}))

	target := filepath.Join(t.TempDir(), "semicolon.csv")
	result, err := exportCSV(context.Background(), serialized, exportCSVOptions{
		FileName:   target,
		Columns:    []string{"status"},
		IncludeKey: true,
		Delimiter:  ';',
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 1, result.Exported)

	records := readCSVRecordsForTest(t, target, ';')
	require.Len(t, records, 2)
	assert.Equal(t, []string{"key", "status"}, records[0])
	assert.Equal(t, []string{"users:1", "200"}, records[1])
}

// TestExportCSV_MissingColumnWritesEmptyCell verifies that export csv missing column writes empty cell.
func TestExportCSV_MissingColumnWritesEmptyCell(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	require.NoError(t, serialized.Set("users:1", map[string]any{"name": "Alice"}))

	target := filepath.Join(t.TempDir(), "missing-column.csv")
	result, err := exportCSV(context.Background(), serialized, exportCSVOptions{
		FileName:   target,
		Columns:    []string{"name", "status"},
		IncludeKey: true,
		Delimiter:  ',',
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 1, result.Exported)

	records := readCSVRecordsForTest(t, target, ',')
	require.Len(t, records, 2)
	assert.Equal(t, []string{"users:1", "Alice", ""}, records[1])
}

// TestExportCSV_NonObjectValueRejects verifies that export csv non object value rejects.
func TestExportCSV_NonObjectValueRejects(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	require.NoError(t, serialized.Set("users:1", "Alice"))

	target := filepath.Join(t.TempDir(), "non-object.csv")
	result, err := exportCSV(context.Background(), serialized, exportCSVOptions{
		FileName:   target,
		Columns:    []string{"name"},
		IncludeKey: true,
		Delimiter:  ',',
	})
	require.Nil(t, result)
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrValueParseFailed)
}

// TestExportCSV_TypedMapValueRejects verifies that export csv typed map value rejects.
func TestExportCSV_TypedMapValueRejects(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, typedMapCSVSerializer{})

	require.NoError(t, serialized.Set("users:1", "placeholder"))

	target := filepath.Join(t.TempDir(), "typed-map.csv")
	result, err := exportCSV(context.Background(), serialized, exportCSVOptions{
		FileName:   target,
		Columns:    []string{"name"},
		IncludeKey: true,
		Delimiter:  ',',
	})
	require.Nil(t, result)
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrValueParseFailed)
	require.ErrorContains(t, err, "must be a JSON object")
}

// TestExportCSV_StringSerializationValueRejects verifies that export csv string serialization value rejects.
func TestExportCSV_StringSerializationValueRejects(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewStringSerializer())

	require.NoError(t, serialized.Set("users:1", "raw-body"))

	target := filepath.Join(t.TempDir(), "string-serialization.csv")
	result, err := exportCSV(context.Background(), serialized, exportCSVOptions{
		FileName:   target,
		Columns:    []string{"status"},
		IncludeKey: true,
		Delimiter:  ',',
	})
	require.Nil(t, result)
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrValueParseFailed)
}

// TestExportCSV_NestedValueRejects verifies that export csv nested value rejects.
func TestExportCSV_NestedValueRejects(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	require.NoError(t, serialized.Set("users:1", map[string]any{
		"name":  "Alice",
		"extra": map[string]any{"id": 1},
	}))

	target := filepath.Join(t.TempDir(), "nested.csv")
	result, err := exportCSV(context.Background(), serialized, exportCSVOptions{
		FileName:   target,
		Columns:    []string{"name", "extra"},
		IncludeKey: true,
		Delimiter:  ',',
	})
	require.Nil(t, result)
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrValueParseFailed)
}

// TestExportCSV_ArrayValueRejects verifies that export csv array value rejects.
func TestExportCSV_ArrayValueRejects(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	require.NoError(t, serialized.Set("users:1", map[string]any{
		"name":  "Alice",
		"roles": []any{"admin", "buyer"},
	}))

	target := filepath.Join(t.TempDir(), "array.csv")
	result, err := exportCSV(context.Background(), serialized, exportCSVOptions{
		FileName:   target,
		Columns:    []string{"name", "roles"},
		IncludeKey: true,
		Delimiter:  ',',
	})
	require.Nil(t, result)
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrValueParseFailed)
}

// TestCSVScalarToString_FormatsFloatsAndJSONNumber verifies that csv scalar to string formats floats and json number.
func TestCSVScalarToString_FormatsFloatsAndJSONNumber(t *testing.T) {
	t.Parallel()

	float64String, err := csvScalarToString(3.14159)
	require.NoError(t, err)
	assert.Equal(t, "3.14159", float64String)

	float32String, err := csvScalarToString(float32(1.5))
	require.NoError(t, err)
	assert.Equal(t, "1.5", float32String)

	jsonNumberString, err := csvScalarToString(json.Number("1e3"))
	require.NoError(t, err)
	assert.JSONEq(t, "1e3", jsonNumberString)
}

// TestExportCSV_RejectsUnsupportedScalarType verifies that export csv rejects unsupported scalar type.
func TestExportCSV_RejectsUnsupportedScalarType(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, unsupportedScalarCSVSerializer{})
	require.NoError(t, serialized.Set("users:1", "placeholder"))

	target := filepath.Join(t.TempDir(), "unsupported-scalar.csv")
	result, err := exportCSV(context.Background(), serialized, exportCSVOptions{
		FileName:   target,
		Columns:    []string{"name", "createdAt"},
		IncludeKey: true,
		Delimiter:  ',',
	})
	require.Nil(t, result)
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrValueParseFailed)
	require.ErrorContains(t, err, "column \"createdAt\"")
}

// TestExportCSV_ContextCanceledBeforeStart verifies that export csv context canceled before start.
func TestExportCSV_ContextCanceledBeforeStart(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := exportCSV(ctx, mem, exportCSVOptions{
		FileName:   filepath.Join(t.TempDir(), "canceled.csv"),
		Columns:    []string{"name"},
		IncludeKey: true,
		Delimiter:  ',',
	})
	require.Nil(t, result)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}

// TestExportCSV_ReplacesExistingTarget verifies that export csv replaces existing target.
func TestExportCSV_ReplacesExistingTarget(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())
	require.NoError(t, serialized.Set("users:1", map[string]any{"name": "Alice"}))

	target := filepath.Join(t.TempDir(), "replace.csv")
	//nolint:forbidigo // file I/O is required for export test setup.
	require.NoError(t, os.WriteFile(target, []byte("stale\ncontent\n"), 0o644))

	result, err := exportCSV(context.Background(), serialized, exportCSVOptions{
		FileName:   target,
		Columns:    []string{"name"},
		IncludeKey: true,
		Delimiter:  ',',
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 1, result.Exported)

	content := readFileForTest(t, target)
	assert.NotContains(t, content, "stale")
	assert.Contains(t, content, "key,name")
	assert.Contains(t, content, "users:1,Alice")
}

// TestReplaceFile_CleansUpTempOnExportFailure verifies that replace file cleans up temp on export failure.
func TestReplaceFile_CleansUpTempOnExportFailure(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())
	require.NoError(t, serialized.Set("users:1", map[string]any{"name": "Alice"}))

	outputDir := filepath.Join(t.TempDir(), "exports")
	require.NoError(t, os.MkdirAll(outputDir, 0o755))

	targetAsDirectory := filepath.Join(outputDir, "responses.csv")
	require.NoError(t, os.MkdirAll(targetAsDirectory, 0o755))

	result, err := exportCSV(context.Background(), serialized, exportCSVOptions{
		FileName:   targetAsDirectory,
		Columns:    []string{"name"},
		IncludeKey: true,
		Delimiter:  ',',
	})
	require.Nil(t, result)
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrSnapshotFinalizeFailed)

	//nolint:forbidigo // file I/O is required for cleanup verification tests.
	entries, readErr := os.ReadDir(outputDir)
	require.NoError(t, readErr)

	for _, entry := range entries {
		assert.False(
			t,
			strings.HasPrefix(entry.Name(), "responses.csv.") && strings.HasSuffix(entry.Name(), ".tmp"),
			"temporary export file must be cleaned after finalize failure: %q",
			entry.Name(),
		)
	}
}

// TestExportCSV_EmptyStoreWritesHeaderOnly verifies that export csv empty store writes header only.
func TestExportCSV_EmptyStoreWritesHeaderOnly(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	target := filepath.Join(t.TempDir(), "empty.csv")
	result, err := exportCSV(context.Background(), serialized, exportCSVOptions{
		FileName:   target,
		Columns:    []string{"name"},
		IncludeKey: true,
		Delimiter:  ',',
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 0, result.Exported)

	records := readCSVRecordsForTest(t, target, ',')
	require.Len(t, records, 1)
	assert.Equal(t, []string{"key", "name"}, records[0])
}

// TestExportCSV_SerializedStoreExportsDeserializedValues verifies that export csv serialized store exports deserialized values.
func TestExportCSV_SerializedStoreExportsDeserializedValues(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	require.NoError(t, serialized.Set("users:1", map[string]any{
		"status": 200,
		"active": true,
	}))

	target := filepath.Join(t.TempDir(), "deserialized.csv")
	result, err := exportCSV(context.Background(), serialized, exportCSVOptions{
		FileName:   target,
		Columns:    []string{"status", "active"},
		IncludeKey: false,
		Delimiter:  ',',
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 1, result.Exported)

	records := readCSVRecordsForTest(t, target, ',')
	require.Len(t, records, 2)
	assert.Equal(t, []string{"status", "active"}, records[0])
	assert.Equal(t, []string{"200", "true"}, records[1])
}

// TestCSVRoundTrip_ConvertsScalarTypesToStrings verifies that csv round trip converts scalar types to strings.
func TestCSVRoundTrip_ConvertsScalarTypesToStrings(t *testing.T) {
	t.Parallel()

	source := store.NewSerializedStore(
		store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
		store.NewJSONSerializer(),
	)
	require.NoError(t, source.Set("users:1", map[string]any{
		"status": 200,
		"active": true,
	}))

	targetCSV := filepath.Join(t.TempDir(), "roundtrip.csv")
	exportResult, err := exportCSV(context.Background(), source, exportCSVOptions{
		FileName:   targetCSV,
		Columns:    []string{"status", "active"},
		IncludeKey: true,
		Delimiter:  ',',
	})
	require.NoError(t, err)
	require.NotNil(t, exportResult)
	assert.EqualValues(t, 1, exportResult.Exported)

	destination := store.NewSerializedStore(
		store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
		store.NewJSONSerializer(),
	)
	importResult, err := importCSV(context.Background(), destination, importCSVOptions{
		FileName:  targetCSV,
		KeyColumn: "key",
		HasHeader: true,
		Delimiter: ',',
	})
	require.NoError(t, err)
	require.NotNil(t, importResult)
	assert.EqualValues(t, 1, importResult.Imported)

	item, err := destination.Get("users:1")
	require.NoError(t, err)

	row, ok := item.(map[string]any)
	require.True(t, ok)

	status, statusIsString := row["status"].(string)
	active, activeIsString := row["active"].(string)

	require.True(t, statusIsString)
	require.True(t, activeIsString)
	assert.Equal(t, "200", status)
	assert.Equal(t, "true", active)
}

// TestValidateCSV_EOFReturnsCheckedAllTrue verifies that validate csv eof returns checked all true.
func TestValidateCSV_EOFReturnsCheckedAllTrue(t *testing.T) {
	t.Parallel()

	target := filepath.Join(t.TempDir(), "validate.csv")
	writeCSVFileForTest(t, target, "id,name", "user:1,Alice", "user:2,Bob")

	result, err := validateCSV(context.Background(), validateCSVOptions{
		FileName:     target,
		KeyColumn:    "id",
		KeyColumnSet: true,
		HasHeader:    true,
		Delimiter:    ',',
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Valid)
	assert.EqualValues(t, 2, result.Rows)
	assert.Positive(t, result.BytesRead)
	assert.True(t, result.CheckedAll)
	assert.Nil(t, result.FirstError)
}

// TestValidateCSV_ValidFileWithoutKeyColumn verifies that validate csv valid file without key column.
func TestValidateCSV_ValidFileWithoutKeyColumn(t *testing.T) {
	t.Parallel()

	target := filepath.Join(t.TempDir(), "validate-no-key.csv")
	writeCSVFileForTest(t, target, "id,name", "user:1,Alice")

	result, err := validateCSV(context.Background(), validateCSVOptions{
		FileName:  target,
		HasHeader: true,
		Delimiter: ',',
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Valid)
	assert.EqualValues(t, 1, result.Rows)
	assert.True(t, result.CheckedAll)
	assert.Nil(t, result.FirstError)
}

// TestValidateCSV_MissingColumnsAreValidWhenLenient verifies that validate csv missing columns are valid when lenient.
func TestValidateCSV_MissingColumnsAreValidWhenLenient(t *testing.T) {
	t.Parallel()

	target := filepath.Join(t.TempDir(), "validate-ragged-missing.csv")
	writeCSVFileForTest(t, target, "id,name,role", "user:1,Alice,admin", "user:2,Bob")

	result, err := validateCSV(context.Background(), validateCSVOptions{
		FileName:     target,
		KeyColumn:    "id",
		KeyColumnSet: true,
		HasHeader:    true,
		Delimiter:    ',',
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Valid)
	assert.EqualValues(t, 2, result.Rows)
	assert.True(t, result.CheckedAll)
	assert.Nil(t, result.FirstError)
}

// TestValidateCSV_ExtraColumnsAreValidWhenLenient verifies that validate csv extra columns are valid when lenient.
func TestValidateCSV_ExtraColumnsAreValidWhenLenient(t *testing.T) {
	t.Parallel()

	target := filepath.Join(t.TempDir(), "validate-ragged-extra.csv")
	writeCSVFileForTest(t, target, "username,password,role", "u3,p3,user,extra")

	result, err := validateCSV(context.Background(), validateCSVOptions{
		FileName:     target,
		KeyColumn:    "username",
		KeyColumnSet: true,
		HasHeader:    true,
		Delimiter:    ',',
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Valid)
	assert.EqualValues(t, 1, result.Rows)
	assert.True(t, result.CheckedAll)
	assert.Nil(t, result.FirstError)
}

// TestValidateCSV_DuplicateHeaderInvalidResult verifies that validate csv duplicate header invalid result.
func TestValidateCSV_DuplicateHeaderInvalidResult(t *testing.T) {
	t.Parallel()

	target := filepath.Join(t.TempDir(), "duplicate-header.csv")
	writeCSVFileForTest(t, target, "id,name,id", "user:1,Alice,user:1")

	result, err := validateCSV(context.Background(), validateCSVOptions{
		FileName:  target,
		HasHeader: true,
		Delimiter: ',',
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Valid)
	require.NotNil(t, result.FirstError)
	assert.EqualValues(t, 1, result.FirstError.Row)
}

// TestValidateCSV_MissingKeyColumnInvalidResult verifies that validate csv missing key column invalid result.
func TestValidateCSV_MissingKeyColumnInvalidResult(t *testing.T) {
	t.Parallel()

	target := filepath.Join(t.TempDir(), "missing-key-column.csv")
	writeCSVFileForTest(t, target, "id,name", "user:1,Alice")

	result, err := validateCSV(context.Background(), validateCSVOptions{
		FileName:     target,
		KeyColumn:    "missing",
		KeyColumnSet: true,
		HasHeader:    true,
		Delimiter:    ',',
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Valid)
	require.NotNil(t, result.FirstError)
	assert.EqualValues(t, 1, result.FirstError.Row)
}

// TestValidateCSV_EmptyKeyInvalidResult verifies that validate csv empty key invalid result.
func TestValidateCSV_EmptyKeyInvalidResult(t *testing.T) {
	t.Parallel()

	target := filepath.Join(t.TempDir(), "empty-key.csv")
	writeCSVFileForTest(t, target, "id,name", ",Alice")

	result, err := validateCSV(context.Background(), validateCSVOptions{
		FileName:     target,
		KeyColumn:    "id",
		KeyColumnSet: true,
		HasHeader:    true,
		Delimiter:    ',',
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Valid)
	require.NotNil(t, result.FirstError)
	assert.EqualValues(t, 2, result.FirstError.Row)
}

// TestValidateCSV_SyntaxErrorInvalidResult verifies that validate csv syntax error invalid result.
func TestValidateCSV_SyntaxErrorInvalidResult(t *testing.T) {
	t.Parallel()

	target := filepath.Join(t.TempDir(), "syntax-error.csv")
	writeCSVFileForTest(t, target, "id,name", "\"user:1,Alice")

	result, err := validateCSV(context.Background(), validateCSVOptions{
		FileName:  target,
		HasHeader: true,
		Delimiter: ',',
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Valid)
	require.NotNil(t, result.FirstError)
}

// TestValidateCSV_LimitReturnsCheckedAllFalse verifies that validate csv limit returns checked all false.
func TestValidateCSV_LimitReturnsCheckedAllFalse(t *testing.T) {
	t.Parallel()

	target := filepath.Join(t.TempDir(), "limit.csv")
	writeCSVFileForTest(t, target, "id,name", "user:1,Alice", ",broken")

	result, err := validateCSV(context.Background(), validateCSVOptions{
		FileName:     target,
		KeyColumn:    "id",
		KeyColumnSet: true,
		HasHeader:    true,
		Delimiter:    ',',
		Limit:        1,
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Valid)
	assert.EqualValues(t, 1, result.Rows)
	assert.False(t, result.CheckedAll)
	assert.Nil(t, result.FirstError)
}

// TestValidateCSV_FileMissingRejects verifies that validate csv file missing rejects.
func TestValidateCSV_FileMissingRejects(t *testing.T) {
	t.Parallel()

	result, err := validateCSV(context.Background(), validateCSVOptions{
		FileName:  filepath.Join(t.TempDir(), "missing.csv"),
		HasHeader: true,
		Delimiter: ',',
	})
	require.Nil(t, result)
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrSnapshotNotFound)
}

// TestValidateCSV_ContextCanceledRejects verifies that validate csv context canceled rejects.
func TestValidateCSV_ContextCanceledRejects(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := validateCSV(ctx, validateCSVOptions{
		FileName:  filepath.Join(t.TempDir(), "ignored.csv"),
		HasHeader: true,
		Delimiter: ',',
	})
	require.Nil(t, result)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}

// writeCSVFileForTest writes csv file for test for tests.
func writeCSVFileForTest(t *testing.T, path string, lines ...string) {
	t.Helper()

	content := strings.Join(lines, "\n") + "\n"
	//nolint:forbidigo // file I/O is required for CSV importer tests.
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
}

// readCSVRecordsForTest reads csv records for test for tests.
func readCSVRecordsForTest(t *testing.T, path string, delimiter rune) [][]string {
	t.Helper()

	content := readFileForTest(t, path)
	reader := csv.NewReader(strings.NewReader(content))
	reader.Comma = delimiter

	records, err := reader.ReadAll()
	require.NoError(t, err)

	return records
}

// readFileForTest reads file for test for tests.
func readFileForTest(t *testing.T, path string) string {
	t.Helper()

	//nolint:forbidigo // file I/O is required for CSV tests.
	content, err := os.ReadFile(path)
	require.NoError(t, err)

	return string(content)
}
