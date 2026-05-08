package kv

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oshokin/xk6-kv/kv/store"
)

func TestExportJSONL_WritesKeyValueRecords(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	_, err := serialized.SetMany([]store.Entry{
		{Key: "user:2", Value: map[string]any{"name": "Bob"}},
		{Key: "user:1", Value: map[string]any{"name": "Alice"}},
		{Key: "order:1", Value: map[string]any{"total": 42}},
	})
	require.NoError(t, err)

	target := filepath.Join(t.TempDir(), "users.jsonl")

	result, err := exportJSONL(serialized, exportJSONLOptions{
		FileName: target,
		Prefix:   "user:",
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 2, result.Exported)
	assert.Positive(t, result.BytesWritten)

	records := readJSONLRecords(t, target)
	require.Len(t, records, 2)
	assert.Equal(t, "user:1", records[0]["key"])
	assert.Equal(t, "user:2", records[1]["key"])
}

func TestExportJSONL_StringSerializerExportsStrings(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewStringSerializer())

	_, err := serialized.SetMany([]store.Entry{
		{Key: "user:2", Value: "Bob"},
		{Key: "user:1", Value: "Alice"},
	})
	require.NoError(t, err)

	target := filepath.Join(t.TempDir(), "users.jsonl")

	result, err := exportJSONL(serialized, exportJSONLOptions{
		FileName: target,
		Prefix:   "user:",
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 2, result.Exported)

	records := readJSONLRecords(t, target)
	require.Len(t, records, 2)
	assert.Equal(t, "user:1", records[0]["key"])
	assert.Equal(t, "Alice", records[0]["value"])
	assert.Equal(t, "user:2", records[1]["key"])
	assert.Equal(t, "Bob", records[1]["value"])
}

func TestExportJSONL_Limit(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	_, err := serialized.SetMany([]store.Entry{
		{Key: "user:1", Value: map[string]any{"name": "Alice"}},
		{Key: "user:2", Value: map[string]any{"name": "Bob"}},
		{Key: "user:3", Value: map[string]any{"name": "Carol"}},
	})
	require.NoError(t, err)

	target := filepath.Join(t.TempDir(), "users.jsonl")

	result, err := exportJSONL(serialized, exportJSONLOptions{
		FileName: target,
		Prefix:   "user:",
		Limit:    2,
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 2, result.Exported)

	records := readJSONLRecords(t, target)
	require.Len(t, records, 2)
}

func TestExportJSONL_EmptyResultCreatesEmptyFile(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	_, err := serialized.SetMany([]store.Entry{
		{Key: "user:1", Value: map[string]any{"name": "Alice"}},
	})
	require.NoError(t, err)

	target := filepath.Join(t.TempDir(), "users.jsonl")

	result, err := exportJSONL(serialized, exportJSONLOptions{
		FileName: target,
		Prefix:   "missing:",
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 0, result.Exported)
	assert.EqualValues(t, 0, result.BytesWritten)

	//nolint:forbidigo // file I/O is required for export verification tests.
	data, err := os.ReadFile(target)
	require.NoError(t, err)
	assert.Empty(t, data)
}

func TestExportJSONL_ReplacesExistingFile(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	_, err := serialized.SetMany([]store.Entry{
		{Key: "user:1", Value: map[string]any{"name": "Alice"}},
	})
	require.NoError(t, err)

	target := filepath.Join(t.TempDir(), "users.jsonl")
	//nolint:forbidigo // file I/O is required for export verification tests.
	require.NoError(t, os.WriteFile(target, []byte("old-content"), 0o644))

	result, err := exportJSONL(serialized, exportJSONLOptions{
		FileName: target,
		Prefix:   "user:",
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 1, result.Exported)

	//nolint:forbidigo // file I/O is required for export verification tests.
	data, err := os.ReadFile(target)
	require.NoError(t, err)
	assert.NotContains(t, string(data), "old-content")

	records := readJSONLRecords(t, target)
	require.Len(t, records, 1)
	assert.Equal(t, "user:1", records[0]["key"])
}

func TestExportJSONL_DoesNotReplaceExistingFileOnWriteError(t *testing.T) {
	t.Parallel()

	target := filepath.Join(t.TempDir(), "users.jsonl")
	//nolint:forbidigo // file I/O is required for export verification tests.
	require.NoError(t, os.WriteFile(target, []byte("old-content"), 0o644))

	failingStore := exportEncodeFailureStore{
		Store: store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
	}

	_, err := exportJSONL(failingStore, exportJSONLOptions{
		FileName: target,
		Prefix:   "user:",
	})
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrSnapshotExportFailed)

	//nolint:forbidigo // file I/O is required for export verification tests.
	data, readErr := os.ReadFile(target)
	require.NoError(t, readErr)
	assert.Equal(t, "old-content", string(data))
}

func TestImportJSONL_ImportsRecords(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	target := filepath.Join(t.TempDir(), "users.jsonl")
	writeJSONLFileForTest(
		t,
		target,
		`{"key":"user:1","value":{"name":"Alice"}}`,
		`{"key":"user:2","value":{"name":"Bob"}}`,
	)

	result, err := importJSONL(serialized, importJSONLOptions{
		FileName:  target,
		BatchSize: 1,
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 2, result.Imported)
	assert.Equal(t, target, result.FileName)
	assert.Positive(t, result.BytesRead)

	items, err := serialized.GetMany([]string{"user:1", "user:2"})
	require.NoError(t, err)
	require.Len(t, items, 2)
	require.NotNil(t, items[0])
	require.NotNil(t, items[1])
	assert.Equal(t, "Alice", items[0].Value.(map[string]any)["name"])
	assert.Equal(t, "Bob", items[1].Value.(map[string]any)["name"])
}

func TestImportJSONL_OverwritesExistingKeys(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	_, err := serialized.SetMany([]store.Entry{
		{Key: "user:1", Value: map[string]any{"name": "Old"}},
	})
	require.NoError(t, err)

	target := filepath.Join(t.TempDir(), "users.jsonl")
	writeJSONLFileForTest(
		t,
		target,
		`{"key":"user:1","value":{"name":"New"}}`,
	)

	result, err := importJSONL(serialized, importJSONLOptions{
		FileName: target,
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 1, result.Imported)

	entry, err := serialized.Get("user:1")
	require.NoError(t, err)
	assert.Equal(t, "New", entry.(map[string]any)["name"])
}

func TestImportJSONL_ImportsNullValue(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	target := filepath.Join(t.TempDir(), "null.jsonl")
	writeJSONLFileForTest(
		t,
		target,
		`{"key":"json:null","value":null}`,
	)

	result, err := importJSONL(serialized, importJSONLOptions{
		FileName: target,
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 1, result.Imported)

	items, err := serialized.GetMany([]string{"json:null"})
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.NotNil(t, items[0])
	assert.Nil(t, items[0].Value)
}

func TestImportJSONL_StringSerializerImportsStrings(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewStringSerializer())

	target := filepath.Join(t.TempDir(), "strings.jsonl")
	writeJSONLFileForTest(
		t,
		target,
		`{"key":"user:1","value":"Alice"}`,
		`{"key":"user:2","value":"Bob"}`,
	)

	result, err := importJSONL(serialized, importJSONLOptions{
		FileName: target,
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 2, result.Imported)

	items, getErr := serialized.GetMany([]string{"user:1", "user:2"})
	require.NoError(t, getErr)
	require.Len(t, items, 2)
	require.NotNil(t, items[0])
	require.NotNil(t, items[1])
	assert.Equal(t, "Alice", items[0].Value)
	assert.Equal(t, "Bob", items[1].Value)
}

func TestImportJSONL_LimitDoesNotImportMoreThanLimit(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	target := filepath.Join(t.TempDir(), "limit.jsonl")
	writeJSONLFileForTest(
		t,
		target,
		`{"key":"user:1","value":{"name":"A"}}`,
		`{"key":"user:2","value":{"name":"B"}}`,
		`{"key":"user:3","value":{"name":"C"}}`,
		`{"key":"broken","value":`,
	)

	result, err := importJSONL(serialized, importJSONLOptions{
		FileName:  target,
		Limit:     3,
		BatchSize: 1000,
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 3, result.Imported)

	items, getErr := serialized.GetMany([]string{"user:1", "user:2", "user:3", "broken"})
	require.NoError(t, getErr)
	require.Len(t, items, 4)
	require.NotNil(t, items[0])
	require.NotNil(t, items[1])
	require.NotNil(t, items[2])
	assert.Nil(t, items[3], "rows beyond limit must not be parsed/imported")
}

func TestImportJSONL_LimitFlushesPartialBatch(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	target := filepath.Join(t.TempDir(), "limit-partial.jsonl")
	writeJSONLFileForTest(
		t,
		target,
		`{"key":"user:1","value":{"name":"A"}}`,
		`{"key":"user:2","value":{"name":"B"}}`,
		`{"key":"user:3","value":{"name":"C"}}`,
		`{"key":"user:4","value":{"name":"D"}}`,
		`{"key":"user:5","value":{"name":"E"}}`,
	)

	result, err := importJSONL(serialized, importJSONLOptions{
		FileName:  target,
		Limit:     3,
		BatchSize: 2,
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 3, result.Imported)

	items, getErr := serialized.GetMany([]string{"user:1", "user:2", "user:3", "user:4"})
	require.NoError(t, getErr)
	require.Len(t, items, 4)
	require.NotNil(t, items[0])
	require.NotNil(t, items[1])
	require.NotNil(t, items[2])
	assert.Nil(t, items[3])
}

func TestImportJSONL_BytesReadRespectsLimit(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	target := filepath.Join(t.TempDir(), "limit-bytes.jsonl")
	writeJSONLFileForTest(
		t,
		target,
		`{"key":"user:1","value":{"name":"A"}}`,
		`{"key":"user:2","value":{"name":"B"}}`,
		`{"key":"user:3","value":{"name":"C"}}`,
		`{"key":"user:4","value":{"name":"D"}}`,
		`{"key":"user:5","value":{"name":"E"}}`,
	)

	info, err := os.Stat(target)
	require.NoError(t, err)

	result, err := importJSONL(serialized, importJSONLOptions{
		FileName:  target,
		Limit:     2,
		BatchSize: 1000,
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 2, result.Imported)
	assert.Positive(t, result.BytesRead)
	assert.Less(t, result.BytesRead, info.Size(), "bytesRead should track consumed bytes under limit")
}

func TestImportJSONL_BatchSizeFlushesMultipleBatches(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	target := filepath.Join(t.TempDir(), "batch.jsonl")
	writeJSONLFileForTest(
		t,
		target,
		`{"key":"user:1","value":{"name":"A"}}`,
		`{"key":"user:2","value":{"name":"B"}}`,
		`{"key":"user:3","value":{"name":"C"}}`,
		`{"key":"user:4","value":{"name":"D"}}`,
		`{"key":"user:5","value":{"name":"E"}}`,
	)

	result, err := importJSONL(serialized, importJSONLOptions{
		FileName:  target,
		BatchSize: 2,
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 5, result.Imported)
}

func TestImportJSONL_ParseErrorReportsCommittedProgress(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	target := filepath.Join(t.TempDir(), "partial-failure.jsonl")
	writeJSONLFileForTest(
		t,
		target,
		`{"key":"user:1","value":{"name":"A"}}`,
		`{"key":"user:2","value":{"name":"B"}}`,
		`{"key":"broken","value":`,
	)

	result, err := importJSONL(serialized, importJSONLOptions{
		FileName:  target,
		BatchSize: 2,
	})
	require.Error(t, err)
	assert.Nil(t, result)
	require.ErrorIs(t, err, store.ErrValueParseFailed)
	require.ErrorContains(t, err, "importJSONL failed after 2 records")
	require.ErrorContains(t, err, "bytes")
	require.ErrorContains(t, err, "previous batches may already be committed")
	require.ErrorContains(t, err, "line 3")

	items, getErr := serialized.GetMany([]string{"user:1", "user:2", "broken"})
	require.NoError(t, getErr)
	require.Len(t, items, 3)
	require.NotNil(t, items[0])
	require.NotNil(t, items[1])
	assert.Nil(t, items[2])
}

func TestImportJSONL_EmptyFileImportsZero(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	target := filepath.Join(t.TempDir(), "empty.jsonl")
	writeJSONLFileForTest(t, target)

	result, err := importJSONL(serialized, importJSONLOptions{
		FileName: target,
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 0, result.Imported)
	assert.EqualValues(t, 0, result.BytesRead)
}

func TestImportJSONL_LastLineWithoutNewline(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	target := filepath.Join(t.TempDir(), "no-newline.jsonl")
	//nolint:forbidigo // file I/O is required for import verification tests.
	require.NoError(t, os.WriteFile(target, []byte(`{"key":"user:1","value":{"name":"Alice"}}`), 0o644))

	result, err := importJSONL(serialized, importJSONLOptions{
		FileName: target,
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.EqualValues(t, 1, result.Imported)
	assert.Positive(t, result.BytesRead)

	item, getErr := serialized.Get("user:1")
	require.NoError(t, getErr)
	assert.Equal(t, "Alice", item.(map[string]any)["name"])
}

func TestReadJSONLLinesWithMaxLineBytes_ExceedsLimitRejectsWithLineNumber(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	const maxLineBytes = 64

	line := `{"key":"user:1","value":"` + strings.Repeat("x", 256) + `"}`

	imported, bytesRead, err := readJSONLLinesWithMaxLineBytes(
		serialized,
		strings.NewReader(line),
		importJSONLOptions{},
		maxLineBytes,
	)
	require.Error(t, err)
	assert.EqualValues(t, 0, imported)
	assert.Positive(t, bytesRead)
	require.ErrorIs(t, err, store.ErrValueParseFailed)
	require.ErrorContains(t, err, "importJSONL line 1 exceeds maxLineBytes (64 bytes)")

	exists, existsErr := serialized.Exists("user:1")
	require.NoError(t, existsErr)
	assert.False(t, exists)
}

func TestReadJSONLLinesWithMaxLineBytes_WithinLimitImportsRecord(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	line := `{"key":"user:1","value":"` + strings.Repeat("x", 128) + `"}`

	imported, bytesRead, err := readJSONLLinesWithMaxLineBytes(
		serialized,
		strings.NewReader(line),
		importJSONLOptions{},
		512,
	)
	require.NoError(t, err)
	assert.EqualValues(t, 1, imported)
	assert.EqualValues(t, len(line), bytesRead)

	value, getErr := serialized.Get("user:1")
	require.NoError(t, getErr)
	assert.Equal(t, strings.Repeat("x", 128), value)
}

func TestImportJSONL_MalformedJSONRejectsWithLineNumber(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	target := filepath.Join(t.TempDir(), "malformed.jsonl")
	writeJSONLFileForTest(
		t,
		target,
		`{"key":"user:1","value":{"name":"Alice"}}`,
		`{"key":"user:2","value":`,
	)

	_, err := importJSONL(serialized, importJSONLOptions{
		FileName: target,
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "importJSONL line 2")
}

func TestImportJSONL_BlankLineRejectsWithLineNumber(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	target := filepath.Join(t.TempDir(), "blank.jsonl")
	writeJSONLFileForTest(
		t,
		target,
		`{"key":"user:1","value":{"name":"Alice"}}`,
		"",
		`{"key":"user:2","value":{"name":"Bob"}}`,
	)

	_, err := importJSONL(serialized, importJSONLOptions{
		FileName: target,
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "importJSONL line 2")
	require.ErrorContains(t, err, "blank line")
}

func TestImportJSONL_MissingKeyRejects(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	target := filepath.Join(t.TempDir(), "missing-key.jsonl")
	writeJSONLFileForTest(
		t,
		target,
		`{"value":{"name":"Alice"}}`,
	)

	_, err := importJSONL(serialized, importJSONLOptions{
		FileName: target,
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "record.key is required")
}

func TestImportJSONL_EmptyKeyRejects(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	target := filepath.Join(t.TempDir(), "empty-key.jsonl")
	writeJSONLFileForTest(
		t,
		target,
		`{"key":"","value":{"name":"Alice"}}`,
	)

	_, err := importJSONL(serialized, importJSONLOptions{
		FileName: target,
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "record.key must be a non-empty string")
}

func TestImportJSONL_MissingValueRejects(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	target := filepath.Join(t.TempDir(), "missing-value.jsonl")
	writeJSONLFileForTest(
		t,
		target,
		`{"key":"user:1"}`,
	)

	_, err := importJSONL(serialized, importJSONLOptions{
		FileName: target,
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "record.value is required")
}

func TestImportJSONL_NonObjectRecordRejects(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	target := filepath.Join(t.TempDir(), "non-object.jsonl")
	writeJSONLFileForTest(
		t,
		target,
		`null`,
	)

	_, err := importJSONL(serialized, importJSONLOptions{
		FileName: target,
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "record must be a JSON object")
}

func TestParseImportJSONLLine_NonObjectRecordRejects(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{
		"null",
		"[]",
		`"abc"`,
		"123",
	} {
		t.Run(raw, func(t *testing.T) {
			t.Parallel()

			_, err := parseImportJSONLLine([]byte(raw+"\n"), 1)
			require.Error(t, err)
			require.ErrorContains(t, err, "record must be a JSON object")
		})
	}
}

func TestImportJSONL_PreviousBatchesRemainWhenLaterLineFails(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	target := filepath.Join(t.TempDir(), "partial.jsonl")
	writeJSONLFileForTest(
		t,
		target,
		`{"key":"user:1","value":{"name":"Alice"}}`,
		`{"key":"user:2","value":{"name":"Bob"}}`,
		`{"key":"","value":{"name":"Broken"}}`,
	)

	_, err := importJSONL(serialized, importJSONLOptions{
		FileName:  target,
		BatchSize: 2,
	})
	require.Error(t, err)

	items, getErr := serialized.GetMany([]string{"user:1", "user:2"})
	require.NoError(t, getErr)
	require.Len(t, items, 2)
	require.NotNil(t, items[0])
	require.NotNil(t, items[1])
}

func TestImportJSONL_FailedUnflushedBatchWritesNothing(t *testing.T) {
	t.Parallel()

	mem := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	serialized := store.NewSerializedStore(mem, store.NewJSONSerializer())

	target := filepath.Join(t.TempDir(), "unflushed-fail.jsonl")
	writeJSONLFileForTest(
		t,
		target,
		`{"key":"user:1","value":{"name":"Alice"}}`,
		`{"key":"user:2","value":{"name":"Bob"}}`,
		`{"key":"","value":{"name":"Broken"}}`,
	)

	_, err := importJSONL(serialized, importJSONLOptions{
		FileName:  target,
		BatchSize: 1000,
	})
	require.Error(t, err)

	items, getErr := serialized.GetMany([]string{"user:1", "user:2"})
	require.NoError(t, getErr)
	require.Len(t, items, 2)
	assert.Nil(t, items[0])
	assert.Nil(t, items[1])
}

func TestResolveImportJSONLBatchSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		batchSize int64
		expected  int64
	}{
		{
			name:      "default for zero",
			batchSize: 0,
			expected:  importJSONLDefaultBatchSize,
		},
		{
			name:      "default for negative",
			batchSize: -1,
			expected:  importJSONLDefaultBatchSize,
		},
		{
			name:      "positive value",
			batchSize: 10,
			expected:  10,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			actual := resolveImportJSONLBatchSize(testCase.batchSize)
			assert.Equal(t, testCase.expected, actual)
		})
	}
}

func TestShouldFlushImportJSONLBatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		imported int64
		batchLen int
		batch    int64
		limit    int64
		expected bool
	}{
		{
			name:     "flush on batch size",
			imported: 0,
			batchLen: 2,
			batch:    2,
			limit:    0,
			expected: true,
		},
		{
			name:     "do not flush below batch size without limit",
			imported: 0,
			batchLen: 1,
			batch:    2,
			limit:    0,
			expected: false,
		},
		{
			name:     "flush when limit reached before batch size",
			imported: 2,
			batchLen: 1,
			batch:    1000,
			limit:    3,
			expected: true,
		},
		{
			name:     "do not flush when limit not reached",
			imported: 1,
			batchLen: 1,
			batch:    1000,
			limit:    3,
			expected: false,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			actual := shouldFlushImportJSONLBatch(
				testCase.imported,
				testCase.batchLen,
				testCase.batch,
				testCase.limit,
			)

			assert.Equal(t, testCase.expected, actual)
		})
	}
}

func TestFlushImportJSONLBatch_RejectsUnexpectedWrittenCount(t *testing.T) {
	t.Parallel()

	storeWithMismatch := setManyMismatchStore{
		Store: store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
	}

	written, err := flushImportJSONLBatch(storeWithMismatch, []store.Entry{
		{Key: "user:1", Value: map[string]any{"name": "Alice"}},
		{Key: "user:2", Value: map[string]any{"name": "Bob"}},
	})
	require.Error(t, err)
	assert.EqualValues(t, 0, written)
	require.ErrorContains(t, err, "store.SetMany returned unexpected written count")
	require.ErrorContains(t, err, "got 1, want 2")
}

type setManyMismatchStore struct {
	store.Store
}

func (s setManyMismatchStore) SetMany(entries []store.Entry) (int64, error) {
	return int64(len(entries) - 1), nil
}

type exportEncodeFailureStore struct {
	store.Store
}

//nolint:revive // this is a test store.
func (s exportEncodeFailureStore) Scan(prefix, afterKey string, limit int64) (*store.ScanPage, error) {
	return &store.ScanPage{
		Entries: []store.Entry{
			{
				Key:   "user:1",
				Value: func() {},
			},
		},
		NextKey: "",
	}, nil
}

func writeJSONLFileForTest(t *testing.T, path string, lines ...string) {
	t.Helper()

	data := strings.Join(lines, "\n")
	if len(lines) > 0 {
		data += "\n"
	}

	//nolint:forbidigo // file I/O is required for JSONL tests.
	require.NoError(t, os.WriteFile(path, []byte(data), 0o644))
}

func readJSONLRecords(t *testing.T, fileName string) []map[string]any {
	t.Helper()

	//nolint:forbidigo // file I/O is required for export verification tests.
	data, err := os.ReadFile(fileName)
	require.NoError(t, err)

	if len(data) == 0 {
		return nil
	}

	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	records := make([]map[string]any, 0, len(lines))

	for _, line := range lines {
		var record map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &record))
		records = append(records, record)
	}

	return records
}
