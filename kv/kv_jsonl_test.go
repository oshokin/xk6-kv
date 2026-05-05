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
