package store

import (
	"strconv"
	"testing"
)

const benchmarkSetManyBatchSize = 10_000

type (
	benchSetManyEntry struct {
		Key   string
		Value any
	}

	benchSetManySerializedEntry struct {
		Key   string
		Value []byte
	}

	benchSetManyErrorDetail struct {
		Key     string
		Name    string
		Message string
	}
)

//nolint:gochecknoglobals // Benchmark sinks and shared fixtures must be package-level to avoid dead-code elimination and setup skew.
var (
	benchSinkEntriesValue []benchSetManyEntry
	benchSinkEntriesPtr   []*benchSetManyEntry
	benchSinkDetailsValue []benchSetManyErrorDetail
	benchSinkDetailsPtr   []*benchSetManyErrorDetail
	benchSinkWrittenCount int
	benchSinkFailureCount int

	benchSetManyInputKeys           []string
	benchSetManyInputValues         map[string]any
	benchSetManyInputValuesWithFail map[string]any
)

func init() {
	benchSetManyInputKeys = make([]string, benchmarkSetManyBatchSize)
	benchSetManyInputValues = make(map[string]any, benchmarkSetManyBatchSize)
	benchSetManyInputValuesWithFail = make(map[string]any, benchmarkSetManyBatchSize)

	for i := range benchmarkSetManyBatchSize {
		key := "k:" + strconv.Itoa(i)
		value := map[string]any{
			"id":   i,
			"name": "user",
		}

		benchSetManyInputKeys[i] = key
		benchSetManyInputValues[key] = value
		benchSetManyInputValuesWithFail[key] = value
	}

	if len(benchSetManyInputKeys) > 0 {
		lastKey := benchSetManyInputKeys[len(benchSetManyInputKeys)-1]
		benchSetManyInputValuesWithFail[lastKey] = func() {}
	}
}

func benchmarkBuildValueEntries() []benchSetManyEntry {
	entries := make([]benchSetManyEntry, benchmarkSetManyBatchSize)

	for i := range benchmarkSetManyBatchSize {
		entries[i] = benchSetManyEntry{
			Key:   "k",
			Value: nil,
		}
	}

	return entries
}

func benchmarkBuildPointerEntries() []*benchSetManyEntry {
	entries := make([]*benchSetManyEntry, benchmarkSetManyBatchSize)

	for i := range benchmarkSetManyBatchSize {
		entries[i] = &benchSetManyEntry{
			Key:   "k",
			Value: nil,
		}
	}

	return entries
}

func benchmarkBuildValueErrorDetails() []benchSetManyErrorDetail {
	details := make([]benchSetManyErrorDetail, benchmarkSetManyBatchSize)

	for i := range benchmarkSetManyBatchSize {
		details[i] = benchSetManyErrorDetail{
			Key:     "k",
			Name:    "SerializerError",
			Message: "failed to serialize",
		}
	}

	return details
}

func benchmarkBuildPointerErrorDetails() []*benchSetManyErrorDetail {
	details := make([]*benchSetManyErrorDetail, benchmarkSetManyBatchSize)

	for i := range benchmarkSetManyBatchSize {
		details[i] = &benchSetManyErrorDetail{
			Key:     "k",
			Name:    "SerializerError",
			Message: "failed to serialize",
		}
	}

	return details
}

func benchmarkImportEntriesValue(keys []string, input map[string]any) ([]benchSetManyEntry, []benchSetManyErrorDetail) {
	entries := make([]benchSetManyEntry, 0, len(keys))
	details := make([]benchSetManyErrorDetail, 0)

	for i := range keys {
		key := keys[i]
		if key == "" {
			details = append(details, benchSetManyErrorDetail{
				Key:     key,
				Name:    "EmptyKey",
				Message: "key must be a non-empty string",
			})

			continue
		}

		entries = append(entries, benchSetManyEntry{
			Key:   key,
			Value: input[key],
		})
	}

	return entries, details
}

func benchmarkImportEntriesPtr(keys []string, input map[string]any) ([]*benchSetManyEntry, []*benchSetManyErrorDetail) {
	entries := make([]*benchSetManyEntry, 0, len(keys))
	details := make([]*benchSetManyErrorDetail, 0)

	for i := range keys {
		key := keys[i]
		if key == "" {
			details = append(details, &benchSetManyErrorDetail{
				Key:     key,
				Name:    "EmptyKey",
				Message: "key must be a non-empty string",
			})

			continue
		}

		entries = append(entries, &benchSetManyEntry{
			Key:   key,
			Value: input[key],
		})
	}

	return entries, details
}

func benchmarkSerializeEntriesValue(
	serializer Serializer,
	entries []benchSetManyEntry,
) ([]benchSetManySerializedEntry, []benchSetManyErrorDetail) {
	serialized := make([]benchSetManySerializedEntry, 0, len(entries))
	details := make([]benchSetManyErrorDetail, 0)

	for i := range entries {
		payload, err := serializer.Serialize(entries[i].Value)
		if err != nil {
			details = append(details, benchSetManyErrorDetail{
				Key:     entries[i].Key,
				Name:    "SerializerError",
				Message: err.Error(),
			})

			continue
		}

		serialized = append(serialized, benchSetManySerializedEntry{
			Key:   entries[i].Key,
			Value: payload,
		})
	}

	return serialized, details
}

func benchmarkSerializeEntriesPtr(
	serializer Serializer,
	entries []*benchSetManyEntry,
) ([]*benchSetManySerializedEntry, []*benchSetManyErrorDetail) {
	serialized := make([]*benchSetManySerializedEntry, 0, len(entries))
	details := make([]*benchSetManyErrorDetail, 0)

	for i := range entries {
		payload, err := serializer.Serialize(entries[i].Value)
		if err != nil {
			details = append(details, &benchSetManyErrorDetail{
				Key:     entries[i].Key,
				Name:    "SerializerError",
				Message: err.Error(),
			})

			continue
		}

		serialized = append(serialized, &benchSetManySerializedEntry{
			Key:   entries[i].Key,
			Value: payload,
		})
	}

	return serialized, details
}

func benchmarkWriteSerializedValue(entries []benchSetManySerializedEntry) int {
	store := make(map[string][]byte, len(entries))

	for i := range entries {
		store[entries[i].Key] = entries[i].Value
	}

	return len(store)
}

func benchmarkWriteSerializedPtr(entries []*benchSetManySerializedEntry) int {
	store := make(map[string][]byte, len(entries))

	for i := range entries {
		store[entries[i].Key] = entries[i].Value
	}

	return len(store)
}

func BenchmarkSetManyArgBuild_ValueSlice(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		benchSinkEntriesValue = benchmarkBuildValueEntries()
	}
}

func BenchmarkSetManyArgBuild_PointerSlice(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		benchSinkEntriesPtr = benchmarkBuildPointerEntries()
	}
}

func BenchmarkSetManyErrorListBuild_ValueSlice(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		benchSinkDetailsValue = benchmarkBuildValueErrorDetails()
	}
}

func BenchmarkSetManyErrorListBuild_PointerSlice(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		benchSinkDetailsPtr = benchmarkBuildPointerErrorDetails()
	}
}

func BenchmarkSetManyEndToEndSuccess_ValueSlices(b *testing.B) {
	serializer := NewJSONSerializer()
	keys := benchSetManyInputKeys
	input := benchSetManyInputValues

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		parsed, parseErrors := benchmarkImportEntriesValue(keys, input)
		if len(parseErrors) > 0 {
			b.Fatal("unexpected parse errors")
		}

		serialized, serializeErrors := benchmarkSerializeEntriesValue(serializer, parsed)
		if len(serializeErrors) > 0 {
			b.Fatal("unexpected serialization errors")
		}

		benchSinkWrittenCount = benchmarkWriteSerializedValue(serialized)
	}
}

func BenchmarkSetManyEndToEndSuccess_PointerSlices(b *testing.B) {
	serializer := NewJSONSerializer()
	keys := benchSetManyInputKeys
	input := benchSetManyInputValues

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		parsed, parseErrors := benchmarkImportEntriesPtr(keys, input)
		if len(parseErrors) > 0 {
			b.Fatal("unexpected parse errors")
		}

		serialized, serializeErrors := benchmarkSerializeEntriesPtr(serializer, parsed)
		if len(serializeErrors) > 0 {
			b.Fatal("unexpected serialization errors")
		}

		benchSinkWrittenCount = benchmarkWriteSerializedPtr(serialized)
	}
}

func BenchmarkSetManyEndToEndSerializeError_ValueSlices(b *testing.B) {
	serializer := NewJSONSerializer()
	keys := benchSetManyInputKeys
	input := benchSetManyInputValuesWithFail

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		parsed, parseErrors := benchmarkImportEntriesValue(keys, input)
		if len(parseErrors) > 0 {
			b.Fatal("unexpected parse errors")
		}

		_, serializeErrors := benchmarkSerializeEntriesValue(serializer, parsed)
		benchSinkFailureCount = len(serializeErrors)
	}
}

func BenchmarkSetManyEndToEndSerializeError_PointerSlices(b *testing.B) {
	serializer := NewJSONSerializer()
	keys := benchSetManyInputKeys
	input := benchSetManyInputValuesWithFail

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		parsed, parseErrors := benchmarkImportEntriesPtr(keys, input)
		if len(parseErrors) > 0 {
			b.Fatal("unexpected parse errors")
		}

		_, serializeErrors := benchmarkSerializeEntriesPtr(serializer, parsed)
		benchSinkFailureCount = len(serializeErrors)
	}
}
