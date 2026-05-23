package store

import (
	"strconv"
	"testing"
)

// benchmarkSetManyBatchSize is a test const used by surrounding tests.
const benchmarkSetManyBatchSize = 10_000

type (
	// benchSetManyEntry is a test type used by bench set many entry tests.
	benchSetManyEntry struct {
		// Key holds test state for bench set many entry.
		Key string
		// Value holds test state for bench set many entry.
		Value any
	}

	// benchSetManySerializedEntry is a test type used by bench set many serialized entry tests.
	benchSetManySerializedEntry struct {
		// Key holds test state for bench set many serialized entry.
		Key string
		// Value holds test state for bench set many serialized entry.
		Value []byte
	}

	// benchSetManyErrorDetail is a test type used by bench set many error detail tests.
	benchSetManyErrorDetail struct {
		// Key holds test state for bench set many error detail.
		Key string
		// Name holds test state for bench set many error detail.
		Name string
		// Message holds test state for bench set many error detail.
		Message string
	}
)

//nolint:gochecknoglobals // benchmark sinks and shared fixtures must be package-level to avoid dead-code elimination and setup skew.
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

// init is a test helper for init.
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

// benchmarkBuildValueEntries is a test helper for benchmark build value entries.
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

// benchmarkBuildPointerEntries is a test helper for benchmark build pointer entries.
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

// benchmarkBuildValueErrorDetails is a test helper for benchmark build value error details.
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

// benchmarkBuildPointerErrorDetails is a test helper for benchmark build pointer error details.
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

// benchmarkImportEntriesValue is a test helper for benchmark import entries value.
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

// benchmarkImportEntriesPtr is a test helper for benchmark import entries ptr.
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

// benchmarkSerializeEntriesValue is a test helper for benchmark serialize entries value.
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

// benchmarkSerializeEntriesPtr is a test helper for benchmark serialize entries ptr.
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

// benchmarkWriteSerializedValue is a test helper for benchmark write serialized value.
func benchmarkWriteSerializedValue(entries []benchSetManySerializedEntry) int {
	store := make(map[string][]byte, len(entries))

	for i := range entries {
		store[entries[i].Key] = entries[i].Value
	}

	return len(store)
}

// benchmarkWriteSerializedPtr is a test helper for benchmark write serialized ptr.
func benchmarkWriteSerializedPtr(entries []*benchSetManySerializedEntry) int {
	store := make(map[string][]byte, len(entries))

	for i := range entries {
		store[entries[i].Key] = entries[i].Value
	}

	return len(store)
}

// BenchmarkSetManyArgBuild_ValueSlice measures set many arg build value slice.
func BenchmarkSetManyArgBuild_ValueSlice(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		benchSinkEntriesValue = benchmarkBuildValueEntries()
	}
}

// BenchmarkSetManyArgBuild_PointerSlice measures set many arg build pointer slice.
func BenchmarkSetManyArgBuild_PointerSlice(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		benchSinkEntriesPtr = benchmarkBuildPointerEntries()
	}
}

// BenchmarkSetManyErrorListBuild_ValueSlice measures set many error list build value slice.
func BenchmarkSetManyErrorListBuild_ValueSlice(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		benchSinkDetailsValue = benchmarkBuildValueErrorDetails()
	}
}

// BenchmarkSetManyErrorListBuild_PointerSlice measures set many error list build pointer slice.
func BenchmarkSetManyErrorListBuild_PointerSlice(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		benchSinkDetailsPtr = benchmarkBuildPointerErrorDetails()
	}
}

// BenchmarkSetManyEndToEndSuccess_ValueSlices measures set many end to end success value slices.
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

// BenchmarkSetManyEndToEndSuccess_PointerSlices measures set many end to end success pointer slices.
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

// BenchmarkSetManyEndToEndSerializeError_ValueSlices measures set many end to end serialize error value slices.
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

// BenchmarkSetManyEndToEndSerializeError_PointerSlices measures set many end to end serialize error pointer slices.
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
