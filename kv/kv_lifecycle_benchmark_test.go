package kv

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oshokin/xk6-kv/kv/store"
)

const (
	// benchmarkRows10k is a test const used by surrounding tests.
	benchmarkRows10k = 10_000
	// benchmarkClaimKeyCount is a test const used by surrounding tests.
	benchmarkClaimKeyCount = 100
)

//nolint:gochecknoglobals // benchmark sinks avoid dead-code elimination.
var (
	benchmarkExportCSVSink      *exportCSVResult
	benchmarkValidateCSVSink    *validateCSVResult
	benchmarkValidateJSONLSink  *validateJSONLResult
	benchmarkClaimKeysSink      *claimKeysResult
	benchmarkReleaseClaimsSink  *releaseClaimsResult
	benchmarkCompleteClaimsSink *completeClaimsResult
	benchmarkRenewClaimsSink    *renewClaimsResult
)

// BenchmarkExportCSV_10kRows_5Columns measures export csv 10k rows 5 columns.
func BenchmarkExportCSV_10kRows_5Columns(b *testing.B) {
	b.ReportAllocs()

	backendStore := store.NewSerializedStore(
		store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}),
		store.NewJSONSerializer(),
	)
	seedBenchmarkExportCSVStore(b, backendStore, benchmarkRows10k)

	outputPath := filepath.Join(b.TempDir(), "export.csv")
	options := exportCSVOptions{
		FileName:   outputPath,
		Prefix:     "bench:user:",
		Columns:    []string{"status", "requestId", "userId", "bodyHash", "latencyMs"},
		IncludeKey: true,
		Delimiter:  ',',
		Limit:      0,
	}

	b.ResetTimer()

	for b.Loop() {
		result, err := exportCSV(context.Background(), backendStore, options)
		if err != nil {
			b.Fatalf("exportCSV failed: %v", err)
		}

		if result.Exported != int64(benchmarkRows10k) {
			b.Fatalf("exported mismatch: got=%d want=%d", result.Exported, benchmarkRows10k)
		}

		benchmarkExportCSVSink = result
	}
}

// BenchmarkValidateCSV_10kRows measures validate csv 10k rows.
func BenchmarkValidateCSV_10kRows(b *testing.B) {
	b.ReportAllocs()

	fileName := filepath.Join(b.TempDir(), "validate.csv")
	writeBenchmarkCSVFixture(b, fileName, benchmarkRows10k)

	options := validateCSVOptions{
		FileName:  fileName,
		KeyColumn: "id",
		Delimiter: ',',
		HasHeader: true,
		Limit:     0,
	}

	b.ResetTimer()

	for b.Loop() {
		result, err := validateCSV(context.Background(), options)
		if err != nil {
			b.Fatalf("validateCSV failed: %v", err)
		}

		if !result.Valid {
			b.Fatalf("validateCSV expected valid=true, got firstError=%+v", result.FirstError)
		}

		if result.Rows != int64(benchmarkRows10k) {
			b.Fatalf("rows mismatch: got=%d want=%d", result.Rows, benchmarkRows10k)
		}

		benchmarkValidateCSVSink = result
	}
}

// BenchmarkValidateJSONL_10kRows measures validate jsonl 10k rows.
func BenchmarkValidateJSONL_10kRows(b *testing.B) {
	b.ReportAllocs()

	fileName := filepath.Join(b.TempDir(), "validate.jsonl")
	writeBenchmarkJSONLFixture(b, fileName, benchmarkRows10k)

	options := validateJSONLOptions{
		FileName: fileName,
		Limit:    0,
	}

	b.ResetTimer()

	for b.Loop() {
		result, err := validateJSONL(context.Background(), options)
		if err != nil {
			b.Fatalf("validateJSONL failed: %v", err)
		}

		if !result.Valid {
			b.Fatalf("validateJSONL expected valid=true, got firstError=%+v", result.FirstError)
		}

		if result.Records != int64(benchmarkRows10k) {
			b.Fatalf("records mismatch: got=%d want=%d", result.Records, benchmarkRows10k)
		}

		benchmarkValidateJSONLSink = result
	}
}

// BenchmarkClaimKeys_100Keys_AllFree measures claim keys 100 keys all free.
func BenchmarkClaimKeys_100Keys_AllFree(b *testing.B) {
	b.ReportAllocs()

	keys := benchmarkClaimKeys("bench:claim:free:")
	backendStore := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	seedBenchmarkClaimStore(b, backendStore, keys)

	options := &claimKeysOptions{
		TTLMs:        60_000,
		AllOrNothing: false,
	}

	b.ResetTimer()

	for b.Loop() {
		result, err := claimKeysBatch(backendStore, keys, options)
		if err != nil {
			b.Fatalf("claimKeysBatch failed: %v", err)
		}

		if len(result.Claimed) != benchmarkClaimKeyCount {
			b.Fatalf("claimed mismatch: got=%d want=%d", len(result.Claimed), benchmarkClaimKeyCount)
		}

		if len(result.Busy) != 0 || len(result.Missing) != 0 {
			b.Fatalf("unexpected partition: busy=%d missing=%d", len(result.Busy), len(result.Missing))
		}

		b.StopTimer()

		for _, claim := range result.Claimed {
			_, releaseErr := backendStore.ReleaseClaim(claim.Ref())
			require.NoError(b, releaseErr)
		}

		b.StartTimer()

		benchmarkClaimKeysSink = result
	}
}

// BenchmarkClaimKeys_100Keys_50PercentBusy measures claim keys 100 keys 50 percent busy.
func BenchmarkClaimKeys_100Keys_50PercentBusy(b *testing.B) {
	b.ReportAllocs()

	keys := benchmarkClaimKeys("bench:claim:busy:")
	backendStore := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	seedBenchmarkClaimStore(b, backendStore, keys)

	const busyCount = benchmarkClaimKeyCount / 2

	for i := range busyCount {
		claim, err := backendStore.ClaimKey(keys[i], &store.ClaimOptions{TTLMs: 3_600_000})
		require.NoError(b, err)
		require.NotNil(b, claim)
	}

	options := &claimKeysOptions{
		TTLMs:        60_000,
		AllOrNothing: false,
	}

	b.ResetTimer()

	for b.Loop() {
		result, err := claimKeysBatch(backendStore, keys, options)
		if err != nil {
			b.Fatalf("claimKeysBatch failed: %v", err)
		}

		if len(result.Claimed) != benchmarkClaimKeyCount-busyCount {
			b.Fatalf("claimed mismatch: got=%d want=%d", len(result.Claimed), benchmarkClaimKeyCount-busyCount)
		}

		if len(result.Busy) != busyCount || len(result.Missing) != 0 {
			b.Fatalf("unexpected partition: busy=%d missing=%d", len(result.Busy), len(result.Missing))
		}

		b.StopTimer()

		for _, claim := range result.Claimed {
			_, releaseErr := backendStore.ReleaseClaim(claim.Ref())
			require.NoError(b, releaseErr)
		}

		b.StartTimer()

		benchmarkClaimKeysSink = result
	}
}

// BenchmarkReleaseClaims_100Claims measures release claims 100 claims.
func BenchmarkReleaseClaims_100Claims(b *testing.B) {
	b.ReportAllocs()

	keys := benchmarkClaimKeys("bench:release:")
	backendStore := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	seedBenchmarkClaimStore(b, backendStore, keys)

	b.ResetTimer()

	for b.Loop() {
		b.StopTimer()
		refs := benchmarkClaimRefsForKeys(b, backendStore, keys)
		b.StartTimer()

		result, err := releaseClaimsBatch(backendStore, refs)
		if err != nil {
			b.Fatalf("releaseClaimsBatch failed: %v", err)
		}

		if result.Released != int64(benchmarkClaimKeyCount) {
			b.Fatalf("released mismatch: got=%d want=%d", result.Released, benchmarkClaimKeyCount)
		}

		benchmarkReleaseClaimsSink = result
	}
}

// BenchmarkCompleteClaims_100Claims measures complete claims 100 claims.
func BenchmarkCompleteClaims_100Claims(b *testing.B) {
	b.ReportAllocs()

	keys := benchmarkClaimKeys("bench:complete:")
	backendStore := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	seedBenchmarkClaimStore(b, backendStore, keys)

	options := &store.CompleteClaimOptions{DeleteKey: false}

	b.ResetTimer()

	for b.Loop() {
		b.StopTimer()
		refs := benchmarkClaimRefsForKeys(b, backendStore, keys)
		b.StartTimer()

		result, err := completeClaimsBatch(backendStore, refs, options)
		if err != nil {
			b.Fatalf("completeClaimsBatch failed: %v", err)
		}

		if result.Completed != int64(benchmarkClaimKeyCount) {
			b.Fatalf("completed mismatch: got=%d want=%d", result.Completed, benchmarkClaimKeyCount)
		}

		benchmarkCompleteClaimsSink = result
	}
}

// BenchmarkRenewClaims_100Claims measures renew claims 100 claims.
func BenchmarkRenewClaims_100Claims(b *testing.B) {
	b.ReportAllocs()

	keys := benchmarkClaimKeys("bench:renew:")
	backendStore := store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true})
	seedBenchmarkClaimStore(b, backendStore, keys)

	options := &store.RenewClaimOptions{TTLMs: 120_000}

	b.ResetTimer()

	for b.Loop() {
		b.StopTimer()
		refs := benchmarkClaimRefsForKeys(b, backendStore, keys)
		b.StartTimer()

		result, err := renewClaimsBatch(backendStore, refs, options)
		if err != nil {
			b.Fatalf("renewClaimsBatch failed: %v", err)
		}

		if result.Renewed != int64(benchmarkClaimKeyCount) {
			b.Fatalf("renewed mismatch: got=%d want=%d", result.Renewed, benchmarkClaimKeyCount)
		}

		b.StopTimer()

		for _, ref := range refs {
			_, releaseErr := backendStore.ReleaseClaim(ref)
			require.NoError(b, releaseErr)
		}

		b.StartTimer()

		benchmarkRenewClaimsSink = result
	}
}

// seedBenchmarkExportCSVStore is a test helper for seed benchmark export csv store.
func seedBenchmarkExportCSVStore(b *testing.B, backendStore store.Store, rows int) {
	b.Helper()

	for i := range rows {
		key := fmt.Sprintf("bench:user:%05d", i)
		value := map[string]any{
			"status":    "ok",
			"requestId": fmt.Sprintf("req-%05d", i),
			"userId":    fmt.Sprintf("user-%05d", i),
			"bodyHash":  fmt.Sprintf("hash-%05d", i),
			"latencyMs": int64(i % 1000),
		}
		require.NoError(b, backendStore.Set(key, value))
	}
}

// writeBenchmarkCSVFixture writes benchmark csv fixture for tests.
func writeBenchmarkCSVFixture(b *testing.B, fileName string, rows int) {
	b.Helper()

	var builder strings.Builder
	builder.Grow(rows * 64)
	builder.WriteString("id,status,requestId,userId,bodyHash,latencyMs\n")

	for i := range rows {
		_, _ = fmt.Fprintf(
			&builder,
			"u%05d,ok,req-%05d,user-%05d,hash-%05d,%d\n",
			i,
			i,
			i,
			i,
			100+(i%100),
		)
	}

	//nolint:forbidigo // benchmark fixture creation requires file I/O.
	require.NoError(b, os.WriteFile(fileName, []byte(builder.String()), 0o644))
}

// writeBenchmarkJSONLFixture writes benchmark jsonl fixture for tests.
func writeBenchmarkJSONLFixture(b *testing.B, fileName string, rows int) {
	b.Helper()

	var builder strings.Builder
	builder.Grow(rows * 96)

	for i := range rows {
		_, _ = fmt.Fprintf(
			&builder,
			`{"key":"u:%05d","value":{"status":"ok","requestId":"req-%05d","userId":"user-%05d"}}`+"\n",
			i,
			i,
			i,
		)
	}

	//nolint:forbidigo // benchmark fixture creation requires file I/O.
	require.NoError(b, os.WriteFile(fileName, []byte(builder.String()), 0o644))
}

// benchmarkClaimKeys is a test helper for benchmark claim keys.
func benchmarkClaimKeys(prefix string) []string {
	keys := make([]string, 0, benchmarkClaimKeyCount)
	for i := range benchmarkClaimKeyCount {
		keys = append(keys, fmt.Sprintf("%s%03d", prefix, i))
	}

	return keys
}

// seedBenchmarkClaimStore is a test helper for seed benchmark claim store.
func seedBenchmarkClaimStore(b *testing.B, backendStore store.Store, keys []string) {
	b.Helper()

	for _, key := range keys {
		require.NoError(b, backendStore.Set(key, "value"))
	}
}

// benchmarkClaimRefsForKeys is a test helper for benchmark claim refs for keys.
func benchmarkClaimRefsForKeys(b *testing.B, backendStore store.Store, keys []string) []*store.ClaimRef {
	b.Helper()

	refs := make([]*store.ClaimRef, 0, len(keys))

	for _, key := range keys {
		claim, err := backendStore.ClaimKey(key, &store.ClaimOptions{TTLMs: 60_000})
		require.NoError(b, err)
		require.NotNil(b, claim)

		refs = append(refs, claim.Ref())
	}

	return refs
}
