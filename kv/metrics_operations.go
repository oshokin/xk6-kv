package kv

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync/atomic"
	"time"

	"go.k6.io/k6/lib"
	"go.k6.io/k6/metrics"
)

const (
	// metricKVOperationsTotal is the k6 metric name for operations total.
	metricKVOperationsTotal = "xk6_kv_operations_total"
	// metricKVOperationDuration is the k6 metric name for operation duration.
	metricKVOperationDuration = "xk6_kv_operation_duration"
	// metricKVOperationFailed is the k6 metric name for operation failed.
	metricKVOperationFailed = "xk6_kv_operation_failed"
	// metricKVErrorsTotal is the k6 metric name for errors total.
	metricKVErrorsTotal = "xk6_kv_errors_total"
	// metricKVEmptyResult is the k6 metric name for empty result.
	metricKVEmptyResult = "xk6_kv_empty_result"
	// metricKVAsyncInFlight is the k6 metric name for async in flight.
	metricKVAsyncInFlight = "xk6_kv_async_in_flight"

	// tagOp is the k6 metric tag key for op.
	tagOp = "op"
	// tagStatus is the k6 metric tag key for status.
	tagStatus = "status"
	// tagErrorType is the k6 metric tag key for error type.
	tagErrorType = "error_type"

	// statusOK is the operation status tag value for ok outcomes.
	statusOK = "ok"
	// statusError is the operation status tag value for error outcomes.
	statusError = "error"

	// opGet is the operation tag for get.
	opGet = "get"
	// opGetMany is the operation tag for get_many.
	opGetMany = "get_many"
	// opSet is the operation tag for set.
	opSet = "set"
	// opSetMany is the operation tag for set_many.
	opSetMany = "set_many"
	// opDeleteMany is the operation tag for delete_many.
	opDeleteMany = "delete_many"
	// opDeleteByPrefix is the operation tag for delete_by_prefix.
	opDeleteByPrefix = "delete_by_prefix"
	// opDelete is the operation tag for delete.
	opDelete = "delete"
	// opExists is the operation tag for exists.
	opExists = "exists"
	// opClear is the operation tag for clear.
	opClear = "clear"
	// opSize is the operation tag for size.
	opSize = "size"
	// opIncrementBy is the operation tag for increment_by.
	opIncrementBy = "increment_by"
	// opGetOrSet is the operation tag for get_or_set.
	opGetOrSet = "get_or_set"
	// opSetIfAbsent is the operation tag for set_if_absent.
	opSetIfAbsent = "set_if_absent"
	// opSwap is the operation tag for swap.
	opSwap = "swap"
	// opCompareAndSwap is the operation tag for compare_and_swap.
	opCompareAndSwap = "compare_and_swap"
	// opCompareAndSwapDetailed is the operation tag for compare_and_swap_detailed.
	opCompareAndSwapDetailed = "compare_and_swap_detailed"
	// opCompareAndDelete is the operation tag for compare_and_delete.
	opCompareAndDelete = "compare_and_delete"
	// opCompareAndDeleteDetailed is the operation tag for compare_and_delete_detailed.
	opCompareAndDeleteDetailed = "compare_and_delete_detailed"
	// opDeleteIfExists is the operation tag for delete_if_exists.
	opDeleteIfExists = "delete_if_exists"
	// opScan is the operation tag for scan.
	opScan = "scan"
	// opScanKeys is the operation tag for scan_keys.
	opScanKeys = "scan_keys"
	// opList is the operation tag for list.
	opList = "list"
	// opListKeys is the operation tag for list_keys.
	opListKeys = "list_keys"
	// opCount is the operation tag for count.
	opCount = "count"
	// opRandomKey is the operation tag for random_key.
	opRandomKey = "random_key"
	// opRandomKeys is the operation tag for random_keys.
	opRandomKeys = "random_keys"
	// opPopRandom is the operation tag for pop_random.
	opPopRandom = "pop_random"
	// opClaimRandom is the operation tag for claim_random.
	opClaimRandom = "claim_random"
	// opClaimKey is the operation tag for claim_key.
	opClaimKey = "claim_key"
	// opClaimRandomMany is the operation tag for claim_random_many.
	opClaimRandomMany = "claim_random_many"
	// opPopRandomMany is the operation tag for pop_random_many.
	opPopRandomMany = "pop_random_many"
	// opReleaseClaim is the operation tag for release_claim.
	opReleaseClaim = "release_claim"
	// opCompleteClaim is the operation tag for complete_claim.
	opCompleteClaim = "complete_claim"
	// opRenewClaim is the operation tag for renew_claim.
	opRenewClaim = "renew_claim"
	// opRebuildKeyList is the operation tag for rebuild_key_list.
	opRebuildKeyList = "rebuild_key_list"
	// opBackup is the operation tag for backup.
	opBackup = "backup"
	// opRestore is the operation tag for restore.
	opRestore = "restore"
	// opExportJSONL is the operation tag for export_jsonl.
	opExportJSONL = "export_jsonl"
	// opExportCSV is the operation tag for export_csv.
	opExportCSV = "export_csv"
	// opImportJSONL is the operation tag for import_jsonl.
	opImportJSONL = "import_jsonl"
	// opImportCSV is the operation tag for import_csv.
	opImportCSV = "import_csv"
	// opValidateCSV is the operation tag for validate_csv.
	opValidateCSV = "validate_csv"
	// opValidateJSONL is the operation tag for validate_jsonl.
	opValidateJSONL = "validate_jsonl"
	// opStats is the operation tag for stats.
	opStats = "stats"
	// opReportStats is the operation tag for report_stats.
	opReportStats = "report_stats"
	// opAllocationStats is the operation tag for allocation_stats.
	opAllocationStats = "allocation_stats"
	// opReleaseClaims is the operation tag for release_claims.
	opReleaseClaims = "release_claims"
	// opCompleteClaims is the operation tag for complete_claims.
	opCompleteClaims = "complete_claims"
	// opRenewClaims is the operation tag for renew_claims.
	opRenewClaims = "renew_claims"
	// opClaimKeys is the operation tag for claim_keys.
	opClaimKeys = "claim_keys"
)

type (
	// kvOperationMetrics holds registered k6 metrics for per-operation KV telemetry.
	kvOperationMetrics struct {
		// operationsTotal counts completed KV operations.
		operationsTotal *metrics.Metric
		// operationDuration records operation latency samples.
		operationDuration *metrics.Metric
		// operationFailed records whether an operation failed.
		operationFailed *metrics.Metric
		// errorsTotal counts classified KV errors by type.
		errorsTotal *metrics.Metric
		// emptyResult records allocation operations that returned no items.
		emptyResult *metrics.Metric
		// asyncInFlight tracks in-flight async KV operations.
		asyncInFlight *metrics.Metric
		// asyncInFlightNow is the current async in-flight operation count.
		asyncInFlightNow atomic.Int64

		// backend is the store backend tag value copied from open options.
		backend string
		// serialization is the value encoding tag copied from open options.
		serialization string
		// trackKeys is the key-tracking tag copied from open options.
		trackKeys string
	}

	// kvOperationSample is a single operation measurement passed to emit.
	kvOperationSample struct {
		// operation is the op tag value for the completed method.
		operation string
		// duration is the elapsed time of the store call.
		duration time.Duration
		// failed is true when the operation ended with an error.
		failed bool
		// errorType is the classified ErrorName when failed is true.
		errorType string
		// emptyResult is true when an allocation-style call returned no items.
		emptyResult bool
	}
)

// newKVOperationMetrics registers per-operation KV metrics in the k6 registry.
func newKVOperationMetrics(registry *metrics.Registry, options Options) (*kvOperationMetrics, error) {
	if registry == nil {
		return nil, errors.New("k6 metrics registry is nil")
	}

	operationsTotal, err := registry.NewMetric(metricKVOperationsTotal, metrics.Counter, metrics.Default)
	if err != nil {
		return nil, fmt.Errorf("register %s: %w", metricKVOperationsTotal, err)
	}

	operationDuration, err := registry.NewMetric(metricKVOperationDuration, metrics.Trend, metrics.Time)
	if err != nil {
		return nil, fmt.Errorf("register %s: %w", metricKVOperationDuration, err)
	}

	operationFailed, err := registry.NewMetric(metricKVOperationFailed, metrics.Rate, metrics.Default)
	if err != nil {
		return nil, fmt.Errorf("register %s: %w", metricKVOperationFailed, err)
	}

	errorsTotal, err := registry.NewMetric(metricKVErrorsTotal, metrics.Counter, metrics.Default)
	if err != nil {
		return nil, fmt.Errorf("register %s: %w", metricKVErrorsTotal, err)
	}

	emptyResult, err := registry.NewMetric(metricKVEmptyResult, metrics.Rate, metrics.Default)
	if err != nil {
		return nil, fmt.Errorf("register %s: %w", metricKVEmptyResult, err)
	}

	asyncInFlight, err := registry.NewMetric(metricKVAsyncInFlight, metrics.Gauge, metrics.Default)
	if err != nil {
		return nil, fmt.Errorf("register %s: %w", metricKVAsyncInFlight, err)
	}

	serialization := options.Serialization
	if serialization == "" {
		serialization = DefaultSerialization
	}

	return &kvOperationMetrics{
		operationsTotal:   operationsTotal,
		operationDuration: operationDuration,
		operationFailed:   operationFailed,
		errorsTotal:       errorsTotal,
		emptyResult:       emptyResult,
		asyncInFlight:     asyncInFlight,
		backend:           options.Backend,
		serialization:     serialization,
		trackKeys:         strconv.FormatBool(options.TrackKeys),
	}, nil
}

// emit records counters, trends, and rates for one completed operation sample.
func (m *kvOperationMetrics) emit(ctx context.Context, state *lib.State, sample kvOperationSample) {
	if m == nil || ctx == nil || state == nil || state.Samples == nil || state.Tags == nil || sample.operation == "" {
		return
	}

	tagsAndMeta := state.Tags.GetCurrentValues()
	baseTags := tagsAndMeta.Tags.
		With(tagBackend, m.backend).
		With(tagTrackKeys, m.trackKeys).
		With(tagSerialization, m.serialization).
		With(tagOp, sample.operation)

	status := statusOK
	if sample.failed {
		status = statusError
	}

	sampleTime := time.Now()

	m.pushSample(ctx, state, m.operationsTotal, baseTags.With(tagStatus, status), tagsAndMeta.Metadata, sampleTime, 1)
	m.pushSample(
		ctx,
		state,
		m.operationDuration,
		baseTags.With(tagStatus, status),
		tagsAndMeta.Metadata,
		sampleTime,
		metrics.D(sample.duration),
	)
	m.pushSample(
		ctx,
		state,
		m.operationFailed,
		baseTags,
		tagsAndMeta.Metadata,
		sampleTime,
		metrics.B(sample.failed),
	)

	if sample.failed && sample.errorType != "" {
		m.pushSample(
			ctx,
			state,
			m.errorsTotal,
			baseTags.With(tagErrorType, sample.errorType),
			tagsAndMeta.Metadata,
			sampleTime,
			1,
		)
	}

	if sample.shouldEmitEmptyResult() {
		m.pushSample(
			ctx,
			state,
			m.emptyResult,
			baseTags,
			tagsAndMeta.Metadata,
			sampleTime,
			metrics.B(sample.emptyResult),
		)
	}
}

// addAsyncInFlight adjusts the in-flight async operation gauge by delta.
func (m *kvOperationMetrics) addAsyncInFlight(ctx context.Context, state *lib.State, delta int64) {
	if m == nil {
		return
	}

	current := m.asyncInFlightNow.Add(delta)
	m.emitAsyncInFlight(ctx, state, current)
}

// emitAsyncInFlight publishes the current async in-flight gauge value.
func (m *kvOperationMetrics) emitAsyncInFlight(ctx context.Context, state *lib.State, value int64) {
	if m == nil || ctx == nil || state == nil || state.Samples == nil || state.Tags == nil {
		return
	}

	tagsAndMeta := state.Tags.GetCurrentValues()
	tags := tagsAndMeta.Tags.
		With(tagBackend, m.backend).
		With(tagTrackKeys, m.trackKeys).
		With(tagSerialization, m.serialization)

	m.pushSample(ctx, state, m.asyncInFlight, tags, tagsAndMeta.Metadata, time.Now(), float64(value))
}

// pushSample appends one metric sample when the VU context is still active.
func (m *kvOperationMetrics) pushSample(
	ctx context.Context,
	state *lib.State,
	metric *metrics.Metric,
	tags *metrics.TagSet,
	metadata map[string]string,
	sampleTime time.Time,
	value float64,
) {
	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		TimeSeries: metrics.TimeSeries{
			Metric: metric,
			Tags:   tags,
		},
		Time:     sampleTime,
		Metadata: metadata,
		Value:    value,
	})
}

// shouldEmitEmptyResult reports whether empty-result metrics apply to this operation.
func (s kvOperationSample) shouldEmitEmptyResult() bool {
	if s.failed {
		return false
	}

	switch s.operation {
	case opRandomKey, opRandomKeys, opPopRandom, opClaimRandom, opClaimKey, opClaimRandomMany, opPopRandomMany:
		return true
	default:
		return false
	}
}

// isEmptyAllocationResult reports whether an allocation operation returned no items.
func isEmptyAllocationResult(op string, result any) bool {
	switch op {
	case opRandomKey:
		key, ok := result.(string)
		return ok && key == ""
	case opRandomKeys:
		keys, ok := result.([]string)
		return ok && len(keys) == 0
	case opPopRandom, opClaimRandom, opClaimKey:
		return result == nil
	case opClaimRandomMany, opPopRandomMany:
		if result == nil {
			return true
		}

		value := reflect.ValueOf(result)

		return value.Kind() == reflect.Slice && value.Len() == 0
	default:
		return false
	}
}
