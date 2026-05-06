package kv

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"go.k6.io/k6/lib"
	"go.k6.io/k6/metrics"
)

const (
	metricKVOperationsTotal   = "xk6_kv_operations_total"
	metricKVOperationDuration = "xk6_kv_operation_duration"
	metricKVOperationFailed   = "xk6_kv_operation_failed"
	metricKVErrorsTotal       = "xk6_kv_errors_total"
	metricKVEmptyResult       = "xk6_kv_empty_result"

	tagOp        = "op"
	tagStatus    = "status"
	tagErrorType = "error_type"

	statusOK    = "ok"
	statusError = "error"

	opGet                      = "get"
	opGetMany                  = "get_many"
	opSet                      = "set"
	opSetMany                  = "set_many"
	opDeleteMany               = "delete_many"
	opDeleteByPrefix           = "delete_by_prefix"
	opDelete                   = "delete"
	opExists                   = "exists"
	opClear                    = "clear"
	opSize                     = "size"
	opIncrementBy              = "increment_by"
	opGetOrSet                 = "get_or_set"
	opSetIfAbsent              = "set_if_absent"
	opSwap                     = "swap"
	opCompareAndSwap           = "compare_and_swap"
	opCompareAndSwapDetailed   = "compare_and_swap_detailed"
	opCompareAndDelete         = "compare_and_delete"
	opCompareAndDeleteDetailed = "compare_and_delete_detailed"
	opDeleteIfExists           = "delete_if_exists"
	opScan                     = "scan"
	opScanKeys                 = "scan_keys"
	opList                     = "list"
	opListKeys                 = "list_keys"
	opCount                    = "count"
	opRandomKey                = "random_key"
	opRandomKeys               = "random_keys"
	opPopRandom                = "pop_random"
	opClaimRandom              = "claim_random"
	opReleaseClaim             = "release_claim"
	opCompleteClaim            = "complete_claim"
	opRebuildKeyList           = "rebuild_key_list"
	opBackup                   = "backup"
	opRestore                  = "restore"
	opExportJSONL              = "export_jsonl"
	opImportJSONL              = "import_jsonl"
	opStats                    = "stats"
	opReportStats              = "report_stats"
)

type (
	kvOperationMetrics struct {
		operationsTotal   *metrics.Metric
		operationDuration *metrics.Metric
		operationFailed   *metrics.Metric
		errorsTotal       *metrics.Metric
		emptyResult       *metrics.Metric

		backend       string
		serialization string
		trackKeys     string
	}

	kvOperationSample struct {
		operation   string
		duration    time.Duration
		failed      bool
		errorType   string
		emptyResult bool
	}
)

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
		backend:           options.Backend,
		serialization:     serialization,
		trackKeys:         strconv.FormatBool(options.TrackKeys),
	}, nil
}

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

func (s kvOperationSample) shouldEmitEmptyResult() bool {
	if s.failed {
		return false
	}

	switch s.operation {
	case opRandomKey, opRandomKeys, opPopRandom, opClaimRandom:
		return true
	default:
		return false
	}
}

func isEmptyAllocationResult(op string, result any) bool {
	switch op {
	case opRandomKey:
		key, ok := result.(string)
		return ok && key == ""
	case opRandomKeys:
		keys, ok := result.([]string)
		return ok && len(keys) == 0
	case opPopRandom, opClaimRandom:
		return result == nil
	default:
		return false
	}
}
