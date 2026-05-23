package kv

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"go.k6.io/k6/lib"
	"go.k6.io/k6/metrics"

	"github.com/oshokin/xk6-kv/kv/store"
)

const (
	// metricKVKeys is the k6 metric name for keys.
	metricKVKeys = "xk6_kv_keys"
	// metricKVClaimsLive is the k6 metric name for claims live.
	metricKVClaimsLive = "xk6_kv_claims_live"
	// metricKVClaimsExpired is the k6 metric name for claims expired.
	metricKVClaimsExpired = "xk6_kv_claims_expired"
	// metricKVIndexKeys is the k6 metric name for index keys.
	metricKVIndexKeys = "xk6_kv_index_keys"
	// metricKVIndexConsistent is the k6 metric name for index consistent.
	metricKVIndexConsistent = "xk6_kv_index_consistent"
	// metricKVDiskSizeBytes is the k6 metric name for disk size bytes.
	metricKVDiskSizeBytes = "xk6_kv_disk_size_bytes"

	// tagBackend is the k6 metric tag key for backend.
	tagBackend = "backend"
	// tagTrackKeys is the k6 metric tag key for track keys.
	tagTrackKeys = "track_keys"
	// tagSerialization is the k6 metric tag key for serialization.
	tagSerialization = "serialization"
	// tagIndex is the k6 metric tag key for index.
	tagIndex = "index"
)

// kvStateMetrics holds registered k6 gauges emitted by reportStats().
type kvStateMetrics struct {
	// keys tracks the number of entries in the store.
	keys *metrics.Metric
	// claimsLive tracks active claim leases.
	claimsLive *metrics.Metric
	// claimsExpired tracks expired but not yet reclaimed claims.
	claimsExpired *metrics.Metric
	// indexKeys tracks key-index structure sizes when indexing is enabled.
	indexKeys *metrics.Metric
	// indexConsistent tracks whether tracked indexes match store contents.
	indexConsistent *metrics.Metric
	// diskSizeBytes tracks on-disk database size for disk backends.
	diskSizeBytes *metrics.Metric
}

// newKVStateMetrics registers reportStats() gauge metrics in the k6 registry.
func newKVStateMetrics(registry *metrics.Registry) (*kvStateMetrics, error) {
	if registry == nil {
		return nil, errors.New("k6 metrics registry is nil")
	}

	keys, err := registry.NewMetric(metricKVKeys, metrics.Gauge, metrics.Default)
	if err != nil {
		return nil, fmt.Errorf("register %s: %w", metricKVKeys, err)
	}

	claimsLive, err := registry.NewMetric(metricKVClaimsLive, metrics.Gauge, metrics.Default)
	if err != nil {
		return nil, fmt.Errorf("register %s: %w", metricKVClaimsLive, err)
	}

	claimsExpired, err := registry.NewMetric(metricKVClaimsExpired, metrics.Gauge, metrics.Default)
	if err != nil {
		return nil, fmt.Errorf("register %s: %w", metricKVClaimsExpired, err)
	}

	indexKeys, err := registry.NewMetric(metricKVIndexKeys, metrics.Gauge, metrics.Default)
	if err != nil {
		return nil, fmt.Errorf("register %s: %w", metricKVIndexKeys, err)
	}

	indexConsistent, err := registry.NewMetric(metricKVIndexConsistent, metrics.Gauge, metrics.Default)
	if err != nil {
		return nil, fmt.Errorf("register %s: %w", metricKVIndexConsistent, err)
	}

	diskSizeBytes, err := registry.NewMetric(metricKVDiskSizeBytes, metrics.Gauge, metrics.Data)
	if err != nil {
		return nil, fmt.Errorf("register %s: %w", metricKVDiskSizeBytes, err)
	}

	return &kvStateMetrics{
		keys:            keys,
		claimsLive:      claimsLive,
		claimsExpired:   claimsExpired,
		indexKeys:       indexKeys,
		indexConsistent: indexConsistent,
		diskSizeBytes:   diskSizeBytes,
	}, nil
}

// emit publishes gauge samples derived from a store stats snapshot.
func (m *kvStateMetrics) emit(
	ctx context.Context,
	state *lib.State,
	snapshot *store.StatsSnapshot,
) error {
	if m == nil {
		return NewError(MetricsUnavailableError, "reportStats metrics are not initialized")
	}

	if ctx == nil || state == nil || state.Samples == nil || state.Tags == nil {
		return NewError(
			MetricsUnavailableError,
			"reportStats() requires active VU context, state and metric sample output",
		)
	}

	serialization := snapshot.Serialization
	if serialization == "" {
		serialization = "unknown"
	}

	baseTagsAndMeta := state.Tags.GetCurrentValues()
	baseTags := baseTagsAndMeta.Tags.
		With(tagBackend, snapshot.Backend).
		With(tagTrackKeys, strconv.FormatBool(snapshot.TrackKeys)).
		With(tagSerialization, serialization)

	m.emitBaseSamples(ctx, state, snapshot, baseTags, baseTagsAndMeta.Metadata, time.Now())

	return nil
}

// emitBaseSamples emits count, claim, and disk gauge samples.
func (m *kvStateMetrics) emitBaseSamples(
	ctx context.Context,
	state *lib.State,
	snapshot *store.StatsSnapshot,
	baseTags *metrics.TagSet,
	metadata map[string]string,
	sampleTime time.Time,
) {
	m.emitGaugeSample(ctx, state, m.keys, baseTags, metadata, sampleTime, float64(snapshot.Count))
	m.emitGaugeSample(ctx, state, m.claimsLive, baseTags, metadata, sampleTime, float64(snapshot.Claims.Live))
	m.emitGaugeSample(ctx, state, m.claimsExpired, baseTags, metadata, sampleTime, float64(snapshot.Claims.Expired))

	if snapshot.Index != nil && snapshot.Index.Enabled {
		m.emitIndexSamples(ctx, state, snapshot, baseTags, metadata, sampleTime)
	}

	if snapshot.Disk != nil {
		m.emitGaugeSample(
			ctx,
			state,
			m.diskSizeBytes,
			baseTags,
			metadata,
			sampleTime,
			float64(snapshot.Disk.SizeBytes),
		)
	}
}

// emitIndexSamples emits per-index and consistency gauge samples.
func (m *kvStateMetrics) emitIndexSamples(
	ctx context.Context,
	state *lib.State,
	snapshot *store.StatsSnapshot,
	baseTags *metrics.TagSet,
	metadata map[string]string,
	sampleTime time.Time,
) {
	m.emitGaugeSample(
		ctx,
		state,
		m.indexKeys,
		baseTags.With(tagIndex, "keys_list"),
		metadata,
		sampleTime,
		float64(snapshot.Index.KeysList),
	)
	m.emitGaugeSample(
		ctx,
		state,
		m.indexKeys,
		baseTags.With(tagIndex, "keys_map"),
		metadata,
		sampleTime,
		float64(snapshot.Index.KeysMap),
	)
	m.emitGaugeSample(
		ctx,
		state,
		m.indexKeys,
		baseTags.With(tagIndex, "ost"),
		metadata,
		sampleTime,
		float64(snapshot.Index.OST),
	)
	m.emitGaugeSample(
		ctx,
		state,
		m.indexConsistent,
		baseTags,
		metadata,
		sampleTime,
		metrics.B(snapshot.Index.Consistent),
	)
}

// emitGaugeSample appends one gauge sample when the VU context is still active.
func (m *kvStateMetrics) emitGaugeSample(
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
