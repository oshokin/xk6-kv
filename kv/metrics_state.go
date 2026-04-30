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
	metricKVKeys            = "xk6_kv_keys"
	metricKVClaimsLive      = "xk6_kv_claims_live"
	metricKVClaimsExpired   = "xk6_kv_claims_expired"
	metricKVIndexKeys       = "xk6_kv_index_keys"
	metricKVIndexConsistent = "xk6_kv_index_consistent"
	metricKVDiskSizeBytes   = "xk6_kv_disk_size_bytes"

	tagBackend       = "backend"
	tagTrackKeys     = "track_keys"
	tagSerialization = "serialization"
	tagIndex         = "index"
)

type kvStateMetrics struct {
	keys            *metrics.Metric
	claimsLive      *metrics.Metric
	claimsExpired   *metrics.Metric
	indexKeys       *metrics.Metric
	indexConsistent *metrics.Metric
	diskSizeBytes   *metrics.Metric
}

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
