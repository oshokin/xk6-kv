package kv

import (
	"github.com/grafana/sobek"

	"github.com/oshokin/xk6-kv/kv/store"
)

// Stats returns a Promise that resolves to a diagnostic snapshot of store state.
func (k *KV) Stats() *sobek.Promise {
	return k.runAsyncWithStoreObserved(
		opStats,
		func(s store.Store) (any, error) {
			snapshot, err := s.Stats()
			if err != nil {
				return nil, err
			}

			if snapshot == nil {
				return nil, unexpectedStoreOutput("store.Stats")
			}

			return snapshot, nil
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

// ReportStats captures a snapshot and emits state gauges to k6 custom metrics.
func (k *KV) ReportStats() *sobek.Promise {
	ctx := k.vu.Context()
	state := k.vu.State()

	return k.runAsyncWithStoreObserved(
		opReportStats,
		func(s store.Store) (any, error) {
			if k.stateMetrics == nil {
				return nil, NewError(MetricsUnavailableError, "reportStats metrics are unavailable")
			}

			snapshot, err := s.Stats()
			if err != nil {
				return nil, err
			}

			if snapshot == nil {
				return nil, unexpectedStoreOutput("store.Stats")
			}

			if err := k.stateMetrics.emit(ctx, state, snapshot); err != nil {
				return nil, err
			}

			return nil, nil
		},
		func(_ *sobek.Runtime, _ any) sobek.Value {
			return sobek.Undefined()
		},
	)
}
