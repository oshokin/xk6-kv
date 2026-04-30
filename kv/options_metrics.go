package kv

// MetricsOptions configures optional extension-emitted k6 metrics.
type MetricsOptions struct {
	// Operations enables automatic per-operation metrics.
	Operations bool `js:"operations"`
}

// Equal checks if two MetricsOptions are equivalent.
func (mo *MetricsOptions) Equal(other *MetricsOptions) bool {
	return mo.operationsEnabled() == other.operationsEnabled()
}

// operationsEnabled reports whether operation metrics are enabled.
func (mo *MetricsOptions) operationsEnabled() bool {
	return mo != nil && mo.Operations
}
