package store

const (
	backendMemoryName = "memory"
	backendDiskName   = "disk"

	serializationJSONName   = "json"
	serializationStringName = "string"
)

type (
	// StatsSnapshot is a structured diagnostic snapshot of the current store.
	StatsSnapshot struct {
		// Backend is the active storage backend ("memory" or "disk").
		Backend string `js:"backend"`
		// Serialization is the active value serialization mode ("json" or "string").
		Serialization string `js:"serialization"`
		// TrackKeys indicates whether in-memory key indexes are enabled.
		TrackKeys bool `js:"trackKeys"`
		// Count is the current number of user keys.
		Count int64 `js:"count"`
		// Claims reports claim lease counters.
		Claims ClaimStats `js:"claims"`
		// Index reports index-related counters when key tracking is enabled.
		Index *IndexStats `js:"index"`
		// Disk reports disk backend details.
		Disk *DiskStats `js:"disk"`
	}

	// ClaimStats contains lease counters.
	ClaimStats struct {
		// Live is the number of currently live claims.
		Live int64 `js:"live"`
		// Expired is the number of expired claims still present in metadata.
		Expired int64 `js:"expired"`
	}

	// IndexStats contains key-index counters and quick consistency checks.
	IndexStats struct {
		// Enabled reports whether index tracking is enabled.
		Enabled bool `js:"enabled"`
		// KeysList is the current keys list size.
		KeysList int64 `js:"keysList"`
		// KeysMap is the current keys map size.
		KeysMap int64 `js:"keysMap"`
		// OST is the current order-statistics tree size.
		OST int64 `js:"ost"`
		// Consistent reports whether index counters match expected key counts.
		Consistent bool `js:"consistent"`
	}

	// DiskStats contains disk backend details.
	DiskStats struct {
		// Path is the bbolt file path.
		Path string `js:"path"`
		// SizeBytes is the current bbolt file size.
		SizeBytes int64 `js:"sizeBytes"`
		// ReadOnly reports whether the store is configured read-only.
		ReadOnly bool `js:"readOnly"`
	}
)
