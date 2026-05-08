package store

const (
	// MaxScanLimit bounds scan page size to avoid oversized JS payloads and heap preallocation.
	MaxScanLimit int64 = 100_000
	// MaxListLimit bounds non-paginated list output while preserving <= 0 "all" semantics.
	MaxListLimit int64 = 250_000
	// MaxDeleteByPrefixLimit bounds one destructive prefix-delete transaction.
	MaxDeleteByPrefixLimit int64 = 100_000
)
