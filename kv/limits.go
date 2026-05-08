package kv

const (
	// MaxExportJSONLLimit bounds one explicit JSONL export request.
	MaxExportJSONLLimit int64 = 1_000_000
	// MaxImportJSONLLimit bounds one explicit JSONL import request.
	MaxImportJSONLLimit int64 = 1_000_000
	// MaxJSONLBatchSize bounds import SetMany batch size and transaction pressure.
	MaxJSONLBatchSize int64 = 10_000
)
