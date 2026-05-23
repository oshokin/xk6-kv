package kv

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/grafana/sobek"

	"github.com/oshokin/xk6-kv/internal/fileutil"
	"github.com/oshokin/xk6-kv/kv/store"
)

const (
	// exportJSONLPageSize is the export j s o n l page size const.
	exportJSONLPageSize int64 = 1000
	// importJSONLDefaultBatchSize is the import j s o n l default batch size const.
	importJSONLDefaultBatchSize int64 = 1000
	// importJSONLDefaultMaxLineBytes is the import j s o n l default max line bytes const.
	importJSONLDefaultMaxLineBytes = 64 << 20 // 64 MiB
)

// errImportJSONLLineTooLong is the err import j s o n l line too long var.
var errImportJSONLLineTooLong = errors.New("importJSONL line exceeds maxLineBytes")

type (
	// exportJSONLResult is the Go-side result returned by the corresponding KV operation.
	exportJSONLResult struct {
		// Exported is the number of rows successfully written to the store.
		Exported int64 `js:"exported"`
		// FileName is the path of the CSV or JSONL file.
		FileName string `js:"fileName"`
		// BytesWritten is the number of payload bytes read from or written to disk.
		BytesWritten int64 `js:"bytesWritten"`
	}

	// importJSONLResult is the Go-side result returned by the corresponding KV operation.
	importJSONLResult struct {
		// Imported is the number of rows successfully written to the store.
		Imported int64 `js:"imported"`
		// FileName is the path of the CSV or JSONL file.
		FileName string `js:"fileName"`
		// BytesRead is the number of payload bytes read from or written to disk.
		BytesRead int64 `js:"bytesRead"`
	}

	// validateJSONLResult is the Go-side result returned by the corresponding KV operation.
	validateJSONLResult struct {
		// Valid reports whether validation finished without errors.
		Valid bool `js:"valid"`
		// Records holds the records value.
		Records int64 `js:"records"`
		// BytesRead is the number of payload bytes read from or written to disk.
		BytesRead int64 `js:"bytesRead"`
		// CheckedAll is true when the validator scanned the entire file.
		CheckedAll bool `js:"checkedAll"`
		// FirstError describes the first validation failure, if any.
		FirstError *validateJSONLFirstError `js:"firstError,omitempty"`
	}

	// validateJSONLFirstError carries error details for the corresponding operation.
	validateJSONLFirstError struct {
		// Line holds the line value.
		Line int64 `js:"line"`
		// Name is the machine-readable error name.
		Name string `js:"name"`
		// Message is a human-readable error description.
		Message string `js:"message"`
	}

	// exportJSONLRecord is an internal export j s o n l record type.
	exportJSONLRecord struct {
		// Key is the store key associated with the claim or entry.
		Key string `json:"key"`
		// Value holds the value value.
		Value any `json:"value"`
	}

	// importJSONLProgressError carries error details for the corresponding operation.
	importJSONLProgressError struct {
		// imported is the number of rows successfully written to the store.
		imported int64
		// bytesRead is the number of payload bytes read from or written to disk.
		bytesRead int64
		// lineNo holds the line no value.
		lineNo int64
		// err is the underlying error that stopped processing.
		err error
	}

	// importJSONLLineState tracks mutable state for a multi-step internal loop.
	importJSONLLineState struct {
		// lineNo holds the line no value.
		lineNo int64
		// imported is the number of rows successfully written to the store.
		imported int64
		// batch is the pending entry batch awaiting flush to the store.
		batch []store.Entry
	}

	// importJSONLAppendLineParams groups parameters passed to an internal helper.
	importJSONLAppendLineParams struct {
		// ctx carries cancellation for the import or export operation.
		ctx context.Context
		// store is the backing KV store implementation.
		store store.Store
		// line holds the line value.
		line []byte
		// bytesRead is the number of payload bytes read from or written to disk.
		bytesRead int64
		// batchSize is the number of rows written per store batch.
		batchSize int64
		// limit caps how many rows or entries are processed.
		limit int64
		// current holds the current value.
		current importJSONLLineState
	}

	// importJSONLReadErrorParams groups parameters passed to an internal helper.
	importJSONLReadErrorParams struct {
		// err is the underlying error that stopped processing.
		err error
		// imported is the number of rows successfully written to the store.
		imported int64
		// bytesRead is the number of payload bytes read from or written to disk.
		bytesRead int64
		// lineNo holds the line no value.
		lineNo int64
		// maxLineBytes holds the max line bytes value.
		maxLineBytes int
	}

	// importJSONLFlushProgressParams groups parameters passed to an internal helper.
	importJSONLFlushProgressParams struct {
		// ctx carries cancellation for the import or export operation.
		ctx context.Context
		// store is the backing KV store implementation.
		store store.Store
		// batch is the pending entry batch awaiting flush to the store.
		batch []store.Entry
		// imported is the number of rows successfully written to the store.
		imported int64
		// bytesRead is the number of payload bytes read from or written to disk.
		bytesRead int64
		// lineNo holds the line no value.
		lineNo int64
	}
)

// Error implements importJSONLProgressError receiver behavior.
func (e *importJSONLProgressError) Error() string {
	commitStatus := "no records were committed"
	if e.imported > 0 {
		commitStatus = "previous batches may already be committed"
	}

	return fmt.Sprintf(
		"importJSONL failed after %d records and %d bytes; %s: line %d: %v",
		e.imported,
		e.bytesRead,
		commitStatus,
		e.lineNo,
		e.err,
	)
}

// Unwrap implements importJSONLProgressError receiver behavior.
func (e *importJSONLProgressError) Unwrap() error {
	return e.err
}

// ExportJSONL exports key/value entries to a JSON Lines file.
func (k *KV) ExportJSONL(options sobek.Value) *sobek.Promise {
	exportOptions, err := importExportJSONLOptions(k.vu.Runtime(), options)
	if err != nil {
		return k.rejectedPromiseObserved(opExportJSONL, err)
	}

	ctx := k.vu.Context()

	return k.runAsyncWithStoreObserved(
		opExportJSONL,
		func(s store.Store) (any, error) {
			return exportJSONL(ctx, s, exportOptions)
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

// ImportJSONL imports key/value entries from a JSON Lines file.
func (k *KV) ImportJSONL(options sobek.Value) *sobek.Promise {
	importOptions, err := importImportJSONLOptions(k.vu.Runtime(), options)
	if err != nil {
		return k.rejectedPromiseObserved(opImportJSONL, err)
	}

	ctx := k.vu.Context()

	return k.runAsyncWithStoreObserved(
		opImportJSONL,
		func(s store.Store) (any, error) {
			return importJSONL(ctx, s, importOptions)
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

// ValidateJSONL validates a JSONL file without writing to the store.
// It still requires an open KV handle because it runs through the shared
// async lifecycle/metrics pipeline used by all KV instance methods.
// With limit > 0, validation applies to the inspected prefix of records only.
// checkedAll=true means validation reached EOF; checkedAll=false means it stopped due to limit.
// Contract: valid=true implies whole-file validity only when checkedAll=true.
func (k *KV) ValidateJSONL(options sobek.Value) *sobek.Promise {
	validateOptions, err := importValidateJSONLOptions(k.vu.Runtime(), options)
	if err != nil {
		return k.rejectedPromiseObserved(opValidateJSONL, err)
	}

	ctx := k.vu.Context()

	return k.runAsyncWithStoreObserved(
		opValidateJSONL,
		func(_ store.Store) (any, error) {
			return validateJSONL(ctx, validateOptions)
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

//nolint:dupl // Export finalization mirrors exportCSV intentionally.
func exportJSONL(ctx context.Context, s store.Store, opts exportJSONLOptions) (*exportJSONLResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	targetDir := filepath.Dir(opts.FileName)
	targetBase := filepath.Base(opts.FileName)

	if err := os.MkdirAll(targetDir, 0o750); err != nil {
		return nil, fmt.Errorf("%w: exportJSONL create output directory: %w", store.ErrSnapshotDirectoryFailed, err)
	}

	//nolint:forbidigo // file I/O is required for JSONL export.
	tempFile, err := os.CreateTemp(targetDir, targetBase+".*.tmp")
	if err != nil {
		return nil, fmt.Errorf("%w: exportJSONL create temporary file: %w", store.ErrSnapshotTempFileFailed, err)
	}

	tempName := tempFile.Name()
	cleanupTemp := true

	defer func() {
		if cleanupTemp {
			//nolint:forbidigo // best-effort cleanup for failed export.
			_ = os.Remove(tempName)
		}
	}()

	exported, err := writeJSONLFile(ctx, tempFile, s, opts)
	if err != nil {
		return nil, err
	}

	// ReplaceFile centralizes best-effort "temp then replace target" semantics used by export paths.
	if err := fileutil.ReplaceFile(tempName, opts.FileName); err != nil {
		return nil, fmt.Errorf("%w: exportJSONL replace target file: %w", store.ErrSnapshotFinalizeFailed, err)
	}

	// Temp file no longer exists after successful rename.
	cleanupTemp = false

	if err := fileutil.SyncParentDir(opts.FileName); err != nil {
		return nil, fmt.Errorf("%w: exportJSONL sync parent directory: %w", store.ErrSnapshotFinalizeFailed, err)
	}

	info, err := os.Stat(opts.FileName)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", store.ErrSnapshotExportFailed, err)
	}

	return &exportJSONLResult{
		Exported:     exported,
		FileName:     opts.FileName,
		BytesWritten: info.Size(),
	}, nil
}

// importJSONL is an internal helper.
func importJSONL(ctx context.Context, s store.Store, opts importJSONLOptions) (*importJSONLResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	//nolint:forbidigo // file I/O is required for JSONL import.
	file, err := os.Open(opts.FileName)
	if err != nil {
		return nil, classifyJSONLFileOpenError(opts.FileName, err)
	}
	defer func() {
		_ = file.Close()
	}()

	imported, bytesRead, err := readJSONLLines(ctx, s, file, opts)
	if err != nil {
		return nil, err
	}

	return &importJSONLResult{
		Imported:  imported,
		FileName:  opts.FileName,
		BytesRead: bytesRead,
	}, nil
}

// validateJSONL validates user-supplied input.
func validateJSONL(ctx context.Context, opts validateJSONLOptions) (*validateJSONLResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	//nolint:forbidigo // file I/O is required for JSONL validation.
	file, err := os.Open(opts.FileName)
	if err != nil {
		return nil, classifyJSONLFileOpenError(opts.FileName, err)
	}
	defer func() {
		_ = file.Close()
	}()

	records, bytesRead, checkedAll, firstErr, validateErr := validateJSONLLines(ctx, file, opts)
	if validateErr != nil {
		return nil, validateErr
	}

	return &validateJSONLResult{
		Valid:      firstErr == nil,
		Records:    records,
		BytesRead:  bytesRead,
		CheckedAll: checkedAll,
		FirstError: firstErr,
	}, nil
}

// classifyJSONLFileOpenError is an internal helper.
func classifyJSONLFileOpenError(fileName string, err error) error {
	switch {
	case errors.Is(err, os.ErrNotExist):
		return fmt.Errorf("%w: %s", store.ErrSnapshotNotFound, fileName)
	case errors.Is(err, fs.ErrPermission):
		return fmt.Errorf("%w: %s", store.ErrSnapshotPermissionDenied, fileName)
	default:
		return fmt.Errorf("%w: %s: %w", store.ErrSnapshotOpenFailed, fileName, err)
	}
}

// readJSONLLines supports CSV import and export helpers.
func readJSONLLines(
	ctx context.Context,
	s store.Store,
	input io.Reader,
	opts importJSONLOptions,
) (int64, int64, error) {
	return readJSONLLinesWithMaxLineBytes(ctx, s, input, opts, importJSONLDefaultMaxLineBytes)
}

// readJSONLLinesWithMaxLineBytes supports CSV import and export helpers.
func readJSONLLinesWithMaxLineBytes(
	ctx context.Context,
	s store.Store,
	input io.Reader,
	opts importJSONLOptions,
	maxLineBytes int,
) (int64, int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}

	if maxLineBytes <= 0 {
		maxLineBytes = importJSONLDefaultMaxLineBytes
	}

	reader := bufio.NewReader(input)
	batchSize := resolveImportJSONLBatchSize(opts.BatchSize)
	state := importJSONLLineState{
		batch: make([]store.Entry, 0),
	}

	var bytesRead int64

	for opts.Limit <= 0 || state.imported+int64(len(state.batch)) < opts.Limit {
		if err := ctx.Err(); err != nil {
			return state.imported, bytesRead, wrapImportJSONLProgressError(state.imported, bytesRead, state.lineNo, err)
		}

		line, consumedBytes, readErr := readImportJSONLLineBounded(reader, maxLineBytes)
		bytesRead += consumedBytes

		nextState, err := appendImportJSONLLine(importJSONLAppendLineParams{
			ctx:       ctx,
			store:     s,
			line:      line,
			bytesRead: bytesRead,
			batchSize: batchSize,
			limit:     opts.Limit,
			current:   state,
		})
		if err != nil {
			return state.imported, bytesRead, err
		}

		state = nextState

		keepReading, err := handleImportJSONLReadError(importJSONLReadErrorParams{
			err:          readErr,
			imported:     state.imported,
			bytesRead:    bytesRead,
			lineNo:       state.lineNo,
			maxLineBytes: maxLineBytes,
		})
		if err != nil {
			return state.imported, bytesRead, err
		}

		if !keepReading {
			break
		}
	}

	written, err := flushImportJSONLBatchWithProgress(importJSONLFlushProgressParams{
		ctx:       ctx,
		store:     s,
		batch:     state.batch,
		imported:  state.imported,
		bytesRead: bytesRead,
		lineNo:    state.lineNo,
	})
	if err != nil {
		return state.imported, bytesRead, err
	}

	state.imported += written

	return state.imported, bytesRead, nil
}

// validateJSONLLines validates user-supplied input.
func validateJSONLLines(
	ctx context.Context,
	input io.Reader,
	opts validateJSONLOptions,
) (int64, int64, bool, *validateJSONLFirstError, error) {
	return validateJSONLLinesWithMaxLineBytes(ctx, input, opts, importJSONLDefaultMaxLineBytes)
}

// validateJSONLLinesWithMaxLineBytes validates user-supplied input.
func validateJSONLLinesWithMaxLineBytes(
	ctx context.Context,
	input io.Reader,
	opts validateJSONLOptions,
	maxLineBytes int,
) (int64, int64, bool, *validateJSONLFirstError, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, false, nil, err
	}

	if maxLineBytes <= 0 {
		maxLineBytes = importJSONLDefaultMaxLineBytes
	}

	reader := bufio.NewReader(input)

	var (
		lineNo     int64
		records    int64
		bytesRead  int64
		checkedAll bool
	)

	for opts.Limit <= 0 || records < opts.Limit {
		if err := ctx.Err(); err != nil {
			return records, bytesRead, false, nil, err
		}

		line, consumedBytes, readErr := readImportJSONLLineBounded(reader, maxLineBytes)
		bytesRead += consumedBytes

		if len(line) > 0 {
			lineNo++

			if _, parseErr := parseImportJSONLLine(line, lineNo); parseErr != nil {
				return records, bytesRead, false, newValidateJSONLFirstError(lineNo, parseErr), nil
			}

			records++
		}

		switch {
		case errors.Is(readErr, errImportJSONLLineTooLong):
			parseErr := fmt.Errorf(
				"%w: validateJSONL line %d exceeds maxLineBytes (%d bytes)",
				store.ErrValueParseFailed,
				lineNo+1,
				maxLineBytes,
			)

			return records, bytesRead, false, newValidateJSONLFirstError(lineNo+1, parseErr), nil
		case errors.Is(readErr, io.EOF):
			checkedAll = true
			return records, bytesRead, checkedAll, nil, nil
		case readErr != nil:
			return records, bytesRead, false, nil, fmt.Errorf(
				"%w: validateJSONL line %d: %w",
				store.ErrSnapshotReadFailed,
				lineNo+1,
				readErr,
			)
		}
	}

	return records, bytesRead, checkedAll, nil, nil
}

// newValidateJSONLFirstError constructs a new ValidateJSONLFirstError value.
func newValidateJSONLFirstError(lineNo int64, err error) *validateJSONLFirstError {
	classified := classifyError(err)

	name := string(UnknownError)
	if classified != nil {
		name = string(classified.Name)
	}

	return &validateJSONLFirstError{
		Line:    lineNo,
		Name:    name,
		Message: err.Error(),
	}
}

// appendImportJSONLLine is an internal helper.
func appendImportJSONLLine(params importJSONLAppendLineParams) (importJSONLLineState, error) {
	state := params.current

	line := params.line
	if len(line) == 0 {
		return state, nil
	}

	state.lineNo++

	entry, parseErr := parseImportJSONLLine(line, state.lineNo)
	if parseErr != nil {
		return importJSONLLineState{},
			wrapImportJSONLProgressError(state.imported, params.bytesRead, state.lineNo, parseErr)
	}

	state.batch = append(state.batch, entry)
	if !shouldFlushImportJSONLBatch(state.imported, len(state.batch), params.batchSize, params.limit) {
		return state, nil
	}

	written, flushErr := flushImportJSONLBatchWithProgress(importJSONLFlushProgressParams{
		ctx:       params.ctx,
		store:     params.store,
		batch:     state.batch,
		imported:  state.imported,
		bytesRead: params.bytesRead,
		lineNo:    state.lineNo,
	})
	if flushErr != nil {
		return importJSONLLineState{}, flushErr
	}

	state.imported += written
	state.batch = state.batch[:0]

	return state, nil
}

// handleImportJSONLReadError is an internal helper.
func handleImportJSONLReadError(params importJSONLReadErrorParams) (bool, error) {
	switch {
	case errors.Is(params.err, errImportJSONLLineTooLong):
		progressErr := fmt.Errorf(
			"%w: importJSONL line %d exceeds maxLineBytes (%d bytes)",
			store.ErrValueParseFailed,
			params.lineNo+1,
			params.maxLineBytes,
		)

		return false, wrapImportJSONLProgressError(params.imported, params.bytesRead, params.lineNo+1, progressErr)
	case errors.Is(params.err, io.EOF):
		return false, nil
	case params.err != nil:
		progressErr := fmt.Errorf(
			"%w: importJSONL line %d: %w",
			store.ErrSnapshotReadFailed,
			params.lineNo+1,
			params.err,
		)

		return false, wrapImportJSONLProgressError(params.imported, params.bytesRead, params.lineNo+1, progressErr)
	default:
		return true, nil
	}
}

// flushImportJSONLBatchWithProgress supports resumable CSV import progress reporting.
func flushImportJSONLBatchWithProgress(params importJSONLFlushProgressParams) (int64, error) {
	batch := params.batch
	if len(batch) == 0 {
		return 0, nil
	}

	if err := params.ctx.Err(); err != nil {
		return 0, wrapImportJSONLProgressError(params.imported, params.bytesRead, params.lineNo, err)
	}

	written, err := flushImportJSONLBatch(params.store, batch)
	if err != nil {
		return 0, wrapImportJSONLProgressError(params.imported, params.bytesRead, params.lineNo, err)
	}

	return written, nil
}

// wrapImportJSONLProgressError supports resumable CSV import progress reporting.
func wrapImportJSONLProgressError(imported, bytesRead, lineNo int64, err error) error {
	if err == nil {
		return nil
	}

	return &importJSONLProgressError{
		imported:  imported,
		bytesRead: bytesRead,
		lineNo:    lineNo,
		err:       err,
	}
}

// readImportJSONLLineBounded supports CSV import and export helpers.
func readImportJSONLLineBounded(reader *bufio.Reader, maxLineBytes int) ([]byte, int64, error) {
	var (
		line     []byte
		consumed int64
	)

	for {
		chunk, err := reader.ReadSlice('\n')
		consumed += int64(len(chunk))

		if len(line)+len(chunk) > maxLineBytes {
			return nil, consumed, errImportJSONLLineTooLong
		}

		line = append(line, chunk...)

		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}

		if err != nil && !errors.Is(err, io.EOF) {
			return line, consumed, err
		}

		return line, consumed, err
	}
}

// resolveImportJSONLBatchSize supports CSV import and export helpers.
func resolveImportJSONLBatchSize(batchSize int64) int64 {
	if batchSize <= 0 {
		return importJSONLDefaultBatchSize
	}

	return batchSize
}

// shouldFlushImportJSONLBatch processes a batch of claims against the store.
func shouldFlushImportJSONLBatch(imported int64, batchLen int, batchSize int64, limit int64) bool {
	if batchSize > 0 && int64(batchLen) >= batchSize {
		return true
	}

	if limit > 0 && imported+int64(batchLen) >= limit {
		return true
	}

	return false
}

// flushImportJSONLBatch supports resumable CSV import progress reporting.
func flushImportJSONLBatch(s store.Store, batch []store.Entry) (int64, error) {
	if len(batch) == 0 {
		return 0, nil
	}

	written, err := s.SetMany(batch)
	if err != nil {
		return 0, err
	}

	if written != int64(len(batch)) {
		return 0, fmt.Errorf(
			"%w: store.SetMany returned unexpected written count: got %d, want %d",
			ErrUnexpectedStoreOutput,
			written,
			len(batch),
		)
	}

	return written, nil
}

// parseImportJSONLLine parses and validates a single options field.
func parseImportJSONLLine(line []byte, lineNo int64) (store.Entry, error) {
	trimmed, err := normalizeImportJSONLLine(line, lineNo)
	if err != nil {
		return store.Entry{}, err
	}

	envelope, err := parseImportJSONLEnvelope(trimmed, lineNo)
	if err != nil {
		return store.Entry{}, err
	}

	key, err := parseImportJSONLRecordKey(envelope, lineNo)
	if err != nil {
		return store.Entry{}, err
	}

	value, err := parseImportJSONLRecordValue(envelope, lineNo)
	if err != nil {
		return store.Entry{}, err
	}

	return store.Entry{Key: key, Value: value}, nil
}

// normalizeImportJSONLLine is an internal helper.
func normalizeImportJSONLLine(line []byte, lineNo int64) ([]byte, error) {
	line = bytes.TrimSuffix(line, []byte("\n"))
	line = bytes.TrimSuffix(line, []byte("\r"))

	trimmed := bytes.TrimSpace(line)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf(
			"%w: importJSONL line %d: blank line is not a valid record",
			store.ErrValueParseFailed,
			lineNo,
		)
	}

	if trimmed[0] != '{' {
		return nil, fmt.Errorf(
			"%w: importJSONL line %d: record must be a JSON object",
			store.ErrValueParseFailed,
			lineNo,
		)
	}

	return trimmed, nil
}

// parseImportJSONLEnvelope parses and validates a single options field.
func parseImportJSONLEnvelope(trimmed []byte, lineNo int64) (map[string]json.RawMessage, error) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &envelope); err != nil {
		return nil, fmt.Errorf(
			"%w: importJSONL line %d: invalid JSON record: %w",
			store.ErrValueParseFailed,
			lineNo,
			err,
		)
	}

	if envelope == nil {
		return nil, fmt.Errorf(
			"%w: importJSONL line %d: record must be a JSON object",
			store.ErrValueParseFailed,
			lineNo,
		)
	}

	return envelope, nil
}

// parseImportJSONLRecordKey parses and validates a single options field.
func parseImportJSONLRecordKey(envelope map[string]json.RawMessage, lineNo int64) (string, error) {
	keyRaw, ok := envelope["key"]
	if !ok {
		return "", fmt.Errorf(
			"%w: importJSONL line %d: record.key is required",
			store.ErrValueParseFailed,
			lineNo,
		)
	}

	var key string
	if err := json.Unmarshal(keyRaw, &key); err != nil {
		return "", fmt.Errorf(
			"%w: importJSONL line %d: record.key must be a string: %w",
			store.ErrValueParseFailed,
			lineNo,
			err,
		)
	}

	if key == "" {
		return "", fmt.Errorf(
			"%w: importJSONL line %d: record.key must be a non-empty string",
			store.ErrValueParseFailed,
			lineNo,
		)
	}

	return key, nil
}

// parseImportJSONLRecordValue parses and validates a single options field.
func parseImportJSONLRecordValue(envelope map[string]json.RawMessage, lineNo int64) (any, error) {
	valueRaw, ok := envelope["value"]
	if !ok {
		return nil, fmt.Errorf(
			"%w: importJSONL line %d: record.value is required",
			store.ErrValueParseFailed,
			lineNo,
		)
	}

	var value any
	if err := json.Unmarshal(valueRaw, &value); err != nil {
		return nil, fmt.Errorf(
			"%w: importJSONL line %d: record.value is invalid JSON: %w",
			store.ErrValueParseFailed,
			lineNo,
			err,
		)
	}

	return value, nil
}

//nolint:forbidigo // file I/O helpers require concrete *os.File handles.
func writeJSONLFile(
	ctx context.Context,
	file *os.File,
	s store.Store,
	opts exportJSONLOptions,
) (int64, error) {
	if err := ctx.Err(); err != nil {
		_ = file.Close()
		return 0, err
	}

	writer := bufio.NewWriter(file)
	encoder := json.NewEncoder(writer)

	exported, err := writeJSONLRecords(ctx, s, opts, encoder)
	if err != nil {
		_ = file.Close()
		return 0, err
	}

	if err := writer.Flush(); err != nil {
		_ = file.Close()
		return 0, fmt.Errorf("%w: %w", store.ErrSnapshotExportFailed, err)
	}

	// Ensure buffered data is committed to stable storage before closing/renaming.
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return 0, fmt.Errorf("%w: %w", store.ErrSnapshotExportFailed, err)
	}

	if err := file.Close(); err != nil {
		return 0, fmt.Errorf("%w: %w", store.ErrSnapshotExportFailed, err)
	}

	return exported, nil
}

// writeJSONLRecords is an internal helper.
func writeJSONLRecords(
	ctx context.Context,
	s store.Store,
	opts exportJSONLOptions,
	encoder *json.Encoder,
) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	var (
		exported int64
		cursor   string
	)

	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		pageLimit := resolveExportJSONLPageLimit(opts.Limit, exported)
		if pageLimit <= 0 {
			break
		}

		page, err := s.Scan(opts.Prefix, cursor, pageLimit)
		if err != nil {
			return 0, err
		}

		if page == nil {
			return 0, unexpectedStoreOutput("store.Scan")
		}

		for _, entry := range page.Entries {
			if err := ctx.Err(); err != nil {
				return 0, err
			}

			record := exportJSONLRecord{
				Key:   entry.Key,
				Value: entry.Value,
			}

			if err := encoder.Encode(record); err != nil {
				return 0, fmt.Errorf("%w: %w", store.ErrSnapshotExportFailed, err)
			}

			exported++
		}

		if page.NextKey == "" {
			break
		}

		cursor = page.NextKey
	}

	return exported, nil
}

// resolveExportJSONLPageLimit supports CSV import and export helpers.
func resolveExportJSONLPageLimit(limit, exported int64) int64 {
	pageLimit := exportJSONLPageSize
	if limit <= 0 {
		return pageLimit
	}

	remaining := limit - exported
	if remaining <= 0 {
		return 0
	}

	if remaining < pageLimit {
		return remaining
	}

	return pageLimit
}
