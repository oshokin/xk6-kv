package kv

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/grafana/sobek"

	"github.com/oshokin/xk6-kv/internal/fileutil"
	"github.com/oshokin/xk6-kv/kv/store"
)

const (
	// importCSVDefaultBatchSize is the import c s v default batch size const.
	importCSVDefaultBatchSize int64 = 1000
	// exportCSVPageSize is the export c s v page size const.
	exportCSVPageSize int64 = 1000
)

type (
	// importCSVResult is the Go-side result returned by the corresponding KV operation.
	importCSVResult struct {
		// Imported is the number of rows successfully written to the store.
		Imported int64 `js:"imported"`
		// FileName is the path of the CSV or JSONL file.
		FileName string `js:"fileName"`
		// BytesRead is the number of payload bytes read from or written to disk.
		BytesRead int64 `js:"bytesRead"`
	}

	// exportCSVResult is the Go-side result returned by the corresponding KV operation.
	exportCSVResult struct {
		// Exported is the number of rows successfully written to the store.
		Exported int64 `js:"exported"`
		// FileName is the path of the CSV or JSONL file.
		FileName string `js:"fileName"`
		// BytesWritten is the number of payload bytes read from or written to disk.
		BytesWritten int64 `js:"bytesWritten"`
	}

	// validateCSVResult is the Go-side result returned by the corresponding KV operation.
	validateCSVResult struct {
		// Valid reports whether validation finished without errors.
		Valid bool `js:"valid"`
		// Rows is the number of CSV rows validated.
		Rows int64 `js:"rows"`
		// BytesRead is the number of payload bytes read from or written to disk.
		BytesRead int64 `js:"bytesRead"`
		// CheckedAll is true when the validator scanned the entire file.
		CheckedAll bool `js:"checkedAll"`
		// FirstError describes the first validation failure, if any.
		FirstError *validateCSVFirstError `js:"firstError,omitempty"`
	}

	// validateCSVFirstError carries error details for the corresponding operation.
	validateCSVFirstError struct {
		// Row is the one-based CSV row number where the error occurred.
		Row int64 `js:"row"`
		// Name is the machine-readable error name.
		Name string `js:"name"`
		// Message is a human-readable error description.
		Message string `js:"message"`
	}

	// importCSVProgressError carries error details for the corresponding operation.
	importCSVProgressError struct {
		// imported is the number of rows successfully written to the store.
		imported int64
		// bytesRead is the number of payload bytes read from or written to disk.
		bytesRead int64
		// rowNo is the current one-based CSV row number.
		rowNo int64
		// err is the underlying error that stopped processing.
		err error
	}

	// importCSVRowsLoopState tracks mutable state for a multi-step internal loop.
	importCSVRowsLoopState struct {
		// imported is the number of rows successfully written to the store.
		imported int64
		// rowNo is the current one-based CSV row number.
		rowNo int64
		// batch is the pending entry batch awaiting flush to the store.
		batch []store.Entry
	}

	// importCSVRowsLoopParams groups parameters passed to an internal helper.
	importCSVRowsLoopParams struct {
		// ctx carries cancellation for the import or export operation.
		ctx context.Context
		// store is the backing KV store implementation.
		store store.Store
		// reader is the source reader for CSV bytes.
		reader *csv.Reader
		// bytesRead is the number of payload bytes read from or written to disk.
		bytesRead func() int64
		// options holds the options value.
		options importCSVOptions
		// headers holds the parsed CSV header column names.
		headers []string
		// keyIndex is the index of the key column within headers.
		keyIndex int
		// batchSize is the number of rows written per store batch.
		batchSize int64
		// state holds the state value.
		state importCSVRowsLoopState
	}

	// importCSVFlushProgressParams groups parameters passed to an internal helper.
	importCSVFlushProgressParams struct {
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
		// rowNo is the current one-based CSV row number.
		rowNo int64
	}
)

// Error implements importCSVProgressError receiver behavior.
func (e *importCSVProgressError) Error() string {
	commitStatus := "no rows were committed"
	if e.imported > 0 {
		commitStatus = "previous batches may already be committed"
	}

	return fmt.Sprintf(
		"importCSV failed after %d rows and %d bytes; %s: row %d: %v",
		e.imported,
		e.bytesRead,
		commitStatus,
		e.rowNo,
		e.err,
	)
}

// Unwrap implements importCSVProgressError receiver behavior.
func (e *importCSVProgressError) Unwrap() error {
	return e.err
}

// ImportCSV imports key/value rows from a CSV file.
func (k *KV) ImportCSV(options sobek.Value) *sobek.Promise {
	importOptions, err := importImportCSVOptions(k.vu.Runtime(), options)
	if err != nil {
		return k.rejectedPromiseObserved(opImportCSV, err)
	}

	ctx := k.vu.Context()

	return k.runAsyncWithStoreObserved(
		opImportCSV,
		func(s store.Store) (any, error) {
			return importCSV(ctx, s, importOptions)
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

// ExportCSV exports tabular object data to a CSV file.
//
// Contract:
//   - each exported value must decode to a plain JSON object row
//     (map[string]any after deserialization);
//   - only top-level scalar fields are supported for selected columns;
//   - missing fields are emitted as empty cells;
//   - nested objects/arrays and scalar store values are rejected.
//
// Use ExportJSONL for non-tabular payloads (for example scalar/string values
// or nested JSON structures).
func (k *KV) ExportCSV(options sobek.Value) *sobek.Promise {
	exportOptions, err := importExportCSVOptions(k.vu.Runtime(), options)
	if err != nil {
		return k.rejectedPromiseObserved(opExportCSV, err)
	}

	ctx := k.vu.Context()

	return k.runAsyncWithStoreObserved(
		opExportCSV,
		func(s store.Store) (any, error) {
			return exportCSV(ctx, s, exportOptions)
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

// ValidateCSV validates a CSV file without writing to the store.
// It still requires an open KV handle because it runs through the shared
// async lifecycle/metrics pipeline used by all KV instance methods.
// When keyColumn is omitted, it runs syntax/header checks only.
// When keyColumn is provided, it validates importCSV-compatible key extraction too.
// CSV row width is lenient: missing cells are treated as empty fields, and
// extra cells are accepted (same normalization contract as importCSV).
// With limit <= 0 (or omitted), validation scans all data rows.
// With limit > 0, validation applies to the inspected prefix of data rows only.
// checkedAll=true means validation reached EOF; checkedAll=false means it stopped due to limit.
// Contract: valid=true implies whole-file validity only when checkedAll=true.
func (k *KV) ValidateCSV(options sobek.Value) *sobek.Promise {
	validateOptions, err := importValidateCSVOptions(k.vu.Runtime(), options)
	if err != nil {
		return k.rejectedPromiseObserved(opValidateCSV, err)
	}

	ctx := k.vu.Context()

	return k.runAsyncWithStoreObserved(
		opValidateCSV,
		func(_ store.Store) (any, error) {
			return validateCSV(ctx, validateOptions)
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

// importCSV is an internal helper.
func importCSV(ctx context.Context, s store.Store, opts importCSVOptions) (*importCSVResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	//nolint:forbidigo // file I/O is required for CSV import.
	file, err := os.Open(opts.FileName)
	if err != nil {
		return nil, classifyJSONLFileOpenError(opts.FileName, err)
	}
	defer func() {
		_ = file.Close()
	}()

	imported, bytesRead, err := readCSVRows(ctx, s, file, opts)
	if err != nil {
		return nil, err
	}

	return &importCSVResult{
		Imported:  imported,
		FileName:  opts.FileName,
		BytesRead: bytesRead,
	}, nil
}

//nolint:dupl // Export finalization mirrors exportJSONL intentionally.
func exportCSV(ctx context.Context, s store.Store, opts exportCSVOptions) (*exportCSVResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	targetDir := filepath.Dir(opts.FileName)
	targetBase := filepath.Base(opts.FileName)

	if err := os.MkdirAll(targetDir, 0o750); err != nil {
		return nil, fmt.Errorf("%w: exportCSV create output directory: %w", store.ErrSnapshotDirectoryFailed, err)
	}

	//nolint:forbidigo // file I/O is required for CSV export.
	tempFile, err := os.CreateTemp(targetDir, targetBase+".*.tmp")
	if err != nil {
		return nil, fmt.Errorf("%w: exportCSV create temporary file: %w", store.ErrSnapshotTempFileFailed, err)
	}

	tempName := tempFile.Name()
	cleanupTemp := true

	defer func() {
		if cleanupTemp {
			//nolint:forbidigo // best-effort cleanup for failed export.
			_ = os.Remove(tempName)
		}
	}()

	exported, err := writeCSVFile(ctx, tempFile, s, opts)
	if err != nil {
		return nil, err
	}

	// ReplaceFile centralizes best-effort "temp then replace target" semantics used by export paths.
	if err := fileutil.ReplaceFile(tempName, opts.FileName); err != nil {
		return nil, fmt.Errorf("%w: exportCSV replace target file: %w", store.ErrSnapshotFinalizeFailed, err)
	}

	// Temp file no longer exists after successful rename.
	cleanupTemp = false

	if err := fileutil.SyncParentDir(opts.FileName); err != nil {
		return nil, fmt.Errorf("%w: exportCSV sync parent directory: %w", store.ErrSnapshotFinalizeFailed, err)
	}

	info, err := os.Stat(opts.FileName)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", store.ErrSnapshotExportFailed, err)
	}

	return &exportCSVResult{
		Exported:     exported,
		FileName:     opts.FileName,
		BytesWritten: info.Size(),
	}, nil
}

// validateCSV validates user-supplied input.
func validateCSV(ctx context.Context, opts validateCSVOptions) (*validateCSVResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	//nolint:forbidigo // file I/O is required for CSV validation.
	file, err := os.Open(opts.FileName)
	if err != nil {
		return nil, classifyJSONLFileOpenError(opts.FileName, err)
	}
	defer func() {
		_ = file.Close()
	}()

	rows, bytesRead, checkedAll, firstErr, validateErr := validateCSVRows(ctx, file, opts)
	if validateErr != nil {
		return nil, validateErr
	}

	return &validateCSVResult{
		Valid:      firstErr == nil,
		Rows:       rows,
		BytesRead:  bytesRead,
		CheckedAll: checkedAll,
		FirstError: firstErr,
	}, nil
}

//nolint:forbidigo // file I/O helpers require concrete *os.File handles.
func writeCSVFile(
	ctx context.Context,
	file *os.File,
	s store.Store,
	opts exportCSVOptions,
) (int64, error) {
	if err := ctx.Err(); err != nil {
		_ = file.Close()
		return 0, err
	}

	writer := csv.NewWriter(file)
	writer.Comma = opts.Delimiter

	exported, err := writeCSVRows(ctx, writer, s, opts)
	if err != nil {
		_ = file.Close()
		return 0, err
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

//nolint:gocognit,funlen // CSV row validation/export flow keeps explicit branching for stable error paths.
func writeCSVRows(
	ctx context.Context,
	writer *csv.Writer,
	s store.Store,
	opts exportCSVOptions,
) (int64, error) {
	header := make([]string, 0, len(opts.Columns)+1)
	if opts.IncludeKey {
		header = append(header, "key")
	}

	header = append(header, opts.Columns...)

	if err := writer.Write(header); err != nil {
		return 0, fmt.Errorf("%w: %w", store.ErrSnapshotExportFailed, err)
	}

	var (
		exported int64
		afterKey string
	)

	for {
		if err := ctx.Err(); err != nil {
			return exported, err
		}

		pageLimit := resolveExportCSVPageLimit(opts.Limit, exported)
		if pageLimit <= 0 {
			break
		}

		page, err := s.Scan(opts.Prefix, afterKey, pageLimit)
		if err != nil {
			return exported, fmt.Errorf("%w: %w", store.ErrSnapshotExportFailed, err)
		}

		if page == nil {
			return exported, unexpectedStoreOutput("store.Scan")
		}

		if len(page.Entries) == 0 {
			break
		}

		for _, entry := range page.Entries {
			if err := ctx.Err(); err != nil {
				return exported, err
			}

			record, recordErr := entryValueToCSVRecord(entry, opts)
			if recordErr != nil {
				return exported, recordErr
			}

			if err := writer.Write(record); err != nil {
				return exported, fmt.Errorf("%w: %w", store.ErrSnapshotExportFailed, err)
			}

			exported++
			if opts.Limit > 0 && exported >= opts.Limit {
				break
			}
		}

		if opts.Limit > 0 && exported >= opts.Limit {
			break
		}

		if page.NextKey == "" {
			break
		}

		afterKey = page.NextKey
	}

	writer.Flush()

	if err := writer.Error(); err != nil {
		return exported, fmt.Errorf("%w: %w", store.ErrSnapshotExportFailed, err)
	}

	return exported, nil
}

// resolveExportCSVPageLimit supports CSV import and export helpers.
func resolveExportCSVPageLimit(limit, exported int64) int64 {
	pageLimit := exportCSVPageSize
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

// entryValueToCSVRecord is an internal helper.
func entryValueToCSVRecord(entry store.Entry, opts exportCSVOptions) ([]string, error) {
	rowObject, ok := entry.Value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf(
			"%w: exportCSV key %q value must be a JSON object (flat table row); use exportJSONL for scalar values",
			store.ErrValueParseFailed,
			entry.Key,
		)
	}

	record := make([]string, 0, len(opts.Columns)+1)
	if opts.IncludeKey {
		record = append(record, entry.Key)
	}

	for _, column := range opts.Columns {
		value, exists := rowObject[column]
		if !exists || value == nil {
			record = append(record, "")
			continue
		}

		scalar, err := csvScalarToString(value)
		if err != nil {
			return nil, fmt.Errorf(
				"exportCSV key %q column %q must be a top-level scalar value: %w",
				entry.Key,
				column,
				err,
			)
		}

		record = append(record, scalar)
	}

	return record, nil
}

// csvScalarToString is an internal helper.
func csvScalarToString(value any) (string, error) {
	switch v := value.(type) {
	case nil:
		return "", nil
	case string:
		return v, nil
	case bool:
		return strconv.FormatBool(v), nil
	case int:
		return strconv.FormatInt(int64(v), 10), nil
	case int8:
		return strconv.FormatInt(int64(v), 10), nil
	case int16:
		return strconv.FormatInt(int64(v), 10), nil
	case int32:
		return strconv.FormatInt(int64(v), 10), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case uint:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case json.Number:
		return v.String(), nil
	default:
		return "", fmt.Errorf(
			"%w: CSV export supports only scalar string, number, boolean, or null fields",
			store.ErrValueParseFailed,
		)
	}
}

// readCSVRows supports CSV import and export helpers.
func readCSVRows(
	ctx context.Context,
	s store.Store,
	input io.Reader,
	opts importCSVOptions,
) (int64, int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}

	reader, counted := newImportCSVReader(input, opts.Delimiter)
	batchSize := resolveImportCSVBatchSize(opts.BatchSize)

	headers, keyIndex, rowNo, err := resolveImportCSVSchema(reader, opts)
	if errors.Is(err, io.EOF) {
		return 0, counted.BytesRead(), nil
	}

	if err != nil {
		return 0, counted.BytesRead(), wrapImportCSVProgressError(0, counted.BytesRead(), max(rowNo, 1), err)
	}

	state, err := readCSVRowsLoop(importCSVRowsLoopParams{
		ctx:       ctx,
		store:     s,
		reader:    reader,
		bytesRead: counted.BytesRead,
		options:   opts,
		headers:   headers,
		keyIndex:  keyIndex,
		batchSize: batchSize,
		state: importCSVRowsLoopState{
			rowNo: rowNo,
			batch: make([]store.Entry, 0),
		},
	})
	if err != nil {
		return state.imported, counted.BytesRead(), err
	}

	written, err := flushImportCSVBatchWithProgress(importCSVFlushProgressParams{
		ctx:       ctx,
		store:     s,
		batch:     state.batch,
		imported:  state.imported,
		bytesRead: counted.BytesRead(),
		rowNo:     max(state.rowNo, 1),
	})
	if err != nil {
		return state.imported, counted.BytesRead(), err
	}

	state.imported += written

	return state.imported, counted.BytesRead(), nil
}

// validateCSVRows validates user-supplied input.
func validateCSVRows(
	ctx context.Context,
	input io.Reader,
	opts validateCSVOptions,
) (int64, int64, bool, *validateCSVFirstError, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, false, nil, err
	}

	reader, counted := newImportCSVReader(input, opts.Delimiter)

	headers, keyIndex, rowNo, err := resolveValidateCSVSchema(reader, opts)
	if errors.Is(err, io.EOF) {
		return 0, counted.BytesRead(), true, nil, nil
	}

	if err != nil {
		if errors.Is(err, store.ErrSnapshotReadFailed) {
			return 0, counted.BytesRead(), false, nil, err
		}

		return 0, counted.BytesRead(), false, newValidateCSVFirstError(max(rowNo, 1), err), nil
	}

	var rows int64

	checkedAll := false

	for opts.Limit <= 0 || rows < opts.Limit {
		if err := ctx.Err(); err != nil {
			return rows, counted.BytesRead(), false, nil, err
		}

		record, nextRowNo, readErr := readNextCSVRecord(reader, rowNo)
		if errors.Is(readErr, io.EOF) {
			checkedAll = true
			break
		}

		if readErr != nil {
			var parseErr *csv.ParseError
			if errors.As(readErr, &parseErr) {
				contentErr := fmt.Errorf("%w: validateCSV row %d: %w", store.ErrValueParseFailed, nextRowNo, readErr)
				return rows, counted.BytesRead(), false, newValidateCSVFirstError(nextRowNo, contentErr), nil
			}

			return rows, counted.BytesRead(), false, nil, fmt.Errorf(
				"%w: validateCSV row %d: %w",
				store.ErrSnapshotReadFailed,
				nextRowNo,
				readErr,
			)
		}

		rowNo = nextRowNo

		if opts.KeyColumnSet {
			if _, parseErr := parseImportCSVRecord(record, rowNo, headers, keyIndex, opts.HasHeader); parseErr != nil {
				return rows, counted.BytesRead(), false, newValidateCSVFirstError(rowNo, parseErr), nil
			}
		}

		rows++
	}

	return rows, counted.BytesRead(), checkedAll, nil, nil
}

// resolveValidateCSVSchema supports CSV import and export helpers.
func resolveValidateCSVSchema(
	reader *csv.Reader,
	opts validateCSVOptions,
) ([]string, int, int64, error) {
	if !opts.HasHeader {
		if !opts.KeyColumnSet {
			return nil, -1, 0, nil
		}

		keyIndex, err := strconv.Atoi(opts.KeyColumn)
		if err != nil || keyIndex < 0 {
			return nil, 0, 1, fmt.Errorf(
				"%w: validateCSV keyColumn must be a non-negative integer when hasHeader=false",
				store.ErrKVOptionsInvalid,
			)
		}

		return nil, keyIndex, 0, nil
	}

	header, err := reader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, 0, 0, io.EOF
		}

		var parseErr *csv.ParseError
		if errors.As(err, &parseErr) {
			return nil, 0, 1, fmt.Errorf("%w: validateCSV header: %w", store.ErrValueParseFailed, err)
		}

		return nil, 0, 1, fmt.Errorf("%w: validateCSV header: %w", store.ErrSnapshotReadFailed, err)
	}

	headers := append([]string(nil), header...)
	for i, column := range headers {
		if strings.TrimSpace(column) == "" {
			return nil, 0, 1, fmt.Errorf(
				"%w: validateCSV header column %d is empty",
				store.ErrValueParseFailed,
				i,
			)
		}
	}

	if duplicate, hasDuplicate := findDuplicateCSVHeader(headers); hasDuplicate {
		return nil, 0, 1, fmt.Errorf(
			"%w: validateCSV header contains duplicate column %q",
			store.ErrValueParseFailed,
			duplicate,
		)
	}

	if !opts.KeyColumnSet {
		return headers, -1, 1, nil
	}

	keyIndex := indexOfString(headers, opts.KeyColumn)
	if keyIndex < 0 {
		return nil, 0, 1, fmt.Errorf(
			"%w: validateCSV keyColumn %q not found in header",
			store.ErrValueParseFailed,
			opts.KeyColumn,
		)
	}

	return headers, keyIndex, 1, nil
}

// newValidateCSVFirstError constructs a new ValidateCSVFirstError value.
func newValidateCSVFirstError(rowNo int64, err error) *validateCSVFirstError {
	classified := classifyError(err)

	name := string(UnknownError)
	if classified != nil {
		name = string(classified.Name)
	}

	return &validateCSVFirstError{
		Row:     rowNo,
		Name:    name,
		Message: err.Error(),
	}
}

// readCSVRowsLoop supports CSV import and export helpers.
func readCSVRowsLoop(params importCSVRowsLoopParams) (importCSVRowsLoopState, error) {
	state := params.state
	if state.batch == nil {
		state.batch = make([]store.Entry, 0)
	}

	for params.options.Limit <= 0 || state.imported+int64(len(state.batch)) < params.options.Limit {
		if err := csvProgressContextError(
			params.ctx,
			state.imported,
			params.bytesRead(),
			max(state.rowNo, 1),
		); err != nil {
			return state, err
		}

		record, nextRowNo, err := readNextCSVRecord(params.reader, state.rowNo)
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return state, wrapImportCSVReadProgressError(nextRowNo, err, state.imported, params.bytesRead())
		}

		state.rowNo = nextRowNo

		entry, parseErr := parseImportCSVRecord(
			record,
			state.rowNo,
			params.headers,
			params.keyIndex,
			params.options.HasHeader,
		)
		if parseErr != nil {
			return state, wrapImportCSVProgressError(state.imported, params.bytesRead(), state.rowNo, parseErr)
		}

		state.batch = append(state.batch, entry)

		if !shouldFlushImportJSONLBatch(state.imported, len(state.batch), params.batchSize, params.options.Limit) {
			continue
		}

		written, flushErr := flushImportCSVBatchWithProgress(importCSVFlushProgressParams{
			ctx:       params.ctx,
			store:     params.store,
			batch:     state.batch,
			imported:  state.imported,
			bytesRead: params.bytesRead(),
			rowNo:     state.rowNo,
		})
		if flushErr != nil {
			return state, flushErr
		}

		state.imported += written
		state.batch = state.batch[:0]
	}

	return state, nil
}

// wrapImportCSVReadProgressError supports resumable CSV import progress reporting.
func wrapImportCSVReadProgressError(nextRowNo int64, err error, imported int64, bytesRead int64) error {
	progressErr := fmt.Errorf("%w: importCSV row %d: %w", store.ErrSnapshotReadFailed, nextRowNo, err)

	return wrapImportCSVProgressError(imported, bytesRead, nextRowNo, progressErr)
}

// csvProgressContextError is an internal helper.
func csvProgressContextError(ctx context.Context, imported, bytesRead, rowNo int64) error {
	if err := ctx.Err(); err != nil {
		return wrapImportCSVProgressError(imported, bytesRead, rowNo, err)
	}

	return nil
}

// flushImportCSVBatchWithProgress supports resumable CSV import progress reporting.
func flushImportCSVBatchWithProgress(params importCSVFlushProgressParams) (int64, error) {
	batch := params.batch
	if len(batch) == 0 {
		return 0, nil
	}

	if err := csvProgressContextError(params.ctx, params.imported, params.bytesRead, params.rowNo); err != nil {
		return 0, err
	}

	written, err := flushImportJSONLBatch(params.store, batch)
	if err != nil {
		return 0, wrapImportCSVProgressError(params.imported, params.bytesRead, params.rowNo, err)
	}

	return written, nil
}

// parseImportCSVRecord parses and validates a single options field.
func parseImportCSVRecord(
	record []string,
	rowNo int64,
	headers []string,
	keyIndex int,
	hasHeader bool,
) (store.Entry, error) {
	if keyIndex < 0 || keyIndex >= len(record) {
		return store.Entry{}, fmt.Errorf(
			"%w: importCSV row %d: key column index %d out of range",
			store.ErrValueParseFailed,
			rowNo,
			keyIndex,
		)
	}

	key := strings.TrimSpace(record[keyIndex])
	if key == "" {
		return store.Entry{}, fmt.Errorf(
			"%w: importCSV row %d: key is empty",
			store.ErrValueParseFailed,
			rowNo,
		)
	}

	value := make(map[string]any, len(record))

	if hasHeader {
		for i, header := range headers {
			cell := ""
			if i < len(record) {
				cell = record[i]
			}

			value[header] = cell
		}

		for i := len(headers); i < len(record); i++ {
			value[fmt.Sprintf("column_%d", i)] = record[i]
		}
	} else {
		for i, cell := range record {
			value[fmt.Sprintf("column_%d", i)] = cell
		}
	}

	return store.Entry{
		Key:   key,
		Value: value,
	}, nil
}

// newImportCSVReader constructs a new ImportCSVReader value.
func newImportCSVReader(input io.Reader, delimiter rune) (*csv.Reader, *countingReader) {
	counted := &countingReader{reader: input}
	reader := csv.NewReader(counted)

	if delimiter == 0 {
		delimiter = ','
	}

	reader.Comma = delimiter
	reader.ReuseRecord = false
	// Keep row-width handling lenient; parseImportCSVRecord normalizes
	// missing fields to empty strings and preserves extra fields as column_N.
	reader.FieldsPerRecord = -1

	return reader, counted
}

// resolveImportCSVSchema supports CSV import and export helpers.
func resolveImportCSVSchema(reader *csv.Reader, opts importCSVOptions) ([]string, int, int64, error) {
	if !opts.HasHeader {
		keyIndex, err := strconv.Atoi(opts.KeyColumn)
		if err != nil || keyIndex < 0 {
			return nil, 0, 1, fmt.Errorf(
				"%w: importCSV keyColumn must be a non-negative integer when hasHeader=false",
				store.ErrKVOptionsInvalid,
			)
		}

		return nil, keyIndex, 0, nil
	}

	header, err := reader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, 0, 0, io.EOF
		}

		return nil, 0, 1, fmt.Errorf("%w: importCSV header: %w", store.ErrSnapshotReadFailed, err)
	}

	headers := append([]string(nil), header...)
	for i, column := range headers {
		if strings.TrimSpace(column) == "" {
			return nil, 0, 1, fmt.Errorf(
				"%w: importCSV header column %d is empty",
				store.ErrValueParseFailed,
				i,
			)
		}
	}

	if duplicate, hasDuplicate := findDuplicateCSVHeader(headers); hasDuplicate {
		return nil, 0, 1, fmt.Errorf(
			"%w: importCSV header contains duplicate column %q",
			store.ErrValueParseFailed,
			duplicate,
		)
	}

	keyIndex := indexOfString(headers, opts.KeyColumn)
	if keyIndex < 0 {
		return nil, 0, 1, fmt.Errorf(
			"%w: importCSV keyColumn %q not found in header",
			store.ErrValueParseFailed,
			opts.KeyColumn,
		)
	}

	return headers, keyIndex, 1, nil
}

// readNextCSVRecord supports CSV import and export helpers.
func readNextCSVRecord(reader *csv.Reader, currentRowNo int64) ([]string, int64, error) {
	record, err := reader.Read()
	if err != nil {
		return nil, currentRowNo + 1, err
	}

	return record, currentRowNo + 1, nil
}

// resolveImportCSVBatchSize supports CSV import and export helpers.
func resolveImportCSVBatchSize(batchSize int64) int64 {
	if batchSize <= 0 {
		return importCSVDefaultBatchSize
	}

	return batchSize
}

// wrapImportCSVProgressError supports resumable CSV import progress reporting.
func wrapImportCSVProgressError(imported, bytesRead, rowNo int64, err error) error {
	if err == nil {
		return nil
	}

	return &importCSVProgressError{
		imported:  imported,
		bytesRead: bytesRead,
		rowNo:     rowNo,
		err:       err,
	}
}

// indexOfString is an internal helper.
func indexOfString(items []string, target string) int {
	for i, item := range items {
		if item == target {
			return i
		}
	}

	return -1
}

// findDuplicateCSVHeader is an internal helper.
func findDuplicateCSVHeader(headers []string) (string, bool) {
	seen := make(map[string]struct{}, len(headers))
	for _, column := range headers {
		if _, ok := seen[column]; ok {
			return column, true
		}

		seen[column] = struct{}{}
	}

	return "", false
}

// countingReader wraps an io.Reader and counts bytes read.
type countingReader struct {
	// reader is the source reader for CSV bytes.
	reader io.Reader
	// read holds the read value.
	read int64
}

// Read implements countingReader receiver behavior.
func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.read += int64(n)

	return n, err
}

// BytesRead implements countingReader receiver behavior.
func (r *countingReader) BytesRead() int64 {
	return r.read
}
