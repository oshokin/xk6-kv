package kv

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/grafana/sobek"

	"github.com/oshokin/xk6-kv/kv/store"
)

const importCSVDefaultBatchSize int64 = 1000

type (
	importCSVResult struct {
		Imported  int64  `js:"imported"`
		FileName  string `js:"fileName"`
		BytesRead int64  `js:"bytesRead"`
	}

	importCSVProgressError struct {
		imported  int64
		bytesRead int64
		rowNo     int64
		err       error
	}
)

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

func (e *importCSVProgressError) Unwrap() error {
	return e.err
}

// ImportCSV imports key/value rows from a CSV file.
func (k *KV) ImportCSV(options sobek.Value) *sobek.Promise {
	importOptions, err := importImportCSVOptions(k.vu.Runtime(), options)
	if err != nil {
		return k.rejectedPromiseObserved(opImportCSV, err)
	}

	return k.runAsyncWithStoreObserved(
		opImportCSV,
		func(s store.Store) (any, error) {
			return importCSV(s, importOptions)
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

func importCSV(s store.Store, opts importCSVOptions) (*importCSVResult, error) {
	//nolint:forbidigo // file I/O is required for CSV import.
	file, err := os.Open(opts.FileName)
	if err != nil {
		return nil, classifyJSONLFileOpenError(opts.FileName, err)
	}
	defer func() {
		_ = file.Close()
	}()

	imported, bytesRead, err := readCSVRows(s, file, opts)
	if err != nil {
		return nil, err
	}

	return &importCSVResult{
		Imported:  imported,
		FileName:  opts.FileName,
		BytesRead: bytesRead,
	}, nil
}

func readCSVRows(s store.Store, input io.Reader, opts importCSVOptions) (int64, int64, error) {
	reader, counted := newImportCSVReader(input, opts.Delimiter)
	batchSize := resolveImportCSVBatchSize(opts.BatchSize)

	var (
		imported int64
		rowNo    int64
		batch    = make([]store.Entry, 0)
	)

	headers, keyIndex, rowNo, err := resolveImportCSVSchema(reader, opts)
	if errors.Is(err, io.EOF) {
		return 0, counted.BytesRead(), nil
	}

	if err != nil {
		return imported, counted.BytesRead(), wrapImportCSVProgressError(
			imported,
			counted.BytesRead(),
			max(rowNo, 1),
			err,
		)
	}

	for opts.Limit <= 0 || imported+int64(len(batch)) < opts.Limit {
		record, nextRowNo, err := readNextCSVRecord(reader, rowNo)
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			progressErr := fmt.Errorf("%w: importCSV row %d: %w", store.ErrSnapshotReadFailed, nextRowNo, err)

			return imported, counted.BytesRead(), wrapImportCSVProgressError(
				imported,
				counted.BytesRead(),
				nextRowNo,
				progressErr,
			)
		}

		rowNo = nextRowNo

		entry, parseErr := parseImportCSVRecord(record, rowNo, headers, keyIndex, opts.HasHeader)
		if parseErr != nil {
			return imported, counted.BytesRead(), wrapImportCSVProgressError(
				imported,
				counted.BytesRead(),
				rowNo,
				parseErr,
			)
		}

		batch = append(batch, entry)

		if shouldFlushImportJSONLBatch(imported, len(batch), batchSize, opts.Limit) {
			written, flushErr := flushImportJSONLBatch(s, batch)
			if flushErr != nil {
				return imported, counted.BytesRead(), wrapImportCSVProgressError(
					imported,
					counted.BytesRead(),
					rowNo,
					flushErr,
				)
			}

			imported += written
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		written, err := flushImportJSONLBatch(s, batch)
		if err != nil {
			return imported, counted.BytesRead(), wrapImportCSVProgressError(imported, counted.BytesRead(), rowNo, err)
		}

		imported += written
	}

	return imported, counted.BytesRead(), nil
}

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

func newImportCSVReader(input io.Reader, delimiter rune) (*csv.Reader, *countingReader) {
	counted := &countingReader{reader: input}
	reader := csv.NewReader(counted)

	if delimiter == 0 {
		delimiter = ','
	}

	reader.Comma = delimiter
	reader.ReuseRecord = false

	return reader, counted
}

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

func readNextCSVRecord(reader *csv.Reader, currentRowNo int64) ([]string, int64, error) {
	record, err := reader.Read()
	if err != nil {
		return nil, currentRowNo + 1, err
	}

	return record, currentRowNo + 1, nil
}

func resolveImportCSVBatchSize(batchSize int64) int64 {
	if batchSize <= 0 {
		return importCSVDefaultBatchSize
	}

	return batchSize
}

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

func indexOfString(items []string, target string) int {
	for i, item := range items {
		if item == target {
			return i
		}
	}

	return -1
}

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

type countingReader struct {
	reader io.Reader
	read   int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.read += int64(n)

	return n, err
}

func (r *countingReader) BytesRead() int64 {
	return r.read
}
