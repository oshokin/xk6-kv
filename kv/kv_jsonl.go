package kv

import (
	"bufio"
	"bytes"
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
	exportJSONLPageSize            int64 = 1000
	importJSONLDefaultBatchSize    int64 = 1000
	importJSONLDefaultMaxLineBytes       = 64 << 20 // 64 MiB
)

var errImportJSONLLineTooLong = errors.New("importJSONL line exceeds maxLineBytes")

type (
	exportJSONLResult struct {
		Exported     int64  `js:"exported"`
		FileName     string `js:"fileName"`
		BytesWritten int64  `js:"bytesWritten"`
	}

	importJSONLResult struct {
		Imported  int64  `js:"imported"`
		FileName  string `js:"fileName"`
		BytesRead int64  `js:"bytesRead"`
	}

	exportJSONLRecord struct {
		Key   string `json:"key"`
		Value any    `json:"value"`
	}
)

// ExportJSONL exports key/value entries to a JSON Lines file.
func (k *KV) ExportJSONL(options sobek.Value) *sobek.Promise {
	exportOptions, err := importExportJSONLOptions(k.vu.Runtime(), options)
	if err != nil {
		return k.rejectedPromiseObserved(opExportJSONL, err)
	}

	return k.runAsyncWithStoreObserved(
		opExportJSONL,
		func(s store.Store) (any, error) {
			return exportJSONL(s, exportOptions)
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

	return k.runAsyncWithStoreObserved(
		opImportJSONL,
		func(s store.Store) (any, error) {
			return importJSONL(s, importOptions)
		},
		func(rt *sobek.Runtime, result any) sobek.Value {
			return rt.ToValue(result)
		},
	)
}

func exportJSONL(s store.Store, opts exportJSONLOptions) (*exportJSONLResult, error) {
	targetDir := filepath.Dir(opts.FileName)
	targetBase := filepath.Base(opts.FileName)

	if err := os.MkdirAll(targetDir, 0o750); err != nil {
		return nil, fmt.Errorf("%w: %w", store.ErrBackupDirectoryFailed, err)
	}

	//nolint:forbidigo // file I/O is required for JSONL export.
	tempFile, err := os.CreateTemp(targetDir, targetBase+".*.tmp")
	if err != nil {
		return nil, fmt.Errorf("%w: %w", store.ErrBackupTempFileFailed, err)
	}

	tempName := tempFile.Name()
	cleanupTemp := true

	defer func() {
		if cleanupTemp {
			//nolint:forbidigo // best-effort cleanup for failed export.
			_ = os.Remove(tempName)
		}
	}()

	exported, err := writeJSONLFile(tempFile, s, opts)
	if err != nil {
		return nil, err
	}

	//nolint:forbidigo // file I/O is required to finalize JSONL export.
	if err := os.Rename(tempName, opts.FileName); err != nil {
		return nil, fmt.Errorf("%w: %w", store.ErrBackupFinalizeFailed, err)
	}

	if err := fileutil.SyncParentDir(opts.FileName); err != nil {
		return nil, fmt.Errorf("%w: %w", store.ErrBackupFinalizeFailed, err)
	}

	cleanupTemp = false

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

func importJSONL(s store.Store, opts importJSONLOptions) (*importJSONLResult, error) {
	//nolint:forbidigo // file I/O is required for JSONL import.
	file, err := os.Open(opts.FileName)
	if err != nil {
		return nil, classifyJSONLFileOpenError(opts.FileName, err)
	}
	defer func() {
		_ = file.Close()
	}()

	imported, bytesRead, err := readJSONLLines(s, file, opts)
	if err != nil {
		return nil, err
	}

	return &importJSONLResult{
		Imported:  imported,
		FileName:  opts.FileName,
		BytesRead: bytesRead,
	}, nil
}

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

func readJSONLLines(s store.Store, input io.Reader, opts importJSONLOptions) (int64, int64, error) {
	return readJSONLLinesWithMaxLineBytes(s, input, opts, importJSONLDefaultMaxLineBytes)
}

func readJSONLLinesWithMaxLineBytes(
	s store.Store,
	input io.Reader,
	opts importJSONLOptions,
	maxLineBytes int,
) (int64, int64, error) {
	if maxLineBytes <= 0 {
		maxLineBytes = importJSONLDefaultMaxLineBytes
	}

	reader := bufio.NewReader(input)
	batchSize := resolveImportJSONLBatchSize(opts.BatchSize)

	var (
		imported  int64
		bytesRead int64
		lineNo    int64
		batch     = make([]store.Entry, 0)
	)

	for opts.Limit <= 0 || imported+int64(len(batch)) < opts.Limit {
		line, consumedBytes, err := readImportJSONLLineBounded(reader, maxLineBytes)
		bytesRead += consumedBytes

		if len(line) > 0 {
			lineNo++

			entry, parseErr := parseImportJSONLLine(line, lineNo)
			if parseErr != nil {
				return imported, bytesRead, parseErr
			}

			batch = append(batch, entry)

			if shouldFlushImportJSONLBatch(imported, len(batch), batchSize, opts.Limit) {
				written, flushErr := flushImportJSONLBatch(s, batch)
				if flushErr != nil {
					return imported, bytesRead, flushErr
				}

				imported += written
				batch = batch[:0]
			}
		}

		if errors.Is(err, errImportJSONLLineTooLong) {
			return imported, bytesRead, fmt.Errorf(
				"%w: importJSONL line %d exceeds maxLineBytes (%d bytes)",
				store.ErrValueParseFailed,
				lineNo+1,
				maxLineBytes,
			)
		}

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return imported, bytesRead, fmt.Errorf(
				"%w: importJSONL line %d: %w",
				store.ErrSnapshotReadFailed,
				lineNo+1,
				err,
			)
		}
	}

	if len(batch) > 0 {
		written, err := flushImportJSONLBatch(s, batch)
		if err != nil {
			return imported, bytesRead, err
		}

		imported += written
	}

	return imported, bytesRead, nil
}

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

func resolveImportJSONLBatchSize(batchSize int64) int64 {
	if batchSize <= 0 {
		return importJSONLDefaultBatchSize
	}

	return batchSize
}

func shouldFlushImportJSONLBatch(imported int64, batchLen int, batchSize int64, limit int64) bool {
	if batchSize > 0 && int64(batchLen) >= batchSize {
		return true
	}

	if limit > 0 && imported+int64(batchLen) >= limit {
		return true
	}

	return false
}

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
func writeJSONLFile(file *os.File, s store.Store, opts exportJSONLOptions) (int64, error) {
	writer := bufio.NewWriter(file)
	encoder := json.NewEncoder(writer)

	exported, err := writeJSONLRecords(s, opts, encoder)
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

func writeJSONLRecords(s store.Store, opts exportJSONLOptions, encoder *json.Encoder) (int64, error) {
	var (
		exported int64
		cursor   string
	)

	for {
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
