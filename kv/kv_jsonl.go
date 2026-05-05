package kv

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/grafana/sobek"

	"github.com/oshokin/xk6-kv/kv/store"
)

const exportJSONLPageSize int64 = 1000

type (
	exportJSONLResult struct {
		Exported     int64  `js:"exported"`
		FileName     string `js:"fileName"`
		BytesWritten int64  `js:"bytesWritten"`
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
