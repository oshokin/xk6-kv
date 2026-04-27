package kv

import (
	"github.com/grafana/sobek"
	"go.k6.io/k6/js/common"
)

type (
	// backupOptions is the JS-facing result of backup().
	backupOptions struct {
		// FileName optionally overrides backup file name.
		FileName string `js:"fileName"`
		// AllowConcurrentWrites permits backup during active writes.
		AllowConcurrentWrites bool `js:"allowConcurrentWrites"`
	}

	// restoreOptions is the JS-facing result of restore().
	restoreOptions struct {
		// FileName selects the backup file to restore from.
		FileName string `js:"fileName"`
		// MaxEntries limits entries restored from backup.
		MaxEntries int64 `js:"maxEntries"`
		// MaxBytes limits bytes restored from backup.
		MaxBytes int64 `js:"maxBytes"`
	}
)

// importBackupOptions converts JS values into BackupOptions.
func importBackupOptions(rt *sobek.Runtime, options sobek.Value) (backupOptions, error) {
	backupOptions := backupOptions{}

	err := ensureOptionalObjectOptions("backup", options)
	if err != nil {
		return backupOptions, err
	}

	if common.IsNullish(options) {
		return backupOptions, nil
	}

	optionsObj := options.ToObject(rt)
	if fileNameValue := optionsObj.Get("fileName"); !common.IsNullish(fileNameValue) {
		fileName, _, parseErr := parseOptionalStringOption("backup", "fileName", fileNameValue)
		if parseErr != nil {
			return backupOptions, parseErr
		}

		backupOptions.FileName = fileName
	}

	allowValue := optionsObj.Get("allowConcurrentWrites")
	if !common.IsNullish(allowValue) {
		allow, _, parseErr := parseOptionalBoolOption("backup", "allowConcurrentWrites", allowValue)
		if parseErr != nil {
			return backupOptions, parseErr
		}

		backupOptions.AllowConcurrentWrites = allow
	}

	return backupOptions, nil
}

// importRestoreOptions converts JS values into RestoreOptions.
func importRestoreOptions(rt *sobek.Runtime, options sobek.Value) (restoreOptions, error) {
	restoreOptions := restoreOptions{}

	err := ensureOptionalObjectOptions("restore", options)
	if err != nil {
		return restoreOptions, err
	}

	if common.IsNullish(options) {
		return restoreOptions, nil
	}

	optionsObj := options.ToObject(rt)

	if fileNameValue := optionsObj.Get("fileName"); !common.IsNullish(fileNameValue) {
		fileName, _, parseErr := parseOptionalStringOption("restore", "fileName", fileNameValue)
		if parseErr != nil {
			return restoreOptions, parseErr
		}

		restoreOptions.FileName = fileName
	}

	if maxEntriesValue := optionsObj.Get("maxEntries"); !common.IsNullish(maxEntriesValue) {
		parsedValue, _, parseErr := parseOptionalInt64Option("restore", "maxEntries", maxEntriesValue)
		if parseErr != nil {
			return restoreOptions, parseErr
		}

		restoreOptions.MaxEntries = parsedValue
	}

	if maxBytesValue := optionsObj.Get("maxBytes"); !common.IsNullish(maxBytesValue) {
		parsedValue, _, parseErr := parseOptionalInt64Option("restore", "maxBytes", maxBytesValue)
		if parseErr != nil {
			return restoreOptions, parseErr
		}

		restoreOptions.MaxBytes = parsedValue
	}

	return restoreOptions, nil
}
