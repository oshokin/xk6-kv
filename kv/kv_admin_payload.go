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
func importBackupOptions(rt *sobek.Runtime, options sobek.Value) backupOptions {
	backupOptions := backupOptions{}
	if common.IsNullish(options) {
		return backupOptions
	}

	optionsObj := options.ToObject(rt)
	if fileNameValue := optionsObj.Get("fileName"); !common.IsNullish(fileNameValue) {
		backupOptions.FileName = fileNameValue.String()
	}

	allowValue := optionsObj.Get("allowConcurrentWrites")
	if !common.IsNullish(allowValue) {
		var allow bool

		err := rt.ExportTo(allowValue, &allow)
		if err == nil {
			backupOptions.AllowConcurrentWrites = allow
		}
	}

	return backupOptions
}

// importRestoreOptions converts JS values into RestoreOptions.
func importRestoreOptions(rt *sobek.Runtime, options sobek.Value) restoreOptions {
	restoreOptions := restoreOptions{}
	if common.IsNullish(options) {
		return restoreOptions
	}

	optionsObj := options.ToObject(rt)

	if fileNameValue := optionsObj.Get("fileName"); !common.IsNullish(fileNameValue) {
		restoreOptions.FileName = fileNameValue.String()
	}

	if maxEntriesValue := optionsObj.Get("maxEntries"); !common.IsNullish(maxEntriesValue) {
		var parsedValue int64

		err := rt.ExportTo(maxEntriesValue, &parsedValue)
		if err == nil {
			restoreOptions.MaxEntries = parsedValue
		}
	}

	if maxBytesValue := optionsObj.Get("maxBytes"); !common.IsNullish(maxBytesValue) {
		var parsedValue int64

		err := rt.ExportTo(maxBytesValue, &parsedValue)
		if err == nil {
			restoreOptions.MaxBytes = parsedValue
		}
	}

	return restoreOptions
}
