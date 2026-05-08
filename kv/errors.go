package kv

import (
	"errors"
	"fmt"

	"github.com/grafana/sobek"

	"github.com/oshokin/xk6-kv/kv/store"
)

var _ error = (*Error)(nil)

// ErrorName represents the name of an error.
type ErrorName string

const (
	// BackupInProgressError is emitted when kv.backup() collides with another backup.
	BackupInProgressError ErrorName = "BackupInProgressError"

	// BackupOptionsRequiredError is emitted when backup options are missing.
	BackupOptionsRequiredError ErrorName = "BackupOptionsRequiredError"

	// BucketNotFoundError is emitted when a requested disk store bucket is missing.
	BucketNotFoundError ErrorName = "BucketNotFoundError"

	// DatabaseNotOpenError is emitted when the database is accessed before it is opened
	// or after it is closed.
	DatabaseNotOpenError ErrorName = "DatabaseNotOpenError"

	// DiskPathError is emitted when disk path resolution or directory creation fails.
	DiskPathError ErrorName = "DiskPathError"

	// DiskStoreDeleteError is emitted when delete-style operations fail.
	DiskStoreDeleteError ErrorName = "DiskStoreDeleteError"

	// DiskStoreExistsError is emitted when exists checks fail.
	DiskStoreExistsError ErrorName = "DiskStoreExistsError"

	// DiskStoreIndexError is emitted when index/count lookups fail.
	DiskStoreIndexError ErrorName = "DiskStoreIndexError"

	// DiskStoreOpenError is emitted when the disk backend cannot be opened.
	DiskStoreOpenError ErrorName = "DiskStoreOpenError"

	// DiskStoreReadError is emitted when reads from the disk backend fail.
	DiskStoreReadError ErrorName = "DiskStoreReadError"

	// DiskStoreScanError is emitted when scan/list operations fail.
	DiskStoreScanError ErrorName = "DiskStoreScanError"

	// DiskStoreSizeError is emitted when size or counting operations fail.
	DiskStoreSizeError ErrorName = "DiskStoreSizeError"

	// DiskStoreWriteError is emitted when writes or mutations to the disk backend fail.
	DiskStoreWriteError ErrorName = "DiskStoreWriteError"

	// InvalidBackendError is emitted when openKv receives an unsupported backend option.
	InvalidBackendError ErrorName = "InvalidBackendError"

	// InvalidCursorError is emitted when scan()/scanKeys() receives a malformed cursor.
	InvalidCursorError ErrorName = "InvalidCursorError"

	// InvalidOptionsError is emitted when API options/inputs cannot be validated.
	InvalidOptionsError ErrorName = "InvalidOptionsError"

	// InvalidSerializationError is emitted when openKv receives an unsupported serialization option.
	InvalidSerializationError ErrorName = "InvalidSerializationError"

	// InternalStoreError is emitted when a store invariant is violated.
	InternalStoreError ErrorName = "InternalStoreError"

	// KVOptionsConflictError is emitted when openKv is called with different options than the existing store.
	KVOptionsConflictError ErrorName = "KVOptionsConflictError"

	// MetricsUnavailableError is emitted when custom metric emission is not available.
	MetricsUnavailableError ErrorName = "MetricsUnavailableError"

	// KeyListRebuildError is emitted when key rebuild logic fails.
	KeyListRebuildError ErrorName = "KeyListRebuildError"

	// KeyNotFoundError is emitted when a requested key does not exist.
	KeyNotFoundError ErrorName = "KeyNotFoundError"

	// RestoreInProgressError is emitted when kv.restore() collides with another restore.
	RestoreInProgressError ErrorName = "RestoreInProgressError"

	// RestoreOptionsRequiredError is emitted when restore options are missing.
	RestoreOptionsRequiredError ErrorName = "RestoreOptionsRequiredError"

	// SerializerError is emitted when serialization or deserialization fails.
	SerializerError ErrorName = "SerializerError"

	// SnapshotBudgetExceededError is emitted when MaxEntries/MaxBytes limits reject the restore.
	SnapshotBudgetExceededError ErrorName = "SnapshotBudgetExceededError"

	// SnapshotExportError is emitted when snapshot export/finalization fails.
	SnapshotExportError ErrorName = "SnapshotExportError"

	// SnapshotIOError is emitted when low-level snapshot IO operations fail.
	SnapshotIOError ErrorName = "SnapshotIOError"

	// SnapshotKeyMissingError is emitted when a snapshot key is missing.
	SnapshotKeyMissingError ErrorName = "SnapshotKeyMissingError"

	// SnapshotNotFoundError is emitted when the snapshot file cannot be located.
	SnapshotNotFoundError ErrorName = "SnapshotNotFoundError"

	// SnapshotPermissionError is emitted when the snapshot file cannot be accessed due to permissions.
	SnapshotPermissionError ErrorName = "SnapshotPermissionError"

	// SnapshotReadError is emitted when snapshot reads/imports fail.
	SnapshotReadError ErrorName = "SnapshotReadError"

	// StoreReadOnlyError is emitted when a mutation is attempted on a read-only disk store.
	StoreReadOnlyError ErrorName = "StoreReadOnlyError"

	// StoreClosedError is emitted when a closed KV handle is used, or when
	// disk store operations run before Open().
	StoreClosedError ErrorName = "StoreClosedError"

	// UnsupportedValueTypeError is emitted when a store rejects a value type.
	UnsupportedValueTypeError ErrorName = "UnsupportedValueTypeError"

	// ValueNumberRequiredError is emitted when a numeric value is required but the provided
	// argument cannot be coerced to a number.
	ValueNumberRequiredError = "ValueNumberRequiredError"

	// ValueParseError is emitted when stored values cannot be parsed.
	ValueParseError ErrorName = "ValueParseError"

	// UnexpectedStoreOutputError is emitted when the store returns a nil value without an accompanying error.
	UnexpectedStoreOutputError ErrorName = "UnexpectedStoreOutputError"

	// UnknownError is emitted when an error cannot be classified into any specific category.
	UnknownError = "UnknownError"
)

// ErrUnexpectedStoreOutput indicates a store implementation returned a nil result without an error.
var ErrUnexpectedStoreOutput = errors.New("unexpected store output")

// Error represents a custom error emitted by the kv module.
type Error struct {
	// Name contains one of the strings associated with an error name.
	Name ErrorName

	// Message represents message or description associated with the given error name.
	Message string

	// Errors carries per-entry details for batch APIs.
	//
	// We intentionally use []ErrorDetail (values), not []*ErrorDetail (pointers),
	// to avoid per-detail heap churn. See store/setmany_slice_shape_benchmark_test.go.
	Errors []ErrorDetail
}

// ErrorDetail describes a single entry-level validation/serialization error.
type ErrorDetail struct {
	// Key is the key that caused the error.
	Key string `js:"key,omitempty"`
	// Name is the name of the error.
	Name string `js:"name"`
	// Message is the message of the error.
	Message string `js:"message"`
}

// NewError returns a new Error instance.
func NewError(name ErrorName, message string) *Error {
	return &Error{
		Name:    name,
		Message: message,
	}
}

// NewErrorWithDetails returns a new Error instance with per-entry details.
func NewErrorWithDetails(name ErrorName, message string, details []ErrorDetail) *Error {
	return &Error{
		Name:    name,
		Message: message,
		Errors:  details,
	}
}

// Error implements the error interface.
func (e *Error) Error() string {
	return string(e.Name) + ": " + e.Message
}

// ToSobekValue converts the error into a native JS object with "name" and "message" properties.
// This ensures JS catch blocks can use err.name === "SomeError".
func (e *Error) ToSobekValue(rt *sobek.Runtime) sobek.Value {
	errorMessage := e.Error()
	obj := rt.NewObject()

	err := obj.Set("name", string(e.Name))
	if err != nil {
		// Fallback: return error message as plain string.
		return rt.ToValue(errorMessage)
	}

	err = obj.Set("message", e.Message)
	if err != nil {
		// Fallback: return error message as plain string.
		return rt.ToValue(errorMessage)
	}

	if len(e.Errors) > 0 {
		err = obj.Set("errors", e.Errors)
		if err != nil {
			// Fallback: return error message as plain string.
			return rt.ToValue(errorMessage)
		}
	}

	return obj
}

// classifyError downgrades internal Go errors to structured kv errors for JS.
//
//nolint:cyclop,funlen // this is a complex function but it is necessary to classify errors.
func classifyError(err error) *Error {
	if err == nil {
		return nil
	}

	var kvErr *Error
	if errors.As(err, &kvErr) {
		return kvErr
	}

	var entryListErr *store.EntryListError
	if errors.As(err, &entryListErr) {
		details := make([]ErrorDetail, 0, len(entryListErr.Errors))
		for _, item := range entryListErr.Errors {
			details = append(details, ErrorDetail{
				Key:     item.Key,
				Name:    item.Name,
				Message: item.Message,
			})
		}

		switch entryListErr.Kind {
		case store.EntryListErrorKindSerialization:
			return NewErrorWithDetails(InvalidOptionsError, entryListErr.Message, details)
		default:
			return NewErrorWithDetails(UnknownError, entryListErr.Message, details)
		}
	}

	switch {
	case errors.Is(err, store.ErrBackupOptionsNil):
		return NewError(BackupOptionsRequiredError, err.Error())
	case errors.Is(err, store.ErrRestoreOptionsNil):
		return NewError(RestoreOptionsRequiredError, err.Error())
	case errors.Is(err, store.ErrBackupInProgress):
		return NewError(BackupInProgressError, err.Error())
	case errors.Is(err, store.ErrRestoreInProgress):
		return NewError(RestoreInProgressError, err.Error())
	case errors.Is(err, store.ErrRestoreBudgetEntriesExceeded),
		errors.Is(err, store.ErrRestoreBudgetBytesExceeded):
		return NewError(SnapshotBudgetExceededError, err.Error())
	case errors.Is(err, store.ErrSnapshotNotFound):
		return NewError(SnapshotNotFoundError, err.Error())
	case errors.Is(err, store.ErrSnapshotPermissionDenied):
		return NewError(SnapshotPermissionError, err.Error())
	case errors.Is(err, store.ErrDiskPathResolveFailed),
		errors.Is(err, store.ErrDiskDirectoryCreateFailed),
		errors.Is(err, store.ErrDiskPathIsDirectory):
		return NewError(DiskPathError, err.Error())
	case errors.Is(err, store.ErrDiskStoreOpenFailed):
		return NewError(DiskStoreOpenError, err.Error())
	case errors.Is(err, store.ErrDiskStoreReadOnly):
		// Keep message stable regardless of which operation wrapped the sentinel.
		return NewError(StoreReadOnlyError, store.ErrDiskStoreReadOnly.Error())
	case errors.Is(err, store.ErrDiskStoreReadFailed):
		return NewError(DiskStoreReadError, err.Error())
	case errors.Is(err, store.ErrValueParseFailed):
		return NewError(ValueParseError, err.Error())
	case errors.Is(err, store.ErrDiskStoreWriteFailed),
		errors.Is(err, store.ErrDiskStoreIncrementFailed),
		errors.Is(err, store.ErrDiskStoreGetOrSetFailed),
		errors.Is(err, store.ErrDiskStoreSwapFailed),
		errors.Is(err, store.ErrDiskStoreCompareSwapFailed):
		return NewError(DiskStoreWriteError, err.Error())
	case errors.Is(err, store.ErrDiskStoreDeleteFailed),
		errors.Is(err, store.ErrDiskStoreDeleteIfExistsFailed),
		errors.Is(err, store.ErrDiskStoreCompareDeleteFailed),
		errors.Is(err, store.ErrDiskStoreClearFailed):
		return NewError(DiskStoreDeleteError, err.Error())
	case errors.Is(err, store.ErrDiskStoreExistsFailed):
		return NewError(DiskStoreExistsError, err.Error())
	case errors.Is(err, store.ErrDiskStoreScanFailed):
		return NewError(DiskStoreScanError, err.Error())
	case errors.Is(err, store.ErrClaimCompletionFailed),
		errors.Is(err, store.ErrUnexpectedHeapType):
		return NewError(InternalStoreError, err.Error())
	case errors.Is(err, store.ErrDiskStoreSizeFailed),
		errors.Is(err, store.ErrDiskStoreCountFailed),
		errors.Is(err, store.ErrDiskStoreStatFailed):
		return NewError(DiskStoreSizeError, err.Error())
	case errors.Is(err, store.ErrDiskStoreRebuildKeysFailed),
		errors.Is(err, store.ErrKeyListRebuildFailed):
		return NewError(KeyListRebuildError, err.Error())
	case errors.Is(err, store.ErrDiskStoreRandomAccessFailed):
		return NewError(DiskStoreIndexError, err.Error())
	case errors.Is(err, store.ErrInvalidBackend):
		return NewError(InvalidBackendError, err.Error())
	case errors.Is(err, store.ErrInvalidCursor):
		return NewError(InvalidCursorError, err.Error())
	case errors.Is(err, store.ErrInvalidSerialization):
		return NewError(InvalidSerializationError, err.Error())
	case errors.Is(err, store.ErrKVOptionsConflict):
		return NewError(KVOptionsConflictError, err.Error())
	case errors.Is(err, store.ErrKVOptionsInvalid),
		errors.Is(err, store.ErrKeyEmpty):
		return NewError(InvalidOptionsError, err.Error())
	case errors.Is(err, store.ErrBackupDirectoryFailed),
		errors.Is(err, store.ErrBackupTempFileFailed),
		errors.Is(err, store.ErrBackupCopyFailed),
		errors.Is(err, store.ErrBackupFinalizeFailed),
		errors.Is(err, store.ErrSnapshotExportFailed):
		return NewError(SnapshotExportError, err.Error())
	case errors.Is(err, store.ErrBBoltSnapshotOpenFailed),
		errors.Is(err, store.ErrBBoltSnapshotCloseFailed),
		errors.Is(err, store.ErrBBoltSnapshotStatFailed),
		errors.Is(err, store.ErrBBoltBucketCreateFailed),
		errors.Is(err, store.ErrBBoltWriteFailed),
		errors.Is(err, store.ErrSnapshotOpenFailed):
		return NewError(SnapshotIOError, err.Error())
	case errors.Is(err, store.ErrSnapshotReadFailed),
		errors.Is(err, store.ErrSnapshotPathResolveFailed):
		return NewError(SnapshotReadError, err.Error())
	case errors.Is(err, store.ErrSnapshotKeyMissing):
		return NewError(SnapshotKeyMissingError, err.Error())
	case errors.Is(err, store.ErrBucketNotFound):
		return NewError(BucketNotFoundError, err.Error())
	case errors.Is(err, store.ErrKeyNotFound):
		return NewError(KeyNotFoundError, err.Error())
	case errors.Is(err, store.ErrDiskStoreClosed):
		return NewError(StoreClosedError, err.Error())
	case errors.Is(err, store.ErrUnsupportedValueType):
		return NewError(UnsupportedValueTypeError, err.Error())
	case errors.Is(err, store.ErrSerializerEncodeFailed),
		errors.Is(err, store.ErrSerializerDecodeFailed):
		return NewError(SerializerError, err.Error())
	case errors.Is(err, ErrUnexpectedStoreOutput):
		return NewError(UnexpectedStoreOutputError, err.Error())
	}

	return NewError(UnknownError, err.Error())
}

// unexpectedStoreOutput returns an error indicating that a store implementation
// returned a nil result without an error.
func unexpectedStoreOutput(method string) error {
	if method == "" {
		return ErrUnexpectedStoreOutput
	}

	return fmt.Errorf("%w: %s returned nil result", ErrUnexpectedStoreOutput, method)
}
