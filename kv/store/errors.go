package store

import "errors"

var (
	// ErrBackupInProgress is returned when a write is attempted during a backup operation.
	ErrBackupInProgress = errors.New("backup in progress")
	// ErrBackupOptionsNil is returned when Backup is invoked with nil options.
	ErrBackupOptionsNil = errors.New("backup options are nil")
	// ErrBackupDirectoryFailed indicates a backup directory operation failed.
	ErrBackupDirectoryFailed = errors.New("backup directory operation failed")
	// ErrBackupTempFileFailed indicates a backup temporary file operation failed.
	ErrBackupTempFileFailed = errors.New("backup temporary file operation failed")
	// ErrBackupCopyFailed indicates a failure while copying snapshot data.
	ErrBackupCopyFailed = errors.New("snapshot copy failed")
	// ErrBackupFinalizeFailed indicates a failure while finalizing snapshot files.
	ErrBackupFinalizeFailed = errors.New("snapshot finalize failed")
	// ErrSnapshotExportFailed indicates a failure while exporting snapshot data.
	ErrSnapshotExportFailed = errors.New("snapshot export failed")
	// ErrBBoltBucketCreateFailed indicates creating a bbolt bucket failed.
	ErrBBoltBucketCreateFailed = errors.New("bolt bucket create failed")
	// ErrBBoltSnapshotCloseFailed indicates closing a bbolt snapshot failed.
	ErrBBoltSnapshotCloseFailed = errors.New("bolt snapshot close failed")
	// ErrBBoltSnapshotOpenFailed indicates opening a bbolt snapshot failed.
	ErrBBoltSnapshotOpenFailed = errors.New("bolt snapshot open failed")
	// ErrBBoltSnapshotStatFailed indicates statting a bbolt snapshot failed.
	ErrBBoltSnapshotStatFailed = errors.New("bolt snapshot stat failed")
	// ErrBBoltWriteFailed indicates writing to bbolt failed.
	ErrBBoltWriteFailed = errors.New("bolt write failed")
	// ErrBucketNotFound is returned when the requested bucket does not exist.
	ErrBucketNotFound = errors.New("bucket not found")
	// ErrDiskDirectoryCreateFailed indicates disk directory creation failed.
	ErrDiskDirectoryCreateFailed = errors.New("disk directory create failed")
	// ErrDiskPathIsDirectory is returned when the disk store path points to a directory.
	ErrDiskPathIsDirectory = errors.New("disk store path is a directory")
	// ErrDiskPathResolveFailed indicates disk path resolution failed.
	ErrDiskPathResolveFailed = errors.New("disk path resolve failed")
	// ErrDiskStoreClearFailed indicates clearing the disk store failed.
	ErrDiskStoreClearFailed = errors.New("disk store clear failed")
	// ErrDiskStoreClosed is returned when disk store operations run before Open().
	ErrDiskStoreClosed = errors.New("disk store is closed; call Open() before performing operations")
	// ErrDiskStoreCompareDeleteFailed indicates compareAndDelete failed.
	ErrDiskStoreCompareDeleteFailed = errors.New("disk store compare and delete failed")
	// ErrDiskStoreCompareSwapFailed indicates compare-and-swap operations failed.
	ErrDiskStoreCompareSwapFailed = errors.New("disk store compare and swap failed")
	// ErrDiskStoreCountFailed indicates counting disk store entries failed.
	ErrDiskStoreCountFailed = errors.New("disk store count failed")
	// ErrDiskStoreDeleteFailed indicates delete operations failed.
	ErrDiskStoreDeleteFailed = errors.New("disk store delete failed")
	// ErrDiskStoreDeleteIfExistsFailed indicates deleteIfExists failed.
	ErrDiskStoreDeleteIfExistsFailed = errors.New("disk store delete-if-exists failed")
	// ErrDiskStoreExistsFailed indicates exists checks failed.
	ErrDiskStoreExistsFailed = errors.New("disk store exists check failed")
	// ErrDiskStoreGetOrSetFailed indicates getOrSet operations failed.
	ErrDiskStoreGetOrSetFailed = errors.New("disk store get or set failed")
	// ErrDiskStoreIncrementFailed indicates increment operations failed.
	ErrDiskStoreIncrementFailed = errors.New("disk store increment failed")
	// ErrDiskStoreOpenFailed indicates opening the disk store failed.
	ErrDiskStoreOpenFailed = errors.New("disk store open failed")
	// ErrDiskStoreRandomAccessFailed indicates random key operations failed.
	ErrDiskStoreRandomAccessFailed = errors.New("disk store random access failed")
	// ErrDiskStoreReadFailed indicates reading from the disk store failed.
	ErrDiskStoreReadFailed = errors.New("disk store read failed")
	// ErrDiskStoreRebuildKeysFailed indicates rebuilding disk store keys failed.
	ErrDiskStoreRebuildKeysFailed = errors.New("disk store rebuild keys failed")
	// ErrDiskStoreScanFailed indicates scanning the disk store failed.
	ErrDiskStoreScanFailed = errors.New("disk store scan failed")
	// ErrDiskStoreSizeFailed indicates computing disk store size failed.
	ErrDiskStoreSizeFailed = errors.New("disk store size failed")
	// ErrDiskStoreStatFailed indicates statting disk store files failed.
	ErrDiskStoreStatFailed = errors.New("disk store stat failed")
	// ErrDiskStoreSwapFailed indicates swap operations failed.
	ErrDiskStoreSwapFailed = errors.New("disk store swap failed")
	// ErrDiskStoreWriteFailed indicates writing to the disk store failed.
	ErrDiskStoreWriteFailed = errors.New("disk store write failed")
	// ErrInvalidBackend is returned when an unsupported backend is specified.
	ErrInvalidBackend = errors.New("invalid backend")
	// ErrInvalidCursor is returned when scan receives a malformed cursor.
	ErrInvalidCursor = errors.New("invalid cursor")
	// ErrInvalidSerialization is returned when an unsupported serialization format is specified.
	ErrInvalidSerialization = errors.New("invalid serialization")
	// ErrKeyListRebuildFailed indicates rebuilding the key list failed.
	ErrKeyListRebuildFailed = errors.New("key list rebuild failed")
	// ErrKeyNotFound is returned when a requested key does not exist in the store.
	ErrKeyNotFound = errors.New("key not found")
	// ErrKVOptionsConflict is returned when openKv is called with different options than the existing store.
	ErrKVOptionsConflict = errors.New("kv options conflict with existing configuration")
	// ErrKVOptionsInvalid is returned when kv options cannot be parsed.
	ErrKVOptionsInvalid = errors.New("kv options invalid")
	// ErrMutationBlocked is returned when mutations are blocked due to snapshot activity.
	ErrMutationBlocked = errors.New("mutation is blocked")
	// ErrRestoreBudgetBytesExceeded is returned when restore MaxBytes cap is hit.
	ErrRestoreBudgetBytesExceeded = errors.New("restore exceeded MaxBytes cap")
	// ErrRestoreBudgetEntriesExceeded is returned when restore MaxEntries cap is hit.
	ErrRestoreBudgetEntriesExceeded = errors.New("restore exceeded MaxEntries cap")
	// ErrRestoreInProgress is returned when a write is attempted during a restore operation.
	ErrRestoreInProgress = errors.New("restore in progress")
	// ErrRestoreOptionsNil is returned when Restore is invoked with nil options.
	ErrRestoreOptionsNil = errors.New("restore options are nil")
	// ErrSerializerEncodeFailed indicates serializing a value failed.
	ErrSerializerEncodeFailed = errors.New("serializer encode failed")
	// ErrSerializerDecodeFailed indicates deserializing a value failed.
	ErrSerializerDecodeFailed = errors.New("serializer decode failed")
	// ErrSnapshotKeyMissing indicates a key was missing during snapshot operation.
	ErrSnapshotKeyMissing = errors.New("snapshot key missing")
	// ErrSnapshotNotFound is returned when a snapshot file cannot be located.
	ErrSnapshotNotFound = errors.New("snapshot file not found")
	// ErrSnapshotOpenFailed is returned when a snapshot file cannot be opened.
	ErrSnapshotOpenFailed = errors.New("snapshot open failed")
	// ErrSnapshotPermissionDenied is returned when snapshot file permissions prevent access.
	ErrSnapshotPermissionDenied = errors.New("snapshot permission denied")
	// ErrSnapshotPathResolveFailed indicates resolving the default snapshot path failed.
	ErrSnapshotPathResolveFailed = errors.New("snapshot path resolve failed")
	// ErrSnapshotReadFailed indicates a failure while reading snapshot contents.
	ErrSnapshotReadFailed = errors.New("snapshot read failed")
	// ErrUnsupportedValueType is returned when a value of an unsupported type is set.
	ErrUnsupportedValueType = errors.New("unsupported value type (want []byte or string)")
	// ErrUnexpectedHeapType indicates an unexpected type was encountered in the iterator heap.
	ErrUnexpectedHeapType = errors.New("unexpected type in heap")
	// ErrValueParseFailed indicates parsing a stored value failed.
	ErrValueParseFailed = errors.New("value parse failed")
)
