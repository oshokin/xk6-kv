# Examples & Error Manual

This directory contains runnable k6 scripts that exercise every major `kv.*` API surface. They double as integration tests and living documentation.

## Running the examples

1. Build or download a k6 binary that bundles this extension (see the root `README.md` for install options).
2. From the repository root, run a script with k6, e.g.

   ```bash
   k6 run examples/backup-and-restore.js
   ```

3. Many scripts mutate on-disk state (e.g. snapshots). They intentionally write inside the repo so it is easy to inspect the artifacts.

> Tip: the `e2e/` directory contains larger, production-style scenarios that stitch multiple APIs together.

## Error Manual

All `kv.*` Promises reject with a structured error that exposes `err.name` (the values below) and `err.message`. The Go layer surfaces fine‑grained sentinel errors so you can distinguish between recoverable conditions (e.g. “snapshot missing” on a first run) and genuine bugs.

### Reading errors in JavaScript

```javascript
try {
  await kv.restore({ fileName: "./snapshots/seed.kv", maxEntries: 10_000 });
} catch (err) {
  switch (err?.name) {
    case "SnapshotNotFoundError":
      console.warn("No previous snapshot yet, continuing with a clean slate.");
      break;
    case "SnapshotBudgetExceededError":
      fail(`Restore rejected because maxEntries/maxBytes caps were exceeded: ${err.message}`);
      break;
    default:
      throw err;
  }
}
```

### Error catalogue

Each entry lists the JavaScript `err.name`, the underlying Go sentinel(s), and the most common trigger.

#### General options & validation

| err.name | Trigger |
| --- | --- |
| `BackupOptionsRequiredError` | `kv.backup()` called with `null`/`undefined` options (Go sentinel: `ErrBackupOptionsNil`). |
| `RestoreOptionsRequiredError` | `kv.restore()` called without options (`ErrRestoreOptionsNil`). |
| `SnapshotBudgetExceededError` | `restore()` exceeds `maxEntries` or `maxBytes` and aborts before applying the snapshot (`ErrRestoreBudgetEntriesExceeded`, `ErrRestoreBudgetBytesExceeded`). |
| `ValueNumberRequiredError` | `incrementBy()` receives a non-number in JS (validated before Go is touched). |
| `UnsupportedValueTypeError` | Attempted to `set()`/`swap()` a value that isn’t a string or `[]byte` once it reaches the store (`ErrUnsupportedValueType`). |
| `ValueParseError` | Disk increments found a non-integer payload (e.g. you stored `"foo"` and later called `incrementBy`) (`ErrValueParseFailed`). |
| `SerializerError` | JSON/string serializer failed to encode/decode a value (`ErrSerializerEncodeFailed`, `ErrSerializerDecodeFailed`). |

#### Concurrency & lifecycle

| err.name | Trigger |
| --- | --- |
| `BackupInProgressError` | Another goroutine is actively snapshotting with `allowConcurrentWrites=false` (the mutation lock returns `ErrBackupInProgress`). |
| `RestoreInProgressError` | A restore is blocking writers when a mutation is attempted (`ErrRestoreInProgress`). |
| `StoreClosedError` | You called a KV method after `kv.close()` or before the store opened (`ErrDiskStoreClosed`). |
| `DatabaseNotOpenError` | `openKv()` failed and you invoked methods on the nil handle (JS-level guard). |

#### Snapshot & filesystem

| err.name | Trigger |
| --- | --- |
| `SnapshotNotFoundError` | The snapshot path does not exist (`ErrSnapshotNotFound`). Expected on first run of backup/restore flows. |
| `SnapshotPermissionError` | OS denied access to the snapshot path (`ErrSnapshotPermissionDenied`). |
| `SnapshotExportError` | Failure while creating directories, temp files, copying data, or finalising the snapshot (`ErrBackupDirectoryFailed`, `ErrBackupTempFileFailed`, `ErrBackupCopyFailed`, `ErrBackupFinalizeFailed`, `ErrSnapshotExportFailed`). |
| `SnapshotIOError` | Low-level BoltDB I/O problems when opening/closing/stat-ing snapshots or creating buckets (`ErrBoltDBSnapshotOpenFailed`, `ErrBoltDBSnapshotCloseFailed`, `ErrBoltDBSnapshotStatFailed`, `ErrBoltDBBucketCreateFailed`, `ErrBoltDBWriteFailed`, `ErrSnapshotOpenFailed`). |
| `SnapshotReadError` | Unable to read/import a snapshot (corrupted file, exceeds safety caps, or default snapshot path resolution failed) (`ErrSnapshotReadFailed`, `ErrSnapshotPathResolveFailed`). |
| `DiskPathError` | Resolving or creating the disk backend path failed (bad path, permissions) (`ErrDiskPathResolveFailed`, `ErrDiskDirectoryCreateFailed`). |

#### Disk backend operations

| err.name | Trigger |
| --- | --- |
| `DiskStoreOpenError` | Could not open BoltDB (usually due to file locks or permissions) (`ErrDiskStoreOpenFailed`). |
| `DiskStoreReadError` | Bolt read transaction failed (`ErrDiskStoreReadFailed`). |
| `DiskStoreWriteError` | Writes, increments, swaps, or compare-and-swap failed (`ErrDiskStoreWriteFailed`, `ErrDiskStoreIncrementFailed`, `ErrDiskStoreGetOrSetFailed`, `ErrDiskStoreSwapFailed`, `ErrDiskStoreCompareSwapFailed`). |
| `DiskStoreDeleteError` | Delete/delete-if-exists/compare-and-delete/clear failed (`ErrDiskStoreDeleteFailed`, `ErrDiskStoreDeleteIfExistsFailed`, `ErrDiskStoreCompareDeleteFailed`, `ErrDiskStoreClearFailed`). |
| `DiskStoreExistsError` | `exists()` failed to run (`ErrDiskStoreExistsFailed`). |
| `DiskStoreScanError` | `scan()`/`list()` failed due to Bolt cursor issues (`ErrDiskStoreScanFailed`). |
| `DiskStoreSizeError` | `size()` or internal key counts/stat queries failed (`ErrDiskStoreSizeFailed`, `ErrDiskStoreCountFailed`, `ErrDiskStoreStatFailed`). |
| `DiskStoreIndexError` | Random-key lookups by index failed (`ErrDiskStoreRandomAccessFailed`). |
| `KeyListRebuildError` | Rebuilding the in-memory index failed (either explicitly via `rebuildKeyList()` or implicitly after restore) (`ErrDiskStoreRebuildKeysFailed`, `ErrKeyListRebuildFailed`). |
| `BucketNotFoundError` | Asked BoltDB for a non-existent bucket (usually indicates a bad path or corrupted DB) (`ErrBucketNotFound`). |

#### Disk path & permissions recap

| err.name | Trigger |
| --- | --- |
| `StoreClosedError` | Methods called after `kv.close()` (disk backend) (`ErrDiskStoreClosed`). |

#### Snapshot presence recap

| err.name | Trigger |
| --- | --- |
| `SnapshotNotFoundError` | Snapshot missing (safe on first run). |
| `SnapshotPermissionError` | OS denies file access. |

### Using the catalogue

- **Retry vs. fail-fast**: treat concurrency codes (`BackupInProgressError`, `RestoreInProgressError`) as transient; most others should fail the test and surface the root cause.
- **Logging**: include `err.message` for path context (the Go layer embeds filenames, caps, etc.).
- **Testing**: `examples/backup-and-restore.js` demonstrates how to branch on `err.name`. Mirror that pattern whenever you wrap `kv.backup()`/`kv.restore()` in production tests.

This document is generated manually from the codebase—if you add a sentinel in Go, wire it through `kv/errors.go` and update this table so scripts and users stay in sync.
