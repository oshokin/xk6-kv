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

All `kv.*` Promises reject with a structured error that exposes `err.name` (the error category) and `err.message` (the full diagnostic detail). The Go layer surfaces fine‑grained sentinel errors that are classified into user-facing categories so you can distinguish between recoverable conditions and genuine bugs.

### Understanding error classification

**Key principle:** Errors are grouped by **recovery strategy**, not by internal implementation.

For example, `DiskStoreWriteError` groups five internal errors (`Set`, `IncrementBy`, `GetOrSet`, `Swap`, `CompareAndSwap` failures) because they all share the same recovery path: check disk permissions, available space, and database state. You don't need to handle each write operation differently—they fail for the same reasons.

This means:

- **`err.name`**: Tells you *what category of problem* occurred (actionable)
- **`err.message`**: Preserves *full diagnostic detail* from the error chain (for debugging)

### Working with classified errors

#### Example 1: Handling missing keys gracefully

```javascript
try {
  const value = await kv.get('user:session:' + __VU);
} catch (err) {
  if (err?.name === 'KeyNotFoundError') {
    // Expected on first iteration - create the session
    await kv.set('user:session:' + __VU, { created: Date.now() });
  } else {
    throw err; // Unexpected error - fail the test
  }
}
```

#### Example 2: Snapshot restore with budget safety

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
    case "SnapshotPermissionError":
      fail(`Cannot access snapshot file - check permissions: ${err.message}`);
      break;
    default:
      throw err;
  }
}
```

#### Example 3: Retry transient concurrency errors

```javascript
async function backupWithRetry(maxRetries = 3) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await kv.backup({ fileName: './backup.kv' });
    } catch (err) {
      if (err?.name === 'BackupInProgressError' && attempt < maxRetries) {
        console.log(`Backup collision, retrying (${attempt}/${maxRetries})...`);
        sleep(0.1);
        continue;
      }
      throw err; // Not transient or max retries exceeded
    }
  }
}
```

### Error catalogue

Each entry lists the JavaScript `err.name`, the underlying Go sentinel(s) it groups, and the most common trigger. Errors in the same category share the same recovery strategy.

#### Data access

| err.name | Trigger | Go sentinels |
| --- | --- | --- |
| `KeyNotFoundError` | `get()` called for a key that doesn't exist in the store. | `ErrKeyNotFound` |
| `InvalidCursorError` | `scan()` called with a malformed cursor string (not base64-encoded or corrupted). | `ErrInvalidCursor` |

#### General options & validation

| err.name | Trigger | Go sentinels |
| --- | --- | --- |
| `InvalidBackendError` | `openKv()` called with unsupported backend (not "memory" or "disk"). | `ErrInvalidBackend` |
| `InvalidOptionsError` | `openKv()` called with options that cannot be parsed. | `ErrKVOptionsInvalid` |
| `InvalidSerializationError` | `openKv()` called with unsupported serialization (not "json" or "string"). | `ErrInvalidSerialization` |
| `KVOptionsConflictError` | `openKv()` called multiple times with different options (first call wins, later calls must match). | `ErrKVOptionsConflict` |
| `BackupOptionsRequiredError` | `kv.backup()` called with `null`/`undefined` options. | `ErrBackupOptionsNil` |
| `RestoreOptionsRequiredError` | `kv.restore()` called without options. | `ErrRestoreOptionsNil` |
| `SnapshotBudgetExceededError` | `restore()` exceeds `maxEntries` or `maxBytes` and aborts before applying the snapshot. | `ErrRestoreBudgetEntriesExceeded`, `ErrRestoreBudgetBytesExceeded` |
| `ValueNumberRequiredError` | `incrementBy()` receives a non-number in JS (validated before Go is touched). | (JS-layer validation) |
| `UnsupportedValueTypeError` | Attempted to `set()`/`swap()` a value that isn't a string or `[]byte` once it reaches the store. | `ErrUnsupportedValueType` |
| `ValueParseError` | Disk increments found a non-integer payload (e.g. you stored `"foo"` and later called `incrementBy`). | `ErrValueParseFailed` |
| `SerializerError` | JSON/string serializer failed to encode/decode a value. | `ErrSerializerEncodeFailed`, `ErrSerializerDecodeFailed` |
| `UnexpectedStoreOutputError` | Store returned a nil result without an accompanying error (indicates a buggy or incompatible backend). | `ErrUnexpectedStoreOutput` |
| `UnknownError` | An error occurred that cannot be classified into any specific category (fallback for unclassified errors). | (Any unclassified error) |

#### Concurrency & lifecycle

| err.name | Trigger | Go sentinels |
| --- | --- | --- |
| `BackupInProgressError` | Another goroutine is actively snapshotting with `allowConcurrentWrites=false` (mutation lock blocks writers). | `ErrBackupInProgress` |
| `RestoreInProgressError` | A restore is blocking writers when a mutation is attempted. | `ErrRestoreInProgress` |
| `StoreClosedError` | You called a KV method after `kv.close()` or before the store opened. | `ErrDiskStoreClosed` |
| `DatabaseNotOpenError` | `openKv()` failed and you invoked methods on the nil handle. | (JS-layer guard) |

#### Snapshot & filesystem

| err.name | Trigger | Go sentinels |
| --- | --- | --- |
| `SnapshotNotFoundError` | The snapshot path does not exist. Expected on first run of backup/restore flows. | `ErrSnapshotNotFound` |
| `SnapshotPermissionError` | OS denied access to the snapshot path. | `ErrSnapshotPermissionDenied` |
| `SnapshotExportError` | Failure while creating directories, temp files, copying data, or finalising the snapshot. | `ErrBackupDirectoryFailed`, `ErrBackupTempFileFailed`, `ErrBackupCopyFailed`, `ErrBackupFinalizeFailed`, `ErrSnapshotExportFailed` |
| `SnapshotIOError` | Low-level BoltDB I/O problems when opening/closing/stat-ing snapshots or creating buckets. | `ErrBoltDBSnapshotOpenFailed`, `ErrBoltDBSnapshotCloseFailed`, `ErrBoltDBSnapshotStatFailed`, `ErrBoltDBBucketCreateFailed`, `ErrBoltDBWriteFailed`, `ErrSnapshotOpenFailed` |
| `SnapshotReadError` | Unable to read/import a snapshot (corrupted file, exceeds safety caps, or default snapshot path resolution failed). | `ErrSnapshotReadFailed`, `ErrSnapshotPathResolveFailed` |
| `DiskPathError` | Resolving or creating the disk backend path failed (bad path, permissions, or path points to a directory). | `ErrDiskPathResolveFailed`, `ErrDiskDirectoryCreateFailed`, `ErrDiskPathIsDirectory` |

#### Disk backend operations

| err.name | Trigger | Go sentinels |
| --- | --- | --- |
| `DiskStoreOpenError` | Could not open BoltDB (usually due to file locks or permissions). | `ErrDiskStoreOpenFailed` |
| `DiskStoreReadError` | Bolt read transaction failed. | `ErrDiskStoreReadFailed` |
| `DiskStoreWriteError` | Write operations failed (set, increment, swap, CAS). All share same recovery: check disk permissions/space. | `ErrDiskStoreWriteFailed`, `ErrDiskStoreIncrementFailed`, `ErrDiskStoreGetOrSetFailed`, `ErrDiskStoreSwapFailed`, `ErrDiskStoreCompareSwapFailed` |
| `DiskStoreDeleteError` | Delete operations failed (delete, deleteIfExists, compareAndDelete, clear). | `ErrDiskStoreDeleteFailed`, `ErrDiskStoreDeleteIfExistsFailed`, `ErrDiskStoreCompareDeleteFailed`, `ErrDiskStoreClearFailed` |
| `DiskStoreExistsError` | `exists()` check failed to execute. | `ErrDiskStoreExistsFailed` |
| `DiskStoreScanError` | `scan()`/`list()` failed due to Bolt cursor issues. | `ErrDiskStoreScanFailed` |
| `DiskStoreSizeError` | Size or count queries failed (grouped because recovery is identical: check DB health). | `ErrDiskStoreSizeFailed`, `ErrDiskStoreCountFailed`, `ErrDiskStoreStatFailed` |
| `DiskStoreIndexError` | Random-key lookups by index failed. | `ErrDiskStoreRandomAccessFailed` |
| `KeyListRebuildError` | Rebuilding the in-memory index failed (explicitly via `rebuildKeyList()` or implicitly after restore). | `ErrDiskStoreRebuildKeysFailed`, `ErrKeyListRebuildFailed` |
| `BucketNotFoundError` | Asked BoltDB for a non-existent bucket (usually indicates bad path or corrupted DB). | `ErrBucketNotFound` |

### Using the catalogue

**Retry vs. fail-fast strategy:**

| Error category | Recovery approach |
| --- | --- |
| **Transient** | Retry with backoff: `BackupInProgressError`, `RestoreInProgressError` |
| **User error** | Fix input and retry: `KeyNotFoundError`, `ValueNumberRequiredError`, `SnapshotNotFoundError` (on first run) |
| **Configuration** | Check paths/permissions: `DiskPathError`, `SnapshotPermissionError`, `DiskStoreOpenError` |
| **System failure** | Fail-fast and investigate: `DiskStoreWriteError`, `SnapshotIOError`, `BucketNotFoundError`, `UnknownError` |

**Best practices:**

1. **Always check `err.name`** for structured error handling—never parse `err.message` strings.
2. **Log `err.message`** for debugging—it contains full context (filenames, limits, wrapped errors).
3. **Group retry logic** by error category, not by operation (e.g., all writes fail the same way).
4. **Test error paths** using the examples in this directory as templates.

**Maintenance note:** This catalogue is manually synchronized with `kv/errors.go`. When adding a new sentinel error:

1. Add the Go sentinel to `kv/store/errors.go`
2. Map it to an error category in `kv/errors.go::classifyError()`
3. Update this table with the new mapping
4. Add example usage if the error introduces a new recovery pattern
