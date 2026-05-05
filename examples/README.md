# Examples & Error Manual

This directory contains runnable k6 scripts that exercise every major `kv.*` API surface. They double as integration tests and living documentation.

## Running the examples

1. Build or download a k6 binary that bundles this extension (see the root `README.md` for install options).
2. From the repository root, run a script with k6, e.g.

   ```bash
   k6 run examples/backup-and-restore.js
   ```

   For claim lease defaults (TTL=30000ms / 30s when omitted), try:

   ```bash
   k6 run examples/claim-random-default-ttl.js
   ```

   That script also demonstrates claim lifecycle handling:
   `completeClaim()` on success and `releaseClaim()` on failure.

   For one-time unique allocation via atomic pop, try:

   ```bash
   k6 run examples/pop-random-unique-users.js
   ```

   For prefix cardinality checks, try:

   ```bash
   k6 run examples/count-prefix.js
   ```

   For all-or-nothing bulk writes with detailed batch diagnostics, try:

   ```bash
   k6 run examples/set-many-all-or-nothing.js
   ```

   For ordered bulk reads with missing/null behavior, try:

   ```bash
   k6 run examples/get-many.js
   ```

   For explicit bulk deletions with deleted/missing counts, try:

   ```bash
   k6 run examples/delete-many.js
   ```

   For bounded destructive prefix deletions, try:

   ```bash
   k6 run examples/delete-by-prefix.js
   ```

   For key-only listing without loading values, try:

   ```bash
   k6 run examples/list-keys.js
   ```

   For portable key/value JSONL exports, try:

   ```bash
   k6 run examples/export-jsonl.js
   ```

   For operation metrics in a realistic worker queue flow, try:

   ```bash
   k6 run examples/metrics-operations-worker-queue.js
   ```

   For state gauges via `reportStats()`, try:

   ```bash
   k6 run examples/metrics-report-stats-health.js
   ```

3. Many scripts mutate on-disk state (e.g. snapshots). They intentionally write inside the repo so it is easy to inspect the artifacts.

> ⚠️ **Snapshot defaults:** When an example uses the memory backend and omits `backup().fileName`, it writes into `.k6.kv`—the same file the disk backend mounts by default. That’s deliberate so you can run `backend: "memory"` for the hot path, dump the dataset in `teardown()`, and later rerun the very same test with `backend: "disk"` without changing paths. If you need a separate artifact (or run disk workloads concurrently), set `fileName` explicitly before running the example.
> Tip: the `e2e/` directory contains larger, production-style scenarios that stitch multiple APIs together.

### `exportJSONL()`

Use `exportJSONL()` to write portable key/value seed data.

```javascript
await kv.exportJSONL({
  fileName: "./exports/users.jsonl",
  prefix: "user:",
});
```

Each line is:

```json
{"key":"some:key","value":...}
```

Invalid input rejects with `InvalidOptionsError`.

Examples:

- `kv.exportJSONL(null)` -> `InvalidOptionsError`
- `kv.exportJSONL({})` -> `InvalidOptionsError`
- `kv.exportJSONL({ fileName: "" })` -> `InvalidOptionsError`
- `kv.exportJSONL({ fileName: "./x.jsonl", limit: 1.5 })` -> `InvalidOptionsError`

## Error Manual

All `kv.*` Promises reject with a structured error that exposes `err.name` (the error category) and `err.message` (the full diagnostic detail). The Go layer surfaces fine-grained sentinel errors that are classified into user-facing categories so you can distinguish between recoverable conditions and genuine bugs.

Batch APIs such as `setMany()` can also return a stable `err.errors` array for per-entry diagnostics. Each entry detail uses:

- `key` - key associated with the failing entry (when available)
- `name` - machine-readable per-entry error name
- `message` - human-readable detail

For `setMany()` in the current API, `err.errors[].name` values are:

- `InvalidEntries` for payload-shape validation failures
- `EmptyKey` for empty-string keys in `setMany()` payloads
- `SerializerError` for per-entry serialization failures

`set()` and `setMany()` reject empty-string keys with `InvalidOptionsError`.

`getMany()` input errors are reported as `InvalidOptionsError`.
Missing keys are not errors: they are returned as `{ exists: false, value: null }`.
Stored JSON `null` values are returned as `{ exists: true, value: null }`.

`deleteMany()` accepts only an array of non-empty strings.
Input errors are reported as `InvalidOptionsError`.
Missing keys are not errors and are counted in `{ deleted, missing }`.

`deleteByPrefix()` accepts only an object with required `prefix` (non-empty string) and
required `limit` (positive integer). Input errors are reported as `InvalidOptionsError`.

`listKeys()` accepts an optional object (`{ prefix?, limit? }`) and returns sorted key names.
Input errors are reported as `InvalidOptionsError`.

Examples:

- `kv.getMany(null)` -> `InvalidOptionsError`
- `kv.getMany({})` -> `InvalidOptionsError`
- `kv.getMany(["ok", 123])` -> `InvalidOptionsError`
- `kv.deleteMany(null)` -> `InvalidOptionsError`
- `kv.deleteMany({})` -> `InvalidOptionsError`
- `kv.deleteMany(["ok", 123])` -> `InvalidOptionsError`
- `kv.deleteMany([""])` -> `InvalidOptionsError`
- `kv.deleteByPrefix(null)` -> `InvalidOptionsError`
- `kv.deleteByPrefix({})` -> `InvalidOptionsError`
- `kv.deleteByPrefix({ prefix: "", limit: 100 })` -> `InvalidOptionsError`
- `kv.deleteByPrefix({ prefix: "tmp:", limit: 0 })` -> `InvalidOptionsError`
- `kv.deleteByPrefix({ prefix: "tmp:", limit: 1.5 })` -> `InvalidOptionsError`
- `kv.listKeys([])` -> `InvalidOptionsError`
- `kv.listKeys({ prefix: 123 })` -> `InvalidOptionsError`
- `kv.listKeys({ limit: 1.5 })` -> `InvalidOptionsError`

### Understanding error classification

**Key principle:** Errors are grouped by **recovery strategy**, not by internal implementation.

For example, `DiskStoreWriteError` groups five internal errors (`Set`, `IncrementBy`, `GetOrSet`, `Swap`, `CompareAndSwap` failures) because they all share the same recovery path: check disk permissions, available space, and database state. You don't need to handle each write operation differently—they fail for the same reasons.

This means:

- **`err.name`**: Tells you *what category of problem* occurred (actionable)
- **`err.message`**: Preserves *full diagnostic detail* from the error chain (for debugging)
- **`err.errors[]`**: Stable per-entry batch details (for APIs like `setMany()`)

### Batch error details (`setMany`)

```javascript
try {
  await kv.setMany({
    "ok": { name: "Alice" },
    "bad": () => {},
  });
} catch (err) {
  if (err?.name === "SerializerError") {
    for (const item of err.errors ?? []) {
      console.log(item.key);     // failing key
      console.log(item.name);    // e.g. "SerializerError"
      console.log(item.message); // serializer message
    }
  } else {
    throw err;
  }
}
```

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

### Memory backup stall (strict mode)

Running `kv.backup()` against the **memory backend** with `allowConcurrentWrites` left at its default `false` takes the mutation gate for the entire export. Every writer (and therefore every VU) blocks from the moment the key snapshot starts until the last chunk is flushed to disk, so large datasets can freeze traffic for minutes. If you need online backups, either pass `allowConcurrentWrites: true` (best-effort snapshot; see `e2e/backup-restore-concurrency.js`) or schedule strict backups during maintenance windows.

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
| `InvalidOptionsError` | Invalid API arguments/options shape (e.g. non-object passed to options-based methods, wrong option field types), or `openKv()` called with options that cannot be parsed. For `setMany()` shape errors, inspect `err.errors[]` for per-entry details. | `ErrKVOptionsInvalid`, (JS-layer validation) |
| `InvalidSerializationError` | `openKv()` called with unsupported serialization (not "json" or "string"). | `ErrInvalidSerialization` |
| `KVOptionsConflictError` | `openKv()` called multiple times with different options (first call wins, later calls must match). | `ErrKVOptionsConflict` |
| `BackupOptionsRequiredError` | `kv.backup()` called with `null`/`undefined` options. | `ErrBackupOptionsNil` |
| `RestoreOptionsRequiredError` | `kv.restore()` called without options. | `ErrRestoreOptionsNil` |
| `MetricsUnavailableError` | `reportStats()` called when custom metrics are unavailable (for example, metrics were not initialized in `openKv()` or no active VU metric sink exists). | (JS-layer metrics guard) |
| `SnapshotBudgetExceededError` | `restore()` exceeds `maxEntries` or `maxBytes` and aborts before applying the snapshot. | `ErrRestoreBudgetEntriesExceeded`, `ErrRestoreBudgetBytesExceeded` |
| `ValueNumberRequiredError` | `incrementBy()` receives a non-number in JS (validated before Go is touched). | (JS-layer validation) |
| `UnsupportedValueTypeError` | Attempted to `set()`/`swap()` a value that isn't a string or `[]byte` once it reaches the store. | `ErrUnsupportedValueType` |
| `ValueParseError` | Disk increments found a non-integer payload (e.g. you stored `"foo"` and later called `incrementBy`). | `ErrValueParseFailed` |
| `SerializerError` | JSON/string serializer failed to encode/decode a value. `setMany()` serialization failures include per-entry diagnostics in `err.errors[]`. | `ErrSerializerEncodeFailed`, `ErrSerializerDecodeFailed` |
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
| `SnapshotIOError` | Low-level bbolt I/O problems when opening/closing/stat-ing snapshots or creating buckets. | `ErrBBoltSnapshotOpenFailed`, `ErrBBoltSnapshotCloseFailed`, `ErrBBoltSnapshotStatFailed`, `ErrBBoltBucketCreateFailed`, `ErrBBoltWriteFailed`, `ErrSnapshotOpenFailed` |
| `SnapshotReadError` | Unable to read/import a snapshot (corrupted file, exceeds safety caps, or default snapshot path resolution failed). | `ErrSnapshotReadFailed`, `ErrSnapshotPathResolveFailed` |
| `SnapshotKeyMissingError` | Snapshot missing an expected key during import. | `ErrSnapshotKeyMissing` |
| `DiskPathError` | Resolving or creating the disk backend path failed (bad path, permissions, or path points to a directory). | `ErrDiskPathResolveFailed`, `ErrDiskDirectoryCreateFailed`, `ErrDiskPathIsDirectory` |

#### Disk backend operations

| err.name | Trigger | Go sentinels |
| --- | --- | --- |
| `DiskStoreOpenError` | Could not open bbolt (usually due to file locks or permissions). | `ErrDiskStoreOpenFailed` |
| `DiskStoreReadError` | bbolt read transaction failed. | `ErrDiskStoreReadFailed` |
| `DiskStoreWriteError` | Write operations failed (set, increment, swap, CAS). All share same recovery: check disk permissions/space. | `ErrDiskStoreWriteFailed`, `ErrDiskStoreIncrementFailed`, `ErrDiskStoreGetOrSetFailed`, `ErrDiskStoreSwapFailed`, `ErrDiskStoreCompareSwapFailed` |
| `DiskStoreDeleteError` | Delete operations failed (delete, deleteIfExists, compareAndDelete, clear). | `ErrDiskStoreDeleteFailed`, `ErrDiskStoreDeleteIfExistsFailed`, `ErrDiskStoreCompareDeleteFailed`, `ErrDiskStoreClearFailed` |
| `DiskStoreExistsError` | `exists()` check failed to execute. | `ErrDiskStoreExistsFailed` |
| `DiskStoreScanError` | `scan()`/`list()` failed due to bbolt cursor issues. | `ErrDiskStoreScanFailed` |
| `DiskStoreSizeError` | Size or count queries failed (grouped because recovery is identical: check DB health). | `ErrDiskStoreSizeFailed`, `ErrDiskStoreCountFailed`, `ErrDiskStoreStatFailed` |
| `DiskStoreIndexError` | Random-key lookups by index failed. | `ErrDiskStoreRandomAccessFailed` |
| `KeyListRebuildError` | Rebuilding the in-memory index failed (explicitly via `rebuildKeyList()` or implicitly after restore). | `ErrDiskStoreRebuildKeysFailed`, `ErrKeyListRebuildFailed` |
| `BucketNotFoundError` | Asked bbolt for a non-existent bucket (usually indicates bad path or corrupted DB). | `ErrBucketNotFound` |

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
