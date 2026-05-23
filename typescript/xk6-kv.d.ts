// Type definitions for xk6-kv
// Project: https://github.com/oshokin/xk6-kv
// Definitions by: xk6-kv contributors

/**
 * xk6-kv provides a persistent key-value store for sharing state across
 * Virtual Users (VUs) during k6 load testing.
 *
 * @example
 * ```javascript
 * import { openKv } from 'k6/x/kv';
 *
 * const kv = openKv({ backend: 'memory' });
 *
 * export default async function() {
 *   await kv.set('counter', 0);
 *   const count = await kv.incrementBy('counter', 1);
 *   console.log(`Counter: ${count}`);
 * }
 * ```
 */
declare module 'k6/x/kv' {
  /**
   * Storage backend type.
   * - `"memory"`: Fast, ephemeral storage shared across VUs (lost between runs)
   * - `"disk"`: Persistent bbolt-based storage (survives between runs)
   */
  export type Backend = 'memory' | 'disk';

  /**
   * Value serialization method.
   * - `"json"`: Values are JSON-encoded/decoded automatically (default)
   * - `"string"`: Strings are stored as-is; non-string values are coerced with Go `%v` formatting.
   *
   * Use `"json"` for structured objects and arrays. `"string"` mode is not JSON and
   * does not preserve object/array shape for round-trips.
   */
  export type Serialization = 'json' | 'string';

  /**
   * Options for opening a key-value store.
   * Must be called in the init context (outside default/setup/teardown functions).
   */
  export interface OpenKvOptions {
    /**
     * Storage backend to use.
     * @default "disk"
     */
    backend?: Backend;

    /**
     * Path to the bbolt file (disk backend only).
     * Ignored when backend is "memory".
     * @default "./.k6.kv"
     */
    path?: string;

    /**
     * Value serialization method.
     *
     * Warning: `"string"` mode is coercive. Non-string values are formatted with
     * Go `%v`, so objects and arrays do not round-trip as JSON structures.
     * Use `"json"` for structured values.
     *
     * @default "json"
     */
    serialization?: Serialization;

    /**
     * Enable in-memory key indexing for O(1) randomKey() performance.
     * When enabled:
     * - randomKey() without prefix -> O(1)
     * - randomKey() with prefix -> O(log n)
     * When disabled:
     * - randomKey() uses two-pass scan (fine for small-to-medium sets)
     * @default false
     */
    trackKeys?: boolean;

    /**
     * Memory backend specific settings.
     */
    memory?: MemoryOptions;

    /**
     * Disk backend specific settings.
     */
    disk?: DiskOptions;

    /**
     * Optional extension-emitted k6 metrics configuration.
     */
    metrics?: OpenKvMetricsOptions;
  }

  /**
   * Optional metrics configuration for openKv().
   */
  export interface OpenKvMetricsOptions {
    /**
     * Enable automatic per-operation metrics.
     * Emits:
     * - xk6_kv_operations_total
     * - xk6_kv_operation_duration
     * - xk6_kv_operation_failed
     * - xk6_kv_errors_total
     * - xk6_kv_empty_result
     * - xk6_kv_async_in_flight
     * @default false
     */
    operations?: boolean;
  }

  /**
   * Memory backend configuration.
   */
  export interface MemoryOptions {
    /**
     * Number of shards for the memory backend.
     * - If <= 0 or omitted: defaults to runtime.NumCPU() (automatic, recommended).
     * - If > 65536: automatically capped at 65536 (MaxShardCount).
     *
     * Sharding improves concurrent performance by reducing lock contention.
     * On high-core systems (e.g., AMD Ryzen 9 9950X with 32 cores),
     * automatic sharding delivers 3.5x faster writes and 2x faster reads.
     *
     * @default 0 (auto-detect based on CPU count).
     */
    shardCount?: number;
  }

  /**
   * Disk backend configuration (subset of bbolt options).
   */
  export interface DiskOptions {
    /**
     * How long to wait for the file lock.
     * - number: milliseconds (e.g., 250, 1000).
     * - string: Go duration with units like "ms", "s", "m", "h" (e.g., "500ms", "1s", "2m", "1h15m").
     */
    timeout?: number | string;
    /**
     * Skip fsync on each commit. Improves throughput but risks recent data on crash.
     */
    noSync?: boolean;
    /**
     * Skip fsync when the file grows. Performance vs durability trade-off.
     */
    noGrowSync?: boolean;
    /**
     * Rebuild freelist on open instead of syncing it. Faster writes; slower recovery.
     */
    noFreelistSync?: boolean;
    /**
     * Preload freelist into memory on open. Uses more RAM, speeds subsequent writes.
     */
    preLoadFreelist?: boolean;
    /**
     * Freelist representation.
     * - "array" (default)
     * - "map" for large/fragmented DBs
     */
    freelistType?: '' | 'array' | 'map';
    /**
     * Open database read-only.
     * Mutating APIs reject with `StoreReadOnlyError`.
     */
    readOnly?: boolean;
    /**
     * Initial mmap size in bytes. 
     * Number = raw bytes. String supports human-friendly suffixes:
     * - Decimal: KB/MB/GB/TB/PB/EB/ZB/YB (e.g., "64MB" = 64_000_000).
     * - Binary: KiB/MiB/GiB/TiB/PiB/EiB/ZiB/YiB (e.g., "64MiB" = 67_108_864).
     * Other examples: "1GiB", "512MiB", "1GB", "4TB", "256KiB".
     * Use 0 to keep bbolt default (no preallocation).
     */
    initialMmapSize?: number | string;
    /**
     * Attempt to mlock the database pages (UNIX only).
     */
    mlock?: boolean;
  }

  /**
   * Options for listing entries.
   */
  export interface ListOptions {
    /**
     * Filter by key prefix. Only keys starting with this string are returned.
     */
    prefix?: string;

    /**
     * Maximum number of results to return.
     * If <= 0 or omitted, returns all matching entries.
     * Positive values must be <= 250,000.
     */
    limit?: number;
  }

  /**
   * Options for listing key names without cloning, deserializing, or returning values.
   */
  export interface ListKeysOptions {
    /**
     * Optional prefix filter.
     * Empty or omitted prefix means all user keys.
     */
    prefix?: string;

    /**
     * Maximum number of keys to return.
     * If omitted or <= 0, all matching keys are returned.
     * Positive values must be <= 250,000.
     */
    limit?: number;
  }

  /**
   * Options for bounded destructive prefix deletion.
   */
  export interface DeleteByPrefixOptions {
    /**
     * Required non-empty key prefix.
     */
    prefix: string;

    /**
     * Required positive integer.
     * Bounds the number of keys deleted by one call.
     * Must be <= 100,000.
     */
    limit: number;
  }

  /**
   * Options for scanning entries with cursor-based pagination.
   */
  export interface ScanOptions {
    /**
     * Filter by key prefix. Only keys starting with this string are returned.
     */
    prefix?: string;

    /**
     * Maximum number of results per page.
     * If <= 0, returns all matching entries (behaves like list()).
     * Positive values must be <= 100,000.
     */
    limit?: number;

    /**
     * Opaque continuation token from the previous page.
     * - Pass empty string or omit to start a new scan.
     * - Pass the cursor from the previous ScanResult to continue.
     * - Reuse only with the same logical scan options (especially prefix).
     * - Pagination is not a long-lived snapshot; concurrent writes may affect later pages.
     */
    cursor?: string;
  }

  /**
   * Options for scanning key names with cursor-based pagination.
   */
  export interface ScanKeysOptions {
    /**
     * Filter by key prefix. Only keys starting with this string are returned.
     */
    prefix?: string;

    /**
     * Maximum number of keys per page.
     * If <= 0, returns all matching keys.
     * Positive values must be <= 100,000.
     */
    limit?: number;

    /**
     * Opaque continuation token from the previous page.
     * - Pass empty string or omit to start a new scan.
     * - Pass the cursor from the previous ScanKeysResult to continue.
     * - Reuse only with the same logical scan options (especially prefix).
     * - Pagination is not a long-lived snapshot; concurrent writes may affect later pages.
     */
    cursor?: string;
  }

  /**
   * Options for random key selection.
   */
  export interface RandomKeyOptions {
    /**
     * Filter by key prefix. Only keys starting with this string are considered.
     */
    prefix?: string;
  }

  /**
   * Options for random key batch selection.
   */
  export interface RandomKeysOptions {
    /**
     * Filter by key prefix. Only keys starting with this string are considered.
     */
    prefix?: string;

    /**
     * Number of keys to return.
     * Must be a positive integer less than or equal to 1,000,000.
     */
    count: number;

    /**
     * Whether returned keys must be unique.
     * Defaults to true.
     */
    unique?: boolean;
  }

  /**
   * Options for counting keys.
   */
  export interface CountOptions {
    /**
     * Filter by key prefix. Only keys starting with this string are counted.
     */
    prefix?: string;
  }

  /**
   * Options for backing up a snapshot.
   */
  export interface BackupOptions {
    /**
     * Destination path for the bbolt snapshot file.
     * Required unless you intentionally want to overwrite the backend's live bbolt file.
     */
    fileName?: string;

    /**
     * When true, allow writes to proceed during backup (best-effort consistency).
     * When false (default), writes are blocked for strict point-in-time consistency.
     */
    allowConcurrentWrites?: boolean;
  }

  /**
   * Summary returned by backup().
   */
  export interface BackupSummary {
    /** Number of entries captured in the snapshot. */
    totalEntries: number;
    /** On-disk size of the snapshot file in bytes. */
    bytesWritten: number;
    /** True when AllowConcurrentWrites was enabled (best-effort snapshot). */
    bestEffort: boolean;
    /** Warning message describing best-effort semantics (present only when bestEffort=true). */
    warning?: string;
  }

  /**
   * Options for restoring a snapshot.
   */
  export interface RestoreOptions {
    /**
     * Path to the bbolt snapshot file produced by backup().
     * Required unless you intentionally want to restore from the backend's live bbolt file.
     */
    fileName?: string;

    /**
     * Maximum number of entries allowed during import.
     * Use to guard against unexpected snapshot sizes. <= 0 disables the guard.
     */
    maxEntries?: number;

    /**
     * Maximum total key/value payload (bytes) allowed during import.
     * <= 0 disables the guard.
     */
    maxBytes?: number;
  }

  /**
   * Summary returned by restore().
   */
  export interface RestoreSummary {
    /** Number of entries hydrated from the snapshot. */
    totalEntries: number;
  }

  /**
   * Options for exporting key/value entries to JSON Lines.
   * Scan-based export, not a point-in-time snapshot.
   * Concurrent writes/deletes may affect later pages.
   * Use backup() for snapshot-style capture.
   */
  export interface ExportJSONLOptions {
    /**
     * Required output JSONL file path.
     */
    fileName: string;

    /**
     * Optional key prefix filter.
     * Empty or omitted prefix means all keys.
     */
    prefix?: string;

    /**
     * Optional maximum number of entries to export.
     * If omitted or <= 0, all matching entries are exported.
     * Positive values must be <= 1,000,000.
     */
    limit?: number;
  }

  /**
   * Summary returned by exportJSONL().
   */
  export interface ExportJSONLResult {
    /** Number of entries written to the JSONL file. */
    exported: number;
    /** Final output file path. */
    fileName: string;
    /** Final file size in bytes. */
    bytesWritten: number;
  }

  /**
   * Options for exporting tabular object data to CSV.
   * Scan-based export, not a point-in-time snapshot.
   * Concurrent writes/deletes may affect later pages.
   * Use backup() for snapshot-style capture.
   *
   * Contract:
   * - each exported value must decode to a JSON object row;
   * - only top-level scalar fields are supported;
   * - missing fields are emitted as empty cells;
   * - nested objects/arrays and scalar store values are rejected.
   * - stores opened with serialization: "string" are generally incompatible
   *   because values decode as strings, not JSON object rows.
   * - nested path selectors (for example "user.id") are not supported.
   * - no flattening, transforms, schema inference, or nested JSON stringification.
   *
   * CSV is a flat text format and does not preserve JSON value types.
   * For example, number/boolean cells round-trip through importCSV() as strings.
   *
   * Use exportJSONL() for type-preserving roundtrips, scalar/string payloads,
   * or nested JSON structures.
   */
  export interface ExportCSVOptions {
    /** Required output CSV file path. */
    fileName: string;
    /**
     * Optional key prefix filter.
     * Empty or omitted prefix means all keys.
     */
    prefix?: string;
    /**
     * Optional maximum number of entries to export.
     * If omitted or <= 0, all matching entries are exported.
     * Positive values must be <= 1,000,000.
     */
    limit?: number;
    /**
     * Optional CSV delimiter.
     * Must be exactly one character and must not be "\r", "\n", "\"", or "\uFFFD".
     * @default ","
     */
    delimiter?: string;
    /**
     * Required list of top-level object fields to export.
     * Columns must be non-empty and unique.
     * Column names are exact; leading/trailing whitespace is significant.
     * When includeKey=true, columns must not contain "key"
     * because exportCSV() writes the store key as the first CSV column.
     */
    columns: string[];
    /**
     * Include the key as the first column.
     * Set includeKey=false if you need to export a value field named "key".
     * @default true
     */
    includeKey?: boolean;
  }

  /**
   * Summary returned by exportCSV().
   */
  export interface ExportCSVResult {
    /** Number of entries written to the CSV file. */
    exported: number;
    /** Final output file path. */
    fileName: string;
    /** Final file size in bytes. */
    bytesWritten: number;
  }

  /**
   * Options for importing key/value entries from JSON Lines.
   */
  export interface ImportJSONLOptions {
    /**
     * Required input JSONL file path.
     */
    fileName: string;

    /**
     * Optional maximum number of records to import.
     * If omitted or <= 0, all records are imported.
     * Positive values must be <= 1,000,000.
     */
    limit?: number;

    /**
     * Optional number of records to write per SetMany batch.
     * If omitted or <= 0, the default batch size is used.
     * Positive values must be <= 10,000.
     */
    batchSize?: number;
  }

  /**
   * Summary returned by importJSONL().
   */
  export interface ImportJSONLResult {
    /** Number of records imported into KV. */
    imported: number;
    /** Input JSONL file path. */
    fileName: string;
    /** Number of input bytes consumed by importer. */
    bytesRead: number;
  }

  /**
   * Options for importing key/value entries from CSV.
   */
  export interface ImportCSVOptions {
    /** Required input CSV file path. */
    fileName: string;
    /**
     * Required key column name (when hasHeader=true) or zero-based column index as string
     * (when hasHeader=false).
     *
     * Row-width handling is lenient:
     * - missing columns are treated as empty strings;
     * - extra columns are preserved as generated column_N fields.
     */
    keyColumn: string;
    /**
     * Optional CSV delimiter.
     * Must be exactly one character and must not be "\r", "\n", "\"", or "\uFFFD".
     * @default ","
     */
    delimiter?: string;
    /** Whether the first row is a header row. @default true */
    hasHeader?: boolean;
    /**
     * Optional maximum number of rows to import.
     * If omitted or 0, all rows are imported.
     * Negative values are rejected.
     * Positive values must be <= 1,000,000.
     */
    limit?: number;
    /**
     * Optional number of rows per SetMany batch.
     * If omitted or 0, the default batch size is used.
     * Negative values are rejected.
     * Positive values must be <= 10,000.
     */
    batchSize?: number;
  }

  /**
   * Summary returned by importCSV().
   */
  export interface ImportCSVResult {
    /** Number of rows imported into KV. */
    imported: number;
    /** Input CSV file path. */
    fileName: string;
    /** Number of input bytes consumed by importer. */
    bytesRead: number;
  }

  /**
   * Options for validating a CSV seed file without importing it.
   *
   * Two modes:
   * - Syntax mode (keyColumn omitted): checks CSV readability/header shape only.
   * - Import-shape mode (keyColumn provided): also validates importCSV-compatible key extraction.
   */
  export interface ValidateCSVOptions {
    /** Required input CSV file path. */
    fileName: string;
    /**
     * Optional key column name/index validation.
     * - hasHeader=true: header column name.
     * - hasHeader=false: zero-based column index encoded as string.
     */
    keyColumn?: string;
    /**
     * Optional CSV delimiter.
     * Must be exactly one character and must not be "\r", "\n", "\"", or "\uFFFD".
     * @default ","
     */
    delimiter?: string;
    /** Whether the first row is a header row. @default true */
    hasHeader?: boolean;
    /**
     * Optional maximum number of rows to validate.
     * If omitted or <= 0, validates all data rows.
     * If > 0, only the first N data rows are inspected.
     */
    limit?: number;
  }

  /**
   * Summary returned by validateCSV().
   */
  export interface ValidateCSVResult {
    /**
     * True when inspected rows satisfy the selected validation mode.
     * Treat this as full-file validity only when checkedAll=true.
     */
    valid: boolean;
    /** Number of rows validated before stop/error (0 is valid for empty files). */
    rows: number;
    /**
     * Diagnostic count of bytes read from the file while validating.
     * This is not a stable resume/cursor offset.
     */
    bytesRead: number;
    /**
     * True when validation reached EOF without being stopped by limit.
     * False when validation stopped because limit was reached.
     * When false, valid=true applies only to the inspected prefix.
     */
    checkedAll: boolean;
    /** First parse/validation error when valid=false. */
    firstError?: {
      row: number;
      name: string;
      message: string;
    };
  }

  /**
   * Options for validating a JSONL seed file without importing it.
   */
  export interface ValidateJSONLOptions {
    /** Required input JSONL file path. */
    fileName: string;
    /**
     * Optional maximum number of records to validate.
     * If omitted or <= 0, validates all records.
     * If > 0, only the first N records are inspected.
     */
    limit?: number;
  }

  /**
   * Summary returned by validateJSONL().
   */
  export interface ValidateJSONLResult {
    /**
     * True when inspected records are valid JSONL import payloads.
     * Treat this as full-file validity only when checkedAll=true.
     */
    valid: boolean;
    /** Number of records validated before stop/error (0 is valid for empty files). */
    records: number;
    /**
     * Diagnostic count of bytes read from the file while validating.
     * This is not a stable resume/cursor offset.
     */
    bytesRead: number;
    /**
     * True when validation reached EOF without being stopped by limit.
     * False when validation stopped because limit was reached.
     * When false, valid=true applies only to the inspected prefix.
     */
    checkedAll: boolean;
    /** First parse/validation error when valid=false. */
    firstError?: {
      line: number;
      name: string;
      message: string;
    };
  }

  /**
   * Diagnostic snapshot returned by stats().
   */
  export interface KVStats {
    /** Active backend implementation. */
    backend: Backend;
    /** Active serialization mode. */
    serialization: Serialization;
    /** Whether key tracking indexes are enabled. */
    trackKeys: boolean;
    /** Current number of user keys. */
    count: number;
    /** Current claim counters. */
    claims: {
      live: number;
      expired: number;
    };
    /** Index counters and consistency (present when trackKeys=true, otherwise null/omitted). */
    index?: {
      enabled: boolean;
      keysList?: number;
      keysMap?: number;
      ost?: number;
      consistent: boolean;
    } | null;
    /** Disk backend details (present for disk backend, otherwise null/omitted). */
    disk?: {
      path: string;
      sizeBytes: number;
      readOnly?: boolean;
    } | null;
  }

  /**
   * Options for allocationStats() prefix-scoped diagnostics.
   */
  export interface AllocationStatsOptions {
    /** Optional key prefix filter. Empty or omitted means all keys. */
    prefix?: string;
  }

  /**
   * Prefix-scoped allocation snapshot returned by allocationStats().
   * This is an on-demand diagnostic helper, not a hot-path control-plane API.
   * It is a pool-availability diagnostic, not a low-level bbolt consistency checker.
   * Use stats()/reportStats() for global store health; use allocationStats({ prefix })
   * for prefix pool health.
   *
   * Disk backend note:
   * - with trackKeys=false, this scans durable bbolt keys matching prefix;
   * - with trackKeys=true, this reports the process-local tracked allocation
   *   index view used by claim APIs (operational claimability view);
   * - it does not rescan bbolt on every call;
   * - use stats().index.consistent and rebuildKeyList() for index
   *   consistency diagnostics/repair after out-of-band durable mutations
   *   before trusting prefix totals.
   * This is not a forensic rescan of durable bbolt truth.
   */
  export interface AllocationStats {
    /** Prefix used for this snapshot. */
    prefix: string;
    /**
     * Number of keys matching prefix.
     *
     * For disk + trackKeys=true this is the operational allocation-index view
     * used by claim APIs, not a forensic durable bbolt rescan after out-of-band
     * database mutations.
     */
    total: number;
    /**
     * Number of matching keys claimable now.
     * Includes keys blocked only by expired claims.
     */
    claimable: number;
    /** Number of matching keys blocked by live claims. */
    claimedLive: number;
    /** Number of matching keys with expired claims (also counted as claimable). */
    claimedExpired: number;
    /** Active backend implementation. */
    backend: Backend;
    /** Whether key tracking indexes are enabled. */
    trackKeys: boolean;
  }

  /**
   * A single key-value entry.
   */
  export interface Entry {
    /** The entry key */
    key: string;
    /** The stored value (type depends on serialization) */
    value: any;
  }

  /**
   * Options for popRandom().
   */
  export interface PopRandomOptions {
    /**
     * Filter by key prefix. Only keys starting with this string are considered.
     */
    prefix?: string;
  }

  /**
   * Options for popRandomMany().
   */
  export interface PopRandomManyOptions {
    /**
     * Filter by key prefix. Only keys starting with this string are considered.
     */
    prefix?: string;
    /**
     * Number of entries to return.
     * Must be a positive integer less than or equal to 1,000,000.
     */
    count: number;
  }

  /**
   * Common options for claim allocation APIs.
   */
  export interface ClaimOptions {
    /**
     * Filter by key prefix. Only keys starting with this string are considered.
     */
    prefix?: string;
    /**
     * Optional claim owner for diagnostics (for example VU/scenario labels).
     * Must be less than or equal to 256 bytes.
     */
    owner?: string;
    /**
     * Lease duration in milliseconds.
     * Must be a positive integer less than or equal to 86,400,000 (24 hours).
     * @default 30000
     */
    ttl?: number;
  }

  /**
   * Options for claimRandom().
   */
  export interface ClaimRandomOptions extends ClaimOptions {}

  /**
   * Options for claimRandomMany().
   */
  export interface ClaimManyOptions extends ClaimOptions {
    /**
     * Number of entries to claim.
     * Must be a positive integer less than or equal to 1,000,000.
     */
    count: number;
  }

  /**
   * Claim reference used by releaseClaim()/completeClaim().
   */
  export interface ClaimRef {
    /** Claim identifier. */
    id: string;
    /** Claimed key. */
    key: string;
    /**
     * Fence token for stale-holder protection.
     *
     * Exposed as a JavaScript number; practical k6 runs should not approach
     * Number.MAX_SAFE_INTEGER for this token.
     */
    token: number;
  }

  /**
   * Active claim payload.
   */
  export interface Claim<T = any> extends ClaimRef {
    /** Claimed entry snapshot. */
    entry: Entry & { value: T };
    /** Optional diagnostic owner, capped at 256 bytes when the claim is created. */
    owner?: string;
    /** Expiration timestamp in Unix milliseconds. */
    expiresAt: number;
  }

  /**
   * Options for completeClaim().
   */
  export interface CompleteClaimOptions {
    /**
     * When true, remove the underlying key on successful completion.
     * @default true
     */
    deleteKey?: boolean;
  }

  /**
   * Options for renewClaim().
   */
  export interface RenewClaimOptions {
    /**
     * New lease duration in milliseconds.
     * Must be a positive integer less than or equal to 86,400,000 (24 hours).
     */
    ttl: number;
  }

  /**
   * Stable per-item batch claim lifecycle failure names.
   * "ClaimNotUpdated" means stale/missing/expired/not-owner claim state.
   */
  export type ClaimBatchFailureName = 'ClaimNotUpdated' | string;

  /**
   * Per-claim failure details returned by batch claim lifecycle helpers.
   */
  export interface ClaimBatchFailure {
    /** Zero-based position from the input claims array. */
    index: number;
    /** Claim identifier. */
    id: string;
    /** Claimed key. */
    key: string;
    /** Stable failure name ("ClaimNotUpdated" for stale/missing claim state). */
    name: ClaimBatchFailureName;
    /** Human-readable failure detail. */
    message: string;
  }

  /**
   * Summary returned by releaseClaims().
   * Stale/missing claim items are returned in failed[] with name "ClaimNotUpdated".
   * failed[].index points to the original input position.
   */
  export interface ReleaseClaimsResult {
    attempted: number;
    released: number;
    failed: ClaimBatchFailure[];
  }

  /**
   * Summary returned by completeClaims().
   * Stale/missing claim items are returned in failed[] with name "ClaimNotUpdated".
   * failed[].index points to the original input position.
   */
  export interface CompleteClaimsResult {
    attempted: number;
    completed: number;
    failed: ClaimBatchFailure[];
  }

  /**
   * Summary returned by renewClaims().
   * Stale/missing claim items are returned in failed[] with name "ClaimNotUpdated".
   * failed[].index points to the original input position.
   */
  export interface RenewClaimsResult {
    attempted: number;
    renewed: number;
    failed: ClaimBatchFailure[];
  }

  /**
   * Options for claimKeys().
   */
  export interface ClaimKeysOptions {
    /**
     * Optional claim owner for diagnostics (for example VU/scenario labels).
     * Must be less than or equal to 256 bytes.
     */
    owner?: string;
    /**
     * Lease duration in milliseconds.
     * Must be a positive integer less than or equal to 86,400,000 (24 hours).
     * @default 30000
     */
    ttl?: number;
    /**
     * When true, stop on first missing/busy key and attempt to rollback claims
     * acquired earlier in this call.
     *
     * This does not make claimKeys() transactional.
     * It only performs best-effort rollback for claims acquired by this call
     * when a later key is missing or busy.
     * Treat this as a best-effort cleanup helper, not a transaction.
     * Expired/missing rollback races do not reject by themselves.
     * Only technical store errors reject the promise.
     * @default false
     */
    allOrNothing?: boolean;
  }

  /**
   * Summary returned by claimKeys().
   * claimKeys() first attempts claimKey() and only when that returns null
   * runs a fallback exists() check to classify missing vs busy.
   * Under concurrent mutation this is not a transaction-level guarantee.
   * It is intended for deterministic fixture reservation in k6 scripts.
   */
  export interface ClaimKeysResult<T = any> {
    /** Successfully claimed entries. */
    claimed: Array<Claim<T>>;
    /** Requested keys that currently have live claims. */
    busy: string[];
    /** Requested keys that were not found. */
    missing: string[];
  }

  /**
   * Result of scan() operation with cursor-based pagination.
   */
  export interface ScanResult {
    /**
     * Page of key-value entries, sorted lexicographically by key.
     */
    entries: Entry[];

    /**
     * Opaque continuation token for the next page.
     * - Non-empty: more results available, pass this to next scan() call
     * - Empty: scan is complete
     */
    cursor: string;

    /**
     * True when the scan has reached the end (cursor is empty).
     */
    done: boolean;
  }

  /**
   * Result of scanKeys() operation with cursor-based pagination.
   */
  export interface ScanKeysResult {
    /**
     * Page of key names, sorted lexicographically.
     */
    keys: string[];

    /**
     * Opaque continuation token for the next page.
     * - Non-empty: more results available, pass this to next scanKeys() call
     * - Empty: scan is complete
     */
    cursor: string;

    /**
     * True when the scan has reached the end (cursor is empty).
     */
    done: boolean;
  }

  /**
   * Result of getOrSet() operation.
   */
  export interface GetOrSetResult<T = any> {
    /**
     * The value stored at the key (either existing or newly set).
     */
    value: T;

    /**
     * True if the value was already present (get), false if it was just set.
     */
    loaded: boolean;
  }

  /**
   * Result of swap() operation.
   */
  export interface SwapResult<T = any> {
    /**
     * The previous value if key existed, null otherwise.
     */
    previous: T | null;

    /**
     * True if the key existed and previous contains its old value.
     * False if the key was created and previous is null.
     */
    loaded: boolean;
  }

  /**
   * Result of setMany() operation.
   */
  export interface SetManyResult {
    /**
     * Number of entries written.
     */
    written: number;
  }

  /**
   * Result of deleteMany() operation.
   */
  export interface DeleteManyResult {
    /**
     * Number of requested keys that existed and were deleted.
     */
    deleted: number;

    /**
     * Number of requested keys that did not exist at deletion time.
     */
    missing: number;
  }

  /**
   * Result of deleteByPrefix() operation.
   */
  export interface DeleteByPrefixResult {
    /**
     * Number of keys physically deleted by this call.
     */
    deleted: number;

    /**
     * True when no matching keys remain after this call.
     */
    done: boolean;
  }

  /**
   * Per-key result item returned by getMany().
   */
  export interface GetManyItem<T = any> {
    /**
     * Requested key (same order/position as input).
     */
    key: string;
    /**
     * True when the key exists in the store.
     */
    exists: boolean;
    /**
     * Stored value when exists=true; null when missing or when stored value is JSON null.
     */
    value: T | null;
  }

  /**
   * Per-entry error details for batch operations.
   */
  export interface ManyEntryError {
    /**
     * Entry key associated with the failure.
     */
    key?: string;
    /**
     * Stable error name for programmatic handling.
     */
    name: string;
    /**
     * Human-readable failure message.
     */
    message: string;
  }

  /**
   * Stable top-level error names surfaced by rejected kv.* promises.
   */
  export type KVErrorName =
    | 'KeyNotFoundError'
    | 'InvalidCursorError'
    | 'InvalidBackendError'
    | 'InvalidOptionsError'
    | 'InvalidSerializationError'
    | 'InternalStoreError'
    | 'KVOptionsConflictError'
    | 'BackupOptionsRequiredError'
    | 'RestoreOptionsRequiredError'
    | 'MetricsUnavailableError'
    | 'SnapshotBudgetExceededError'
    | 'ValueNumberRequiredError'
    | 'UnsupportedValueTypeError'
    | 'ValueParseError'
    | 'SerializerError'
    | 'UnexpectedStoreOutputError'
    | 'UnknownError'
    | 'BackupInProgressError'
    | 'RestoreInProgressError'
    | 'OperationCanceledError'
    | 'StoreReadOnlyError'
    | 'StoreClosedError'
    | 'DatabaseNotOpenError'
    | 'SnapshotNotFoundError'
    | 'SnapshotPermissionError'
    | 'SnapshotExportError'
    | 'SnapshotIOError'
    | 'SnapshotReadError'
    | 'SnapshotKeyMissingError'
    | 'DiskPathError'
    | 'DiskStoreOpenError'
    | 'DiskStoreReadError'
    | 'DiskStoreWriteError'
    | 'DiskStoreDeleteError'
    | 'DiskStoreExistsError'
    | 'DiskStoreScanError'
    | 'DiskStoreSizeError'
    | 'DiskStoreIndexError'
    | 'KeyListRebuildError'
    | 'BucketNotFoundError';

  /**
   * Structured error payload rejected by kv.* promises.
   *
   * xk6-kv returns a plain object, not a JavaScript Error instance. Do not rely
   * on `err instanceof Error`; use `err.name` for machine-readable handling and
   * `err.message` for human-readable diagnostics.
   *
   * Batch APIs such as setMany() may include per-entry diagnostics in errors[].
   */
  export interface KVError {
    readonly name: KVErrorName;
    readonly message: string;
    readonly errors?: readonly ManyEntryError[];
  }

  /**
   * Options for compareAndSwapDetailed() and compareAndDeleteDetailed().
   */
  export interface CompareDetailedOptions {
    /**
     * When true and the compare fails while the key exists, include the stored
     * value as `current` in the result. Ignored when the operation succeeds.
     *
     * @default false
     */
    includeCurrentOnMismatch?: boolean;
  }

  /**
   * compareAndSwapDetailed() result when the compare matched and the new value was written.
   */
  export interface CompareAndSwapDetailedSuccess {
    /** True when newValue was applied. */
    swapped: true;
    /** Always `"swapped"` on success. */
    reason: 'swapped';
  }

  /**
   * compareAndSwapDetailed() result when oldValue did not match the stored value
   * or the key was absent when a value was expected.
   */
  export interface CompareAndSwapDetailedMismatch<T = any> {
    /** False when the compare did not succeed. */
    swapped: false;
    /** Always `"mismatch"` for this branch. */
    reason: 'mismatch';
    /** True if the key exists, false if it is absent. */
    existed: boolean;
    /**
     * The stored value when `includeCurrentOnMismatch` is true, the key exists,
     * and the compare failed. Omitted otherwise.
     */
    current?: T;
  }

  /**
   * Result of compareAndSwapDetailed(): success or mismatch with optional `current`.
   */
  export type CompareAndSwapDetailedResult<T = any> =
    | CompareAndSwapDetailedSuccess
    | CompareAndSwapDetailedMismatch<T>;

  /**
   * compareAndDeleteDetailed() result when the compare matched and the key was removed.
   */
  export interface CompareAndDeleteDetailedSuccess {
    /** True when the key was deleted. */
    deleted: true;
    /** Always `"deleted"` on success. */
    reason: 'deleted';
  }

  /**
   * compareAndDeleteDetailed() result when oldValue did not match the stored value
   * or the key was absent when a value was expected.
   */
  export interface CompareAndDeleteDetailedMismatch<T = any> {
    /** False when the key was not deleted. */
    deleted: false;
    /** Always `"mismatch"` for this branch. */
    reason: 'mismatch';
    /** True if the key exists, false if it is absent. */
    existed: boolean;
    /**
     * The stored value when `includeCurrentOnMismatch` is true, the key exists,
     * and the compare failed. Omitted otherwise.
     */
    current?: T;
  }

  /**
   * Result of compareAndDeleteDetailed(): success or mismatch with optional `current`.
   */
  export type CompareAndDeleteDetailedResult<T = any> =
    | CompareAndDeleteDetailedSuccess
    | CompareAndDeleteDetailedMismatch<T>;

  /**
   * Key-value store instance.
   * All methods are Promise-based except close() which is synchronous.
   */
  export interface KV {
    // ==================== Basic Operations ====================

    /**
     * Retrieves a value by key.
     *
     * @param key - The key to retrieve
     * @returns Promise that resolves to the stored value
     * @throws If key doesn't exist or database is not open
     *
     * @example
     * ```javascript
     * const value = await kv.get('myKey');
     * console.log(value);
     * ```
     */
    get(key: string): Promise<any>;

    /**
     * Reads many keys and returns per-key results in the same order as the input keys.
     *
     * Missing keys return { exists: false, value: null }.
     * Stored JSON null values return { exists: true, value: null }.
     *
     * Duplicate keys are allowed and produce duplicate values.
     * Empty-string keys are accepted for reads and typically resolve as missing.
     * Mutating APIs (`set`, `setMany`, `delete`, `deleteMany`) reject empty keys.
     */
    getMany<T = any>(keys: string[]): Promise<Array<GetManyItem<T>>>;

    /**
     * Sets a key-value pair.
     * Creates the key if absent, overwrites if present.
     *
     * @param key - The key to set (must be a non-empty string)
     * @param value - The value to store (must be JSON-serializable if serialization="json")
     * @returns Promise that resolves to the value that was set
     *
     * @example
     * ```javascript
     * await kv.set('user:1', { name: 'Alice', age: 30 });
     * ```
     */
    set(key: string, value: any): Promise<any>;

    /**
     * Sets many key-value pairs in one logical batch.
     *
     * The method validates input and serializes every value before mutating the store.
     * On any validation/serialization failure, it rejects and writes nothing.
     * Top-level rejection is `InvalidOptionsError`; per-entry diagnostics keep
     * precise names in `errors[]` (for example `SerializerError`).
     * This all-or-nothing guarantee applies to validation/serialization outcomes;
     * it is not a cross-key snapshot-isolation contract for concurrent readers on
     * the memory backend.
     * Rejected errors may include a stable `errors` array with `ManyEntryError` items.
     * Common per-entry names are `InvalidEntries`, `EmptyKey`, and `SerializerError`.
     *
     * @param entries - Object map of key/value pairs to write (keys must be non-empty strings)
     * @returns Promise that resolves to { written }
     */
    setMany<T = any>(entries: Record<string, T>): Promise<SetManyResult>;

    /**
     * Deletes many explicit non-empty keys.
     *
     * Missing keys are not errors and are counted in `missing`.
     * Duplicate keys are processed in input order.
     *
     * @param keys - Array of explicit keys to delete (every key must be a non-empty string)
     * @returns Promise that resolves to { deleted, missing }
     */
    deleteMany(keys: string[]): Promise<DeleteManyResult>;

    /**
     * Deletes up to `limit` keys whose names start with `prefix`.
     *
     * This is a destructive operation. `prefix` must be non-empty and `limit`
     * must be a positive integer.
     *
     * Use listKeys({ prefix, limit }) first when you want a read-only preview.
     *
     * @param options - Required bounded prefix-delete options
     * @returns Promise that resolves to { deleted, done }
     */
    deleteByPrefix(options: DeleteByPrefixOptions): Promise<DeleteByPrefixResult>;

    /**
     * Removes a key-value pair.
     * Always resolves successfully, even if key didn't exist.
     *
     * @param key - The key to delete
     * @returns Promise that resolves to true
     *
     * @example
     * ```javascript
     * await kv.delete('tempKey');
     * ```
     */
    delete(key: string): Promise<boolean>;

    /**
     * Checks if a key exists.
     *
     * @param key - The key to check
     * @returns Promise that resolves to true if key exists, false otherwise
     *
     * @example
     * ```javascript
     * if (await kv.exists('config')) {
     *   console.log('Config exists');
     * }
     * ```
     */
    exists(key: string): Promise<boolean>;

    /**
     * Removes all entries from the store.
     *
     * Always resolves to `true` (and rejects on error) so callers can treat
     * the returned value as a simple success confirmation.
     *
     * @returns Promise that resolves to `true` when the operation completes
     *
     * @example
     * ```javascript
     * const ok = await kv.clear();
     * if (ok) {
     *   console.log("Store cleared");
     * }
     * ```
     */
    clear(): Promise<boolean>;

    /**
     * Returns the current number of keys in the store.
     *
     * @returns Promise that resolves to the number of keys
     *
     * @example
     * ```javascript
     * const size = await kv.size();
     * console.log(`Store contains ${size} keys`);
     * ```
     */
    size(): Promise<number>;

    // ==================== Atomic Operations ====================

    /**
     * Atomically increments a numeric value by delta.
     * If the key doesn't exist, it's treated as 0.
     *
     * @param key - The key to increment
     * @param delta - The amount to add (can be negative)
     * @returns Promise that resolves to the new value
     * @throws If existing value is not a number
     *
     * @example
     * ```javascript
     * await kv.incrementBy('counter', 5);  // Returns 5
     * await kv.incrementBy('counter', -2); // Returns 3
     * ```
     */
    incrementBy(key: string, delta: number): Promise<number>;

    /**
     * Atomically gets the existing value if present, or sets it if absent.
     *
     * @param key - The key to get or set
     * @param value - The value to set if key doesn't exist
     * @returns Promise that resolves to { value, loaded }
     *
     * @example
     * ```javascript
     * const { value, loaded } = await kv.getOrSet('config', { theme: 'dark' });
     * if (loaded) {
     *   console.log('Config already existed');
     * } else {
     *   console.log('Config was just created');
     * }
     * ```
     */
    getOrSet<T = any>(key: string, value: T): Promise<GetOrSetResult<T>>;

    /**
     * Atomically replaces the value at key with a new value.
     * Returns the previous value if key existed.
     *
     * @param key - The key to swap
     * @param value - The new value to set
     * @returns Promise that resolves to { previous, loaded }
     *
     * @example
     * ```javascript
     * const { previous, loaded } = await kv.swap('status', 'running');
     * if (loaded) {
     *   console.log(`Previous status: ${previous}`);
     * }
     * ```
     */
    swap<T = any>(key: string, value: T): Promise<SwapResult<T>>;

    /**
     * Atomically sets newValue only if current value equals oldValue.
     * This is a building block for local first-writer-wins coordination
     * among VUs sharing one xk6-kv process/store.
     * It is not a distributed lock service.
     *
     * Pass `null` (or leave undefined) for `oldValue` to express
     * "swap only if the key is currently absent" (set-if-not-exists semantics).
     *
     * @param key - The key to update
     * @param oldValue - The expected current value
     * @param newValue - The new value to set
     * @returns Promise that resolves to true if swap succeeded, false otherwise
     *
     * @example
     * ```javascript
     * // Implement local first-writer-wins coordination in one k6 process.
     * const acquired = await kv.compareAndSwap('coord:resource', null, myId);
     * if (acquired) {
     *   // This VU won the local coordination race
     * }
     * ```
     */
    compareAndSwap(key: string, oldValue: any, newValue: any): Promise<boolean>;

    /**
     * Atomically sets newValue only if current value equals oldValue, returning a
     * structured result for logging and incident triage.
     *
     * On success the result is `{ swapped: true, reason: "swapped" }` and never
     * includes `current`. On mismatch the result is
     * `{ swapped: false, reason: "mismatch", existed, current? }`; `current` is
     * present only when `includeCurrentOnMismatch` is true and the key exists.
     *
     * Pass `null` (or leave undefined) for `oldValue` to express
     * "swap only if the key is currently absent", same as compareAndSwap().
     *
     * @param key - The key to update
     * @param oldValue - The expected current value
     * @param newValue - The new value to set
     * @param options - Optional flags controlling mismatch payloads
     * @returns Promise that resolves to CompareAndSwapDetailedResult
     *
     * @example
     * ```javascript
     * const result = await kv.compareAndSwapDetailed('flag', prev, next, {
     *   includeCurrentOnMismatch: true
     * });
     * if (!result.swapped && result.existed && result.current) {
     *   console.log('Lost race; store has', result.current);
     * }
     * ```
     */
    compareAndSwapDetailed<T = any>(
      key: string,
      oldValue: any,
      newValue: any,
      options?: CompareDetailedOptions
    ): Promise<CompareAndSwapDetailedResult<T>>;

    /**
     * Atomically sets key only if it is currently absent.
     *
     * Equivalent to `compareAndSwap(key, null, value)` with a clearer name for
     * bootstrap and local coordination flows.
     *
     * @param key - The key to initialize
     * @param value - The value to store when the key is absent
     * @returns Promise that resolves to true if the value was inserted, false if the key already existed
     *
     * @example
     * ```javascript
     * const created = await kv.setIfAbsent('coord:job:1', workerId);
     * if (created) {
     *   // This worker won the local bootstrap race
     * }
     * ```
     */
    setIfAbsent(key: string, value: any): Promise<boolean>;

    /**
     * Deletes the key if it exists.
     * More informative than delete() which always returns true.
     *
     * @param key - The key to delete
     * @returns Promise that resolves to true if key was deleted, false if it didn't exist
     *
     * @example
     * ```javascript
     * const deleted = await kv.deleteIfExists('tempFile');
     * console.log(`File deleted: ${deleted}`);
     * ```
     */
    deleteIfExists(key: string): Promise<boolean>;

    /**
     * Atomically deletes the key only if current value equals oldValue.
     * Useful for "delete only if not processed" scenarios.
     *
     * @param key - The key to delete
     * @param oldValue - The expected current value
     * @returns Promise that resolves to true if key was deleted, false otherwise
     *
     * @example
     * ```javascript
     * const deleted = await kv.compareAndDelete('job:123', 'failed');
     * if (deleted) {
     *   console.log('Failed job removed');
     * }
     * ```
     */
    compareAndDelete(key: string, oldValue: any): Promise<boolean>;

    /**
     * Atomically deletes the key only if current value equals oldValue, returning a
     * structured result for logging and incident triage.
     *
     * On success the result is `{ deleted: true, reason: "deleted" }` and never
     * includes `current`. On mismatch the result is
     * `{ deleted: false, reason: "mismatch", existed, current? }`; `current` is
     * present only when `includeCurrentOnMismatch` is true and the key exists.
     *
     * Unlike compareAndSwap(), `null` and `undefined` for `oldValue` are normal
     * expected values compared through the configured serializer (for example
     * JSON null), not an absent-key sentinel.
     *
     * @param key - The key to delete
     * @param oldValue - The expected current value
     * @param options - Optional flags controlling mismatch payloads
     * @returns Promise that resolves to CompareAndDeleteDetailedResult
     *
     * @example
     * ```javascript
     * const result = await kv.compareAndDeleteDetailed('session:1', expected, {
     *   includeCurrentOnMismatch: true
     * });
     * if (result.deleted) {
     *   console.log('Session removed');
     * }
     * ```
     */
    compareAndDeleteDetailed<T = any>(
      key: string,
      oldValue: any,
      options?: CompareDetailedOptions
    ): Promise<CompareAndDeleteDetailedResult<T>>;

    // ==================== Query Operations ====================

    /**
     * Streams entries using cursor-based pagination.
     * Keys are returned in lexicographic order.
     *
     * Use this when the keyspace is too large to materialize with list()
     * or when you need restart-safe pagination.
     * Treat cursor as an opaque continuation token; do not parse or construct it manually.
     * Pagination is not a long-lived snapshot: if keys are inserted or deleted
     * between page calls, later pages may reflect those changes.
     *
     * @param options - Optional filters (prefix, limit, cursor)
     * @returns Promise that resolves to { entries, cursor, done }
     *
     * @example
     * ```javascript
     * let cursor = '';
     * while (true) {
     *   const { entries, cursor: nextCursor, done } = await kv.scan({
     *     prefix: 'user:',
     *     limit: 100,
     *     cursor
     *   });
     *
     *   for (const entry of entries) {
     *     console.log(entry.key, entry.value);
     *   }
     *
     *   if (done) break;
     *   cursor = nextCursor;
     * }
     * ```
     */
    scan(options?: ScanOptions): Promise<ScanResult>;

    /**
     * Streams key names using cursor-based pagination.
     * Keys are returned in lexicographic order.
     *
     * This is the key-only equivalent of scan().
     * It does not clone, deserialize, or return values.
     * Pagination is not a long-lived snapshot: if keys are inserted or deleted
     * between page calls, later pages may reflect those changes.
     */
    scanKeys(options?: ScanKeysOptions): Promise<ScanKeysResult>;

    /**
     * Returns key-value entries sorted lexicographically by key.
     * If limit <= 0 or omitted, returns all matching entries.
     *
     * @param options - Optional filters (prefix, limit)
     * @returns Promise that resolves to array of { key, value } objects
     *
     * @example
     * ```javascript
     * // List all entries
     * const all = await kv.list();
     *
     * // List with prefix
     * const users = await kv.list({ prefix: 'user:' });
     *
     * // List with limit
     * const first10 = await kv.list({ limit: 10 });
     * ```
     */
    list(options?: ListOptions): Promise<Entry[]>;

    /**
     * Lists key names without returning values.
     *
     * Keys are returned in ascending lexicographic order.
     * This method is read-only and does not clone, deserialize, or return values.
     * This materializes matching keys in memory.
     * Use scanKeys() for cursor-based pagination over large keyspaces.
     */
    listKeys(options?: ListKeysOptions): Promise<string[]>;

    /**
     * Returns the number of keys that start with the provided prefix.
     *
     * - `count()` (or omitted options) is equivalent to `size()`
     * - Prefix matching is byte-wise and consistent with scan/list/randomKey
     *
     * @param options - Optional key prefix filter
     * @returns Promise that resolves to the number of matching keys
     *
     * @example
     * ```javascript
     * const usersCount = await kv.count({ prefix: 'user:' });
     * const totalCount = await kv.count();
     * ```
     */
    count(options?: CountOptions): Promise<number>;

    /**
     * Returns a random key, optionally filtered by prefix.
     * Resolves to empty string ("") when no key matches (including empty store).
     *
     * No-match is not an error. The promise may still reject for invalid options,
     * closed stores, backend I/O errors, or other technical failures.
     *
     * Performance:
     * - trackKeys=true:
     *   - disk backend: no prefix -> O(1), prefix -> O(log n)
     *   - memory backend: includes a small per-shard selection step; with prefix,
     *     per-shard range checks precede final rank/select.
     * - trackKeys=false -> scan-based path with linear cost in matching keys
     *
     * @param options - Optional prefix filter
     * @returns Promise that resolves to a random key or ""
     *
     * @example
     * ```javascript
     * // Random key from entire store
     * const anyKey = await kv.randomKey();
     *
     * // Random key with prefix
     * const userKey = await kv.randomKey({ prefix: 'user:' });
     * if (userKey) {
     *   console.log(`Selected user: ${userKey}`);
     * }
     * ```
     */
    randomKey(options?: RandomKeyOptions): Promise<string>;

    /**
     * Returns random key names matching an optional prefix.
     *
     * This method returns keys only. It does not clone, deserialize, or return values.
     *
     * `count` is required and must be in range [1, 1,000,000].
     *
     * `unique` defaults to true. When unique is true and fewer matching keys exist
     * than requested, all available matching keys are returned in random order.
     *
     * Performance:
     * - trackKeys=true: indexed sampling for small samples; memory first builds
     *   shard ranges, and disk may fall back to cursor scan for near-full unique samples.
     * - trackKeys=false: collects candidates via scanKeys() and samples in memory
     *   (linear in matching keys).
     *
     * Use claimRandom() or popRandom() when you need exclusive allocation.
     */
    randomKeys(options: RandomKeysOptions): Promise<string[]>;

    /**
     * Claims one random free matching entry and removes it.
     * Resolves to null when no matching entry exists.
     */
    popRandom<T = any>(options?: PopRandomOptions): Promise<(Entry & { value: T }) | null>;

    /**
     * Claims up to count random free matching entries, decodes them, and
     * completes each claim with deleteKey=true.
     *
     * If completion fails after some entries were already deleted,
     * completed deletes are not rolled back.
     * Remaining live claims are released best-effort.
     *
     * Resolves to [] when no matching entry exists.
     */
    popRandomMany<T = any>(options: PopRandomManyOptions): Promise<Array<Entry & { value: T }>>;

    /**
     * Leases a random matching free entry.
     * Resolves to null when no free entry exists.
     * If options.ttl is omitted, lease duration defaults to 30000ms (30 seconds).
     * The maximum lease duration is 86,400,000ms (24 hours).
     */
    claimRandom<T = any>(options?: ClaimRandomOptions): Promise<Claim<T> | null>;

    /**
     * Leases a specific key.
     * Resolves to null when the key is missing or already live-claimed.
     */
    claimKey<T = any>(key: string, options?: ClaimOptions): Promise<Claim<T> | null>;

    /**
     * Leases up to count unique random free matching entries from one store call.
     * Resolves to [] when no free entry exists.
     */
    claimRandomMany<T = any>(options: ClaimManyOptions): Promise<Array<Claim<T>>>;

    /**
     * Releases a live claim.
     * Returns true only when id/key/token match an active non-expired claim.
     */
    releaseClaim(claim: ClaimRef): Promise<boolean>;

    /**
     * Releases many claims in one call.
     * Sequential convenience helper over releaseClaim().
     * Returns partial success details and is not a cross-claim transaction.
     * Stale/missing claims are reported in failed[] with name "ClaimNotUpdated".
     * Technical storage errors reject the whole promise and may happen after
     * earlier items in the same batch were already applied. Rejection messages
     * include applied-progress context ("after releasing X of Y").
     */
    releaseClaims(claims: ClaimRef[]): Promise<ReleaseClaimsResult>;

    /**
     * Completes a live claim.
     * By default, completion also removes the underlying key.
     */
    completeClaim(claim: ClaimRef, options?: CompleteClaimOptions): Promise<boolean>;

    /**
     * Completes many claims in one call.
     * Sequential convenience helper over completeClaim().
     * Returns partial success details and is not a cross-claim transaction.
     * Stale/missing claims are reported in failed[] with name "ClaimNotUpdated".
     * Technical storage errors reject the whole promise and may happen after
     * earlier items in the same batch were already applied. Rejection messages
     * include applied-progress context ("after completing X of Y").
     */
    completeClaims(claims: ClaimRef[], options?: CompleteClaimOptions): Promise<CompleteClaimsResult>;

    /**
     * Extends a live claim lease without changing the claim token.
     * Returns false for stale, expired, missing claims, or claims that no longer own the key.
     */
    renewClaim(claim: ClaimRef, options: RenewClaimOptions): Promise<boolean>;

    /**
     * Extends many live claim leases in one call.
     * Sequential convenience helper over renewClaim().
     * Returns partial success details and is not a cross-claim transaction.
     * Stale/missing claims are reported in failed[] with name "ClaimNotUpdated".
     * Technical storage errors reject the whole promise and may happen after
     * earlier items in the same batch were already applied. Rejection messages
     * include applied-progress context ("after renewing X of Y").
     */
    renewClaims(claims: ClaimRef[], options: RenewClaimOptions): Promise<RenewClaimsResult>;

    /**
     * Leases explicit keys and reports claimed, busy, and missing sets.
     * With allOrNothing=true, this call stops on first missing/busy key and
     * only attempts best-effort rollback for claims acquired in this call
     * when a later key is missing or busy.
     * allOrNothing does not make claimKeys() transactional.
     * Use allOrNothing=false when you need a full diagnostic partition for all
     * requested keys.
     * Missing/busy classification is best-effort: claimKeys() first attempts
     * ClaimKey(), then uses fallback Exists() only for null-claim classification.
     * This is not a transaction-level guarantee under concurrent mutation.
     * A key can be reclassified if another operation mutates it between
     * claim attempt and fallback existence check.
     */
    claimKeys<T = any>(keys: string[], options?: ClaimKeysOptions): Promise<ClaimKeysResult<T>>;

    /**
     * Rebuilds in-memory key indexes from the underlying store.
     * Only useful when trackKeys=true.
     *
     * - Memory backend: typically unnecessary unless manual corruption occurred
     * - Disk backend: index is automatically rebuilt at startup; call this
     *   only when the DB is mutated out-of-band during the run
     *
     * The operation is O(n) over keys, so prefer running it in setup()
     * or infrequently between stages - not every iteration.
     *
     * @returns Promise that resolves to true when finished
     *
     * @example
     * ```javascript
     * export async function setup() {
     *   await kv.rebuildKeyList();
     * }
     * ```
     */
    rebuildKeyList(): Promise<boolean>;

    /**
     * Returns a structured snapshot of current store diagnostics.
     *
     * This call is intended for explicit observability/debugging checks.
     */
    stats(): Promise<KVStats>;

    /**
     * Returns prefix-scoped on-demand allocation diagnostics.
     * Intended for explicit pool-health checks, not per-iteration hot paths.
     * It is a pool-availability diagnostic, not a low-level bbolt consistency checker.
     * Disk backend with trackKeys=true uses the tracked allocation index view on
     * writable handles, and falls back to durable bbolt scan on read-only handles.
     * trackKeys=false uses bbolt prefix scan.
     * Expired claims are reported separately and are also claimable.
     * Prefix is intentionally excluded from metrics labels to avoid high cardinality.
     */
    allocationStats(options?: AllocationStatsOptions): Promise<AllocationStats>;

    /**
     * Emits current state gauges into k6 custom metrics.
     *
     * Call this explicitly at points where you want state samples.
     */
    reportStats(): Promise<void>;

    // ==================== Snapshot Operations ====================

    /**
     * Streams the current store contents into a bbolt snapshot on disk.
     * When the disk backend is asked to back up to its own DB path the call
     * succeeds but only returns metadata (no copy is produced).
     *
     * @param options - Destination path and concurrency mode.
     * @returns Promise that resolves to snapshot metadata.
     */
    backup(options?: BackupOptions): Promise<BackupSummary>;

    /**
     * Replaces the store contents with entries from a snapshot file created via backup().
     * Restoring from the disk backend's live DB path is treated as a no-op summary.
     *
     * @param options - Source snapshot path and optional safety limits.
     * @returns Promise that resolves to the number of entries imported.
     */
    restore(options?: RestoreOptions): Promise<RestoreSummary>;

    /**
     * Exports key/value entries to a JSON Lines file.
     *
     * Each line is a JSON object: { "key": string, "value": any }.
     * Values are exported after normal KV deserialization, not as raw backend bytes.
     * This is scan-based export, not a point-in-time snapshot.
     * Concurrent writes/deletes may affect later pages.
     * Use backup() for snapshot-style capture.
     *
     * @param options - Required export options.
     * @returns Promise that resolves to export summary metadata.
     */
    exportJSONL(options: ExportJSONLOptions): Promise<ExportJSONLResult>;

    /**
     * Exports key/value entries to a CSV file.
     *
     * CSV export is intentionally a flat object table:
     * - each value must decode to a JSON object;
     * - only requested top-level scalar fields are supported;
     * - missing fields become empty cells;
     * - nested object/array fields and scalar store values reject.
     * This is scan-based export, not a point-in-time snapshot.
     * Concurrent writes/deletes may affect later pages.
     * Use backup() for snapshot-style capture.
     *
     * Use exportJSONL() for scalar/string payloads or nested JSON structures.
     */
    exportCSV(options: ExportCSVOptions): Promise<ExportCSVResult>;

    /**
     * Imports key/value entries from a JSON Lines file.
     *
     * Each line must be a JSON object: { "key": string, "value": any }.
     * The import is streaming and batch-atomic, not file-atomic. Previously committed
     * batches are not rolled back if a later line is invalid. Rejection messages include
     * committed progress (records, bytes, and line context) without changing the error name.
     *
     * @param options - Required import options.
     * @returns Promise that resolves to import summary metadata.
     */
    importJSONL(options: ImportJSONLOptions): Promise<ImportJSONLResult>;

    /**
     * Imports key/value entries from a CSV file.
     *
     * Each row is imported as a value object. The key is taken from keyColumn.
     * When hasHeader=true, keyColumn is a header name; when hasHeader=false,
     * keyColumn must be a zero-based column index encoded as a string.
     * Row-width handling is lenient: missing cells are imported as empty strings,
     * and extra cells are preserved as generated column_N fields.
     */
    importCSV(options: ImportCSVOptions): Promise<ImportCSVResult>;

    /**
     * Validates CSV file content without writing to the store.
     *
     * Two modes:
     * - Syntax mode (keyColumn omitted): CSV readability/header shape checks.
     * - Import-shape mode (keyColumn provided): syntax checks + importCSV-style key extraction checks.
     *
     * Content errors resolve with { valid:false, firstError }.
     * Invalid options and file I/O errors reject.
     * Uses encoding/csv parsing compatible with importCSV().
     * Row-width handling is lenient (same as importCSV()):
     * missing cells are treated as empty strings, and extra cells are accepted.
     * If options.limit <= 0 or omitted, all data rows are inspected.
     * If options.limit > 0, only the first N data rows are inspected.
     * checkedAll=true means EOF was reached; checkedAll=false means limit stopped validation.
     * Contract: valid=true implies whole-file validity only when checkedAll=true.
     * Empty files are valid and return { valid:true, rows:0 }.
     * Validation APIs are KV-instance methods for API consistency and metrics collection.
     * They require an open KV handle (StoreClosedError after close()) but do not
     * read or write KV store contents.
     */
    validateCSV(options: ValidateCSVOptions): Promise<ValidateCSVResult>;

    /**
     * Validates JSONL file structure/import-shape without writing to the store.
     *
     * Content errors resolve with { valid:false, firstError }.
     * Invalid options and file I/O errors reject.
     * If options.limit <= 0 or omitted, all records are inspected.
     * If options.limit > 0, only the first N records are inspected.
     * checkedAll=true means EOF was reached; checkedAll=false means limit stopped validation.
     * Contract: valid=true implies whole-file validity only when checkedAll=true.
     * Empty files are valid and return { valid:true, records:0 }.
     * Validation APIs are KV-instance methods for API consistency and metrics collection.
     * They require an open KV handle (StoreClosedError after close()) but do not
     * read or write KV store contents.
     */
    validateJSONL(options: ValidateJSONLOptions): Promise<ValidateJSONLResult>;

    // ==================== Lifecycle ====================

    /**
     * Synchronously closes the underlying store and releases resources.
     * Should be called in teardown() when done with the store.
     *
     * - This KV handle becomes closed; further async calls reject with StoreClosedError
     * - Disk backend: when this is the last open handle, releases bbolt file handle and in-memory indexes
     * - Memory backend: no backend resources to release, but this handle still becomes closed
     * - Safe to call multiple times
     * - Reference-counted: shared store closes only when last close() is called
     *
     * @example
     * ```javascript
     * export function teardown() {
     *   kv.close();
     * }
     * ```
     */
    close(): void;
  }

  /**
   * Opens a key-value store with the specified options.
   * Must be called in the init context (outside default/setup/teardown functions).
   *
   * The first successful call initializes the shared store for all VUs.
   * Later calls must provide equivalent options; conflicting options throw
   * KVOptionsConflictError.
   *
   * @param options - Storage and serialization configuration
   * @returns KV store instance
   *
   * @example
   * ```javascript
   * import { openKv } from 'k6/x/kv';
   *
   * // Default: disk backend with JSON serialization
   * const kv = openKv();
   *
   * // In-memory backend with key tracking
   * const kv = openKv({
   *   backend: 'memory',
   *   trackKeys: true
   * });
   *
   * // Disk backend with custom path
   * const kv = openKv({
   *   backend: 'disk',
   *   path: './my-test.db'
   * });
   * ```
   */
  export function openKv(options?: OpenKvOptions): KV;
}

