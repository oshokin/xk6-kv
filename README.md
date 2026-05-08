# xk6-kv

[![Go Report Card](https://goreportcard.com/badge/github.com/oshokin/xk6-kv)](https://goreportcard.com/report/github.com/oshokin/xk6-kv)
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)

> **Fork Notice**  
> This project is a fork of [oleiade/xk6-kv](https://github.com/oleiade/xk6-kv), extended with **additional atomic primitives** and **random-key utilities**:
>
> - Atomic operations: `incrementBy`, `getOrSet`, `swap`, `compareAndSwap`, `deleteIfExists`, `compareAndDelete`.
> - Random keys: `randomKey()` plus batch `randomKeys()` for key-only sampling workflows.
> - Optional key tracking for faster random key sampling (disk & memory backends).
> - Disk backend path overrides for custom artifact persistence.
>
> All code is licensed under **GNU AGPL v3.0**.

A k6 extension providing a persistent key-value store to share state across Virtual Users (VUs). Supports both **in-memory** and **persistent bbolt** backends, deterministic `list()` ordering, and high-level atomic helpers designed for safe concurrent access in load tests.

## Features

- 🔒 **Thread-Safe**: Secure state sharing across Virtual Users
- 🔄 **Flexible Storage**: In-memory (ephemeral) or disk-based (persistent) backends
- 📊 **Atomic Operations**: Increment, compare-and-swap, and more
- 🎲 **Random Key Selection**: Uniform sampling with optional prefix filtering
- 🔍 **Key Tracking**: Optional faster random key access via in-memory indexing
- 🏷️ **Prefix Support**: Filter operations by key prefixes
- 📦 **Stable bbolt Bucket**: Disk backend always uses the `k6` bucket (**original upstream bug tied it to the DB path and could orphan data - now fixed**)
- 🧭 **Cursor Scanning**: Stream large datasets via `scan()` / `scanKeys()` with continuation tokens
- 📝 **Serialization**: JSON or string serialization
- 💾 **Snapshots**: Export/import bbolt files for backups and data seeding
- 📘 **TypeScript Support**: Full type declarations for IntelliSense and type safety
- ⚡ **High Performance**: Optimized for concurrent workloads
- 🔀 **Automatic Sharding**: Memory backend automatically shards data across CPU cores for 2-3.5x performance gains on multi-core systems
- 🧰 **bbolt tuning knobs**: Exposes timeout, noSync/noGrowSync/noFreelistSync, freelist type, preLoadFreelist, initial mmap size, and mlock so advanced users can dial back fsync overhead when durability trade-offs are acceptable

## Installation

### Option 1: Pre-built Binaries (Recommended)

Download k6 binaries with xk6-kv from the [Releases page](https://github.com/oshokin/xk6-kv/releases).

**Linux:**

```bash
curl -L https://github.com/oshokin/xk6-kv/releases/latest/download/k6-linux-amd64.tar.gz -o k6.tar.gz
tar -xzf k6.tar.gz && chmod +x k6
./k6 version
```

**macOS:**

```bash
curl -L https://github.com/oshokin/xk6-kv/releases/latest/download/k6-darwin-arm64.tar.gz -o k6.tar.gz
tar -xzf k6.tar.gz && chmod +x k6
./k6 version
```

**Windows (PowerShell):**

```powershell
Invoke-WebRequest -Uri "https://github.com/oshokin/xk6-kv/releases/latest/download/k6-windows-amd64.zip" -OutFile k6.zip
Expand-Archive -Path k6.zip -DestinationPath .
.\k6.exe version
```

### Option 2: Build from Source

1. Install [xk6](https://github.com/grafana/xk6):

```bash
go install go.k6.io/xk6/cmd/xk6@latest
```

1. Build k6 with xk6-kv:

```bash
# Latest version
xk6 build --with github.com/oshokin/xk6-kv@latest

# Specific version
xk6 build --with github.com/oshokin/xk6-kv@v1.3.6
```

1. Verify:

```bash
./k6 version
```

> **Requirements**: Go 1.25 or higher.

## Quick Start

```javascript
import { openKv } from "k6/x/kv";

const kv = openKv(); // Default: disk backend

export async function setup() {
    await kv.clear();
}

export default async function () {
    await kv.set("foo", "bar");
    await kv.set("user:1", { name: "Alice" });
    
    const key = await kv.randomKey({ prefix: "user:" });
    if (key) {
        console.log(`Random user: ${key}`);
    }
    
    const entries = await kv.list({ prefix: "user:" });
    console.log(`Found ${entries.length} users`);
}

export async function teardown() {
    kv.close();
}
```

> **Async note:** every `kv.*` helper returns a Promise. In k6 scripts you should mark your `setup`, `default`, or helper functions as `async` (or use `.then`) and `await` each call—otherwise errors are swallowed and the operation may never finish.
>
> **Concurrency note:** avoid unbounded `Promise.all()` with very large KV batches (for example, tens of thousands of `kv.set()` calls at once). Prefer `setMany()` for object-map writes, sequential `await`, or bounded concurrency.

Bounded concurrency helper:

```javascript
async function mapLimit(items, limit, fn) {
  const width = Math.max(1, Math.min(limit, items.length || 1));
  const workers = Array.from({ length: width }, async (_, worker) => {
    for (let i = worker; i < items.length; i += width) {
      await fn(items[i], i);
    }
  });

  await Promise.all(workers);
}
```

## Error Handling

Every rejected `kv.*` promise carries a **typed error object** with `name` and `message` fields. Check `err.name` (or `err.Name` when k6 serialises it as Go struct) instead of matching raw strings.

Batch APIs such as `setMany()` may also include a stable `err.errors` array. Each item in `err.errors` has this shape:

```typescript
{
  key?: string
  name: string
  message: string
}
```

General error handling example:

```javascript
try {
  await kv.backup({ fileName: "./snapshots/run.kv" });
} catch (err) {
  if (err?.name === "BackupInProgressError") {
    console.log("Another VU is already writing a snapshot—safe to ignore.");
  } else if (err?.name === "SnapshotPermissionError") {
    fail(`Backup path is not writable: ${err.message}`);
  } else {
    throw err;
  }
}
```

Batch error details example:

```javascript
try {
  await kv.setMany({
    "ok": { name: "Alice" },
    "bad": () => {}, // JSON serializer rejects functions
  });
} catch (err) {
  if (err?.name === "InvalidOptionsError") {
    for (const item of err.errors ?? []) {
      console.log(item.key, item.name, item.message);
    }
  } else {
    throw err;
  }
}
```

High-level categories:

- **Options & inputs** - typed guards such as `BackupOptionsRequiredError`, `ValueNumberRequiredError`, `UnsupportedValueTypeError`.
- **Concurrency & lifecycle** - e.g. `BackupInProgressError`, `RestoreInProgressError`, `StoreReadOnlyError`, `StoreClosedError`.
- **Disk & snapshot IO** - precise signals for path issues, permission problems, bbolt failures, or restore budget overruns.

📚 A complete catalogue with root causes and remediation tips lives in [`examples/README.md`](./examples/README.md#error-manual).

## TypeScript Support

Full TypeScript support with IntelliSense and type safety! Copy the [`typescript/`](./typescript/) folder to your project for a ready-to-use starter kit.\
See [`typescript/README.md`](./typescript/README.md) for complete setup instructions.
`typescript/` is a local starter example project (template), not a published npm package.

## API Reference

### `openKv(options?)`

Opens a key-value store. **Must be called in the init context** (outside of default/setup/teardown functions).
The returned KV handle is shared across VUs, so treat its lifecycle as test-wide.

> ⚠️ **Configuration lock-in:** the first successful `openKv()` call fixes the store configuration
> for the whole test run. All subsequent `openKv()` calls must provide equivalent options;
> otherwise `openKv()` throws `KVOptionsConflictError`.

```typescript
interface OpenKvOptions {
  backend?: "memory" | "disk"        // default: "disk"
  path?: string                      // default: "./.k6.kv" (disk only)
  serialization?: "json" | "string"  // default: "json"
  trackKeys?: boolean                // default: false
  memory?: {
    shardCount?: number              // default: 0 (auto-detect, memory only)
    // when omitted: defaults are applied
  }
  disk?: {
    timeout?: number | string         // wait for file lock; number=ms, string=Go duration (e.g. "1s"); default 1s
    noSync?: boolean                  // disable fsync on commit; default false
    noGrowSync?: boolean              // skip fsync on growth; default false
    noFreelistSync?: boolean          // rebuild freelist on open; default false
    preLoadFreelist?: boolean         // load freelist into memory; default false
    freelistType?: "" | "array" | "map" // freelist representation; default "array"
    readOnly?: boolean                // open DB read-only; mutating APIs reject with StoreReadOnlyError (requires pre-existing DB/bucket); default false
    initialMmapSize?: number | string // initial mmap size; number=bytes, string supports SI ("MB") and IEC ("MiB"); 0 keeps default/no preallocation (default)
    mlock?: boolean                   // mlock pages (UNIX); default false
    // when omitted: bbolt defaults are applied
  }
  metrics?: {
    operations?: boolean             // default: false
  }
}
```

**Options:**

- `backend`: `"memory"` (ephemeral, fastest) or `"disk"` (persistent bbolt)
- `serialization`: `"json"` (structured) or `"string"` (string/raw bytes)
- `trackKeys`: Enable in-memory key indexing for faster `randomKey()`/`randomKeys()` selection (see Performance & Complexity)
- `path`: (Disk only) Override bbolt file location
- `memory.shardCount`: (Memory only) Number of shards for concurrent performance. If `<= 0` or omitted, defaults to `runtime.NumCPU()` (automatic, recommended). If `> 65536`, automatically capped at 65536. Ignored by disk backend. When `memory` is omitted, defaults are applied.
- `disk`: (Disk only) Optional bbolt tuning. When `disk` is omitted, bbolt defaults apply (1s lock timeout, syncs enabled, array freelist, etc.).
- `disk.readOnly`: Requires the bbolt file (and `k6` bucket) to already exist; opening in read-only mode cannot create the bucket and will fail if the file is missing or empty.
- `metrics.operations`: Enables automatic per-method metrics (`xk6_kv_operations_total`, `xk6_kv_operation_duration`, `xk6_kv_operation_failed`, `xk6_kv_errors_total`, `xk6_kv_empty_result`).

**Note:** With `serialization: "string"`, string values are stored as-is. Non-string values are converted with Go `fmt` `%v` formatting (for example, an object can become `map[a:1]`). This mode is not JSON and is not intended for structured value round-trips; use `serialization: "json"` for objects/arrays.

```javascript
const kv = openKv({ serialization: 'string' });

await kv.set('x', { a: 1 });
console.log(await kv.get('x')); // Go-style string, not { a: 1 }
```

> ⚠️ **Snapshot path sharing:** If you omit `backup().fileName` or `restore().fileName`, the memory backend deliberately falls back to the same `.k6.kv` file the disk backend uses. This lets you run ultra-fast tests with `backend: "memory"` and then immediately replay the generated dataset via `backend: "disk"` without touching paths. If you *don't* want that coupling (for example, you run disk workloads concurrently), always pass an explicit `fileName`.

**Memory Backend Sharding:**

The memory backend shards data across multiple internal partitions to improve concurrent performance by reducing lock contention:

- **Automatic (recommended)**: Set `memory.shardCount: 0` (or omit) to auto-detect based on CPU count (e.g., 32 shards on a 32-core system)
- **Manual**: Set `memory.shardCount` to a specific value (1-65536) for fine-tuned control
- **Performance**: On high-core systems sharding delivers:
  - **3.5x faster** `set()` operations
  - **2x faster** `get()` operations
- **How it works**: Keys are distributed across shards using a hash function, allowing concurrent operations on different shards to proceed in parallel
- **Maximum**: Shard count is capped at 65536 (2^16) to provide excellent hash distribution while keeping memory overhead minimal (~5MB for empty shard structures)
- **Memory-only**: Sharding applies only to the `"memory"` backend; disk backend uses bbolt's transaction-based concurrency

### KV Methods

All methods return Promises except `close()`.

#### Basic Operations

- **`get(key: string): Promise<any>`** - Retrieves a value by key. Throws if key doesn't exist.
- **`getMany(keys: string[]): Promise<Array<{ key: string, exists: boolean, value: any | null }>>`** - Reads many keys in one logical batch and preserves input order. Missing keys return `{ exists: false, value: null }`; stored JSON `null` returns `{ exists: true, value: null }`; duplicate keys are allowed. Empty-string keys are accepted for reads and resolve like missing keys in normal public API usage.
  > `getMany()` backend note: disk reads run inside one bbolt read transaction; memory reads use per-shard locks and do not provide cross-key snapshot isolation under concurrent writes.
  > Mutating APIs (`set`, `setMany`, `delete`, `deleteMany`) reject empty-string keys.
- **`set(key: string, value: any): Promise<any>`** - Sets a key-value pair. Empty-string keys are rejected.
- **`setMany(entries: Record<string, any>): Promise<{ written: number }>`** - Writes an object map in one logical batch. Keys must be non-empty strings. Validates the input shape and serializes all values before writing; rejects with `err.errors` and writes nothing if any entry fails. `setMany()` provides all-or-nothing validation/serialization semantics, but is not intended to provide cross-key snapshot isolation for concurrent readers on the memory backend.
- **`deleteMany(keys: string[]): Promise<{ deleted: number, missing: number }>`** - Deletes an explicit list of non-empty keys. Missing keys are not errors and are counted in `missing`; duplicate keys are processed in input order. Rejects invalid input before deleting anything.
- **`deleteByPrefix(options: { prefix: string, limit: number }): Promise<{ deleted: number, done: boolean }>`** - Deletes up to `limit` keys matching a non-empty prefix. This operation is destructive and bounded by explicit limit.
- **`delete(key: string): Promise<boolean>`** - Removes a key-value pair (always resolves to `true`).
- **`exists(key: string): Promise<boolean>`** - Checks if a key exists.
- **`clear(): Promise<boolean>`** - Removes all entries (always resolves to `true`).  
  > `clear()` is destructive and best used in setup/teardown or maintenance windows. Under concurrent writers, new keys can appear immediately after the wipe completes. With `trackKeys: true`, in-memory indexes are reset together with store state.
- **`size(): Promise<number>`** - Returns current store size (number of keys).

`getMany()` example:

```javascript
const items = await kv.getMany(["user:1", "user:missing", "user:null"]);
// items[0] -> { key: "user:1", exists: true,  value: ... }
// items[1] -> { key: "user:missing", exists: false, value: null }
// items[2] -> { key: "user:null", exists: true, value: null }
```

`deleteMany()` example:

```javascript
const result = await kv.deleteMany(["user:1", "user:2", "user:missing"]);
// result -> { deleted: 2, missing: 1 }
```

`deleteByPrefix()` example:

```javascript
const result = await kv.deleteByPrefix({
  prefix: "tmp:",
  limit: 1000,
});
// result -> { deleted: <number>, done: <boolean> }
```

`deleteByPrefix()` details:

- `prefix` is required and must be a non-empty string.
- `limit` is required, must be a positive integer, and is capped at `100000`.
- `done === true` means no matching keys remain after this call.
- If `done === false`, repeat the same call until completion.
- Use `listKeys({ prefix, limit })` first for a read-only preview.

Backend note:

- disk backend deletes inside one bbolt write transaction;
- memory backend deletes through shard locks and does not provide cross-key visibility transaction under concurrent writes;
- claim metadata is removed for physically deleted keys.

#### Atomic Operations

- **`incrementBy(key: string, delta: number): Promise<number>`** - Atomically increments numeric value. Treats missing keys as `0`.
  > JS numbers are IEEE‑754 doubles, so anything above `Number.MAX_SAFE_INTEGER` (~9e15) loses precision before the value reaches Go. Keep counters below that threshold or encode larger values as strings/custom objects in your script before calling `incrementBy`.
- **`getOrSet(key: string, value: any): Promise<{ value: any, loaded: boolean }>`** - Gets existing value or sets if absent. `loaded: true` means pre-existing.
- **`swap(key: string, value: any): Promise<{ previous: any|null, loaded: boolean }>`** - Replaces value atomically. Returns previous value if existed.
- **`compareAndSwap(key: string, oldValue: any, newValue: any): Promise<boolean>`** - Sets `newValue` only if current value equals `oldValue`. Pass `null`/`undefined` as `oldValue` to mean "only if the key is absent" (set-if-not-exists).
- **`compareAndSwapDetailed(key: string, oldValue: any, newValue: any, options?: { includeCurrentOnMismatch?: boolean }): Promise<{ swapped: true, reason: "swapped" } | { swapped: false, reason: "mismatch", existed: boolean, current?: any }>`** - Detailed CAS diagnostics. On mismatch, includes `existed` and optionally `current` (only when `includeCurrentOnMismatch: true` and key existed).
- **`setIfAbsent(key: string, value: any): Promise<boolean>`** - Convenience API for first-writer-wins key initialization. Equivalent to `compareAndSwap(key, null, value)`.
- **`deleteIfExists(key: string): Promise<boolean>`** - Deletes key if it exists. Returns `true` if deleted.
- **`compareAndDelete(key: string, oldValue: any): Promise<boolean>`** - Deletes key only if current value equals `oldValue`.
- **`compareAndDeleteDetailed(key: string, oldValue: any, options?: { includeCurrentOnMismatch?: boolean }): Promise<{ deleted: true, reason: "deleted" } | { deleted: false, reason: "mismatch", existed: boolean, current?: any }>`** - Detailed compare-and-delete diagnostics. `oldValue: null/undefined` is treated as regular expected-value comparison through the configured serializer (not an absent-key sentinel).

##### Backwards compatibility

- `compareAndSwap()` and `compareAndDelete()` behavior and return types are unchanged.
- `compareAndSwapDetailed()` / `compareAndDeleteDetailed()` are additive opt-in APIs for richer mismatch diagnostics.
- `setIfAbsent()` is a convenience API over absent-key CAS semantics; existing APIs are not redefined.

#### Query Operations

- **`scan(options?: ScanOptions): Promise<ScanResult>`** - Streams entries in lexicographic order using cursor-based pagination.

  ```typescript
  interface ScanOptions {
    prefix?: string; // Filter by key prefix
    limit?: number;  // Max results per page; positive values are capped at 100000
    cursor?: string; // Opaque cursor produced by the previous page ("" starts a new scan)
  }

  interface ScanResult {
    entries: Array<{ key: string; value: any }>;
    cursor: string; // Opaque cursor for the next page
    done: boolean;  // True when the scan reached the end of the prefix window
  }
  ```

  Use `scan()` with a bounded positive `limit` when the keyspace is too large to materialize with `list()` or when you need restart-safe pagination.
  Treat `cursor` as an opaque continuation token. Do not parse, modify, or construct it manually.
  Use a cursor only with the same logical scan options that produced it, especially the same `prefix`.
  Pagination is cursor-based, but it is not a long-lived snapshot. If keys are inserted or deleted between page calls, later pages may reflect those changes.
  On disk backend, each `scan()` call is a separate bbolt read transaction. On memory backend, concurrent writes can also affect what you observe during scan/list iteration.

- **`scanKeys(options?: ScanKeysOptions): Promise<ScanKeysResult>`** - Streams key names in lexicographic order using cursor-based pagination.

  ```typescript
  interface ScanKeysOptions {
    prefix?: string; // Filter by key prefix
    limit?: number;  // Max keys per page; positive values are capped at 100000
    cursor?: string; // Opaque cursor produced by the previous page ("" starts a new scan)
  }

  interface ScanKeysResult {
    keys: string[];
    cursor: string; // Opaque cursor for the next page
    done: boolean;  // True when the scan reached the end of the prefix window
  }
  ```

  `scanKeys()` is the key-only equivalent of `scan()`.
  It does not clone, deserialize, or return values.
  Use `scanKeys()` when the keyspace is too large to materialize with `listKeys()`.
  Treat `cursor` as an opaque continuation token. Do not parse, modify, or construct it manually.
  Use a cursor only with the same logical scan options that produced it, especially the same `prefix`.
  Pagination is cursor-based, but it is not a long-lived snapshot. If keys are inserted or deleted between page calls, later pages may reflect those changes.
  For exclusive allocation workflows, use `claimRandom()` or `popRandom()` instead of scan/list pagination.

  ```javascript
  let cursor = "";

  do {
    const page = await kv.scanKeys({
      prefix: "user:",
      cursor,
      limit: 1000,
    });

    const users = await kv.getMany(page.keys);
    cursor = page.cursor;
  } while (!page.done);
  ```

- **`list(options?: ListOptions): Promise<Array<{ key: string; value: any }>>`** - Returns entries sorted lexicographically by key.

  ```typescript
  interface ListOptions {
    prefix?: string  // Filter by key prefix
    limit?: number   // Max results; positive values are capped at 250000
  }
  ```

- **`listKeys(options?: ListKeysOptions): Promise<string[]>`** - Lists key names without returning values.

  ```typescript
  interface ListKeysOptions {
    prefix?: string  // Optional key prefix filter
    limit?: number   // Max keys; positive values are capped at 250000
  }
  ```

  `listKeys()` is read-only, returns keys in ascending lexicographic order, and does not clone, deserialize, or return values.
  Useful flow before destructive calls:

  ```javascript
  const keys = await kv.listKeys({ prefix: "tmp:", limit: 1000 });
  await kv.deleteMany(keys);
  ```

  Backend note:
  - disk backend reads key names from bbolt cursors so results reflect the durable store;
  - memory backend uses key-only shard iterators and merges them lexicographically;
  - `listKeys()` is not cursor-paginated; use `scanKeys()` for key-only cursor pagination;
  - use `scan()` when you need key/value entries.

- **`count(options?: CountOptions): Promise<number>`** - Returns number of keys matching prefix.  
  `count()` (or omitted options) is equivalent to `size()`.

  ```typescript
  interface CountOptions {
    prefix?: string  // Filter by key prefix
  }
  ```

- **`randomKey(options?: RandomKeyOptions): Promise<string>`** - Returns a random key, or `""` if none match.

  ```typescript
  interface RandomKeyOptions {
    prefix?: string  // Optional prefix filter
  }
  ```

  `randomKey()` only returns a key string and does not create/observe leases.

- **`randomKeys(options: RandomKeysOptions): Promise<string[]>`** - Returns random key names matching an optional prefix.

  ```typescript
  interface RandomKeysOptions {
    prefix?: string  // Optional prefix filter
    count: number    // Required integer in range [1, 1000000]
    unique?: boolean // Defaults to true
  }
  ```

  ```javascript
  const keys = await kv.randomKeys({
    prefix: "user:",
    count: 100,
    unique: true,
  });

  const users = await kv.getMany(keys);
  ```

  `randomKeys()` returns keys only. It does not clone, deserialize, or return values.
  `count` is capped at `1000000` to protect the k6 process from unbounded allocations.
  When `unique` is `true` and fewer matching keys exist than requested, all available matching keys are returned in random order.
  Use `claimRandom()` or `popRandom()` when you need exclusive allocation.

- **`popRandom(options?: { prefix?: string }): Promise<{ key: string, value: any } | null>`** - Atomically picks and removes one random matching entry. Resolves to `null` when no match exists.

- **`claimRandom(options?: { prefix?: string, owner?: string, ttl?: number }): Promise<{ id: string, key: string, token: number, owner?: string, expiresAt: number, entry: { key: string, value: any } } | null>`** - Atomically leases one random matching entry. Live claims are excluded from later `claimRandom()` and `popRandom()` calls until released/completed or expired. If `ttl` is omitted, the default lease is **30000ms (30 seconds)**. `ttl` must be a positive integer and is capped at **86400000ms (24 hours)**. `owner` is optional diagnostic metadata capped at **256 bytes** and is not emitted as a metrics label.

- **`releaseClaim(claim: { id: string, key: string, token: number }): Promise<boolean>`** - Releases a live claim back to the pool. Returns `false` for stale/expired/missing claims.

- **`completeClaim(claim, options?: { deleteKey?: boolean }): Promise<boolean>`** - Completes a live claim. By default it also deletes the underlying key (`deleteKey: true`).

  Claim lifecycle guidance:

  - Use `completeClaim()` on **success** (`deleteKey: true` consumes the item permanently).
  - Use `releaseClaim()` on **failure** to return the item to the pool.

  ```javascript
  const claim = await kv.claimRandom({ prefix: 'user:' });
  if (claim === null) return;

  try {
    // do work with claim.entry.value
    await kv.completeClaim(claim); // success path
  } catch (err) {
    await kv.releaseClaim(claim); // failure path
    throw err;
  }
  ```

> ⚠️ `claimRandom()` is a local coordination primitive for VUs sharing the same `xk6-kv` process/store. It is not a distributed lock service.
> `claimRandom()` and `popRandom()` are random allocation helpers optimized for low/moderate live-claim occupancy. Under very high live-claim occupancy, fallback selection can be biased toward scan order, but exclusivity is still preserved.

- **`rebuildKeyList(): Promise<boolean>`** - Rebuilds in-memory key indexes (useful for disk backend with `trackKeys: true`).

- **`stats(): Promise<KVStats>`** - Returns a structured diagnostic snapshot of the current store state.

  ```typescript
  interface KVStats {
    backend: "memory" | "disk"
    serialization: "json" | "string"
    trackKeys: boolean
    count: number
    claims: { live: number; expired: number }
    index?: {
      enabled: boolean
      keysList?: number
      keysMap?: number
      ost?: number
      consistent: boolean
    } | null
    disk?: {
      path: string
      sizeBytes: number
      readOnly?: boolean
    } | null
  }
  ```

  ```javascript
  const snapshot = await kv.stats();
  console.log(snapshot.count, snapshot.claims.live);
  ```

- **`reportStats(): Promise<void>`** - Emits state gauges to k6 custom metrics using the current snapshot.

  Emitted metrics:
  - `xk6_kv_keys`
  - `xk6_kv_claims_live`
  - `xk6_kv_claims_expired`
  - `xk6_kv_index_keys` (with `index=keys_list|keys_map|ost`)
  - `xk6_kv_index_consistent`
  - `xk6_kv_disk_size_bytes` (disk backend only)

  ```javascript
  await kv.reportStats();
  ```

- **`metrics.operations` (openKv option)** - Enables automatic operation metrics for every async KV method except sync `close()`.

  Emitted metrics:
  - `xk6_kv_operations_total` (Counter, tags: `op`, `backend`, `status`, `track_keys`, `serialization`)
  - `xk6_kv_operation_duration` (Trend in milliseconds, tags: `op`, `backend`, `status`, `track_keys`, `serialization`)
  - `xk6_kv_operation_failed` (Rate, tags: `op`, `backend`, `track_keys`, `serialization`)
  - `xk6_kv_errors_total` (Counter, tags: `op`, `backend`, `error_type`, `track_keys`, `serialization`)
  - `xk6_kv_empty_result` (Rate for `random_key`/`random_keys`/`pop_random`/`claim_random`, tags: `op`, `backend`, `track_keys`, `serialization`)
  - `xk6_kv_async_in_flight` (Gauge for async store operations currently running, tags: `backend`, `track_keys`, `serialization`)

  `xk6_kv_async_in_flight` is the current saturation signal for the async bridge. There is no `waiting` metric because xk6-kv does not currently have an async limiter or queue; if one is added later, a low-cardinality waiting gauge can be added alongside it.

  ```javascript
  const kv = openKv({
    backend: "memory",
    metrics: { operations: true },
  });
  ```

  Quickstart:

  ```javascript
  import { openKv } from "k6/x/kv";

  const kv = openKv({
    backend: "memory",
    trackKeys: true,
    metrics: { operations: true },
  });

  export const options = {
    thresholds: {
      "xk6_kv_operation_failed": ["rate==0"],
      "xk6_kv_empty_result{op:claim_random}": ["rate<0.05"],
      "xk6_kv_index_consistent{track_keys:true}": ["value==1"],
    },
  };

  export default async function () {
    await kv.claimRandom({ prefix: "user:" });
    await kv.reportStats();
  }
  ```

  > Troubleshooting: if k6 reports `no metric name "xk6_kv_..." found`, make sure:
  > 1) `openKv({ metrics: { operations: true } })` runs in init context, and
  > 2) you rebuilt your binary with this extension (`task build-k6`).

#### Snapshot Operations

- **`backup(options?: BackupOptions): Promise<BackupSummary>`**  
  Writes the current dataset to a bbolt file. Always set `fileName` (leaving it blank points at the backend’s live bbolt file) and use `allowConcurrentWrites: true` for a best-effort dump that releases writers sooner (summary includes `bestEffort` + `warning` so you can alarm on it).  
  > **Memory backend caution:** when `allowConcurrentWrites` is left at the default `false`, the memory backend holds its global mutation gate for the entire duration of the backup (from key snapshot through streaming). On large datasets that can pause every writer/VU for minutes. Enable `allowConcurrentWrites: true` if you need the cluster to keep serving traffic during the export (accepting the best-effort snapshot) or schedule strict backups during quiet windows.
  >
  > **Shared-file workflow:** Leaving `fileName` blank while running the memory backend is intentional—it writes into `.k6.kv`, the same file the disk backend mounts by default. That makes a common DX pattern possible: run the hot path with `backend: "memory"`, call `backup()` without arguments in `teardown()`, and later rerun the same test with `backend: "disk"` to replay the captured dataset. If you want snapshots to live somewhere else (or you run disk workloads in parallel), provide an explicit `fileName` so you don’t clobber the shared DB.
  >
  > **File replacement semantics:** backup/export writes to a temp file in the destination directory, fsyncs, then renames into place and syncs the parent directory. On Unix-like filesystems, same-directory rename is atomic. On platforms where `os.Rename` is not guaranteed atomic, treat this as a best-effort crash-safety strategy; it still avoids intentionally writing partial data directly into the destination file.

- **`restore(options?: RestoreOptions): Promise<RestoreSummary>`**  
  Replaces the dataset with a snapshot produced by `backup()`. Optional `maxEntries` / `maxBytes` guards protect against oversized or corrupted inputs.

- **`exportJSONL(options: { fileName: string, prefix?: string, limit?: number }): Promise<{ exported: number, fileName: string, bytesWritten: number }>`**  
  Exports key/value entries to a JSON Lines file for portable seed data and diff-friendly snapshots. Each line is:
  `{"key":"...","value":...}`. Values are exported after normal KV deserialization, not as raw backend bytes.

- **`importJSONL(options: { fileName: string, limit?: number, batchSize?: number }): Promise<{ imported: number, fileName: string, bytesRead: number }>`**  
  Imports key/value entries from a JSON Lines file. Each line must be:
  `{"key":"...","value":...}`.

```typescript
await kv.backup({
  fileName: "./backups/kv-latest.kv",
  allowConcurrentWrites: true,
});
 
await kv.restore({ fileName: "./backups/kv-latest.kv" });
```

> **Disk backend note:** pointing `fileName` at the *currently mounted* bbolt path is treated as a no-op (backup just returns metadata; restore leaves the DB untouched), so when you’re already running `backend: "disk"` you still need a different `fileName`. The “shared `.k6.kv` trick” only applies when you begin on the memory backend and want to seed the disk backend later.

`exportJSONL()` example:

```javascript
const result = await kv.exportJSONL({
  fileName: "./exports/users.jsonl",
  prefix: "user:",
});

console.log(result.exported);
console.log(result.bytesWritten);
```

`exportJSONL()` options:

- `fileName` is required and must be a non-empty string.
- `prefix` is optional; empty or omitted means all keys.
- `limit` is optional; if omitted or `<= 0`, all matching entries are exported. Positive values are capped at `1000000`.

`exportJSONL()` writes to a temporary file, flushes and fsyncs it, renames it into place, then syncs the parent directory. On Unix-like filesystems this gives atomic replacement in the common same-directory case. On platforms where `os.Rename` is not guaranteed atomic, this remains a best-effort crash-safety strategy and still avoids intentionally writing partial data directly into the target file.

`importJSONL()` example:

```javascript
const result = await kv.importJSONL({
  fileName: "./exports/users.jsonl",
  batchSize: 1000,
});

console.log(result.imported);
console.log(result.bytesRead);
```

`importJSONL()` options:

- `fileName` is required and must be a non-empty string.
- `limit` is optional; if omitted or `<= 0`, all records are imported. Positive values are capped at `1000000`.
- `batchSize` is optional; if omitted or `<= 0`, the default batch size is used. Positive values are capped at `10000`.
- `importJSONL()` enforces a per-line safety cap of 64 MiB; oversized records are rejected with a line-numbered parse error.

`importJSONL()` streams records and writes them in `setMany()` batches. Existing keys are overwritten.
`bytesRead` reports how many input bytes were consumed by the importer.

Each line must be a JSON object with:

- `key` - required non-empty string.
- `value` - required JSON value, including `null`.

`importJSONL()` rejects blank lines and malformed JSON records. Errors include the source line number.
The import is batch-atomic, not file-atomic: if a later line is invalid, already committed batches remain imported, while the currently failed batch is not partially written. Failure messages include committed progress (`records`, `bytes`, and line context) so callers can locate the bad record and decide whether a retry is safe.

- **`close(): void`** - Synchronously closes this KV handle. Call once in `teardown()`.
  After `close()`, this handle rejects async operations with `StoreClosedError` on both backends.
  Closing one handle does not affect other open handles until the shared store refcount reaches zero.
  Do **not** call `close()` from `default()` iterations.

### Performance Notes

- **`randomKey()` complexity by backend:**
  - `trackKeys: true`:
    - disk backend: no prefix -> O(1); prefix -> O(log n) via key index.
    - memory backend: no prefix includes a small O(shards) selection step; prefix performs per-shard range selection (O(shards * log n)) before final rank/select.
  - `trackKeys: false` (default): scan-based path with linear cost in the matching keyspace.
  Achieving tracked-path speeds means keys are mirrored in memory helper structures, so large datasets consume more RAM and index slices/maps do not shrink automatically. Budget for that footprint or rebuild indexes periodically.
- **Random key workloads:** Calling `randomKey()` repeatedly with `trackKeys: false` (especially on the disk backend) keeps a read transaction open while it counts and selects keys, which can stall the lone bbolt writer until the call finishes. Turn on `trackKeys` (for O(1)/O(log n) sampling) or throttle/redesign these workloads to avoid head-of-line blocking.
- **`randomKeys()` complexity by backend:** With `trackKeys: true`, both backends use key indexes for small samples; memory first builds shard ranges (O(shards * log n)) and then selects sampled keys by rank, while disk may fall back to cursor scan for near-full unique samples. With `trackKeys: false`, it collects candidates via `scanKeys()` and samples in memory (linear in matching keys).
- **Memory `trackKeys: false` scan/list costs:** `scan()`, `scanKeys()`, `list()`, and `listKeys()` use untracked shard-map iteration. On large keyspaces, repeated pagination can become expensive; if these operations are hot, prefer `trackKeys: true`.
- **Disk backend and `trackKeys`:** bbolt is the persistent source of truth. With `trackKeys: true`, the disk backend maintains an exact derived in-memory key index rebuilt from bbolt on open/restore and updated after successful mutations. That index accelerates `randomKey()`, `randomKeys()`, and `count()`, while cursor-style key reads (`scanKeys()` and `listKeys()`) still read from bbolt.
- **Bound large reads and writes:** For large keyspaces, prefer `scan()` / `scanKeys()` with bounded positive `limit` values instead of `list()` / `listKeys()` or unlimited `limit <= 0` calls. Oversized positive limits are rejected to avoid unbounded allocations and long transactions.
- **`count()` / `count({ prefix })` complexity:**
  - `count()` (same as `size()`):
    - memory backend: O(shards)
    - disk backend with `trackKeys: true`: O(1)
    - disk backend with `trackKeys: false`: bbolt bucket stats (not guaranteed constant-time)
  - `count({ prefix })`:
    - memory + `trackKeys: true`: O(shards * log n)
    - memory + `trackKeys: false`: O(n)
    - disk + `trackKeys: true`: O(log n)
    - disk + `trackKeys: false`: O(k), where `k` is number of keys under prefix
- **Prefix cardinality on large disk datasets:** If `count({ prefix })` is hot on disk, prefer `trackKeys: true`. Without tracking, the disk path walks a bbolt cursor through matching keys.
- Both backends are optimized for concurrent workloads, but there's synchronization overhead between VUs

## Usage Examples

Complete examples are available in the [`examples/`](./examples) directory, and production-grade k6 scenarios live under [`e2e/`](./e2e).

Observability-focused scripts:

- Example operation metrics in a worker queue: [`examples/metrics-operations-worker-queue.js`](./examples/metrics-operations-worker-queue.js)
- Example `stats()` / `reportStats()` health snapshots: [`examples/metrics-report-stats-health.js`](./examples/metrics-report-stats-health.js)
- Example batch random key sampling + hydration: [`examples/random-keys.js`](./examples/random-keys.js)
- E2E lease-worker observability scenario: [`e2e/subscription-renewal-lease-observability.js`](./e2e/subscription-renewal-lease-observability.js)
- E2E credential pool drain scenario: [`e2e/credential-pool-drain-observability.js`](./e2e/credential-pool-drain-observability.js)

### Producer / Consumer

```javascript
import { openKv } from "k6/x/kv";

const kv = openKv({ backend: "memory", trackKeys: true });

export async function producer() {
    const id = (await kv.get("latest-id")) || 0;
    await kv.set(`token-${id}`, "value");
    await kv.set("latest-id", id + 1);
}

export async function consumer() {
    const key = await kv.randomKey({ prefix: "token-" });
    if (key) {
        await kv.get(key);
        await kv.delete(key);
    }
}
```

### Random Key With Prefix

```javascript
import { openKv } from "k6/x/kv";

const kv = openKv({ backend: "disk", trackKeys: true });

export default async function () {
    await kv.set("user:1", { name: "Alice" });
    await kv.set("user:2", { name: "Bob" });
    
    const key = await kv.randomKey({ prefix: "user:" });
    if (key) {
        const user = await kv.get(key);
        console.log(`Random user: ${JSON.stringify(user)}`);
    }
}
```

## Development

This project uses a [Taskfile](https://taskfile.dev) for cross-platform development (Windows, Linux, macOS, WSL).

### Development Quick Start

```bash
# Install Task (once)
# macOS/Linux: brew install go-task/tap/go-task
# Windows: scoop install task

# Install dev tools
task install-tools

# Format and lint
task lint-fix

# Run tests
task test

# Build k6 with local extension
task build-k6
```

### Available Tasks

| Task | Description |
| ------ | ------------- |
| `task install-tools` | Install dev tools (gofumpt, golangci-lint, xk6) |
| `task lint-fix` | Fix linting issues and format code |
| `task lint` | Check code without modifications |
| `task test` | Run unit tests |
| `task test-race` | Run tests with race detector |
| `task build-k6` | Build k6 with local extension |
| `task test-e2e-all` | Run all end-to-end scenarios |
| `task clean` | Remove ./bin and .k6.kv |
| `task install-githooks` | Enable semantic commit validation |
| `task version-check` | Preview next version |

### Code Quality

- **gofumpt** for formatting (stricter than gofmt)
- **golangci-lint** with 40+ enabled linters
- **gci** and **goimports** for import organization
- **golines** for line length (max 120 characters)

Run `task lint-fix` before committing.

## Versioning & Releases

This project uses **automated semantic versioning** based on commit messages:

- **`fix: description`** -> Patch version (1.0.0 -> 1.0.1)
- **`feat: description`** -> Minor version (1.0.0 -> 1.1.0)
- **`major: description`** -> Major version (1.0.0 -> 2.0.0)

When commits are pushed to `main`:

1. Commit messages are validated
2. Tests and builds run across platforms
3. Version is calculated and release is created automatically
4. Pre-built k6 binaries are attached to the GitHub release

### Git Hooks

Enable semantic commit validation:

```bash
task install-githooks
```

Check next version:

```bash
task version-check
```

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Set up git hooks: `task install-githooks`
4. Make changes and commit using semantic format:

   - `fix: your bug fix description`
   - `feat: your new feature description`
   - `major: your breaking change description`

5. Ensure code passes linting (`task lint`) and tests (`task test`)
6. Push and open a Pull Request

**Note:** Pull requests from forks skip semantic commit validation for easier external contributions.

## License

GNU AGPL v3.0
