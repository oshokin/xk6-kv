# xk6-kv

[![Go Report Card](https://goreportcard.com/badge/github.com/oshokin/xk6-kv)](https://goreportcard.com/report/github.com/oshokin/xk6-kv)
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)

**Mutable local test-data pools, leases, and response capture for k6 — without Redis.**

> If you only need read-only fixtures, use k6 [SharedArray](https://grafana.com/docs/k6/latest/javascript-api/k6-data/sharedarray/). If you need mutable local allocation, leases, or response capture, use **xk6-kv**.

Use **xk6-kv** when your k6 test needs writable shared state inside one k6 process:

- give each VU a **unique user or credential** without races;
- **claim** or **pop** rows from a local pool (CSV/JSONL import, `setMany`, or runtime writes);
- build **task queues** and producer/consumer flows;
- share **mutable** state between scenarios;
- **export** captured data after a run (`exportJSONL`, `exportCSV`, disk backend, snapshots).

## Why not just SharedArray?

[`SharedArray`](https://grafana.com/docs/k6/latest/javascript-api/k6-data/sharedarray/) is read-only after initialization and is not meant for communication between VUs. **xk6-kv** adds a process-local, concurrency-safe store with:

- lease-based allocation: `claimRandom`, `claimKey`, `claimKeys`, `claimRandomMany`;
- one-shot drains: `popRandom`, `popRandomMany`;
- streaming seed import: `importCSV`, `importJSONL`;
- portable export: `exportJSONL`, `exportCSV`, `backup`, `restore`;
- CAS-style operations (`compareAndSwap`, `incrementBy`, …) and operation metrics.

## xk6-kv vs built-in k6

| Need | k6 built-in | xk6-kv |
| --- | --- | --- |
| Read-only shared fixtures | `SharedArray` | yes (import once, read many) |
| Mutable shared state across VUs | no | yes (memory or disk backend) |
| Unique credential / user allocation | manual indexing / external store | `claimRandom`, `claimKey`, `claimKeys`, `claimRandomMany` |
| One-shot task queue (consume once) | manual | `popRandom`, `popRandomMany` |
| Large CSV/JSONL seed files | load in `setup` yourself | `importCSV`, `importJSONL` |
| Response capture to file | `handleSummary` / custom scripts | `exportJSONL`, `exportCSV`, `backup` / `restore` |
| Local mutable coordination without Redis | no | yes |

## When not to use xk6-kv

- You need **coordination across multiple k6 processes** or machines (use Redis, a queue, or your orchestrator).
- You need **Grafana Cloud** to resolve the extension automatically without a custom binary ([Extensions Registry](https://grafana.com/docs/k6/latest/extensions/explanations/extensions-registry/) listing helps discovery; you still build `xk6` or use release binaries).
- You need a **production database** or **distributed locks** — claim APIs are local coordination primitives inside one k6 process, not a lock service.
- You cannot ship a **custom k6 binary** (`xk6 build --with …` or [pre-built releases](https://github.com/oshokin/xk6-kv/releases)).

## Why not Redis?

Use **Redis** (or similar) when VUs across **multiple k6 processes or machines** must share the same pool or queue.

Use **xk6-kv** when you only need **mutable state inside one k6 process** and want fewer moving parts: no extra service, no network hop, claims/pops coordinated in-process.

## Start here (existing scripts)

From the repo root, with a k6 binary that includes this extension:

```bash
# Lease-based unique allocation (complete on success, release on failure)
k6 run examples/claim-random-default-ttl.js

# One-time user pool drain (no duplicate allocation)
k6 run examples/pop-random-unique-users.js

# CSV seed import
k6 run examples/import-csv.js

# Export a prefix to JSONL after writes
k6 run examples/export-jsonl.js

# Export a prefix to flat CSV after writes
k6 run examples/export-csv.js
```

Production-style scenarios (metrics, thresholds, disk paths): [`e2e/`](./e2e/) — e.g. [`e2e/import-csv-portable-seed.js`](./e2e/import-csv-portable-seed.js), [`e2e/credential-pool-drain-observability.js`](./e2e/credential-pool-drain-observability.js). Full script index: [`examples/README.md`](./examples/README.md).

## License

This project is **GNU AGPL v3.0** (fork of [oleiade/xk6-kv](https://github.com/oleiade/xk6-kv), extended with claims, batch allocation, CSV/JSONL, metrics, and more). AGPL is acceptable for many test-tooling workflows, but some companies block AGPL dependencies in their supply chain. If you need a permissive license for a specific use case, open an issue with context — relicensing requires rights on the full codebase.

## Core features

- **Unique allocation:** `claimRandom`, `claimKey`, `claimKeys`, `claimRandomMany`, `renewClaim`, `releaseClaim`, `completeClaim`, `renewClaims`, `releaseClaims`, `completeClaims`.
- **One-shot queues:** `popRandom`, `popRandomMany` (each successful pop removes the key).
- **Large seed files:** streaming `importCSV` and `importJSONL`.
- **Response capture & handoff:** `exportJSONL`, `exportCSV`, `validateCSV`, `validateJSONL`, `backup` / `restore`, disk backend.
- **Batch KV:** `setMany`, `getMany`, `deleteMany`.
- **Prefix workflows:** `scan`, `scanKeys`, `list`, `listKeys`, `deleteByPrefix`, `count`, `randomKey`, `randomKeys`.
- **CAS-style ops:** `incrementBy`, `getOrSet`, `swap`, `compareAndSwap`, `compareAndDelete`, and related helpers.
- **Observability:** optional operation metrics, `stats()`, `allocationStats()`, `reportStats()`.
- **Backends:** memory (sharded, fast) or disk/bbolt (persistent); optional `trackKeys` for faster random/claim paths on large datasets.
- **TypeScript:** declarations in [`typescript/`](./typescript/) for IntelliSense.

## Not implemented intentionally

`xk6-kv` stays a local k6 test-data lifecycle engine. The following are intentionally out of scope:

- no Redis backend;
- no distributed locks;
- no general key TTL (outside claim leases);
- no general transactions;
- no ETL transforms/schema inference/compression pipelines;
- no `feedNext` in this release.

## 60-second example

Unique allocation with `claimRandom` (full script: [`examples/claim-random-default-ttl.js`](./examples/claim-random-default-ttl.js)):

```javascript
import { openKv } from "k6/x/kv";

const kv = openKv({ backend: "memory", trackKeys: true });

export async function setup() {
  await kv.setMany({
    "user:1": { name: "Alice" },
    "user:2": { name: "Bob" },
  });
}

export default async function () {
  const claim = await kv.claimRandom({ prefix: "user:" });
  if (claim === null) return;

  try {
    console.log(claim.entry.value.name);
    await kv.completeClaim(claim, { deleteKey: false });
  } catch (err) {
    await kv.releaseClaim(claim);
    throw err;
  }
}
```

Build or download a k6 binary with this extension — see [Installation](#installation) below.

## Installation

### Option 1: Pre-built Binaries (Recommended)

Download k6 binaries with xk6-kv from the [Releases page](https://github.com/oshokin/xk6-kv/releases).

Release artifacts are named `xk6-kv_<version>_<os>_<arch>.tar.gz` for Linux/macOS and
`xk6-kv_<version>_<os>_<arch>.zip` for Windows. Set `VERSION` to the release you want
to install (replace with the [latest release](https://github.com/oshokin/xk6-kv/releases/latest)), for example `<XK6_KV_VERSION>`.

**Linux:**

```bash
VERSION=<XK6_KV_VERSION>
curl -L "https://github.com/oshokin/xk6-kv/releases/download/${VERSION}/xk6-kv_${VERSION}_linux_amd64.tar.gz" -o k6.tar.gz
tar -xzf k6.tar.gz && chmod +x k6
./k6 version
```

**macOS:**

```bash
VERSION=<XK6_KV_VERSION>
curl -L "https://github.com/oshokin/xk6-kv/releases/download/${VERSION}/xk6-kv_${VERSION}_darwin_arm64.tar.gz" -o k6.tar.gz
tar -xzf k6.tar.gz && chmod +x k6
./k6 version
```

**Windows (PowerShell):**

```powershell
$VERSION = "<XK6_KV_VERSION>"
Invoke-WebRequest -Uri "https://github.com/oshokin/xk6-kv/releases/download/$VERSION/xk6-kv_${VERSION}_windows_amd64.zip" -OutFile k6.zip
Expand-Archive -Path k6.zip -DestinationPath .
.\k6.exe version
```

### Option 2: Build from Source

1. Install [xk6](https://github.com/grafana/xk6):

```bash
go install go.k6.io/xk6/cmd/xk6@v1.4.1
```

1. Build k6 with xk6-kv:

```bash
# Latest version
xk6 build --with github.com/oshokin/xk6-kv@latest

# Pin a specific release
xk6 build --with github.com/oshokin/xk6-kv@<XK6_KV_VERSION>

# k6 v1.7.x legacy line (frozen at v1.4.31)
xk6 build --with github.com/oshokin/xk6-kv@v1.4.31
```

1. Verify:

```bash
./k6 version
```

> **Requirements**: Go 1.25.11 or higher.

## Compatibility

Current development targets **k6 v2.0.x** (`go.k6.io/k6/v2`). The JavaScript import is unchanged:

```javascript
import { openKv } from "k6/x/kv";
```

Use **xk6 v1.4.1** or newer when building this extension from source; xk6 resolves the required k6 major version from the extension dependency graph.

| xk6-kv version | k6 core version | Notes |
| --- | --- | --- |
| v1.5.0+ | v2.0.x | Current supported line |
| v1.4.31 | v1.7.x | Frozen legacy line; pin this tag when building against k6 v1 |

Projects that still require k6 v1.7.x should pin the extension explicitly:

```bash
xk6 build --with github.com/oshokin/xk6-kv@v1.4.31
```

## Quick Start

Allocation flows: [60-second example](#60-second-example) and [Start here](#start-here-existing-scripts).

### Basic KV operations

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

Every rejected `kv.*` promise carries a **typed error object** with `name` and `message` fields. It is a structured plain object, not a JavaScript `Error` instance, so do not rely on `err instanceof Error`. Check `err.name` (or `err.Name` when k6 serialises it as Go struct) instead of matching raw strings.

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
- **Concurrency & lifecycle** - e.g. `BackupInProgressError`, `RestoreInProgressError`, `OperationCanceledError`, `StoreReadOnlyError`, `StoreClosedError`.
- **Disk & snapshot IO** - precise signals for path issues, permission problems, bbolt failures, or restore budget overruns.

### OperationCanceledError

`OperationCanceledError` is returned when the owning k6 VU context is canceled or reaches its deadline while an async operation is running.

Typical causes:

- test abort;
- scenario stop;
- iteration timeout;
- Ctrl+C;
- k6 engine shutdown.

This is a control-flow error, not data corruption. Long-running operations such as `importJSONL`, `exportJSONL`, `exportCSV`, `importCSV`, `validateCSV`, `validateJSONL`, `backup`, and `restore` may stop early. For imports, already committed batches may remain committed.

📚 A complete catalogue with root causes and remediation tips lives in [`examples/README.md`](./examples/README.md#error-manual).

## TypeScript Support

Full TypeScript support with IntelliSense and type safety! Copy the [`typescript/`](./typescript/) folder to your project for a ready-to-use starter kit.\
See [`typescript/README.md`](./typescript/README.md) for complete setup instructions.
`typescript/` is a local starter example project (template), not a published npm package.

## API overview

| Area | Methods |
| --- | --- |
| **Allocation** | `claimRandom`, `claimKey`, `claimKeys`, `claimRandomMany`, `releaseClaim`, `releaseClaims`, `renewClaim`, `renewClaims`, `completeClaim`, `completeClaims`, `popRandom`, `popRandomMany` |
| **Import / export** | `importCSV`, `importJSONL`, `exportJSONL`, `exportCSV`, `validateCSV`, `validateJSONL`, `backup`, `restore`, `rebuildKeyList` |
| **Batch** | `setMany`, `getMany`, `deleteMany` |
| **Query** | `get`, `set`, `delete`, `exists`, `list`, `listKeys`, `scan`, `scanKeys`, `count`, `size`, `randomKey`, `randomKeys`, `clear`, `close` |
| **CAS-style** | `incrementBy`, `getOrSet`, `swap`, `compareAndSwap`, `setIfAbsent`, `deleteIfExists`, `compareAndDelete` |
| **Observability** | `stats`, `allocationStats`, `reportStats` (+ optional operation metrics via `openKv({ metrics: … })`) |

Full signatures, options, and error shapes: [**API Reference**](#api-reference) below. Scenario recipes: [Usage Examples](#usage-examples) and [`examples/README.md`](./examples/README.md).

## API Reference

### `openKv(options?)`

Opens a key-value store. **Must be called in the init context** (outside of default/setup/teardown functions).
The returned KV handle is shared across VUs, so treat its lifecycle as test-wide.

> ⚠️ **Configuration lock-in:** the first successful `openKv()` call fixes the store configuration
> for the whole test run. All subsequent `openKv()` calls must provide equivalent options;
> otherwise `openKv()` throws `KVOptionsConflictError`. Calling `close()` does not reset this
> process-wide configuration.

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
  Prefer `scanKeys()` with a bounded positive `limit` for large stores or load-test paths.
  Avoid `limit <= 0` on large keyspaces unless you intentionally want to materialize every matching key in one VU.
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
  Use it for small datasets, debugging, setup validation, and bounded previews.
  For large stores, prefer `scanKeys({ limit })` so each VU only materializes one page at a time.
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
  No-match is not an error, but the promise may reject for invalid options, closed stores, backend I/O errors, or other technical failures.

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
  Use `claimRandom()`, `claimKey()`, `claimRandomMany()`, `popRandom()`, or `popRandomMany()` when you need exclusive allocation.

- **`popRandom(options?: { prefix?: string }): Promise<{ key: string, value: any } | null>`** - Claims one random free matching entry and removes it. Resolves to `null` when no match exists.

- **`claimRandom(options?: { prefix?: string, owner?: string, ttl?: number }): Promise<{ id: string, key: string, token: number, owner?: string, expiresAt: number, entry: { key: string, value: any } } | null>`** - Leases one random matching free entry. Live claims are excluded from later `claimRandom()` and `popRandom()` calls until released/completed or expired. If `ttl` is omitted, the default lease is **30000ms (30 seconds)**. `ttl` must be a positive integer and is capped at **86400000ms (24 hours)**. `owner` is optional diagnostic metadata capped at **256 bytes** and is not emitted as a metrics label.

- **`claimKey(key: string, options?: { owner?: string, ttl?: number }): Promise<{ id: string, key: string, token: number, owner?: string, expiresAt: number, entry: { key: string, value: any } } | null>`** - Leases one specific key. Resolves to `null` when the key is missing or already live-claimed.

- **`claimKeys(keys: string[], options?: { owner?: string, ttl?: number, allOrNothing?: boolean }): Promise<{ claimed: Array<{ id: string, key: string, token: number, owner?: string, expiresAt: number, entry: { key: string, value: any } }>, busy: string[], missing: string[] }>`** - Leases explicit keys in one call and returns `claimed` + `busy` + `missing` sets. Duplicate/empty keys reject as invalid options. With `allOrNothing: true`, processing stops on the first `busy`/`missing` key and only attempts best-effort rollback for claims acquired by this call when a later key is missing or busy. `allOrNothing` is a best-effort cleanup helper and does **not** make `claimKeys()` transactional. Use `allOrNothing: false` when you need a full diagnostic partition of all requested keys; `allOrNothing: true` is optimized for fast reservation and early rollback. `claimKeys()` accepts explicit keys only; it does not support prefix filtering in options.
  > `claimKeys()` classifies `missing`/`busy` on a best-effort basis under concurrent mutation. It first attempts `ClaimKey()` and only when that returns `null` does a fallback `Exists()` check for `missing` vs `busy` classification. It is designed for deterministic fixture reservation, not transaction-level classification; a key can still be reclassified by concurrent mutation between claim attempt and fallback existence check. Only technical storage errors reject rollback; expiry/missing races during rollback are tolerated.

- **`claimRandomMany(options: { prefix?: string, count: number, owner?: string, ttl?: number }): Promise<Array<{ id: string, key: string, token: number, owner?: string, expiresAt: number, entry: { key: string, value: any } }>>`** - Leases up to `count` unique random free matching entries from one store call. Resolves to `[]` when no free matches exist.

- **`popRandomMany(options: { prefix?: string, count: number }): Promise<Array<{ key: string, value: any }>>`** - Claims up to `count` random free matching entries, decodes them, and completes each claim with `deleteKey=true`. Resolves to `[]` when no free matches exist. Completed deletes are not rolled back if a later completion fails. Remaining live claims are released best-effort.

- **`releaseClaim(claim: { id: string, key: string, token: number }): Promise<boolean>`** - Releases a live claim back to the pool. Returns `false` for stale/expired/missing claims.

- **`releaseClaims(claims: Array<{ id: string, key: string, token: number }>): Promise<{ attempted: number, released: number, failed: Array<{ index: number, id: string, key: string, name: string, message: string }> }>`** - Batch release helper with partial success. Payload shape errors reject; stale/missing claims are reported in `failed` with `name: "ClaimNotUpdated"`. Technical storage errors reject the whole promise, include applied-progress details in the error message (`after releasing X of Y`), and may happen after earlier items in the same batch were already applied.

- **`completeClaim(claim, options?: { deleteKey?: boolean }): Promise<boolean>`** - Completes a live claim. By default it also deletes the underlying key (`deleteKey: true`).

- **`completeClaims(claims: Array<{ id: string, key: string, token: number }>, options?: { deleteKey?: boolean }): Promise<{ attempted: number, completed: number, failed: Array<{ index: number, id: string, key: string, name: string, message: string }> }>`** - Batch completion helper with partial success and the same `deleteKey` default as `completeClaim()`. Stale/missing claims are reported in `failed` with `name: "ClaimNotUpdated"`. Technical storage errors reject the whole promise, include applied-progress details in the error message (`after completing X of Y`), and may happen after earlier items in the same batch were already applied.

- **`renewClaim(claim: { id: string, key: string, token: number }, options: { ttl: number }): Promise<boolean>`** - Extends a live claim lease without changing the claim token. Returns `false` for stale/expired/missing claims or when the claim no longer owns the key.

- **`renewClaims(claims: Array<{ id: string, key: string, token: number }>, options: { ttl: number }): Promise<{ attempted: number, renewed: number, failed: Array<{ index: number, id: string, key: string, name: string, message: string }> }>`** - Batch lease-renew helper with partial success and per-item failure diagnostics. Stale/missing claims are reported in `failed` with `name: "ClaimNotUpdated"`. Technical storage errors reject the whole promise, include applied-progress details in the error message (`after renewing X of Y`), and may happen after earlier items in the same batch were already applied.

  Batch lifecycle helper semantics:

  - `releaseClaims()`, `completeClaims()`, and `renewClaims()` are lifecycle convenience helpers.
  - They execute claim operations sequentially in Go and return partial success summaries.
  - Per-item stale/missing failures use stable `name: "ClaimNotUpdated"` in the `failed[]` array; `failed[].index` maps each failure to the original input position.
  - Technical storage errors reject the whole promise and can happen after earlier items in the same batch were already applied.
  - Rejection messages include applied-progress context (`after releasing/completing/renewing X of Y`) and earlier successful items are not rolled back.
  - They are not cross-claim transactions and are intended for cleanup ergonomics, not backend-native high-throughput batch mutation.

  Claim lifecycle guidance:

  - Use `completeClaim()` / `completeClaims()` on **success** (`deleteKey: true` consumes items permanently).
  - Use `releaseClaim()` / `releaseClaims()` on **failure** to return items to the pool.

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

> ⚠️ Claim APIs (`claimRandom`, `claimKey`, `claimKeys`, `claimRandomMany`, `releaseClaim`, `releaseClaims`, `renewClaim`, `renewClaims`, `completeClaim`, `completeClaims`, `popRandom`, `popRandomMany`) are local coordination primitives for VUs sharing the same `xk6-kv` process/store. They are not distributed lock services.
> `claimRandom()` and `popRandom()` are random allocation helpers optimized for low/moderate live-claim occupancy. Under very high live-claim occupancy, fallback selection can be biased toward scan order, but exclusivity is still preserved.
> `claim.token` is exposed as a JavaScript number. It should not approach `Number.MAX_SAFE_INTEGER` in practical k6 runs; if that ever becomes realistic, a future major API should expose it as a string.

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

- **`allocationStats(options?: { prefix?: string }): Promise<{ prefix: string, total: number, claimable: number, claimedLive: number, claimedExpired: number, backend: "memory" | "disk", trackKeys: boolean }>`** - Returns prefix-scoped allocation health for claim workflows.
  `prefix` is optional; empty/omitted means all keys.

  ```javascript
  const pool = await kv.allocationStats({ prefix: "users:" });
  console.log(pool.total, pool.claimable, pool.claimedLive, pool.claimedExpired);
  ```

  Use `stats()` / `reportStats()` for global store health. Use `allocationStats({ prefix })` when you manage independent pools by prefix.
  `allocationStats()` is intentionally an on-demand pool-availability diagnostic (not a metrics label dimension and not a low-level bbolt consistency checker). Prefix tags would create high-cardinality time-series in Prometheus-style metrics.
  Backend behavior:
  - memory backend: scans matching in-memory keyspace/shards;
  - disk backend + `trackKeys: true`: writable handles read process-local operational key index used by claim APIs;
  - disk backend + `trackKeys: true` + read-only open: falls back to durable bbolt key/claim scan;
  - disk backend + `trackKeys: false`: scans durable bbolt keys for the prefix.
  Treat `total` as "keys visible to the allocation index" for disk `trackKeys: true`, not as a forensic durable bbolt key count after out-of-band mutations.
  After out-of-band durable mutations, check `stats().index.consistent` and call `rebuildKeyList()` before trusting prefix totals.
  Recovery pattern:

  ```javascript
  const stats = await kv.stats();
  if (stats.backend === "disk" && stats.trackKeys && stats.index && !stats.index.consistent) {
    await kv.rebuildKeyList();
  }

  const pool = await kv.allocationStats({ prefix: "users:" });
  ```

  For large prefixes it may still be scan-heavy; use it in setup/teardown, health checks, or low-frequency diagnostics, not on every hot-path iteration.
  Expired claims are counted as claimable; allocation operations can lazily reap or overwrite expired claim metadata.
  The diagnostic call itself does not promise physical cleanup of stale lease records.

- **`reportStats(): Promise<void>`** - Emits state gauges to k6 custom metrics using the current snapshot.

  Emitted metrics:
  - `xk6_kv_keys`
  - `xk6_kv_claims_live`
  - `xk6_kv_claims_expired`
  - `xk6_kv_index_keys` (with `index=keys_list|keys_map|ost`)
  - `xk6_kv_index_consistent`
  - `xk6_kv_disk_size_bytes` (disk backend only)

  Prefix-specific diagnostics are intentionally excluded from metrics labels to avoid high-cardinality time-series. Use `allocationStats({ prefix })` for those checks.

  ```javascript
  await kv.reportStats();
  ```

- **`metrics.operations` (openKv option)** - Enables automatic operation metrics for every async KV method except sync `close()`.

  Emitted metrics:
  - `xk6_kv_operations_total` (Counter, tags: `op`, `backend`, `status`, `track_keys`, `serialization`)
  - `xk6_kv_operation_duration` (Trend in milliseconds, tags: `op`, `backend`, `status`, `track_keys`, `serialization`)
  - `xk6_kv_operation_failed` (Rate, tags: `op`, `backend`, `track_keys`, `serialization`)
  - `xk6_kv_errors_total` (Counter, tags: `op`, `backend`, `error_type`, `track_keys`, `serialization`)
  - `xk6_kv_empty_result` (Rate for `random_key`/`random_keys`/`pop_random`/`claim_random`/`claim_key`/`claim_random_many`/`pop_random_many`, tags: `op`, `backend`, `track_keys`, `serialization`)
  - `xk6_kv_async_in_flight` (Gauge for async store operations currently running, tags: `backend`, `track_keys`, `serialization`)

  Batch lifecycle helpers (`releaseClaims` / `completeClaims` / `renewClaims`) can resolve with item-level `failed[]` entries without promise rejection. Those partial outcomes are returned in API results and are not emitted as operation-level error metrics unless the promise itself rejects on a technical storage error.

  `xk6_kv_async_in_flight` is the current saturation signal for background store operations in the async bridge. It is decremented when the store goroutine queues its event-loop completion callback, so it is not a count of unresolved JavaScript promises. During shutdown/cancellation, the VU context may already be canceled and the final decrement sample can be dropped by the k6 metrics sink, even though xk6-kv still decrements its internal in-flight counter. There is no `waiting` metric because xk6-kv does not currently have an async limiter or queue; if one is added later, a low-cardinality waiting gauge can be added alongside it.

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
      "xk6_kv_async_in_flight": ["value<1000"],
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
  > **File replacement semantics:** backup/export writes to a temp file in the destination directory, fsyncs, then replaces the destination and syncs the parent directory. On Unix-like filesystems this is same-directory atomic rename. On Windows, replacement uses best-effort remove-then-rename fallback when direct rename cannot replace an existing file, so treat it as crash-safer overwrite rather than a portable transactional primitive.

- **`restore(options?: RestoreOptions): Promise<RestoreSummary>`**  
  Replaces the dataset with a snapshot produced by `backup()`. Optional `maxEntries` / `maxBytes` guards protect against oversized or corrupted inputs.

- **`exportJSONL(options: { fileName: string, prefix?: string, limit?: number }): Promise<{ exported: number, fileName: string, bytesWritten: number }>`**  
  Exports key/value entries to a JSON Lines file for portable seed data and diff-friendly snapshots. Each line is:
  `{"key":"...","value":...}`. Values are exported after normal KV deserialization, not as raw backend bytes.
  This is a scan-based export, not a long-lived point-in-time snapshot. Concurrent writes/deletes may affect later pages; use `backup()` for snapshot-style capture.

- **`exportCSV(options: { fileName: string, prefix?: string, limit?: number, delimiter?: string, columns: string[], includeKey?: boolean }): Promise<{ exported: number, fileName: string, bytesWritten: number }>`**  
  Exports tabular object data to CSV for portable handoff/reporting. Header row is always written. Values are exported after normal KV deserialization, not as raw backend bytes.
  This is a scan-based export, not a long-lived point-in-time snapshot. Concurrent writes/deletes may affect later pages; use `backup()` for snapshot-style capture.

- **`importJSONL(options: { fileName: string, limit?: number, batchSize?: number }): Promise<{ imported: number, fileName: string, bytesRead: number }>`**  
  Imports key/value entries from a JSON Lines file. Each line must be:
  `{"key":"...","value":...}`.

- **`importCSV(options: { fileName: string, keyColumn: string, delimiter?: string, hasHeader?: boolean, limit?: number, batchSize?: number }): Promise<{ imported: number, fileName: string, bytesRead: number }>`**  
  Imports key/value rows from a CSV file. Each row becomes one object value; `keyColumn` selects the key field.

- **`validateCSV(options: { fileName: string, keyColumn?: string, delimiter?: string, hasHeader?: boolean, limit?: number }): Promise<{ valid: boolean, rows: number, bytesRead: number, checkedAll: boolean, firstError?: { row: number, name: string, message: string } }>`**  
  Read-only CSV preflight check with two modes:
  - syntax mode (`keyColumn` omitted): validates CSV readability, delimiter/header shape;
  - import-shape mode (`keyColumn` provided): validates syntax plus key extraction rules used by `importCSV()`.
  Validation APIs are KV-instance methods for API consistency and metrics collection.
  They require an open KV handle (reject with `StoreClosedError` after `close()`), but do not read or write KV store contents.
  Content errors resolve with `valid: false`; invalid options and file I/O errors reject.
  `bytesRead` is diagnostic progress information only. It is not a stable resume offset and should not be used as a cursor.
  `checkedAll=true` means validation reached EOF. `checkedAll=false` means validation stopped because `limit` was reached.
  Contract: treat `valid: true` as full-file validity only when `checkedAll === true`.
  `delimiter` follows the same contract as `importCSV()` (`1` character, not `\r`, `\n`, `"`, or Unicode replacement rune `\uFFFD`).
  If omitted or `<= 0`, `limit` validates all rows.
  When `limit > 0`, only the first `N` data rows are checked, so `valid: true` applies to the inspected prefix.

- **`validateJSONL(options: { fileName: string, limit?: number }): Promise<{ valid: boolean, records: number, bytesRead: number, checkedAll: boolean, firstError?: { line: number, name: string, message: string } }>`**  
  Read-only JSONL preflight check. Content errors resolve with `valid: false`; invalid options and file I/O errors reject.
  Validation APIs are KV-instance methods for API consistency and metrics collection.
  They require an open KV handle (reject with `StoreClosedError` after `close()`), but do not read or write KV store contents.
  `bytesRead` is diagnostic progress information only. It is not a stable resume offset and should not be used as a cursor.
  `checkedAll=true` means validation reached EOF. `checkedAll=false` means validation stopped because `limit` was reached.
  Contract: treat `valid: true` as full-file validity only when `checkedAll === true`.
  When `limit > 0`, only the first `N` records are checked, so `valid: true` applies to the inspected prefix.

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

`exportJSONL()` writes to a temporary file, flushes and fsyncs it, replaces the target, then syncs the parent directory. On Unix-like filesystems this gives atomic replacement in the common same-directory case. On Windows, replacement falls back to remove-then-rename when needed; this is still best-effort crash safety and avoids intentionally writing partial data directly into the target file.

`exportCSV()` example:

```javascript
const result = await kv.exportCSV({
  fileName: "./exports/responses.csv",
  prefix: "responses:",
  includeKey: true,
  columns: ["status", "requestId", "userId", "bodyHash"],
});

console.log(result.exported);
console.log(result.bytesWritten);
```

`exportCSV()` options:

- `fileName` is required and must be a non-empty string.
- `columns` is required, must be non-empty, and column names must be unique non-empty strings.
- Column names are exact; leading/trailing whitespace is significant.
- `includeKey` is optional and defaults to `true`.
- When `includeKey=true`, `columns` must not contain `"key"` because `exportCSV()` writes the store key as the first CSV column.
- Use `includeKey=false` if you need to export a value field named `"key"`.
- `delimiter` is optional and must be a valid CSV delimiter: exactly one character, not `\r`, `\n`, `"`, or Unicode replacement rune `\uFFFD`. Default is comma.
- `prefix` is optional; empty or omitted means all keys.
- `limit` is optional; if omitted or `<= 0`, all matching entries are exported. Positive values are capped at `1000000`.

`exportCSV()` data-shape constraints:

- `exportCSV()` exports tabular object data.
- Each exported value must decode to a plain JSON object row.
- Internal contract: value deserialization must produce `map[string]any`.
- Only top-level scalar fields are supported.
- Missing requested fields are written as empty cells.
- Nested objects and arrays are rejected.
- Nested path expressions (for example, `user.id`) are not supported.
- Scalar/non-object stored values are rejected.
- Stores opened with `serialization: "string"` are generally incompatible with `exportCSV()` because values decode as strings, not JSON object rows.
- Supported scalar field types: string, boolean, integer/float, `json.Number`, and `null`.
- CSV is a flat text format and does not preserve JSON value types across roundtrips.
- Numbers/booleans are exported as text cells and `importCSV()` reads those cells as strings.
- Use `exportJSONL()` + `importJSONL()` for type-preserving roundtrips.
- For scalar/string values, use `exportJSONL()`.
- No flattening, schema inference, transforms, or automatic nested JSON stringification are performed.

Rejected-shape examples:

```javascript
await kv.set("bad:1", {
  status: 200,
  body: { id: 123 },
});

await kv.set("bad:2", "raw body");

await kv.exportCSV({
  fileName: "./bad.csv",
  prefix: "bad:",
  columns: ["status", "body"],
});
// rejects:
// - bad:1 -> nested object column ("body")
// - bad:2 -> scalar/non-object row value
// use exportJSONL() for nested/scalar data
```

`exportCSV()` uses the same temp-file -> flush/sync -> rename -> parent-dir-sync replacement strategy as `exportJSONL()`.

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

`importCSV()` streams rows and writes them in `setMany()` batches. Existing keys are overwritten.
`keyColumn` is required; with `hasHeader: true` it is a header name, with `hasHeader: false` it must be a zero-based column index encoded as string.
CSV rows are lenient by width: if a row has fewer columns than the header, missing fields are imported as empty strings; if a row has more columns than the header, extra fields are preserved as `column_N`.
Rows with missing/empty keys are rejected with row-numbered parse errors.

`importCSV()` options:

- `fileName` is required and must be a non-empty string.
- `keyColumn` is required (column name when `hasHeader: true`, zero-based index string when `hasHeader: false`).
- `delimiter` is optional and must be a valid CSV delimiter: exactly one character, not `\r`, `\n`, `"`, or Unicode replacement rune `\uFFFD`.
- `hasHeader` is optional and defaults to `true`.
- `limit` is optional; if omitted or `0`, all rows are imported. Negative values are rejected. Positive values are capped at `1000000`.
- `batchSize` is optional; if omitted or `0`, the default batch size is used. Negative values are rejected. Positive values are capped at `10000`.
- CSV import/validation currently relies on Go `encoding/csv` and does not enforce a per-record byte cap; avoid feeding untrusted arbitrarily large rows. For bounded per-record guards, prefer JSONL (`64 MiB` per-line limit).

`validateCSV()` example:

```javascript
const csvCheck = await kv.validateCSV({
  fileName: "./examples/fixtures/users.csv",
  keyColumn: "id",
  hasHeader: true,
});

if (!csvCheck.valid) {
  throw new Error(`csv invalid at row ${csvCheck.firstError.row}: ${csvCheck.firstError.message}`);
}
```

`validateJSONL()` example:

```javascript
const jsonlCheck = await kv.validateJSONL({
  fileName: "./examples/fixtures/users.jsonl",
});

if (!jsonlCheck.valid) {
  throw new Error(`jsonl invalid at line ${jsonlCheck.firstError.line}: ${jsonlCheck.firstError.message}`);
}
```

Validation semantics (`validateCSV()` / `validateJSONL()`):

- Read-only: no writes to the KV store.
- `validateCSV()` has two modes:
  - syntax mode (`validateCSV({ fileName, hasHeader, delimiter })`) checks CSV readability/header shape;
  - import-shape mode (`validateCSV({ fileName, keyColumn, hasHeader, delimiter })`) also validates key extraction compatibility with `importCSV()`.
- Validation methods are KV-instance methods for API consistency and metrics collection.
- They require an open KV handle (`StoreClosedError` after `close()`), but they do not read or write KV store contents.
- They are read-only (no KV writes) but still go through normal operation lifecycle/metrics paths.
- Stops at first malformed content record.
- Malformed content resolves a structured invalid result (`valid: false`, `firstError`).
- Invalid options, file open/permission/system I/O failures, and context cancellation reject.
- `bytesRead` is a diagnostic byte counter for data read from the file, not a stable resume/cursor offset.
- `validateCSV()` uses the same delimiter contract as `importCSV()` / `exportCSV()` (exactly one character, not `\r`, `\n`, `"`, or `\uFFFD`).
- `validateCSV()` uses Go `encoding/csv` parsing compatible with `importCSV()`.
- CSV row-width handling is lenient (same as `importCSV()`): missing columns are treated as empty fields; extra columns are accepted.
- In import-shape mode (`keyColumn` set), this means key checks run against normalized rows (`""` for missing cells, `column_N` for extras).
- `validateCSV()` `limit` defaults to full-file validation when omitted or `<= 0`.
- `validateJSONL()` `limit` defaults to full-file validation when omitted or `<= 0`.
- `checkedAll=true` means validation reached EOF.
- `checkedAll=false` means validation was stopped by `limit`; in this case `valid: true` confirms only inspected prefix validity.
- Contract: `valid: true` means "whole file valid" only when `checkedAll === true`.
- With `limit > 0`, only the inspected prefix is validated:
  - CSV: first `N` data rows;
  - JSONL: first `N` records.
  In that mode, `valid: true` does not imply the whole file is valid.
- Empty CSV/JSONL files are considered valid preflight input (`rows: 0` / `records: 0`).
- If your workflow requires non-empty data, assert `rows > 0` or `records > 0` in script checks.

- **`close(): void`** - Synchronously closes this KV handle. Call once in `teardown()`.
  After `close()`, this handle rejects async operations with `StoreClosedError` on both backends.
  Operations that already started before `close()` may still resolve or reject normally.
  Closing one handle does not affect other open handles until the shared store refcount reaches zero.
  It also does not allow later `openKv()` calls to switch backend, path, serialization, or key-tracking options.
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
- **Disk claim allocation:** with `trackKeys: true`, claim metadata is stored in process-local in-memory OST metadata (not bbolt). `claimRandom()`, `claimKey()`, `claimRandomMany()`, `releaseClaim()`, `renewClaim()`, and `completeClaim({ deleteKey: false })` stay on the in-memory path; only durable key deletes (`popRandom()`, `popRandomMany()`, `completeClaim({ deleteKey: true })`) require bbolt `Update()`. With `trackKeys: false`, claim metadata remains in the bbolt claims-bucket fallback path, and batch random claim allocation may scan/materialize a large candidate set. For high-throughput disk random allocation, prefer `trackKeys: true`.

#### AllocationStats Benchmark Matrix

`allocationStats()` is diagnostic and can be scan-heavy on large prefixes. Use this benchmark matrix to profile your environment:

```bash
go test ./kv/store -run '^$' -bench 'AllocationStats' -benchtime=3s -count=3 -benchmem
```

Scenarios covered in the benchmark:

- memory `trackKeys=false` with prefix coverage `1%`, `10%`, `100%`
- memory `trackKeys=true` with prefix coverage `1%`, `10%`, `100%`
- disk `trackKeys=false` with prefix coverage `1%`, `10%`, `100%`
- disk `trackKeys=true` with prefix coverage `1%`, `10%`, `100%`
- memory keyspace-scaling matrix (`1k`, `10k`, `100k` total keys with `10%` prefix matches)

Memory keyspace-scaling command (to quantify O(N)-style diagnostics pressure before optimizing):

```bash
go test ./kv/store -run '^$' -bench 'MemoryStore_AllocationStats_KeyspaceScaling' -benchtime=3s -count=3 -benchmem
```

#### Disk Allocation Benchmark Snapshot

On the current benchmark machine (`linux/amd64`, `AMD Ryzen 9 9950X`, short benchmark windows), disk random allocation favors `trackKeys: true` for claim operations:

| Operation | `trackKeys=true` | `trackKeys=false` | Note |
| --- | ---: | ---: | --- |
| `claimRandom` | ~`1.5-1.6 us/op` | ~`0.48-1.28 ms/op` | tracked path avoids bbolt claim writes |
| `claimRandomMany` (`count=100`) | ~`58-64 us/op` | fallback path substantially slower under load | near-linear scaling with count |
| `renewClaim` (tracked) | ~`170-180 ns/op` | N/A | in-memory claim metadata update |
| `popRandom` | ~`0.6-2.6 ms/op` | ~`0.6-2.6 ms/op` | durable delete path dominates |

Treat these as directional numbers for this machine/runtime profile; rerun benchmarks in your environment for release gating.

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

Runnable entry points: [Start here](#start-here-existing-scripts). Complete examples are in [`examples/`](./examples); production-style scenarios are in [`e2e/`](./e2e).

Observability-focused scripts:

- Example operation metrics in a worker queue: [`examples/metrics-operations-worker-queue.js`](./examples/metrics-operations-worker-queue.js)
- Example `stats()` / `reportStats()` health snapshots: [`examples/metrics-report-stats-health.js`](./examples/metrics-report-stats-health.js)
- Example batch random key sampling + hydration: [`examples/random-keys.js`](./examples/random-keys.js)
- E2E lease-worker observability scenario: [`e2e/subscription-renewal-lease-observability.js`](./e2e/subscription-renewal-lease-observability.js)
- E2E credential pool drain scenario: [`e2e/credential-pool-drain-observability.js`](./e2e/credential-pool-drain-observability.js)
- Batch claim allocation example: [`examples/claim-random-many.js`](./examples/claim-random-many.js)
- Batch claim lifecycle example: [`examples/claim-batch-lifecycle.js`](./examples/claim-batch-lifecycle.js)
- Explicit fixture reservation example: [`examples/claim-keys.js`](./examples/claim-keys.js)
- Batch pop allocation example: [`examples/pop-random-many.js`](./examples/pop-random-many.js)
- CSV import example: [`examples/import-csv.js`](./examples/import-csv.js)
- CSV export example: [`examples/export-csv.js`](./examples/export-csv.js)
- Allocation diagnostics example: [`examples/allocation-stats.js`](./examples/allocation-stats.js)
- Import preflight validation example: [`examples/validate-import-files.js`](./examples/validate-import-files.js)
- E2E batch lifecycle scenario: [`e2e/batch-claim-lifecycle.js`](./e2e/batch-claim-lifecycle.js)
- E2E explicit fixture reservation scenario: [`e2e/claim-keys-explicit-fixtures.js`](./e2e/claim-keys-explicit-fixtures.js)
- E2E allocation diagnostics scenario: [`e2e/allocation-stats-pool-health.js`](./e2e/allocation-stats-pool-health.js)
- E2E CSV export scenario: [`e2e/export-csv-response-capture.js`](./e2e/export-csv-response-capture.js)

### Allocation Recipes

- **Unique users:** import a user pool (`importCSV()` or `setMany()`), allocate with `claimRandom()` / `claimRandomMany()`, then `completeClaim()` / `completeClaims()` on success or `releaseClaim()` / `releaseClaims()` on failure.
- **Credential pool:** fetch exact credentials with `claimKey()` / `claimKeys()`, extend long-running work with `renewClaim()` / `renewClaims()`, and always release stale/failed attempts.
- **Response capture:** write response envelopes with `kv.set("responses:<id>", payload)` during the run, then `exportJSONL()` or `exportCSV()` in teardown/summary.
- **Pool diagnostics:** use global `stats()` for store-wide health and `allocationStats({ prefix })` for prefix-scoped claimability snapshots.
- **Cross-run handoff:** use disk backend + `backup()`/`restore()` or `exportJSONL()`/`importJSONL()`; claim leases are process-local and are cleared on writable open/clear/restore/rebuild/close.

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

The Taskfile includes internal helpers (`ensure-bin`, tool installers) plus user-facing tasks below.

| Command | What it does |
| --- | --- |
| `task` | Show common tasks (`task -l`). |
| `task install-githooks` | Configure local commit hooks (`.githooks`). |
| `task remove-githooks` | Disable local commit hooks for this repository. |
| `task version-check` | Predict next semantic version from commits. |
| `task pin-tool-versions` | Sync pinned tool versions in CI/local files. |
| `task install-tools` | Install all pinned dev tools (`golangci-lint`, `xk6`, `govulncheck`). |
| `task lint-fix` | Run `golangci-lint` with autofix. |
| `task lint` | Run `golangci-lint` checks. |
| `task vulncheck` | Run `govulncheck` using `GOTOOLCHAIN=go<GO_VERSION>` from `taskfile.yaml` (kept in sync with `go.mod` via `pin-tool-versions`). |
| `task xk6-lint-community` | Run `xk6 lint --preset community .` with pinned toolchain; auto-installs `gosec` into `./bin` if missing. |
| `task test` | Run unit tests (`go test -v ./...`). |
| `task test-race` | Run unit tests with race detector. |
| `task test-typescript` | Run TypeScript declaration smoke tests (`npm ci` + `npm test` in `typescript/`). |
| `task release-check` | Run full release readiness checks (`lint`, `xk6-lint-community`, `test-race`, `test-typescript`, and E2E via `scripts/release_check_e2e.sh` / `scripts/release_check_e2e.ps1`). |
| `task build-k6` | Build `./bin/k6` with the local `xk6-kv` extension. |
| `task test-e2e-all` | Run all `e2e/*.js` scenarios across backend/key-tracking matrix. |
| `task test-e2e-single E2E_SCENARIO=tenant-prefix-count-window` | Run one scenario across backend/key-tracking matrix. |
| `task clean` | Remove generated binaries and repository-local temporary artifacts. |

#### E2E recipe notes

- `test-e2e-single` requires `E2E_SCENARIO`.
- `test-e2e-all` and `test-e2e-single` support `VUS` and `ITERATIONS` overrides.
- E2E scenarios require the Taskfile-built binary (`task build-k6`) because they exercise the local `k6/x/kv` extension.
- Temporary E2E database, snapshot, CSV, and JSONL artifacts are removed by `scripts/run_e2e_scenario.sh` / `scripts/run_e2e_scenario.ps1`; `task clean` removes all repository-local scratch files under `tmp/`.
- Example: `task test-e2e-single E2E_SCENARIO=tenant-prefix-count-window VUS=20 ITERATIONS=200`.

### Code Quality

- **gofumpt** for formatting (stricter than gofmt)
- **golangci-lint** with 40+ enabled linters
- **gci** and **goimports** for import organization
- **golines** for line length (max 120 characters)

Run `task lint-fix` before committing.

### Public API Change Checklist

When adding or changing a public JS method, update all of:

- Go method implementation and operation metric name.
- `typescript/xk6-kv.d.ts`.
- `README.md` API table/reference.
- user-facing example or e2e scenario when behavior is externally visible.
- `typescript/api-smoke.ts`.

## Versioning & Releases

This project uses **automated semantic versioning** based on commit messages:

Before public release, run `task release-check`.

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
