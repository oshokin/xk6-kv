# xk6-kv

[![Go Report Card](https://goreportcard.com/badge/github.com/oshokin/xk6-kv)](https://goreportcard.com/report/github.com/oshokin/xk6-kv)
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)

> **Fork Notice**  
> This project is a fork of [oleiade/xk6-kv](https://github.com/oleiade/xk6-kv), extended with **additional atomic primitives** and **random-key utilities**:
>
> - Atomic operations: `incrementBy`, `getOrSet`, `swap`, `compareAndSwap`, `deleteIfExists`, `compareAndDelete`.
> - Random keys: `randomKey()` with **prefix-aware selection** and optional O(1) performance.
> - Optional key tracking for **O(1) random sampling** (disk & memory backends).
> - Disk backend path overrides for custom artifact persistence.
>
> All code is licensed under **GNU AGPL v3.0**.

A k6 extension providing a persistent key-value store to share state across Virtual Users (VUs). Supports both **in-memory** and **persistent bbolt** backends, deterministic `list()` ordering, and high-level atomic helpers designed for safe concurrent access in load tests.

## Features

- 🔒 **Thread-Safe**: Secure state sharing across Virtual Users
- 🔄 **Flexible Storage**: In-memory (ephemeral) or disk-based (persistent) backends
- 📊 **Atomic Operations**: Increment, compare-and-swap, and more
- 🎲 **Random Key Selection**: Uniform sampling with optional prefix filtering
- 🔍 **Key Tracking**: Optional O(1) random key access via in-memory indexing
- 🏷️ **Prefix Support**: Filter operations by key prefixes
- 📦 **Stable bbolt Bucket**: Disk backend always uses the `k6` bucket (**original upstream bug tied it to the DB path and could orphan data - now fixed**)
- 🧭 **Cursor Scanning**: Stream large datasets via `scan()` with continuation tokens
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

2. Build k6 with xk6-kv:

```bash
# Latest version
xk6 build --with github.com/oshokin/xk6-kv@latest

# Specific version
xk6 build --with github.com/oshokin/xk6-kv@v1.3.6
```

3. Verify:

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

## Error Handling

Every rejected `kv.*` promise carries a **typed error object** with `name` and `message` fields. Check `err.name` (or `err.Name` when k6 serialises it as Go struct) instead of matching raw strings:

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

High-level categories:

- **Options & inputs** - typed guards such as `BackupOptionsRequiredError`, `ValueNumberRequiredError`, `UnsupportedValueTypeError`.
- **Concurrency & lifecycle** - e.g. `BackupInProgressError`, `RestoreInProgressError`, `StoreClosedError`.
- **Disk & snapshot IO** - precise signals for path issues, permission problems, bbolt failures, or restore budget overruns.

📚 A complete catalogue with root causes and remediation tips lives in [`examples/README.md`](./examples/README.md#error-manual).

## TypeScript Support

Full TypeScript support with IntelliSense and type safety! Copy the [`typescript/`](./typescript/) folder to your project for a ready-to-use starter kit.\
See [`typescript/README.md`](./typescript/README.md) for complete setup instructions.

## API Reference

### `openKv(options?)`

Opens a key-value store. **Must be called in the init context** (outside of default/setup/teardown functions).

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
    readOnly?: boolean                // open DB read-only; requires pre-existing DB/bucket; default false
    initialMmapSize?: number | string // initial mmap size; number=bytes, string supports SI ("MB") and IEC ("MiB"); 0 keeps default/no preallocation (default)
    mlock?: boolean                   // mlock pages (UNIX); default false
    // when omitted: bbolt defaults are applied
  }
}
```

**Options:**

- `backend`: `"memory"` (ephemeral, fastest) or `"disk"` (persistent bbolt)
- `serialization`: `"json"` (structured) or `"string"` (raw bytes)
- `trackKeys`: Enable in-memory key indexing for O(1) `randomKey()` performance
- `path`: (Disk only) Override bbolt file location
- `memory.shardCount`: (Memory only) Number of shards for concurrent performance. If `<= 0` or omitted, defaults to `runtime.NumCPU()` (automatic, recommended). If `> 65536`, automatically capped at 65536. Ignored by disk backend. When `memory` is omitted, defaults are applied.
- `disk`: (Disk only) Optional bbolt tuning. When `disk` is omitted, bbolt defaults apply (1s lock timeout, syncs enabled, array freelist, etc.).
- `disk.readOnly`: Requires the bbolt file (and `k6` bucket) to already exist; opening in read-only mode cannot create the bucket and will fail if the file is missing or empty.

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
- **`set(key: string, value: any): Promise<any>`** - Sets a key-value pair.
- **`delete(key: string): Promise<boolean>`** - Removes a key-value pair (always resolves to `true`).
- **`exists(key: string): Promise<boolean>`** - Checks if a key exists.
- **`clear(): Promise<boolean>`** - Removes all entries (always resolves to `true`).
- **`size(): Promise<number>`** - Returns current store size (number of keys).

#### Atomic Operations

- **`incrementBy(key: string, delta: number): Promise<number>`** - Atomically increments numeric value. Treats missing keys as `0`.
- **`getOrSet(key: string, value: any): Promise<{ value: any, loaded: boolean }>`** - Gets existing value or sets if absent. `loaded: true` means pre-existing.
- **`swap(key: string, value: any): Promise<{ previous: any|null, loaded: boolean }>`** - Replaces value atomically. Returns previous value if existed.
- **`compareAndSwap(key: string, oldValue: any, newValue: any): Promise<boolean>`** - Sets `newValue` only if current value equals `oldValue`. Pass `null`/`undefined` as `oldValue` to mean "only if the key is absent" (set-if-not-exists).
- **`deleteIfExists(key: string): Promise<boolean>`** - Deletes key if it exists. Returns `true` if deleted.
- **`compareAndDelete(key: string, oldValue: any): Promise<boolean>`** - Deletes key only if current value equals `oldValue`.

#### Query Operations

- **`scan(options?: ScanOptions): Promise<ScanResult>`** - Streams entries in lexicographic order using cursor-based pagination.

  ```typescript
  interface ScanOptions {
    prefix?: string; // Filter by key prefix
    limit?: number;  // Max results per page (<= 0 means "read to the end")
    cursor?: string; // Base64 cursor produced by the previous page ("" starts a new scan)
  }

  interface ScanResult {
    entries: Array<{ key: string; value: any }>;
    cursor: string; // Base64 cursor for the next page
    done: boolean;  // True when the scan reached the end of the prefix window
  }
  ```

  Use `scan()` when the keyspace is too large to materialize with `list()` or when you need restart-safe pagination.

- **`list(options?: ListOptions): Promise<Array<{ key: string; value: any }>>`** - Returns entries sorted lexicographically by key.

  ```typescript
  interface ListOptions {
    prefix?: string  // Filter by key prefix
    limit?: number   // Max results (<= 0 means no limit)
  }
  ```

- **`randomKey(options?: RandomKeyOptions): Promise<string>`** - Returns a random key, or `""` if none match.

  ```typescript
  interface RandomKeyOptions {
    prefix?: string  // Optional prefix filter
  }
  ```

- **`rebuildKeyList(): Promise<boolean>`** - Rebuilds in-memory key indexes (useful for disk backend with `trackKeys: true`).

#### Snapshot Operations

- **`backup(options?: BackupOptions): Promise<BackupSummary>`**  
  Writes the current dataset to a bbolt file. Always set `fileName` (leaving it blank points at the backend’s live bbolt file) and use `allowConcurrentWrites: true` for a best-effort dump that releases writers sooner (summary includes `bestEffort` + `warning` so you can alarm on it).

- **`restore(options?: RestoreOptions): Promise<RestoreSummary>`**  
  Replaces the dataset with a snapshot produced by `backup()`. Optional `maxEntries` / `maxBytes` guards protect against oversized or corrupted inputs.

```typescript
await kv.backup({
  fileName: "./backups/kv-latest.kv",
  allowConcurrentWrites: true,
});
 
await kv.restore({ fileName: "./backups/kv-latest.kv" });
```

> **Disk backend note:** pointing `fileName` at the live bbolt path is treated as a no-op (backup just returns metadata; restore leaves the DB untouched), so always write to / read from a different file.

- **`close(): void`** - Synchronously closes the store. Call in `teardown()`.

### Performance Notes

- **`trackKeys: true`**: `randomKey()` without prefix -> O(1); with prefix -> O(log n). Achieving those speeds means every key is mirrored in memory across multiple helper structures, so large datasets consume noticeably more RAM and the slices/maps never shrink automatically. Budget for that footprint or rebuild the index periodically.
- **`trackKeys: false`** (default): `randomKey()` falls back to a full-map/two-transaction scan, so heavy use remains O(n). Enable tracking or redesign workloads that call `randomKey()` frequently to avoid linear-time pauses.
- Both backends are optimized for concurrent workloads, but there's synchronization overhead between VUs

## Usage Examples

Complete examples are available in the [`examples/`](./examples) directory, and production-grade k6 scenarios live under [`e2e/`](./e2e).

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
|------|-------------|
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
