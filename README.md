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

A k6 extension providing a persistent key-value store to share state across Virtual Users (VUs). Supports both **in-memory** and **persistent BoltDB** backends, deterministic `list()` ordering, and high-level atomic helpers designed for safe concurrent access in load tests.

## Features

- 🔒 **Thread-Safe**: Secure state sharing across Virtual Users
- 🔄 **Flexible Storage**: In-memory (ephemeral) or disk-based (persistent) backends
- 📊 **Atomic Operations**: Increment, compare-and-swap, and more
- 🎲 **Random Key Selection**: Uniform sampling with optional prefix filtering
- 🔍 **Key Tracking**: Optional O(1) random key access via in-memory indexing
- 🏷️ **Prefix Support**: Filter operations by key prefixes
- 🧭 **Cursor Scanning**: Stream large datasets via `scan()` with continuation tokens
- 📝 **Serialization**: JSON or string serialization
- 📘 **TypeScript Support**: Full type declarations for IntelliSense and type safety
- ⚡ **High Performance**: Optimized for concurrent workloads

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
}
```

**Options:**

- `backend`: `"memory"` (ephemeral, fastest) or `"disk"` (persistent BoltDB)
- `serialization`: `"json"` (structured) or `"string"` (raw bytes)
- `trackKeys`: Enable in-memory key indexing for O(1) `randomKey()` performance
- `path`: (Disk only) Override BoltDB file location

### KV Methods

All methods return Promises except `close()`.

#### Basic Operations

- **`get(key: string): Promise<any>`** - Retrieves a value by key. Throws if key doesn't exist.
- **`set(key: string, value: any): Promise<any>`** - Sets a key-value pair.
- **`delete(key: string): Promise<boolean>`** - Removes a key-value pair (always resolves to `true`).
- **`exists(key: string): Promise<boolean>`** - Checks if a key exists.
- **`clear(): Promise<void>`** - Removes all entries.
- **`size(): Promise<number>`** - Returns current store size (number of keys).

#### Atomic Operations

- **`incrementBy(key: string, delta: number): Promise<number>`** - Atomically increments numeric value. Treats missing keys as `0`.
- **`getOrSet(key: string, value: any): Promise<{ value: any, loaded: boolean }>`** - Gets existing value or sets if absent. `loaded: true` means pre-existing.
- **`swap(key: string, value: any): Promise<{ previous: any|null, loaded: boolean }>`** - Replaces value atomically. Returns previous value if existed.
- **`compareAndSwap(key: string, oldValue: any, newValue: any): Promise<boolean>`** - Sets `newValue` only if current value equals `oldValue`.
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

- **`close(): void`** - Synchronously closes the store. Call in `teardown()`.

### Performance Notes

- **`trackKeys: true`**: `randomKey()` without prefix -> O(1); with prefix -> O(log n)
- **`trackKeys: false`**: `randomKey()` uses two-pass scan (fine for small-to-medium sets)
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
