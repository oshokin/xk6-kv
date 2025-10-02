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

A k6 extension providing a persistent key-value store to share state across Virtual Users (VUs).\
It supports both an **in-memory** backend and a **persistent BoltDB** backend, deterministic `list()` ordering, and high-level atomic helpers designed for safe concurrent access in load tests.

## Table of Contents

- [Features](#features)
- [Why Use xk6-kv](#why-use-xk6-kv)
- [Installation](#installation)
  - [Option 1: Pre-built Binaries](#option-1-pre-built-binaries-recommended)
  - [Option 2: Build from Source](#option-2-build-from-source)
- [Quick Start](#quick-start)
- [API Reference](#api-reference)
  - [openKv](#openkv)
  - [KV Methods](#kv-methods)
  - [Types](#types)
- [Usage Examples](#usage-examples)
  - [Producer / Consumer](#producer--consumer)
  - [Random Key With Prefix](#random-key-with-prefix)
  - [Rebuild The Key Index](#rebuild-the-key-index)
- [Development](#development)
  - [Using the Taskfile](#using-the-taskfile)
  - [Running Tests](#running-tests)
  - [Building Locally](#building-locally)
- [Versioning & Releases](#versioning--releases)
  - [Semantic Commit Format](#semantic-commit-format)
  - [Setting Up Git Hooks](#setting-up-git-hooks)
  - [Check Next Version](#check-next-version)
  - [Automated Releases](#automated-releases)
  - [Using Pre-built Binaries](#using-pre-built-binaries)
- [Contributing](#contributing)
  - [Development Workflow](#development-workflow)
  - [Code Quality Standards](#code-quality-standards)
  - [Cross-Platform Development](#cross-platform-development)

## Features

- 🔒 **Thread-Safe**: Secure state sharing across Virtual Users.
- 🔌 **Easy Integration**: Simple API that works seamlessly with k6.
- 🔄 **Flexible Storage**: Choose between in-memory or disk-based persistence.
- 🪶 **Lightweight**: No external dependencies required.
- 📊 **Atomic Operations**: Support for atomic increment, compare-and-swap, and more.
- 🔍 **Key Tracking**: Optional in-memory indexing for fast random key access.
- 🏷️ **Prefix Support**: Filter operations by key prefixes.
- 📝 **Serialization Options**: JSON or string serialization.
- 🎲 **Random Key Selection**: Uniform random sampling with optional prefix filtering.
- ⚡ **High Performance**: Optimized for concurrent workloads with BoltDB (disk) or in-memory maps.

## Why Use xk6-kv?

- **State Sharing Made Simple**: Managing state across multiple VUs in k6 can be challenging. **xk6-kv** provides a straightforward solution for sharing state, making your load testing scripts cleaner and more maintainable.
- **Built for Safety**: Thread safety is crucial in load testing. **xk6-kv** is designed specifically for k6's parallel VU execution model, ensuring your shared state operations remain safe and reliable.
- **Storage Options**: Choose the backend that fits your needs:
  - **Memory**: Fast, ephemeral storage that's shared across VUs within a single test run.
  - **Disk**: Persistent storage using BoltDB for data that needs to survive between test runs.
- **Lightweight Alternative**: While other solutions like Redis exist and are compatible with k6 for state sharing, **xk6-kv** offers a more lightweight, integrated approach:
  - No external services required.
  - Simple setup and configuration.
  - Built-in atomic operations for common patterns.

> **Note**: For extremely high-performance requirements or distributed test environments, consider using the [k6 Redis module](https://k6.io/docs/javascript-api/k6-experimental/redis/) instead.

## Installation

### Option 1: Pre-built Binaries (Recommended)

Download k6 binaries with xk6-kv already included from the [Releases page](https://github.com/oshokin/xk6-kv/releases).

**Linux:**

```bash
# Download for your architecture (amd64 or arm64)
curl -L https://github.com/oshokin/xk6-kv/releases/latest/download/k6-linux-amd64.tar.gz -o k6.tar.gz
tar -xzf k6.tar.gz
chmod +x k6
./k6 version
```

**macOS:**

```bash
# Download for your architecture (amd64 or arm64/Apple Silicon)
curl -L https://github.com/oshokin/xk6-kv/releases/latest/download/k6-darwin-arm64.tar.gz -o k6.tar.gz
tar -xzf k6.tar.gz
chmod +x k6
./k6 version
```

**Windows (PowerShell):**

```powershell
# Download for your architecture (amd64 or arm64)
Invoke-WebRequest -Uri "https://github.com/oshokin/xk6-kv/releases/latest/download/k6-windows-amd64.zip" -OutFile k6.zip
Expand-Archive -Path k6.zip -DestinationPath .
.\k6.exe version
```

### Option 2: Build from Source

1. First, ensure you have [xk6](https://github.com/grafana/xk6) installed:

```bash
go install go.k6.io/xk6/cmd/xk6@latest
```

2. Build a k6 binary with the xk6-kv extension:

```bash
# Build the latest version
xk6 build --with github.com/oshokin/xk6-kv@latest

# Or build a specific version
xk6 build --with github.com/oshokin/xk6-kv@v1.3.6
```

3. The built binary will be in your current directory:

```bash
./k6 version
```

> **Requirements**: Go 1.25 or higher is recommended for building from source.

## Quick Start

```javascript
import { openKv } from "k6/x/kv";

// Open a key-value store with the default backend (disk).
const kv = openKv();

// Or specify a backend explicitly:
// const kv = openKv({ backend: "disk" });   // Disk-based persistent backend (default).
// const kv = openKv({ backend: "memory" }); // In-memory backend.

export async function setup() {
    // Start with a clean state.
    await kv.clear();
}

export default async function () {
    // Set a bunch of keys.
    await kv.set("foo", "bar");
    await kv.set("abc", 123);
    await kv.set("easy as", [1, 2, 3]);
    await kv.set("user:1", { name: "Alice" });
    await kv.set("user:2", { name: "Bob" });

    // Grab any random key ("" if none).
    const any = await kv.randomKey();
    if (any) {
      console.log(`random key = ${any}, value = ${JSON.stringify(await kv.get(any))}`);
    }

    // Prefix-aware random.
    const u = await kv.randomKey({ prefix: "user:" }); // "" if no match.
    if (u) {
      console.log(`random user key = ${u}`);
    }

    const abcExists = await kv.exists("a b c")
    if (!abcExists) {
      await kv.set("a b c", { "123": "baby you and me girl"});
    }

    console.log(`current size of the KV store: ${await kv.size()}`)

    const entries = await kv.list({ prefix: "a" });
    for (const entry of entries) {
        console.log(`found entry: ${JSON.stringify(entry)}`);
    }

    await kv.delete("foo");
}

// Always close the store when done, typically in teardown().
export async function teardown() {
  kv.close();
}
```

## API Reference

### `openKv`

Opens a key-value store with the specified backend. **Must be called in the init context** (outside of default/setup/teardown functions).

```typescript
function openKv(options?: OpenKvOptions): KV

type Backend = "memory" | "disk"

interface OpenKvOptions {
  backend?: Backend               // default: "disk"
  path?: string                   // default: "" (falls back to ./.k6.kv)
  serialization?: "json"|"string" // default: "json"
  trackKeys?: boolean             // default: false
}
```

**Parameters:**

- **`backend`**: Storage backend to use.
  - `"memory"`: Shared in-process KV for all VUs in a test run (fastest, ephemeral).
  - `"disk"`: Persistent BoltDB file (survives between runs).
  
- **`serialization`**: Value serialization method.
  - `"json"`: Values are JSON-encoded/decoded automatically (default).
  - `"string"`: Values are stored as strings/bytes.
  
- **`trackKeys`**: Enable in-memory key indexing.
  - `true`: Maintain an in-memory index of keys; enables **O(1)** `randomKey()` (no prefix) and **O(log n)** prefix `randomKey()`.
  - `false`: No index; `randomKey()` uses a simple two-pass scan (default).
  
- **`path`**: (Disk backend only) Override the location of the BoltDB file.
  - Defaults to `./.k6.kv` if empty or invalid.
  - Ignored when `backend` is `"memory"`.

**Performance Notes:**

While both backends are optimized for performance and suitable for most load testing scenarios, be aware that:

- There is some overhead due to synchronization between VUs.
- Consider this overhead when analyzing your test results.
- For extremely high throughput requirements, you might need alternative solutions like Redis.

### KV Methods

All methods are **Promise-based**, except for `close()` which is synchronous.

#### `get(key: string): Promise<any>`

Retrieves a value by key. Throws if key doesn't exist.

```javascript
const value = await kv.get("myKey");
console.log(value);
```

#### `set(key: string, value: any): Promise<any>`

Sets a key-value pair. Accepts any JSON-serializable value if `serialization="json"`.

```javascript
await kv.set("myKey", { name: "Alice", age: 30 });
```

#### `incrementBy(key: string, delta: number): Promise<number>`

Atomically increments the numeric value at `key` by `delta`. If the key does not exist, it is treated as `0`. Returns the new value.

```javascript
await kv.incrementBy("counter", 5);  // Returns 5
await kv.incrementBy("counter", -2); // Returns 3
```

#### `getOrSet(key: string, value: any): Promise<{ value: any, loaded: boolean }>`

Atomically gets the existing value if the key exists, or sets it to `value` if it doesn't.

Returns an object:

- `loaded: true` means the value was pre-existing.
- `loaded: false` means the value was just set.

```javascript
const { value, loaded } = await kv.getOrSet("app_config", { theme: "dark" });
console.log(`config: ${JSON.stringify(value)}, loaded: ${loaded}`);
```

#### `swap(key: string, value: any): Promise<{ previous: any|null, loaded: boolean }>`

Atomically replaces the value at `key` with `value`.

Returns an object:

- `loaded: true` means the key existed and `previous` contains its old value.
- `loaded: false` means the key was created and `previous` is `null`.

```javascript
const { previous, loaded } = await kv.swap("status", "running");
console.log(`previous status: ${previous}, swapped: ${loaded}`);
```

#### `compareAndSwap(key: string, oldValue: any, newValue: any): Promise<boolean>`

Atomically sets the value at `key` to `newValue` only if its current value equals `oldValue`. Returns `true` if the swap was successful, `false` otherwise.

This is the fundamental building block for implementing locks and other synchronization primitives.

```javascript
const casSuccess = await kv.compareAndSwap("version", 1, 2);
console.log(`CAS success: ${casSuccess}`);
```

#### `delete(key: string): Promise<boolean>`

Removes a key-value pair. Always resolves to `true` (even if key didn't exist).

```javascript
await kv.delete("myKey");
```

#### `exists(key: string): Promise<boolean>`

Checks if a given key exists.

```javascript
const exists = await kv.exists("myKey");
console.log(`Key exists: ${exists}`);
```

#### `deleteIfExists(key: string): Promise<boolean>`

Deletes the key if it exists. Returns `true` if the key was deleted, `false` if it did not exist.

More informative than `delete()`, which always returns `true`.

```javascript
const deleted = await kv.deleteIfExists("temp_file");
console.log(`temporary file deleted: ${deleted}`);
```

#### `compareAndDelete(key: string, oldValue: any): Promise<boolean>`

Atomically deletes the key only if its current value equals `oldValue`. Returns `true` if the key was deleted, `false` otherwise.

Useful for "delete only if not processed" scenarios.

```javascript
const cndSuccess = await kv.compareAndDelete("job", "failed");
console.log(`conditional delete success: ${cndSuccess}`);
```

#### `clear(): Promise<void>`

Removes all entries. Resolves when done.

```javascript
await kv.clear();
```

#### `size(): Promise<number>`

Returns current store size (number of keys).

```javascript
const size = await kv.size();
console.log(`Store contains ${size} keys`);
```

#### `list(options?: ListOptions): Promise<Array<{ key: string; value: any }>>`

Returns key-value entries sorted lexicographically by key. If `limit <= 0` or omitted, there is no limit.

```javascript
// List all entries
const allEntries = await kv.list();

// List with prefix filter
const userEntries = await kv.list({ prefix: "user:" });

// List with limit
const firstThree = await kv.list({ limit: 3 });
```

#### `randomKey(options?: RandomKeyOptions): Promise<string>`

Returns a random key, optionally filtered by prefix. Resolves to `""` (empty string) when no key matches (including empty storage).

**Never throws** - always safe to call.

```javascript
// Random key from entire store
const anyKey = await kv.randomKey();

// Random key with prefix
const userKey = await kv.randomKey({ prefix: "user:" });

if (userKey) {
  console.log(`Selected user: ${userKey}`);
}
```

#### `rebuildKeyList(): Promise<boolean>`

Rebuilds in-memory key indexes from the underlying store if supported. Resolves to `true` when finished (no-op + `true` when `trackKeys` is disabled).

The operation is **O(n)** over keys, so prefer running it in `setup()` or infrequently between stages - not every iteration.

- On the **memory** backend it's typically unnecessary unless you manually corrupted/replaced the in-memory structures.
- On the **disk** backend with `trackKeys: true`, the index is **automatically rebuilt at startup**. Call `rebuildKeyList()` manually only when the DB is mutated **out-of-band during the run** or if you need to **recover without reopening the store**.

```javascript
const ok = await kv.rebuildKeyList();
console.log(`Rebuild complete: ${ok}`);
```

#### `close(): void`

**Synchronously** closes the underlying store and releases any associated resources (e.g., file handles for the disk backend).

This method should be called in the `teardown()` function or when you are certain no further operations will be performed on the KV instance. It does not return a Promise because the caller usually needs to know the outcome immediately.

- On the **disk** backend it releases the BoltDB file handle and in-memory indexes. Safe to call multiple times. Recommended to call once in `teardown()`.
- On the **memory** backend it's a no-op (safe to call, does nothing).
- If multiple `KV` instances share the same store, the underlying store closes only when the last `close()` is called (reference-counted).

```javascript
export async function teardown() {
  kv.close();
}
```

### Types

```typescript
interface RandomKeyOptions {
  prefix?: string  // Optional prefix filter
}

interface ListOptions {
    prefix?: string;  // Filter by key prefix
    limit?: number;   // Max number of results (<= 0 means "no limit")
}
```

#### Performance Notes

When **`trackKeys: true`**:

- `randomKey()` with **no prefix** → **O(1)** (uniform index sampling).
- `randomKey({prefix})` → **O(log n)** (order-statistics index by lexicographic order).
- `list()` still performs a full scan in memory backend (sorted output), and uses BoltDB cursor on disk.

When **`trackKeys: false`**:

- `randomKey()` (with or without `prefix`) → **two-pass scan**:
  1. Count matches.
  2. Select the r-th match.
- Minimal memory overhead; fine for small-to-medium sets or when `randomKey()` is rarely used.

**Empty / no-match behavior:**  
`randomKey()` **never throws**; it resolves to the empty string `""` if the store (or the prefix slice) is empty.

## Usage Examples

You can find complete, runnable examples in the [`examples/`](./examples) directory of this repository.

### Producer / Consumer

A common use case for xk6-kv is sharing state between VUs for workflows such as producer-consumer patterns or rendez-vous points.\
The following example demonstrates a producer-consumer workflow where one VU produces tokens and another consumes them, coordinating through the shared key-value store.

```javascript
import { sleep } from "k6";
import { openKv } from "k6/x/kv";

export let options = {
  scenarios: {
    producer: {
      executor: "shared-iterations",
      vus: 1,
      iterations: 10,
      exec: "producer",
    },
    consumer: {
      executor: "shared-iterations",
      vus: 1,
      iterations: 10,
      startTime: "5s",
      exec: "consumer",
    },
    randomConsumer: {
      executor: "shared-iterations",
      vus: 1,
      iterations: 10,
      startTime: "2s",
      exec: "randomConsumer",
    },
  },
};

const kv = openKv({
  backend: "memory",
  trackKeys: true,
});

export async function producer() {
  let latestProducerID = 0;
  if (await kv.exists(`latest-producer-id`)) {
    latestProducerID = await kv.get(`latest-producer-id`);
  }

  console.log(`[producer]-> adding token ${latestProducerID}`);
  await kv.set(`token-${latestProducerID}`, "token-value");
  await kv.set(`latest-producer-id`, latestProducerID + 1);

  // Let's simulate a delay between producing tokens.
  sleep(1);
}

export async function consumer() {
  console.log("[consumer]<- waiting for next token");

  // Let's list the existing tokens, and consume the first we find.
  const entries = await kv.list({ prefix: "token-" });
  if (entries.length > 0) {
    await kv.get(entries[0].key);
    console.log(`[consumer]<- consumed token ${entries[0].key}`);
    await kv.delete(entries[0].key);
  } else {
    console.log("[consumer]<- no tokens available");
  }

  // Let's simulate a delay between consuming tokens.
  sleep(1);
}

export async function randomConsumer() {
  console.log("[randomConsumer]<- attempting random token retrieval");

  // Pick a random key (O(1) when in-memory keys are tracked).
  const key = await kv.randomKey({ prefix: "token-" });
  if (!key) {
    console.log("[randomConsumer]<- no tokens available");
  } else {
    const value = await kv.get(key);
    console.log(`[randomConsumer]<- random key: ${key}, value: ${value}`);
    await kv.delete(key);
  }

  // Let's simulate a delay between consuming tokens.
  sleep(1);
}

// It's good practice to close the store in teardown.
export async function teardown() {
  kv.close();
}
```

### Random Key With Prefix

This pattern is ideal when your keys are namespaced (e.g., `"user:"`, `"email:"`, `"order:"`) and you need to sample uniformly from a specific subset without calling `list()` first.\
Because `randomKey()` never throws on empty results, you can safely compose it in hot paths and just branch if the return is falsy - no try/catch dance needed.

```javascript
import { openKv } from "k6/x/kv";
const kv = openKv({ backend: "disk", trackKeys: true });

export default async function () {
  await kv.set("user:1", { name: "Oleg" });
  await kv.set("user:2", { name: "Alina" });
  await kv.set("user:3", { name: "Anna" });
  await kv.set("order:1", { id: 1 });

  const k = await kv.randomKey({ prefix: "user:" }); // Returns "user:1" or "user:2" or "user:3" or "".
  if (k) {
    const user = await kv.get(k);
    console.log(`Random user ${k}: ${JSON.stringify(user)}`);
  }
}

// It's good practice to close the store in teardown.
export async function teardown() {
  kv.close();
}
```

### Rebuild The Key Index

Use this when you rely on `trackKeys: true` and want to guarantee that the in-memory index (used by `randomKey()`) is consistent with the underlying storage.

```javascript
import { openKv } from "k6/x/kv";

// Rebuild is mostly useful with the disk backend or after crashes.
const kv = openKv({ backend: "disk", trackKeys: true });

export default async function () {
  const ok = await kv.rebuildKeyList(); // True when finished.
  const key = await kv.randomKey();     // "" if storage empty.
  if (key) console.log(`random = ${key}`);
}

// It's good practice to close the store in teardown.
export async function teardown() {
  kv.close();
}
```

## Development

### Using the Taskfile

This repo ships with a [Taskfile](https://taskfile.dev) that automates formatting, linting, building a custom **k6** with this extension, and running end-to-end scenarios against both backends.

The taskfile is **fully cross-platform** and works identically on **Linux, macOS, Windows (native PowerShell), and WSL**.

#### Install Task (once)

**Linux/macOS/WSL**

```bash
# Homebrew (macOS / Linuxbrew)
brew install go-task/tap/go-task

# Or curl (installs to /usr/local/bin by default)
sh -c "$(curl --location https://taskfile.dev/install.sh)" -- -d -b /usr/local/bin
```

**Windows (PowerShell)**

```powershell
# Scoop (recommended)
scoop install task

# Or Chocolatey
choco install go-task

# Or download from https://github.com/go-task/task/releases
```

> **Requirements:**
> - Go 1.25+ in your PATH
> - For `test-race` and `build-k6` on Windows: gcc (MinGW-w64) or use WSL
> - Task automatically handles platform differences (PowerShell vs Bash)

#### Discover available tasks

```bash
task --list-all
```

You should see tasks like `install-tools`, `lint-fix`, `build-k6`, `test`, and `test-e2e-*`.

#### Local tool bootstrap (installs into `./bin/`)

All tools install into the repo-local `./bin/` (ignored by git), so your global environment stays clean.

```bash
# Install everything (gofumpt, golangci-lint, xk6)
task install-tools

# Or install just one:
task install-gofumpt
task install-lint
task install-xk6
```

**Version Management:**
- Uses "latest" by default for easy updates
- Checks installed versions to avoid unnecessary reinstalls
- Works on all platforms (Windows, Linux, macOS)

#### Format and lint

```bash
# Lint and auto-fix issues, then format with gofumpt
task lint-fix

# Lint without fixing
task lint
```

**Note:** The standalone `fmt` task has been removed. Use `lint-fix` for a complete code quality pass.

#### Build k6 with this extension

Builds a **k6** binary into `./bin/k6` using your **local** xk6-kv code:

```bash
# Build with your current local changes
task build-k6

# The extension is built from your working directory
# No need to commit or push first!
```

**Windows Note:** Building k6 requires CGO (gcc). The task will display a helpful message if gcc is missing. Options:
- Install [MinGW-w64](https://www.mingw-w64.org/)
- Use WSL for building
- Download pre-built binaries from [Releases](https://github.com/oshokin/xk6-kv/releases)

### Running Tests

#### Unit tests (fast, local)

```bash
# Run all unit tests
task test

# Run tests with race detector (requires CGO/gcc)
task test-race
```

**Windows Note:** The race detector requires gcc (MinGW-w64). If not installed, the task will display a helpful message. Use WSL or install gcc to enable race detection.

#### End-to-end scenario tests

By default, tasks run the script at `e2e/get-or-set.js`. You can point to a different JS with `E2E_JS`.

**Memory backend**

```bash
# No key tracking (prefix randomKey uses a two-pass scan)
task test-e2e-memory-no-track

# With key tracking (O(1) randomKey for no-prefix, O(log n) for prefix)
task test-e2e-memory-track
```

**Disk backend**

```bash
# No key tracking
task test-e2e-disk-no-track

# With key tracking
task test-e2e-disk-track
```

#### Overriding scenario parameters

All e2e tasks accept the following overrides (defaults in parentheses):

- `E2E_JS` (default: `e2e/get-or-set.js`) - which scenario file to run
- `VUS` (default: 50) - virtual users
- `ITERATIONS` (default: 1000) - total iterations
- `TOTAL_FAKE_ORDERS` (default: 1000) - dataset size used by the scenario
- `RETRY_WAIT_MS` (default: 50) - backoff between retries inside the scenario
- `MAX_RETRY` (default: 5) - max retry attempts

**Examples:**

```bash
# Point to a different scenario file
task test-e2e-memory-track E2E_JS="e2e/some-other-test.js"

# Heavier run
task test-e2e-disk-track VUS=100 ITERATIONS=5000

# Tweak scenario knobs
task test-e2e-memory-no-track TOTAL_FAKE_ORDERS=2000 RETRY_WAIT_MS=100 MAX_RETRY=7
```

> The tasks set `KV_BACKEND` and `KV_TRACK_KEYS` for you. Your e2e script should read them via `__ENV` (as in the example scenario).

#### Cross-Platform Testing

The taskfile works identically across platforms:

```bash
# Windows (PowerShell)
task test

# Linux/macOS/WSL (Bash)
task test

# All tasks use the same commands - platform handling is automatic!
```

### Building Locally

#### Local Development Build

The `build-k6` task builds k6 with your **local** xk6-kv code (no need to push first):

```bash
# Build k6 with your current local changes
task build-k6

# The built binary is in ./bin/k6
./bin/k6 version
# Shows: github.com/oshokin/xk6-kv (devel)
```

This uses `--with github.com/oshokin/xk6-kv=.` to tell xk6 to use your working directory instead of fetching from GitHub.

**Cross-Platform Support:**
- Linux/macOS/WSL: Native build with bash
- Windows: PowerShell-based build with helpful CGO messages

#### Clean local artifacts

Removes `./bin/` (tooling & k6 binary) and the `.k6.kv` scratch file if present.

```bash
task clean
```

Works on all platforms with a single command!

#### Task Summary

| Task | Description | Notes |
|------|-------------|-------|
| `task install-tools` | Install dev tools (gofumpt, golangci-lint, xk6) | Cross-platform, installs to `./bin/` |
| `task lint-fix` | Fix linting issues and format code | Combines golangci-lint --fix + gofumpt |
| `task lint` | Check code without modifications | Read-only linting |
| `task test` | Run unit tests | Fast, no external dependencies |
| `task test-race` | Run tests with race detector | Requires gcc on Windows |
| `task build-k6` | Build k6 with local extension | Requires gcc on Windows |
| `task clean` | Remove ./bin and .k6.kv | Clean slate |
| `task install-githooks` | Enable semantic commit validation | Recommended for contributors |
| `task version-check` | Preview next version | Based on commit messages |

All tasks work identically on Windows (PowerShell), Linux (Bash), macOS (Bash), and WSL!

## Versioning & Releases

This project uses **automated semantic versioning** based on commit messages. When you push to the `main` branch, commits are analyzed to determine version bumps and create releases automatically.

### Semantic Commit Format

All commits must follow this format:

- **`fix: description`** → Patch version bump (1.0.0 → 1.0.1)
- **`feat: description`** → Minor version bump (1.0.0 → 1.1.0)  
- **`major: description`** → Major version bump (1.0.0 → 2.0.0)

**Examples:**

```bash
fix: correct race condition in disk backend
feat: add TTL support for keys
major: remove deprecated swap() method
```

### Setting Up Git Hooks

To enforce commit message format locally:

```bash
# Enable git hooks (commit messages will be validated)
task install-githooks

# Disable git hooks (if needed)
task remove-githooks
```

### Check Next Version

See what version would be released based on your commits:

```bash
task version-check
```

All version management tasks work on Windows, Linux, and macOS!

### Automated Releases

When commits are pushed to `main`:

1. **Commit Policy Workflow** validates commit messages (skipped for fork PRs).
2. **Extension Check Workflow** runs lint, tests, and build verification across platforms.
3. **Release Workflow** (only on `main` after PR merge):
   - Semantic versioning script analyzes commits since the last tag.
   - New Git tag is created automatically (e.g., `v1.4.0`).
   - k6 binaries with xk6-kv are built for:
     - Linux (amd64, arm64)
     - macOS (amd64, arm64/Apple Silicon)
     - Windows (amd64, arm64)
   - GitHub Release is created with:
     - Changelog from commits
     - Pre-built k6 binaries as downloadable archives (`.tar.gz` for Unix, `.zip` for Windows)

### Using Pre-built Binaries

See [Installation - Option 1](#option-1-pre-built-binaries-recommended) above.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.\
For major changes, please open an issue first to discuss what you would like to change.\
If proposing multiple features, split them into separate Pull Requests so each can be discussed/reviewed independently.

### Development Workflow

1. Fork the repository.
2. Create your feature branch (`git checkout -b feature/AmazingFeature`).
3. Set up git hooks: `task install-githooks`.
4. Make your changes and commit using semantic format:
   - `fix: your bug fix description`
   - `feat: your new feature description`
   - `major: your breaking change description`
5. Push to the branch (`git push origin feature/AmazingFeature`).
6. Open a Pull Request.

**Note:**

- All commits must follow the semantic commit format. The CI will validate this automatically for maintainers.
- Pull requests from forks **skip semantic commit validation** and are treated as `fix:` by default for easier external contributions.
- Make sure your code passes linting (`task lint`) and tests (`task test`) before submitting.

### Code Quality Standards

This project uses:

- **gofumpt** for code formatting (stricter than gofmt)
- **golangci-lint** for comprehensive linting with 40+ enabled linters
- **gci** and **goimports** for import organization
- **golines** for line length enforcement (max 120 characters)

Run `task lint-fix` before committing to ensure your code is properly formatted and passes all checks. This single command:
1. Runs `golangci-lint run --fix` to auto-fix linting issues
2. Runs `gofumpt -l -w .` to format all Go code

### Cross-Platform Development

The development environment works seamlessly across:
- **Windows** (native PowerShell)
- **Linux** (native Bash)
- **macOS** (native Bash)
- **WSL** (Windows Subsystem for Linux)

All tasks use platform-appropriate commands automatically. No manual adjustments needed!
