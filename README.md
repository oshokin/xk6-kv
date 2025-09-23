# xk6-kv

[![Go Report Card](https://goreportcard.com/badge/github.com/oshokin/xk6-kv)](https://goreportcard.com/report/github.com/oshokin/xk6-kv)
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)

> **Fork Notice**  
> This project is a fork of [oleiade/xk6-kv](https://github.com/oleiade/xk6-kv), extended with **additional atomic primitives** and **random-key utilities**:
>
> - Atomic ops: `incrementBy`, `getOrSet`, `swap`, `compareAndSwap`, `deleteIfExists`, `compareAndDelete`.
> - Random keys: `randomKey()` with **prefix-aware selection**.
> - Optional key tracking for **O(1) random sampling** (disk & memory backends).
>
> All code is licensed under **GNU AGPL v3.0**.

A k6 extension providing a persistent key-value store to share state across Virtual Users (VUs).\
It supports both an **in-memory** backend and a **persistent BoltDB** backend, deterministic `list()` ordering, and high-level atomic helpers designed for safe concurrent access in load tests.

## Table of Contents
- [Features](#features)
- [Why Use xk6-kv](#why-use-xk6-kv)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [API Reference](#api-reference)
  - [openKv](#openkv)
  - [KV Methods](#kv-methods)
  - [Types](#types)
- [Usage Examples](#usage-examples)
  - [Producer / Consumer](#producer--consumer)
  - [Random Key With Prefix](#random-key-with-prefix)
  - [Rebuild The Key Index](#rebuild-the-key-index)
- [Using the Taskfile](#using-the-taskfile)
- [Contributing](#contributing)

## Features

- 🔒 **Thread-Safe**: Secure state sharing across Virtual Users
- 🔌 **Easy Integration**: Simple API that works seamlessly with k6
- 🔄 **Flexible Storage**: Choose between in-memory or disk-based persistence
- 🪶 **Lightweight**: No external dependencies required
- 📊 **Atomic Operations**: Support for atomic increment, compare-and-swap, and more
- 🔍 **Key Tracking**: Optional in-memory indexing for fast random key access
- 🏷️ **Prefix Support**: Filter operations by key prefixes
- 📝 **Serialization Options**: JSON or string serialization

## Why Use xk6-kv?

- **State Sharing Made Simple**: Managing state across multiple VUs in k6 can be challenging. **xk6-kv** provides a straightforward solution for sharing state, making your load testing scripts cleaner and more maintainable.
- **Built for Safety**: Thread safety is crucial in load testing. **xk6-kv** is designed specifically for k6's parallel VU execution model, ensuring your shared state operations remain safe and reliable.
- **Storage Options**: Choose the backend that fits your needs:
  - **Memory**: Fast, ephemeral storage that's shared across VUs
  - **Disk**: Persistent storage using BoltDB for data that needs to survive between test runs
- **Lightweight Alternative**: While other solutions like Redis exist and are compatible with k6 for state sharing, **xk6-kv** offers a more lightweight, integrated approach:
    - No external services required
    - Simple setup and configuration

> **Note**: For extremely high-performance requirements, consider using the k6 Redis module instead.

## Installation

1. First, ensure you have [xk6](https://github.com/grafana/xk6) installed:
```bash
go install go.k6.io/xk6/cmd/xk6@latest
```

2. Build a k6 binary with the xk6-kv extension:
```bash
xk6 build --with github.com/oshokin/xk6-kv
```

3. Import the kv module in your script, at the top of your test script:
```javascript
import { openKv } from "k6/x/kv";
```

4. The built binary will be in your current directory. You can move it to your PATH or use it directly:
```bash
./k6 run script.js
```

## Quick Start

```javascript
import { openKv } from "k6/x/kv";

// Open a key-value store with the default backend (disk)
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

    // Grab any random key ("" if none):
    const any = await kv.randomKey();
    if (any) {
      console.log(`random key = ${any}, value = ${JSON.stringify(await kv.get(any))}`);
    }

    // Prefix-aware random:
    const u = await kv.randomKey({ prefix: "user:" }); // "" if no match
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
```

## API Reference

### `openKv`
Opens a key-value store with the specified backend. **Must be called in the init context**.

```typescript
function openKv(options?: OpenKvOptions): KV

type Backend = "memory" | "disk"

interface OpenKvOptions {
  backend?: Backend               // default: "disk"
  serialization?: "json"|"string" // default: "json"
  trackKeys?: boolean             // default: false
}
```

* `memory`: shared in-process KV for all VUs in a test run (fastest).
* `disk`: persistent BoltDB file (survives between runs).
* `serialization`:
  * `"json"`: values are JSON-encoded/decoded automatically.
  * `"string"`: values are stored as strings/bytes.
* `trackKeys`:
  * `true`: maintain an in-memory index of keys; enables **O(1)** randomKey (no prefix) and **O(log n)** prefix randomKey.
  * `false`: no index; randomKey uses a simple two-pass scan.

#### Performance Notes

While both backends are optimized for performance and suitable for most load testing scenarios, be aware that:
- There is some overhead due to synchronization between VUs
- Consider this overhead when analyzing your test results
- For extremely high throughput requirements, you might need alternative solutions

### KV Methods

All methods are **Promise-based**, except for `close` which is synchronous.

- `get(key: string): Promise<any>`
  - Retrieves a value by key. Throws if key doesn't exist.

- `set(key: string, value: any): Promise<any>`
  - Sets a key-value pair. 
  - Accepts any JSON-serializable value if serialization="json".

- `incrementBy(key: string, delta: number): Promise<number>`
  - Atomically increments the numeric value at `key` by `delta`.
  - If the key does not exist, it is treated as `0`.
  - Returns the new value.

- `getOrSet(key: string, value: any): Promise<{ value: any, loaded: boolean }>`
  - Atomically gets the existing value if the key exists, or sets it to `value` if it doesn't.
  - Returns an object: `{ value: any, loaded: boolean }`.
    - `loaded: true` means the value was pre-existing.
    - `loaded: false` means the value was just set.

- `swap(key: string, value: any): Promise<{ previous: any|null, loaded: boolean }>`
  - Atomically replaces the value at `key` with `value`.
  - Returns an object: `{ previous: any|null, loaded: boolean }`.
    - `loaded: true` means the key existed and `previous` contains its old value.
    - `loaded: false` means the key was created and `previous` is `null`.

- `compareAndSwap(key: string, oldValue: any, newValue: any): Promise<boolean>`
  - Atomically sets the value at `key` to `newValue` only if its current value equals `oldValue`.
  - Returns `true` if the swap was successful, `false` otherwise.
  - This is the fundamental building block for implementing locks and other synchronization primitives.

- `delete(key: string): Promise<boolean>`
  - Removes a key-value pair.
  - Resolves to true on success.

- `exists(key: string): Promise<boolean>`
  - Checks if a given key exists.

- `deleteIfExists(key: string): Promise<boolean>`
  - Deletes the key if it exists.
  - Returns `true` if the key was deleted, `false` if it did not exist.
  - More informative than `delete`, which always returns `true`.

- `compareAndDelete(key: string, oldValue: any): Promise<boolean>`
  - Atomically deletes the key only if its current value equals `oldValue`.
  - Returns `true` if the key was deleted, `false` otherwise.
  - Useful for "delete only if not processed" scenarios.

- `clear(): Promise<void>`
  - Removes all entries.
  - Resolves to true when done.

- `size(): Promise<number>`
  - Returns current store size.

- `list(options?: ListOptions): Promise<Array<{ key: string; value: any }>>`
  - Returns key-value entries sorted lexicographically by key. 
  - If limit <= 0 or omitted, there is no limit.

- `randomKey(options?: RandomKeyOptions): Promise<string>`
  - Returns a random key, optionally filtered by prefix. 
  - Resolves to "" (empty string) when no key matches (including empty storage).

- `rebuildKeyList(): Promise<boolean>`
  - Rebuilds in-memory key indexes from the underlying store if supported.
  - Resolves to true when finished (no-op + true when trackKeys is disabled).
  - The operation is **O(n)** over keys, so prefer running it in `setup()` or infrequently between stages - not every iteration.
  - On the **memory** backend it's typically unnecessary unless you manually corrupted/replaced the in-memory structures.
  - On the **disk** backend with `trackKeys: true`, the index is **automatically rebuilt at startup**. Call `rebuildKeyList()` manually only when the DB is mutated **out-of-band during the run** or if you need to **recover without reopening the store**.

- `close(): void`
  - **Synchronously** closes the underlying store and releases any associated resources (e.g., file handles for the disk backend).
  - This method should be called in the `teardown()` function or when you are certain no further operations will be performed on the KV instance.
  - It does not return a Promise because the caller usually needs to know the outcome immediately.
  - On the **disk** backend it releases the BoltDB file handle and in-memory indexes. Safe to call multiple times. Recommended to call once in `teardown()`.
  - On the **memory** backend it's a no-op (safe to call, does nothing).
  - If multiple `KV` instances share the same store, the underlying store closes only when the last `close()` is called (reference-counted).

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

When **trackKeys: true**:

* `randomKey()` with **no prefix** -> **O(1)** (uniform index sampling).
* `randomKey({prefix})` -> **O(log n)** (order-statistics index by lexicographic order).
* `list()` still performs a full scan in memory backend (sorted output), and uses BoltDB cursor on disk.

When **trackKeys: false**:

* `randomKey()` (with or without `prefix`) -> **two-pass scan**:
  1. Count matches;
  2. Select the r-th match.
* Minimal memory overhead; fine for small-to-medium sets or when randomKey is rarely used.

**Empty / no-match behavior:**
`randomKey()` **never throws**; it resolves to the empty string `""` if the store (or the prefix slice) is empty.

#### Examples

```javascript
await kv.set("a", 123);
console.log(await kv.get("a"));     // 123
console.log(await kv.exists("a"));  // true
console.log(await kv.size());       // 1

await kv.set("prefix:1", "X");
await kv.set("prefix:2", "Y");

console.log(await kv.list());                       // all entries (sorted)
console.log(await kv.list({ prefix: "prefix:" }));  // only "prefix:*"
console.log(await kv.list({ limit: 1 }));           // first entry

console.log(await kv.randomKey());                       // random key or ""
console.log(await kv.randomKey({ prefix: "prefix:" }));  // random "prefix:*" or ""

console.log(await kv.incrementBy("counter", 5));  // 5
console.log(await kv.incrementBy("counter", -2)); // 3

const { value, loaded } = await kv.getOrSet("app_config", { theme: "dark" });
console.log(`config: ${JSON.stringify(value)}, loaded: ${loaded}`);

const { previous, loaded: swapped } = await kv.swap("status", "running");
console.log(`previous status: ${previous}, swapped: ${swapped}`);

const casSuccess = await kv.compareAndSwap("version", 1, 2);
console.log(`CAS success: ${casSuccess}`);

const deleted = await kv.deleteIfExists("temp_file");
console.log(`temporary file deleted: ${deleted}`);

const cndSuccess = await kv.compareAndDelete("job", "failed");
console.log(`conditional delete success: ${cndSuccess}`);

await kv.delete("a");

// Wipe all.
await kv.clear();

// Remember to close the store when done, typically in teardown().
kv.close();
```

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

This pattern is ideal when your keys are namespaced (e.g., `"user:"`, `"email:"`, `"order:"`) and you need to sample uniformly from a specific subset without calling list() first.\
Because `randomKey()` never throws on empty results, you can safely compose it in hot paths and just branch if the return is falsy - no try/catch dance needed.

```javascript
import { openKv } from "k6/x/kv";
const kv = openKv({ backend: "disk", trackKeys: true });

export default async function () {
  await kv.set("user:1", { name: "Oleg" });
  await kv.set("user:2", { name: "Alina" });
  await kv.set("user:3", { name: "Anna" });
  await kv.set("order:1", { id: 1 });

  const k = await kv.randomKey({ prefix: "user:" }); // returns "user:1" or "user:2" or "user:3" or ""
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

Use this when you rely on `trackKeys: true` and want to guarantee that the in-memory index (used by `randomKey`) is consistent with the underlying storage.

```javascript
import { openKv } from "k6/x/kv";

// Rebuild is mostly useful with the disk backend or after crashes.
const kv = openKv({ backend: "disk", trackKeys: true });

export default async function () {
  const ok = await kv.rebuildKeyList(); // true when finished
  const key = await kv.randomKey();     // "" if storage empty
  if (key) console.log(`random = ${key}`);
}

// It's good practice to close the store in teardown.
export async function teardown() {
  kv.close();
}
```

## Using the Taskfile

This repo ships with a [Taskfile](https://taskfile.dev) that automates formatting, linting, building a custom **k6** with this extension, and running end-to-end scenarios against both backends.

### Install Task (once)

**Linux/macOS**

```bash
# Homebrew (macOS / Linuxbrew)
brew install go-task/tap/go-task

# Or curl (installs to /usr/local/bin by default)
sh -c "$(curl --location https://taskfile.dev/install.sh)" -- -d -b /usr/local/bin
```

**Windows (PowerShell)**

```powershell
# Scoop
scoop install task

# Or download releases from https://github.com/go-task/task/releases
```

> You also need a working Go toolchain (1.23+ recommended) in your PATH.

### Discover available tasks

```bash
task
```

You should see targets like `install:*`, `fmt`, `lint`, `build:k6`, and `test:e2e:*`.

### Local tool bootstrap (installs into `./bin/`)

All tools install into the repo-local `./bin/` (ignored by git), so your global environment stays clean.

```bash
# Install everything (gofumpt, golangci-lint, xk6)
task install:tools

# Or install just one:
task install:gofumpt
task install:golangci
task install:xk6
```

### Format and lint

```bash
# Format the codebase with gofumpt
task fmt

# Lint and auto-fix what can be auto-fixed
task lint:fix

# Lint without fixing
task lint
```

### Build k6 with this extension

Builds a **k6** binary into `./bin/k6` using your fork of **xk6-kv**.

```bash
# Build with the default ref (KV_FORK_REF=latest)
task build:k6

# Build with a specific tag/branch/commit of your fork
task build:k6 KV_FORK_REF=v1.3.3
task build:k6 KV_FORK_REF=main
task build:k6 KV_FORK_REF=commit-sha
```

### Run the end-to-end scenario

By default, tasks run the script at `e2e/get-or-set.js`. You can point to a different JS with `E2E_JS`.

**Memory backend**

```bash
# No key tracking (prefix randomKey uses a two-pass scan)
task test:e2e:memory:no-track

# With key tracking (O(1) randomKey for no-prefix, O(log n) for prefix)
task test:e2e:memory:track
```

**Disk backend**

```bash
# No key tracking
task test:e2e:disk:no-track

# With key tracking
task test:e2e:disk:track
```

#### Overriding scenario parameters

All e2e tasks accept the following overrides (defaults in parentheses):

* `E2E_JS` (default: `e2e/get-or-set.js`) - which scenario file to run
* `VUS` (50) - virtual users
* `ITERATIONS` (1000) - total iterations
* `TOTAL_FAKE_ORDERS` (1000) - dataset size used by the scenario
* `RETRY_WAIT_MS` (50) - backoff between retries inside the scenario
* `MAX_RETRY` (5) - max retry attempts

Examples:

```bash
# Point to a different scenario file
task test:e2e:memory:track E2E_JS="e2e/some-other-test.js"

# Heavier run
task test:e2e:disk:track VUS=100 ITERATIONS=5000

# Tweak scenario knobs
task test:e2e:memory:no-track TOTAL_FAKE_ORDERS=2000 RETRY_WAIT_MS=100 MAX_RETRY=7
```

> The tasks set `KV_BACKEND` and `KV_TRACK_KEYS` for you. Your e2e script should read them via `__ENV` (as in the example scenario).

### Unit tests (fast, local)

```bash
task test
task test:race
```

### Clean local artifacts

Removes `./bin/` (tooling & k6 binary) and the `.k6.kv` scratch file if present.

```bash
task clean
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.\
For major changes, please open an issue first to discuss what you would like to change.\
If proposing multiple features, split them into separate Pull Requests so each can be discussed/reviewed independently.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request