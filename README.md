# xk6-kv

[![Go Report Card](https://goreportcard.com/badge/github.com/oshokin/xk6-kv)](https://goreportcard.com/report/github.com/oshokin/xk6-kv)
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)

A k6 extension providing a persistent key-value store for sharing state across Virtual Users (VUs) during load testing.
It supports an in-memory backend and a persistent BoltDB backend, optional key tracking for O(1) random sampling, and prefix-aware randomKey().

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
- [Contributing](#contributing)

## Features

- 🔒 **Thread-Safe**: Secure state sharing across Virtual Users
- 🔌 **Easy Integration**: Simple API that works seamlessly with k6
- 🔄 **Flexible Storage**: Choose between in-memory or disk-based persistence
- 🪶 **Lightweight**: No external dependencies required

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

// Or specify a backend explicitly
// const kv = openKv({ backend: "disk" });   // Disk-based persistent backend (default)
// const kv = openKv({ backend: "memory" }); // In-memory backend

export async function setup() {
    // Start with a clean state
    await kv.clear();
}

export default async function () {
    // Set a bunch of keys
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

### KV methods

All methods are **Promise-based**.

- `set(key: string, value: any): Promise<any>`
  - Sets a key-value pair. 
  - Accepts any JSON-serializable value if serialization="json".

- `get(key: string): Promise<any>`
  - Retrieves a value by key. Throws if key doesn't exist.

- `randomKey(options?: RandomKeyOptions): Promise<string>`
  - Returns a random key, optionally filtered by prefix. 
  - Resolves to "" (empty string) when no key matches (including empty storage).

- `delete(key: string): Promise<boolean>`
  - Removes a key-value pair.
  - Resolves to true on success.

- `exists(key: string): Promise<boolean>`
  - Checks if a given key exists.

- `list(options?: ListOptions): Promise<Array<{ key: string; value: any }>>`
  - Returns key–value entries sorted lexicographically by key. 
  - If limit <= 0 or omitted, there is no limit.

- `clear(): Promise<void>`
  - Removes all entries.
  - Resolves to true when done.

- `size(): Promise<number>`
  - Returns current store size.

- `rebuildKeyList(): Promise<boolean>`
  - Rebuilds in-memory key indexes from the underlying store if supported. 
  - Resolves to true when finished (no-op + true when trackKeys is disabled).
  - The operation is **O(n)** over keys, so prefer running it in `setup()` or infrequently between stages - not every iteration.
  - On the **memory** backend it’s typically unnecessary unless you manually corrupted/replaced the in-memory structures.
  - On the **disk** backend with `trackKeys: true`, the index is **automatically rebuilt at startup**. Call `rebuildKeyList()` manually only when the DB is mutated **out-of-band during the run** or if you need to **recover without reopening the store**.

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

await kv.delete("a");
await kv.clear();  // wipe all
```

## Usage Examples

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

  // Let's simulate a delay between producing tokens
  sleep(1);
}

export async function consumer() {
  console.log("[consumer]<- waiting for next token");

  // Let's list the existing tokens, and consume the first we find
  const entries = await kv.list({ prefix: "token-" });
  if (entries.length > 0) {
    await kv.get(entries[0].key);
    console.log(`[consumer]<- consumed token ${entries[0].key}`);
    await kv.delete(entries[0].key);
  } else {
    console.log("[consumer]<- no tokens available");
  }

  // Let's simulate a delay between consuming tokens
  sleep(1);
}

export async function randomConsumer() {
  console.log("[randomConsumer]<- attempting random token retrieval");

  // Pick a random key (O(1) when in-memory keys are tracked)
  const key = await kv.randomKey({ prefix: "token-" });
  if (!key) {
    console.log("[randomConsumer]<- no tokens available");
  } else {
    const value = await kv.get(key);
    console.log(`[randomConsumer]<- random key: ${key}, value: ${value}`);
    await kv.delete(key);
  }

  // Let's simulate a delay between consuming tokens
  sleep(1);
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