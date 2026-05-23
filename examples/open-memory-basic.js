// Minimal memory backend example with default settings.
//
// Covered methods: set, get.
// Handy quick check that the extension is loaded and writable.

import { openKv } from "k6/x/kv";

// Minimal memory backend: all optional settings omitted.
// Defaults applied:
// - serialization: "json"
// - trackKeys: false
// - memory.shardCount: auto (runtime.NumCPU, capped at 65536)
const kv = openKv({ backend: "memory" });

export default async function () {
  await kv.set("hello", "world");
  const value = await kv.get("hello");
  console.log(`memory-basic value: ${value}`);
}

export function teardown() {
  kv.close();
}
