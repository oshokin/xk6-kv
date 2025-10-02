// Comprehensive smoke test for core KV methods on the in-memory backend.
//
// Covered methods: set, get, exists, list (with prefix & limit), size, clear, delete.

import { openKv } from "k6/x/kv";
import { expect } from "https://jslib.k6.io/k6-testing/0.5.0/index.js";

// Create an in-memory KV store (fastest; data is ephemeral across runs).
const kv = openKv({ backend: "memory" });

export async function setup() {
  // Ensure we start from a clean state.
  await kv.clear();
}

export default async function () {
  // Put a variety of value types under explicit keys.
  await kv.set("greeting:string", "hello");
  await kv.set("answer:number", 42);
  await kv.set("bag:array", [1, 1, 2, 3, 5, 8]);
  await kv.set("user:1", { id: 1, name: "Oleg" });

  // Validate that we can read them back.
  const retrievedGreeting = await kv.get("greeting:string");
  const retrievedAnswer = await kv.get("answer:number");
  const retrievedArray = await kv.get("bag:array");
  const retrievedUser = await kv.get("user:1");

  expect(retrievedGreeting).toEqual("hello");
  expect(retrievedAnswer).toEqual(42);
  expect(retrievedArray).toEqual([1, 1, 2, 3, 5, 8]);
  expect(retrievedUser).toEqual({ id: 1, name: "Oleg" });

  // Existence checks for both present and absent keys.
  expect(await kv.exists("answer:number")).toEqual(true);
  expect(await kv.exists("nonexistent:key")).toEqual(false);

  // List with a prefix filter; expect lexicographic order by key.
  const userEntries = await kv.list({ prefix: "user:" });
  expect(userEntries).toHaveLength(1);
  expect(userEntries[0].key).toEqual("user:1");

  // List with a hard limit (<= 0 means "no limit"); here we request the first entry.
  const limitedEntries = await kv.list({ limit: 1 });
  expect(limitedEntries).toHaveLength(1);

  // Size reflects number of distinct keys currently stored.
  const currentSize = await kv.size();
  expect(currentSize).toBeGreaterThan(0);

  // Deleting a key is idempotent; deleting a missing key still resolves to true.
  expect(await kv.delete("greeting:string")).toEqual(true);
  expect(await kv.delete("greeting:string")).toEqual(true);

  // Wipe all keys and confirm the store is empty.
  await kv.clear();
  expect(await kv.size()).toEqual(0);
}
