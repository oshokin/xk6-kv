// A focused disk-backend example that writes, reads, deletes, and lists,
// with explicit teardown to close the persistent store.
//
// Methods covered: set, get, exists, delete, list, close.
// Based on and expanding the disk example from the repository.

import { openKv } from "k6/x/kv";
import { expect } from "https://jslib.k6.io/k6-testing/0.5.0/index.js";

// Use the persistent bbolt-backed store (data can persist between runs).
const kv = openKv({ backend: "disk" });

export async function setup() {
  await kv.clear();
}

export default async function () {
  await kv.set("persistence:key", "survives-if-not-cleared");
  expect(await kv.get("persistence:key")).toEqual("survives-if-not-cleared");

  // Delete and verify non-existence.
  await kv.delete("persistence:key");
  expect(await kv.exists("persistence:key")).toEqual(false);

  // Empty list after cleanup.
  const allEntries = await kv.list();
  expect(allEntries).toHaveLength(0);
}

export async function teardown() {
  // Always close disk stores in teardown to release resources.
  kv.close();
}
