// Demonstrates rebuilding the in-memory key index (useful with disk backend when trackKeys is enabled
// and you've had out-of-band mutations or crash recovery needs).
//
// Covered method: rebuildKeyList; plus we close the disk store during teardown via close().

import { openKv } from "k6/x/kv";
import { expect } from "https://jslib.k6.io/k6-testing/0.5.0/index.js";

// Disk backend + in-memory index of keys.
const kv = openKv({
  backend: "disk",
  trackKeys: true
});

export async function setup() {
  // Start fresh.
  await kv.clear();

  // Seed a couple of entries; index will be rebuilt explicitly during the test.
  await kv.set("alpha", 1);
  await kv.set("beta", 2);
  await kv.set("bravo", 3);
}

export default async function () {
  // Force index rebuild; this is O(n) across existing keys and resolves to true.
  const wasRebuilt = await kv.rebuildKeyList();
  expect(wasRebuilt).toEqual(true);

  // Sanity: listing should reflect the seeded keys.
  const entries = await kv.list();
  expect(entries).toHaveLength(3);
}

export async function teardown() {
  // Close disk-backed store to release file handles and memory.
  kv.close();
}
