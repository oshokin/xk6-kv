// Random selection of keys with and without prefix filtering.
// When trackKeys is enabled, no-prefix selection is O(1) and prefix selection is O(log n). 
// When disabled, selection is a two-pass scan.
// The function never throws and returns "" when there are no matches.
//
// Covered method: randomKey.

import { openKv } from "k6/x/kv";
import { expect } from "https://jslib.k6.io/k6-testing/0.5.0/index.js";

// Use key tracking to exercise the optimized path.
const kv = openKv({
  backend: "memory",
  trackKeys: true
});

export async function setup() {
  await kv.clear();
  await kv.set("user:1", { id: 1 });
  await kv.set("user:2", { id: 2 });
  await kv.set("order:1", { id: 1 });
}

export default async function () {
  // No prefix: expect any of the three keys.
  const anyRandomKey = await kv.randomKey();
  expect(["user:1", "user:2", "order:1"]).toContain(anyRandomKey);

  // Prefix "user:" -> constrained sample.
  const randomUserKey = await kv.randomKey({ prefix: "user:" });
  expect(["user:1", "user:2"]).toContain(randomUserKey);

  // Empty slice returns empty string.
  const noMatchKey = await kv.randomKey({ prefix: "not:present:" });
  expect(noMatchKey).toEqual("");
}
