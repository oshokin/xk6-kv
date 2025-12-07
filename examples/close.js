// Verifies that close() is synchronous and safe to call once in teardown for disk backends.
// This is intentionally minimal beyond the close() call to emphasize teardown hygiene.
//
// Covered method: close (synchronous).

import { openKv } from "k6/x/kv";
import { expect } from "https://jslib.k6.io/k6-testing/0.5.0/index.js";

const kv = openKv({ backend: "disk" });

export async function setup() {
  await kv.clear();
  await kv.set("healthcheck", "ok");
}

export default async function () {
  expect(await kv.get("healthcheck")).toEqual("ok");
}

export async function teardown() {
  // Explicitly close to release bbolt file handles and any in-memory indexes.
  kv.close();

  // Optional: a second close() should be safe/no-op in a ref-counted scenario.
  // kv.close();
}
