// Demonstrates the difference between "always true" delete() and "conditional" deleteIfExists().
//
// Covered methods: delete, deleteIfExists.

import { openKv } from "k6/x/kv";
import { expect } from "https://jslib.k6.io/k6-testing/0.5.0/index.js";

const kv = openKv({ backend: "memory" });

export async function setup() {
  await kv.clear();
  await kv.set("temp:maybe", "value");
}

export default async function () {
  // delete() resolves to true even for missing keys; useful for idempotent cleanup.
  expect(await kv.delete("temp:maybe")).toEqual(true);
  expect(await kv.delete("temp:maybe")).toEqual(true);

  // deleteIfExists() returns a boolean indicating if a deletion actually happened.
  const wasDeletedFirst = await kv.deleteIfExists("temp:maybe");
  expect(wasDeletedFirst).toEqual(false);

  await kv.set("temp:maybe", 123);
  const wasDeletedSecond = await kv.deleteIfExists("temp:maybe");
  expect(wasDeletedSecond).toEqual(true);
}
