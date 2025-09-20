// Compare-and-swap (CAS) is the classic atomic primitive: replace if-and-only-if current value equals the expected old value.
// This example covers both success and failure paths.
//
// Covered method: compareAndSwap.

import { openKv } from "k6/x/kv";
import { expect } from "https://jslib.k6.io/k6-testing/0.5.0/index.js";

const kv = openKv({ backend: "memory" });

export async function setup() {
  await kv.clear();
  await kv.set("release:version", 1);
}

export default async function () {
  // Wrong expectation fails the swap.
  const casFailure = await kv.compareAndSwap("release:version", 2, 3);
  expect(casFailure).toEqual(false);
  expect(await kv.get("release:version")).toEqual(1);

  // Correct expectation succeeds and installs the new value atomically.
  const casSuccess = await kv.compareAndSwap("release:version", 1, 2);
  expect(casSuccess).toEqual(true);
  expect(await kv.get("release:version")).toEqual(2);
}
