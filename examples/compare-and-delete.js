// Compare-and-delete removes a key only if its current value equals the expected old value.
// This is handy for "delete only if not processed" or "delete only if state is still X".
//
// Covered method: compareAndDelete.

import { openKv } from "k6/x/kv";
import { expect } from "https://jslib.k6.io/k6-testing/0.5.0/index.js";

const kv = openKv({ backend: "memory" });

export async function setup() {
  await kv.clear();
  await kv.set("workflow:job:42:state", "failed");
}

export default async function () {
  // Wrong expected value -> deletion must not happen.
  const firstAttemptDeleted = await kv.compareAndDelete("workflow:job:42:state", "running");
  expect(firstAttemptDeleted).toEqual(false);
  expect(await kv.exists("workflow:job:42:state")).toEqual(true);

  // Correct expected value -> deletion must happen atomically.
  const secondAttemptDeleted = await kv.compareAndDelete("workflow:job:42:state", "failed");
  expect(secondAttemptDeleted).toEqual(true);
  expect(await kv.exists("workflow:job:42:state")).toEqual(false);
}
