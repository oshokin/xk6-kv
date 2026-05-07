// setMany all-or-nothing batch example with per-entry error diagnostics.
//
// Covered methods: clear, setMany, get, exists.

import { openKv } from "k6/x/kv";
import { expect } from "https://jslib.k6.io/k6-testing/0.5.0/index.js";

const kv = openKv({ backend: "memory" });

export async function setup() {
  await kv.clear();
}

export default async function () {
  // Success path: one logical batch write for multiple keys.
  const success = await kv.setMany({
    "profile:1": { id: 1, name: "Alice" },
    "profile:2": { id: 2, name: "Bob" },
    "profile:3": { id: 3, name: "Carol" },
  });

  expect(success.written).toEqual(3);
  expect(await kv.get("profile:1")).toEqual({ id: 1, name: "Alice" });
  expect(await kv.get("profile:2")).toEqual({ id: 2, name: "Bob" });
  expect(await kv.get("profile:3")).toEqual({ id: 3, name: "Carol" });

  // Failure path: serializer error rejects the whole batch.
  try {
    await kv.setMany({
      "batch:ok": { id: "ok" },
      "batch:bad": () => {},
    });
    throw new Error("expected setMany() to reject");
  } catch (err) {
    expect(err?.name).toEqual("InvalidOptionsError");
    expect(Array.isArray(err?.errors)).toEqual(true);
    expect(err.errors.length).toEqual(1);
    expect(err.errors[0].name).toEqual("SerializerError");
    expect(typeof err.errors[0].message).toEqual("string");
  }

  // All-or-nothing guarantee: "batch:ok" is not written when one entry fails.
  expect(await kv.exists("batch:ok")).toEqual(false);
}

export function teardown() {
  kv.close();
}
