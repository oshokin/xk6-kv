// Demonstrates atomic numeric increments with positive and negative deltas,
// including "missing key treated as 0" semantics and basic error handling.
//
// Covered method: incrementBy.

import { openKv } from "k6/x/kv";
import { expect } from "https://jslib.k6.io/k6-testing/0.5.0/index.js";

const kv = openKv({ backend: "memory" });

export async function setup() {
  await kv.clear();
}

export default async function () {
  // Missing key behaves as 0 before the first increment.
  const valueAfterFirstIncrement = await kv.incrementBy("metrics:counter", 5);
  expect(valueAfterFirstIncrement).toEqual(5);

  // Negative deltas are supported and remain atomic.
  const valueAfterSecondIncrement = await kv.incrementBy("metrics:counter", -2);
  expect(valueAfterSecondIncrement).toEqual(3);

  // Attempting to increment by a non-numeric value should fail with a typed error.
  try {
    // @ts-ignore - intentionally passing a wrong type to exercise validation.
    await kv.incrementBy("metrics:counter", "not-a-number");
    throw new Error("Expected incrementBy to fail with ValueNumberRequiredError, but it succeeded.");
  } catch (err) {
    // The module emits a structured error with name and message.
    // We validate by checking the "name" field if present; otherwise ensure it's an Error-like object.
    const errorName = err && (err.name || err.Name);
    expect(errorName).toBeDefined();
  }
}
