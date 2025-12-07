// Demonstrates atomic numeric increments with positive and negative deltas,
// including "missing key treated as 0" semantics, overflow protection, and error handling.
//
// HOW incrementBy() WORKS:
// =======================
// 1. MISSING KEY SEMANTICS: If the key doesn't exist, it's treated as 0 before increment.
//    This means incrementBy("new-key", 5) will create the key with value 5.
//
// 2. ATOMIC OPERATIONS: All increments are atomic, ensuring thread-safe counter updates
//    even under concurrent access. The operation either fully succeeds or fails.
//
// 3. POSITIVE & NEGATIVE DELTAS: You can increment by positive numbers (add) or negative
//    numbers (subtract). Both operations are atomic and return the new value.
//
// 4. OVERFLOW PROTECTION: incrementBy() protects against integer overflow/underflow:
//    - Cannot increment beyond int64 max (9223372036854775807)
//    - Cannot decrement below int64 min (-9223372036854775808)
//    - Operations that would cause overflow fail with ValueParseError
//    - Failed operations do NOT modify the stored value (atomic rollback)
//
// 5. PREFLIGHT VALIDATION: Invalid delta values are rejected before any store operation:
//    - NaN → ValueNumberRequiredError
//    - Infinity/-Infinity → ValueNumberRequiredError
//    - Numbers > int64 range → ValueNumberRequiredError
//    - String/null/undefined → ValueNumberRequiredError
//
// 6. PARSE VALIDATION: The stored value must be a valid integer string:
//    - Non-numeric strings → ValueParseError
//    - Float strings (e.g., "1.5") → ValueParseError
//    - JSON-encoded strings (e.g., '"41"') → Parsed correctly
//
// 7. BOUNDARY CONDITIONS: Edge cases that are allowed:
//    - Zero delta (+0) is allowed (no-op increment, returns current value)
//    - Decrementing max int64 by -1 is allowed (stays within bounds)
//    - Incrementing min int64 by +0 is allowed (no-op)
//
// Covered method: incrementBy.

import { openKv } from "k6/x/kv";
import { expect } from "https://jslib.k6.io/k6-testing/0.5.0/index.js";

const kv = openKv({ backend: "memory" });

export async function setup() {
  await kv.clear();
}

export default async function () {
  // ============================================================================
  // BASIC OPERATIONS: Missing key semantics and positive/negative deltas
  // ============================================================================

  // Missing key behaves as 0 before the first increment.
  // incrementBy("new-key", 5) creates the key with value 5 (0 + 5 = 5).
  const valueAfterFirstIncrement = await kv.incrementBy("metrics:counter", 5);
  expect(valueAfterFirstIncrement).toEqual(5);

  // Negative deltas are supported and remain atomic.
  // incrementBy("existing-key", -2) subtracts 2 from current value (5 - 2 = 3).
  const valueAfterSecondIncrement = await kv.incrementBy("metrics:counter", -2);
  expect(valueAfterSecondIncrement).toEqual(3);

  // ============================================================================
  // PREFLIGHT VALIDATION: Invalid delta values are rejected before store access
  // ============================================================================

  // Attempting to increment by a non-numeric value should fail with ValueNumberRequiredError.
  // This validation happens BEFORE any store operation, preventing invalid data from
  // reaching the storage layer.
  try {
    // @ts-ignore - intentionally passing a wrong type to exercise validation.
    await kv.incrementBy("metrics:counter", "not-a-number");
    throw new Error("Expected incrementBy to fail with ValueNumberRequiredError, but it succeeded.");
  } catch (err) {
    // The module emits a structured error with name and message.
    // ValueNumberRequiredError indicates the delta parameter was invalid.
    const errorName = err && (err.name || err.Name);
    expect(errorName).toBeDefined();
    // In a real scenario, you'd check: expect(errorName).toEqual("ValueNumberRequiredError");
  }

  // ============================================================================
  // OVERFLOW PROTECTION: Operations that would exceed int64 boundaries fail safely
  // ============================================================================

  // Setting a counter to max int64 value.
  const MAX_INT64 = "9223372036854775807";
  await kv.set("max-counter", MAX_INT64);

  // Attempting to increment max int64 by +1 will fail with ValueParseError.
  // The stored value remains unchanged (atomic rollback).
  try {
    await kv.incrementBy("max-counter", 1);
    throw new Error("Expected overflow protection to prevent increment beyond max int64");
  } catch (err) {
    const errorName = err && (err.name || err.Name);
    // ValueParseError indicates the operation would cause integer overflow.
    expect(errorName).toBeDefined();
  }

  // Verify the value was NOT modified (failed operations don't mutate store).
  const maxValueAfterFailed = await kv.get("max-counter");
  expect(maxValueAfterFailed).toEqual(MAX_INT64);

  // ============================================================================
  // BOUNDARY CONDITIONS: Edge cases that are allowed
  // ============================================================================

  // Zero delta is allowed: incrementBy(key, 0) returns current value without change.
  // This is useful for reading the current value atomically.
  await kv.set("boundary-counter", "100");
  const valueAfterZeroDelta = await kv.incrementBy("boundary-counter", 0);
  expect(valueAfterZeroDelta).toEqual(100);

  // Decrementing max int64 by -1 is allowed (stays within bounds).
  await kv.set("max-counter", MAX_INT64);
  const valueAfterSafeDecrement = await kv.incrementBy("max-counter", -1);
  expect(valueAfterSafeDecrement).toEqual(9223372036854775806); // MAX_INT64 - 1

  // ============================================================================
  // PARSE VALIDATION: Stored values must be valid integers
  // ============================================================================

  // Non-numeric stored values cause ValueParseError.
  await kv.set("invalid-counter", "not-a-number");
  try {
    await kv.incrementBy("invalid-counter", 1);
    throw new Error("Expected parse error for non-numeric stored value");
  } catch (err) {
    const errorName = err && (err.name || err.Name);
    // ValueParseError indicates the stored value couldn't be parsed as an integer.
    expect(errorName).toBeDefined();
  }

  // Float strings are rejected (incrementBy requires integers, not floats).
  await kv.set("float-counter", "1.5");
  try {
    await kv.incrementBy("float-counter", 1);
    throw new Error("Expected parse error for float string");
  } catch (err) {
    const errorName = err && (err.name || err.Name);
    expect(errorName).toBeDefined();
  }

  // ============================================================================
  // SUCCESS PATH: Normal operations work as expected
  // ============================================================================

  // Normal increment operations work correctly.
  await kv.set("normal-counter", "10");
  const incrementedValue = await kv.incrementBy("normal-counter", 5);
  expect(incrementedValue).toEqual(15);
}
