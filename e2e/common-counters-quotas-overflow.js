import { check } from 'k6';
import { createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: COUNTER OVERFLOW PROTECTION & QUOTA VALIDATION
// =============================================================================
//
// This test validates that incrementBy() operations are protected against
// integer overflow, invalid inputs, and edge cases that could corrupt counter
// values or cause undefined behavior. This is critical for:
//
// - Rate limiting systems (prevent counter wraparound causing DoS bypass).
// - Quota management (prevent negative quotas from overflow).
// - Billing systems (prevent account balance corruption).
// - Analytics platforms (prevent metric corruption from invalid deltas).
// - Inventory systems (prevent stock count corruption).
// - Session management (prevent session counter overflow).
//
// REAL-WORLD PROBLEM SOLVED:
// Invalid or out-of-range values passed to incrementBy() operations.
// Without proper validation, you get:
// - Integer overflow causing counters to wrap to negative values.
// - NaN propagation corrupting all subsequent operations.
// - Type confusion allowing string/null deltas to bypass validation.
// - Precision loss from JavaScript numbers exceeding int64 range.
// - Silent failures where operations appear to succeed but mutate nothing.
//
// ATOMIC OPERATIONS TESTED:
// - incrementBy(): Counter increment with overflow protection.
// - set(): Initialize counter values at boundaries.
// - get(): Verify values remain unchanged after failed operations.
//
// VALIDATION CATEGORIES:
// - Preflight validation: Reject invalid deltas before any store operation.
// - Overflow protection: Prevent int64 overflow/underflow.
// - Parse validation: Reject non-numeric stored values.
// - Boundary testing: Verify edge cases (max/min int64, zero deltas).
// - Mutation prevention: Ensure failed operations don't modify store.
//
// PERFORMANCE CHARACTERISTICS:
// - Low-frequency but critical validation (every increment must be checked).
// - Essential for data integrity and security.
// - Must handle edge cases without performance degradation.

// Test name prefix for generating test-specific database paths.
const OVERFLOW_TEST_NAME = 'counters-quotas-overflow';

// Maximum int64 value as string (used to test overflow protection).
const MAX_INT64_STR = '9223372036854775807';

// Minimum int64 value as string (used to test underflow protection).
const MIN_INT64_STR = '-9223372036854775808';

// Non-numeric string value (used to test parse error handling).
const NON_NUMERIC = 'not-a-number';

// Maximum safe JavaScript integer (2^53 - 1) that can be represented exactly.
const MAX_SAFE_INT = 9007199254740991;

// options configures the load profile and pass/fail thresholds.
// Single VU, single iteration since this is a validation test, not a concurrency test.
export const overflowOptions = {
  vus: 1,
  iterations: 1,
  thresholds: {
    'checks{overflow:preflight}': ['rate>0.99'],
    'checks{overflow:store}': ['rate>0.99']
  }
};

// createOverflowScenario returns a test function that validates overflow protection
// for a specific serialization format (json or string). This allows testing both
// serialization backends to ensure overflow guards work consistently.
export function createOverflowScenario(serialization = 'json') {
  const scenarioName = `${OVERFLOW_TEST_NAME}-${serialization}`;

  return async function runOverflowScenario() {
    const kv = createKv(scenarioName, { serialization });
    const setup = createSetup(kv);
    const teardown = createTeardown(kv, scenarioName);

    await setup();
    try {
      await overflowGuardTest(kv, serialization);
    } finally {
      await teardown();
    }
  };
}

// expectError verifies that a function throws an error with the expected error name.
// Returns true if the error matches, false if no error or wrong error type.
async function expectError(fn, expectedName) {
  try {
    await fn();
    return false;
  } catch (err) {
    return err?.name === expectedName;
  }
}

// captureError safely executes a function and returns the error object if one occurs,
// or null if the function succeeds. Used to inspect error properties.
async function captureError(fn) {
  try {
    await fn();
    return null;
  } catch (err) {
    return err;
  }
}

// overflowGuardTest validates all overflow protection and input validation mechanisms.
// Tests are organized into categories: preflight validation, overflow protection,
// parse validation, boundary conditions, and mutation prevention.
async function overflowGuardTest(kv, serialization) {
  // PREFLIGHT VALIDATION: Reject invalid deltas before any store operation.
  // These checks prevent invalid data from reaching the store layer.

  // Test NaN rejection (capture error to inspect properties).
  const inspectErr = await captureError(() => kv.incrementBy('inspect', NaN));
  const preflightNaN = inspectErr?.name === 'ValueNumberRequiredError';

  // Test huge number rejection (exceeds int64 range, should be rejected preflight).
  const preflightBig = await expectError(() => kv.incrementBy('huge', 1e20), 'ValueNumberRequiredError');

  // Test string delta rejection (type validation).
  const preflightString = await expectError(() => kv.incrementBy('string-delta', '123'), 'ValueNumberRequiredError');

  // Test null delta rejection (type validation).
  const preflightNull = await expectError(() => kv.incrementBy('null-delta', null), 'ValueNumberRequiredError');

  // Test Infinity rejection (invalid numeric value).
  const preflightInf = await expectError(() => kv.incrementBy('inf-delta', Infinity), 'ValueNumberRequiredError');

  // Test -Infinity rejection (invalid numeric value).
  const preflightNegInf = await expectError(() => kv.incrementBy('ninf-delta', -Infinity), 'ValueNumberRequiredError');

  // OVERFLOW PROTECTION: Prevent int64 overflow/underflow.
  // These tests ensure that operations that would exceed int64 boundaries are rejected.

  // Test positive overflow protection: incrementing max int64 by 1 should fail.
  await kv.set('max', MAX_INT64_STR);
  const overflowPositive = await expectError(() => kv.incrementBy('max', 1), 'ValueParseError');
  const maxValueAfterFailed = await kv.get('max');

  // Test boundary conditions: operations that don't cause overflow should succeed.
  // Zero delta should be allowed (no-op increment).
  const boundaryMaxNoop = await expectError(() => kv.incrementBy('max', 0), 'ValueParseError') === false;

  // Decrementing max int64 by 1 should be allowed (stays within bounds).
  const boundaryMaxMinusOne = await expectError(() => kv.incrementBy('max', -1), 'ValueParseError') === false;
  await kv.set('max', MAX_INT64_STR);

  // Test negative overflow protection: decrementing min int64 by 1 should fail.
  await kv.set('min', MIN_INT64_STR);
  const overflowNegative = await expectError(() => kv.incrementBy('min', -1), 'ValueParseError');
  const minValueAfterFailed = await kv.get('min');

  // Zero delta should be allowed on min int64 (no-op increment).
  const boundaryMinNoop = await expectError(() => kv.incrementBy('min', 0), 'ValueParseError') === false;

  // PARSE VALIDATION: Reject non-numeric stored values.
  // These tests ensure that incrementBy() only works on numeric values.

  // Test non-numeric string rejection (cannot parse stored value as integer).
  await kv.set('nonint', NON_NUMERIC);
  const parseError = await expectError(() => kv.incrementBy('nonint', 1), 'ValueParseError');
  const nonIntValueAfterFailed = await kv.get('nonint');

  // Test float string rejection (incrementBy requires integers, not floats).
  await kv.set('float-string', '1.5');
  const parseErrorFloatString = await expectError(() => kv.incrementBy('float-string', 1), 'ValueParseError');

  // SUCCESS PATH: Verify that valid operations work correctly.
  // Test normal increment operation (should succeed).
  await kv.set('ok', '0');
  const okValue = await kv.incrementBy('ok', 1);

  // Test increment with MAX_SAFE_INT (largest JavaScript integer that can be represented exactly).
  // This validates that large but valid integers are handled correctly.
  const safeInt = await kv.incrementBy('safe', MAX_SAFE_INT);

  // Validate all test results using k6 check() assertions.
  // Checks are organized by category: preflight, overflow, boundaries, parsing, success, mutation prevention.
  check({ serialization }, {
    // Preflight validation checks.
    'increment preflight rejects NaN': () => preflightNaN,
    'increment preflight rejects >int64': () => preflightBig,
    'increment preflight rejects string delta': () => preflightString,
    'increment preflight rejects null delta': () => preflightNull,
    'increment preflight rejects Infinity': () => preflightInf,
    'increment preflight rejects -Infinity': () => preflightNegInf,
    'error object exposes name': () => inspectErr && inspectErr.name === 'ValueNumberRequiredError',

    // Overflow protection checks.
    'increment overflow guarded (+)': () => overflowPositive,
    'increment overflow guarded (-)': () => overflowNegative,

    // Boundary condition checks (operations that should succeed).
    'boundary max +0 allowed': () => boundaryMaxNoop,
    'boundary max -1 allowed': () => boundaryMaxMinusOne,
    'boundary min +0 allowed': () => boundaryMinNoop,

    // Parse validation checks.
    'increment rejects non-numeric stored value': () => parseError,
    'increment rejects float string stored value': () => parseErrorFloatString,

    // Success path checks (verify normal operations work).
    'success path works': () => okValue === 1,
    'accepts MAX_SAFE_INT': () => safeInt === MAX_SAFE_INT,

    // Mutation prevention checks (failed operations must not modify store).
    'failed ops did not mutate max': () => maxValueAfterFailed === MAX_INT64_STR,
    'failed ops did not mutate min': () => minValueAfterFailed === MIN_INT64_STR,
    'failed ops did not mutate nonint': () => nonIntValueAfterFailed === NON_NUMERIC
  });
}

