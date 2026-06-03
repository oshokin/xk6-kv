import { check } from 'k6';
import { createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: PRE-FLIGHT VALIDATION FOR IMPORT FILES
// =============================================================================
//
// This scenario models CI-style preflight checks that validate seed artifacts
// before any import step mutates the working dataset.
//
// REAL-WORLD PROBLEM SOLVED:
// Validation APIs should provide safe guardrails for automation:
// - validateCSV()/validateJSONL() return deterministic summary shape.
// - malformed content resolves with valid=false and firstError details.
// - invalid options still reject with InvalidOptionsError.
// - validation runs are read-only and do not write to KV.
//
// METHODS TESTED:
// - validateCSV(): valid and malformed-content paths.
// - validateJSONL(): valid and malformed-content paths.
// - option validation guards for both methods.

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'validate-import-files';

// CSV file path for validateCSV validation.
const CSV_PATH = './examples/fixtures/users.csv';

// JSONL file path for validateJSONL validation.
const JSONL_PATH = './examples/fixtures/users.jsonl';

// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME, {
  metrics: {
    operations: true,
  },
});

// options configures a deterministic single-run API contract check.
export const options = {
  vus: 1,
  iterations: 1,
  thresholds: {
    'checks{validateImportFiles:methods-available}': ['rate==1'],
    'checks{validateImportFiles:validateCSV-valid}': ['rate==1'],
    'checks{validateImportFiles:validateJSONL-valid}': ['rate==1'],
    'checks{validateImportFiles:validateCSV-invalid-content}': ['rate==1'],
    'checks{validateImportFiles:validateJSONL-invalid-content}': ['rate==1'],
    'checks{validateImportFiles:invalid-options-rejects}': ['rate==1'],
  },
};

// setup clears previous state before validation checks start.
export const setup = createSetup(kv);

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv);

// validateImportFiles verifies read-only file preflight behavior.
export default async function validateImportFiles() {
  await kv.clear();

  const methodsAvailable =
    typeof kv.validateCSV === 'function' &&
    typeof kv.validateJSONL === 'function';

  check(true, {
    'validateImportFiles:methods-available': () => methodsAvailable,
  });

  if (!methodsAvailable) {
    return;
  }

  const validCSV = await kv.validateCSV({
    fileName: CSV_PATH,
    keyColumn: 'id',
    hasHeader: true,
  });

  const validJSONL = await kv.validateJSONL({
    fileName: JSONL_PATH,
  });

  // Cross-format payloads intentionally trigger malformed-content validation.
  const invalidCSVContent = await kv.validateCSV({
    fileName: JSONL_PATH,
    keyColumn: 'id',
    hasHeader: true,
  });

  const invalidJSONLContent = await kv.validateJSONL({
    fileName: CSV_PATH,
  });

  let invalidCSVOptionsErr = null;
  try {
    await kv.validateCSV(null);
  } catch (err) {
    invalidCSVOptionsErr = err;
  }

  let invalidJSONLOptionsErr = null;
  try {
    await kv.validateJSONL({ fileName: 123 });
  } catch (err) {
    invalidJSONLOptionsErr = err;
  }

  check(validCSV, {
    'validateImportFiles:validateCSV-valid': (result) =>
      result.valid === true &&
      result.rows > 0 &&
      result.bytesRead > 0 &&
      result.checkedAll === true,
  });

  check(validJSONL, {
    'validateImportFiles:validateJSONL-valid': (result) =>
      result.valid === true &&
      result.records > 0 &&
      result.bytesRead > 0 &&
      result.checkedAll === true,
  });

  check(invalidCSVContent, {
    'validateImportFiles:validateCSV-invalid-content': (result) =>
      result.valid === false,
  });

  check(invalidJSONLContent, {
    'validateImportFiles:validateJSONL-invalid-content': (result) =>
      result.valid === false,
  });

  check(true, {
    'validateImportFiles:invalid-options-rejects': () =>
      invalidCSVOptionsErr !== null &&
      invalidCSVOptionsErr.name === 'InvalidOptionsError' &&
      invalidJSONLOptionsErr !== null &&
      invalidJSONLOptionsErr.name === 'InvalidOptionsError',
  });
}
