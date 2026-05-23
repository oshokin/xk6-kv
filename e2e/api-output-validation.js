import { check, sleep } from 'k6';
import file from 'k6/x/file';
import { getSnapshotPath, createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: API OUTPUT VALIDATION & CONTRACT TESTING
// =============================================================================
//
// This test validates that all xk6-kv API methods return correctly structured
// JavaScript objects with proper field names in camelCase. 
// This is critical for:
//
// - API contract validation (ensuring JS consumers get expected shapes).
// - Type safety in TypeScript definitions.
// - Regression prevention (catching breaking changes in output format).
//
// REAL-WORLD PROBLEM SOLVED:
// JavaScript/TypeScript consumers rely on consistent API output structure.
// Without proper validation, you get:
// - Runtime errors from missing/incorrectly named fields.
// - Broken integrations when field names change.
//
// METHODS TESTED:
// - Basic operations (get, getMany, set, setMany, delete, exists, clear, size).
// - Atomic operations (incrementBy, getOrSet, swap, compareAndSwap variants,
//   setIfAbsent, deleteIfExists, compareAndDelete variants).
// - Query/coordination operations (list, scan, count, randomKey, randomKeys, popRandom,
//   claimRandom, releaseClaim, completeClaim).
// - Observability/lifecycle (rebuildKeyList, stats, reportStats, close).
// - Snapshot operations (backup, restore).

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'api-output-validation';

// Snapshot file path for backup/restore validation.
const SNAPSHOT_PATH = getSnapshotPath(TEST_NAME);

// JSONL file path for exportJSONL validation.
const EXPORT_JSONL_PATH = './tmp/api-output-validation-export.jsonl';

// CSV file path for exportCSV validation.
const EXPORT_CSV_PATH = './tmp/api-output-validation-export.csv';

// CSV file path for exportCSV limit validation.
const EXPORT_CSV_LIMIT_PATH = './tmp/api-output-validation-export-limited.csv';

// CSV file path for exportCSV missing column validation.
const EXPORT_CSV_MISSING_COLUMN_PATH = './tmp/api-output-validation-export-missing-column.csv';

// JSONL file path for importJSONL validation.
const IMPORT_JSONL_PATH = './tmp/api-output-validation-import.jsonl';

// CSV file path for importCSV validation.
const IMPORT_CSV_PATH = './examples/fixtures/users.csv';

// CSV file path for missing-file validation.
const MISSING_CSV_PATH = './tmp/api-output-validation-missing.csv';

// CSV fixture for validateCSV duplicate-header validation.
const INVALID_CSV_DUPLICATE_HEADER_PATH = './examples/fixtures/invalid-users-duplicate-header.csv';

// CSV fixture for validateCSV empty-key validation.
const INVALID_CSV_EMPTY_KEY_PATH = './examples/fixtures/invalid-users-empty-key.csv';

// CSV fixture for validateCSV syntax validation.
const INVALID_CSV_SYNTAX_PATH = './examples/fixtures/invalid-users-syntax.csv';

// JSONL fixture for validateJSONL malformed validation.
const INVALID_JSONL_MALFORMED_PATH = './examples/fixtures/invalid-users-malformed.jsonl';

// JSONL fixture for validateJSONL blank-line validation.
const INVALID_JSONL_BLANK_LINE_PATH = './examples/fixtures/invalid-users-blank-line.jsonl';

// JSONL fixture for validateJSONL missing-key validation.
const INVALID_JSONL_MISSING_KEY_PATH = './examples/fixtures/invalid-users-missing-key.jsonl';

// JSONL file path for missing-file validation.
const MISSING_JSONL_PATH = './tmp/api-output-validation-missing.jsonl';

// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME);

// expectErrorName validates that an action throws an error with the expected name.
async function expectErrorName(action, expectedName) {
  try {
    await action();
    return false;
  } catch (err) {
    return err !== null && err !== undefined && err.name === expectedName;
  }
}

// getOptionalFirstError extracts the first error from a validation result.
function getOptionalFirstError(result) {
  if (!result || typeof result !== 'object') {
    return null;
  }

  if (Object.prototype.hasOwnProperty.call(result, 'firstError')) {
    return result.firstError ?? null;
  }

  if (Object.prototype.hasOwnProperty.call(result, 'firstError,omitempty')) {
    return result['firstError,omitempty'] ?? null;
  }

  return null;
}

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: 1,
  iterations: 1,
  thresholds: {
    'checks{api:set-returns-value}': ['rate>0.999'],
    'checks{api:get-basic}': ['rate>0.999'],
    'checks{api:setMany-structure}': ['rate>0.999'],
    'checks{api:setMany-written}': ['rate>0.999'],
    'checks{api:setMany-values}': ['rate>0.999'],
    'checks{api:setMany-empty-key-rejects}': ['rate>0.999'],
    'checks{api:setMany-empty-key-has-errors}': ['rate>0.999'],
    'checks{api:setMany-empty-key-detail-name}': ['rate>0.999'],
    'checks{api:setMany-empty-key-no-partial-write}': ['rate>0.999'],
    'checks{api:getMany-returns-array}': ['rate>0.999'],
    'checks{api:getMany-preserves-length}': ['rate>0.999'],
    'checks{api:getMany-item-shape}': ['rate>0.999'],
    'checks{api:getMany-missing-exists-false}': ['rate>0.999'],
    'checks{api:getMany-null-exists-true}': ['rate>0.999'],
    'checks{api:deleteMany-returns-object}': ['rate>0.999'],
    'checks{api:deleteMany-deleted-count}': ['rate>0.999'],
    'checks{api:deleteMany-missing-count}': ['rate>0.999'],
    'checks{api:deleteMany-removed-first-key}': ['rate>0.999'],
    'checks{api:deleteMany-removed-second-key}': ['rate>0.999'],
    'checks{api:delete-idempotent}': ['rate>0.999'],
    'checks{api:exists-true}': ['rate>0.999'],
    'checks{api:exists-false}': ['rate>0.999'],
    'checks{api:deleteIfExists-true}': ['rate>0.999'],
    'checks{api:deleteIfExists-false}': ['rate>0.999'],
    'checks{api:incrementBy-positive}': ['rate>0.999'],
    'checks{api:incrementBy-negative}': ['rate>0.999'],
    'checks{api:compareAndSwap-mismatch}': ['rate>0.999'],
    'checks{api:compareAndSwap-success}': ['rate>0.999'],
    'checks{api:compareAndDelete-mismatch}': ['rate>0.999'],
    'checks{api:compareAndDelete-success}': ['rate>0.999'],
    'checks{api:count-prefix}': ['rate>0.999'],
    'checks{api:count-total}': ['rate>0.999'],
    'checks{api:popRandom-structure}': ['rate>0.999'],
    'checks{api:popRandom-deletes-key}': ['rate>0.999'],
    'checks{api:claimRandom-structure}': ['rate>0.999'],
    'checks{api:releaseClaim-success}': ['rate>0.999'],
    'checks{api:completeClaim-success}': ['rate>0.999'],
    'checks{api:completeClaim-deleteKey-false}': ['rate>0.999'],
    'checks{api:rebuildKeyList-boolean}': ['rate>0.999'],
    'checks{api:stats-structure}': ['rate>0.999'],
    'checks{api:stats-claims-fields}': ['rate>0.999'],
    'checks{api:reportStats-void}': ['rate>0.999'],
    'checks{api:close-idempotent}': ['rate>0.999'],
    'checks{api:getOrSet-structure}': ['rate>0.999'],
    'checks{api:getOrSet-existing}': ['rate>0.999'],
    'checks{api:getOrSet-new}': ['rate>0.999'],
    'checks{api:swap-structure}': ['rate>0.999'],
    'checks{api:swap-new}': ['rate>0.999'],
    'checks{api:swap-existing}': ['rate>0.999'],
    'checks{api:list-structure}': ['rate>0.999'],
    'checks{api:listKeys:returns array}': ['rate==1'],
    'checks{api:listKeys:matching count}': ['rate==1'],
    'checks{api:listKeys:lexicographic order}': ['rate==1'],
    'checks{api:deleteByPrefix:first returns object}': ['rate==1'],
    'checks{api:deleteByPrefix:first deleted limited count}': ['rate==1'],
    'checks{api:deleteByPrefix:first reports not done}': ['rate==1'],
    'checks{api:deleteByPrefix:second deletes remaining}': ['rate==1'],
    'checks{api:deleteByPrefix:second reports done}': ['rate==1'],
    'checks{api:exportJSONL:returns object}': ['rate==1'],
    'checks{api:exportJSONL:exported count}': ['rate==1'],
    'checks{api:exportJSONL:wrote bytes}': ['rate==1'],
    'checks{api:importJSONL:returns object}': ['rate==1'],
    'checks{api:importJSONL:imported count}': ['rate==1'],
    'checks{api:importJSONL:read bytes}': ['rate==1'],
    'checks{api:importJSONL:first item exists}': ['rate==1'],
    'checks{api:importJSONL:second item exists}': ['rate==1'],
    'checks{api:list-entry-structure}': ['rate>0.999'],
    'checks{api:list-null-values}': ['rate>0.999'],
    'checks{api:scan-structure}': ['rate>0.999'],
    'checks{api:scan-types}': ['rate>0.999'],
    'checks{api:scan-entry-structure}': ['rate>0.999'],
    'checks{api:scan-pagination}': ['rate>0.999'],
    'checks{api:scan-invalid-cursor-rejects}': ['rate>0.999'],
    'checks{api:scanKeys-structure}': ['rate>0.999'],
    'checks{api:scanKeys-types}': ['rate>0.999'],
    'checks{api:scanKeys-key-types}': ['rate>0.999'],
    'checks{api:scanKeys-pagination}': ['rate>0.999'],
    'checks{api:scanKeys-invalid-cursor-rejects}': ['rate>0.999'],
    'checks{api:randomKey-empty}': ['rate>0.999'],
    'checks{api:randomKeys-is-array}': ['rate>0.999'],
    'checks{api:randomKeys-key-types}': ['rate>0.999'],
    'checks{api:randomKeys-empty-is-array}': ['rate>0.999'],
    'checks{api:backup-structure}': ['rate>0.999'],
    'checks{api:backup-fields}': ['rate>0.999'],
    'checks{api:restore-structure}': ['rate>0.999'],
    'checks{api:restore-fields}': ['rate>0.999'],
    'checks{api:list-negative-numbers}': ['rate>0.999'],
    'checks{api:list-zero-value}': ['rate>0.999'],
    'checks{api:list-array-values}': ['rate>0.999'],
    'checks{api:list-deeply-nested}': ['rate>0.999'],
    'checks{api:list-unicode-values}': ['rate>0.999'],
    'checks{api:list-special-chars}': ['rate>0.999'],
    'checks{api:scan-array-values}': ['rate>0.999'],
    'checks{api:backup-int64-marshalling}': ['rate>0.999'],
    'checks{api:getOrSet-array-value}': ['rate>0.999'],
    'checks{api:getOrSet-negative-number}': ['rate>0.999'],
    'checks{api:getOrSet-unicode-value}': ['rate>0.999'],
    'checks{api:restore-array-preservation}': ['rate>0.999'],
    'checks{api:restore-negative-number-preservation}': ['rate>0.999'],
    'checks{api:restore-unicode-preservation}': ['rate>0.999'],
    'checks{api:restore-deeply-nested-preservation}': ['rate>0.999'],
    'checks{api:getOrSet-boolean-true}': ['rate>0.999'],
    'checks{api:getOrSet-boolean-false}': ['rate>0.999'],
    'checks{api:getOrSet-float-value}': ['rate>0.999'],
    'checks{api:compareAndSwapDetailed-success-structure}': ['rate>0.999'],
    'checks{api:compareAndSwapDetailed-success-fields}': ['rate>0.999'],
    'checks{api:compareAndSwapDetailed-mismatch-no-current}': ['rate>0.999'],
    'checks{api:compareAndSwapDetailed-mismatch-with-current}': ['rate>0.999'],
    'checks{api:compareAndSwapDetailed-missing-key}': ['rate>0.999'],
    'checks{api:compareAndSwapDetailed-null-absent-sentinel}': ['rate>0.999'],
    'checks{api:setIfAbsent-first-write}': ['rate>0.999'],
    'checks{api:setIfAbsent-second-write}': ['rate>0.999'],
    'checks{api:setIfAbsent-value-preserved}': ['rate>0.999'],
    'checks{api:compareAndDeleteDetailed-success-structure}': ['rate>0.999'],
    'checks{api:compareAndDeleteDetailed-success-fields}': ['rate>0.999'],
    'checks{api:compareAndDeleteDetailed-mismatch-no-current}': ['rate>0.999'],
    'checks{api:compareAndDeleteDetailed-mismatch-with-current}': ['rate>0.999'],
    'checks{api:compareAndDeleteDetailed-missing-key}': ['rate>0.999'],
    'checks{api:compareAndDeleteDetailed-null-literal}': ['rate>0.999'],
    'checks{api:compareAndDeleteDetailed-undefined-literal}': ['rate>0.999'],
    'checks{api:list-boolean-true}': ['rate>0.999'],
    'checks{api:list-boolean-false}': ['rate>0.999'],
    'checks{api:list-float-value}': ['rate>0.999'],
    'checks{api:list-empty-array}': ['rate>0.999'],
    'checks{api:list-empty-object}': ['rate>0.999'],
    'checks{api:scan-empty-array}': ['rate>0.999'],
    'checks{api:scan-empty-object}': ['rate>0.999'],
    'checks{api:scan-boolean-true}': ['rate>0.999'],
    'checks{api:scan-float-value}': ['rate>0.999'],
    'checks{api:restore-boolean-true-preservation}': ['rate>0.999'],
    'checks{api:restore-boolean-false-preservation}': ['rate>0.999'],
    'checks{api:restore-float-preservation}': ['rate>0.999'],
    'checks{api:restore-empty-array-preservation}': ['rate>0.999'],
    'checks{api:restore-empty-object-preservation}': ['rate>0.999'],
    'checks{api:claim-batch-methods}': ['rate>0.999'],
    'checks{api:claimKeys-structure}': ['rate>0.999'],
    'checks{api:claimKeys-busy-missing}': ['rate>0.999'],
    'checks{api:claimKeys-all-or-nothing-rollback}': ['rate>0.999'],
    'checks{api:claimKeys-empty-array}': ['rate>0.999'],
    'checks{api:releaseClaims-partial}': ['rate>0.999'],
    'checks{api:releaseClaims-failure-shape}': ['rate>0.999'],
    'checks{api:releaseClaims-empty-array}': ['rate>0.999'],
    'checks{api:completeClaims-partial}': ['rate>0.999'],
    'checks{api:completeClaims-default-delete}': ['rate>0.999'],
    'checks{api:completeClaims-empty-array}': ['rate>0.999'],
    'checks{api:renewClaims-partial}': ['rate>0.999'],
    'checks{api:renewClaims-failure-shape}': ['rate>0.999'],
    'checks{api:renewClaims-empty-array}': ['rate>0.999'],
    'checks{api:allocationStats-structure}': ['rate>0.999'],
    'checks{api:allocationStats-prefix-counts}': ['rate>0.999'],
    'checks{api:allocationStats-all-snapshot}': ['rate>0.999'],
    'checks{api:importCSV-contract}': ['rate>0.999'],
    'checks{api:exportCSV-structure}': ['rate>0.999'],
    'checks{api:exportCSV-prefix-count}': ['rate>0.999'],
    'checks{api:exportCSV-limit}': ['rate>0.999'],
    'checks{api:exportCSV-missing-column-allowed}': ['rate>0.999'],
    'checks{api:exportCSV-non-object-rejects}': ['rate>0.999'],
    'checks{api:exportCSV-nested-rejects}': ['rate>0.999'],
    'checks{api:validateCSV-valid}': ['rate>0.999'],
    'checks{api:validateCSV-without-keyColumn}': ['rate>0.999'],
    'checks{api:validateCSV-limit}': ['rate>0.999'],
    'checks{api:validateCSV-duplicate-header-invalid}': ['rate>0.999'],
    'checks{api:validateCSV-empty-key-invalid}': ['rate>0.999'],
    'checks{api:validateCSV-syntax-invalid}': ['rate>0.999'],
    'checks{api:validateJSONL-valid}': ['rate>0.999'],
    'checks{api:validateJSONL-limit}': ['rate>0.999'],
    'checks{api:validateJSONL-invalid-json}': ['rate>0.999'],
    'checks{api:validateJSONL-blank-line}': ['rate>0.999'],
    'checks{api:validateJSONL-missing-key}': ['rate>0.999'],
    'checks{api:input-validation-new-methods}': ['rate>0.999'],
    'checks{api:input-validation-matrix}': ['rate>0.999'],
    'checks{api:post-close-rejects}': ['rate>0.999']
  }
};

// setup initializes the store with test data.
export const setup = createSetup(kv);

// baseTeardown is the base teardown function that 
// closes the store and removes the test-specific database file.
const baseTeardown = createTeardown(kv, TEST_NAME);

// teardown closes stores and removes generated export artifacts.
export async function teardown() {
  await baseTeardown();

  try {
    file.deleteFile(EXPORT_JSONL_PATH);
  } catch (err) {
    // Ignore cleanup errors if the file doesn't exist or is already deleted.
  }

  try {
    file.deleteFile(IMPORT_JSONL_PATH);
  } catch (err) {
    // Ignore cleanup errors if the file doesn't exist or is already deleted.
  }

  try {
    file.deleteFile(EXPORT_CSV_PATH);
  } catch (err) {
    // Ignore cleanup errors if the file doesn't exist or is already deleted.
  }

  try {
    file.deleteFile(EXPORT_CSV_LIMIT_PATH);
  } catch (err) {
    // Ignore cleanup errors if the file doesn't exist or is already deleted.
  }

  try {
    file.deleteFile(EXPORT_CSV_MISSING_COLUMN_PATH);
  } catch (err) {
    // Ignore cleanup errors if the file doesn't exist or is already deleted.
  }
}

// apiOutputValidationTest validates that all API outputs use camelCase field names
// and that Go-to-JavaScript marshalling preserves all value types correctly.
export default async function apiOutputValidationTest() {
  await kv.clear();

  // Initialize store with various value types to test marshalling edge cases:
  // - Basic types: strings, numbers, null, objects, arrays, booleans
  // - Edge cases: negative numbers, zero, floating point, empty arrays/objects
  // - Unicode: emojis, Chinese characters, Russian Cyrillic text
  // - Special characters: punctuation and symbols
  await kv.set('foo', 'bar');
  await kv.set('nullKey', null);
  await kv.set('objKey', { nested: 'value' });
  await kv.set('numKey', 123);
  await kv.set('negativeKey', -456);
  await kv.set('zeroKey', 0);
  await kv.set('floatKey', 3.14159);
  await kv.set('boolTrueKey', true);
  await kv.set('boolFalseKey', false);
  await kv.set('arrayKey', [1, 2, 3, 'test']);
  await kv.set('emptyArrayKey', []);
  await kv.set('emptyObjectKey', {});
  await kv.set('deepNestedKey', { level1: { level2: { level3: 'deep' } } });
  await kv.set('unicodeKey', '🚀 Hello 世界 🌍 енот жарит котлеты');
  await kv.set('specialCharsKey', '!@#$%^&*()_+-=[]{}|;:,.<>?');

  // set(): Validate that set returns the value that was written.
  const setReturn = await kv.set('setReturnKey', { state: 'ok' });
  check(setReturn, {
    'api:set-returns-value': () =>
      typeof setReturn === 'object' && setReturn.state === 'ok'
  });

  // setMany(): Validate response shape and all-or-nothing success path.
  const setManyResult = await kv.setMany({
    setManyKeyA: { role: 'primary' },
    setManyKeyB: { role: 'secondary' }
  });
  const setManyKeys = Object.keys(setManyResult);
  const setManyValueA = await kv.get('setManyKeyA');
  const setManyValueB = await kv.get('setManyKeyB');
  check(setManyResult, {
    'api:setMany-structure': () =>
      setManyKeys.includes('written') && setManyKeys.length === 1,
    'api:setMany-written': () =>
      typeof setManyResult.written === 'number' && setManyResult.written === 2,
    'api:setMany-values': () =>
      setManyValueA.role === 'primary' && setManyValueB.role === 'secondary'
  });

  // setMany(): Validate empty-key rejection shape and all-or-nothing behavior.
  const setManyGuardKey = 'setManyEmptyKeyGuard';
  let setManyEmptyKeyError = null;
  try {
    await kv.setMany({
      [setManyGuardKey]: { role: 'must-not-write' },
      '': 'invalid-empty-key'
    });
  } catch (err) {
    setManyEmptyKeyError = err;
  }
  const setManyGuardExists = await kv.exists(setManyGuardKey);
  check(true, {
    'api:setMany-empty-key-rejects': () =>
      setManyEmptyKeyError && setManyEmptyKeyError.name === 'InvalidOptionsError',
    'api:setMany-empty-key-has-errors': () =>
      Array.isArray(setManyEmptyKeyError?.errors) && setManyEmptyKeyError.errors.length === 1,
    'api:setMany-empty-key-detail-name': () =>
      setManyEmptyKeyError?.errors?.[0]?.name === 'EmptyKey',
    'api:setMany-empty-key-no-partial-write': () => setManyGuardExists === false
  });

  const getManyItems = await kv.getMany([
    'setManyKeyA',
    'api:missing',
    'nullKey'
  ]);
  check(getManyItems, {
    'api:getMany-returns-array': (items) => Array.isArray(items),
    'api:getMany-preserves-length': (items) => items.length === 3,
    'api:getMany-item-shape': (items) =>
      items.every(
        (item) =>
          item &&
          typeof item.key === 'string' &&
          typeof item.exists === 'boolean' &&
          Object.prototype.hasOwnProperty.call(item, 'value')
      ),
    'api:getMany-missing-exists-false': (items) =>
      items[1].key === 'api:missing' && items[1].exists === false && items[1].value === null,
    'api:getMany-null-exists-true': (items) =>
      items[2].key === 'nullKey' && items[2].exists === true && items[2].value === null
  });

  await kv.setMany({
    'api:delete:1': { value: 1 },
    'api:delete:2': { value: 2 }
  });

  const deleteManyResult = await kv.deleteMany([
    'api:delete:1',
    'api:delete:2',
    'api:delete:missing'
  ]);

  check(deleteManyResult, {
    'api:deleteMany-returns-object': (result) =>
      result !== null && typeof result === 'object',
    'api:deleteMany-deleted-count': (result) =>
      result.deleted === 2,
    'api:deleteMany-missing-count': (result) =>
      result.missing === 1
  });

  const deleteManyItems = await kv.getMany([
    'api:delete:1',
    'api:delete:2'
  ]);

  check(deleteManyItems, {
    'api:deleteMany-removed-first-key': (items) => items[0].exists === false,
    'api:deleteMany-removed-second-key': (items) => items[1].exists === false
  });

  // get(): Validate direct scalar retrieval from set() data.
  const basicGet = await kv.get('foo');
  check(basicGet, {
    'api:get-basic': () => basicGet === 'bar'
  });

  // exists(): Validate true/false responses for present and absent keys.
  const existsTrue = await kv.exists('foo');
  const existsFalse = await kv.exists('missing-key');
  check(true, {
    'api:exists-true': () => existsTrue === true,
    'api:exists-false': () => existsFalse === false
  });

  // delete(): Validate idempotent boolean behavior.
  await kv.set('deleteKey', { once: true });
  const deleteFirst = await kv.delete('deleteKey');
  const deleteSecond = await kv.delete('deleteKey');
  check(true, {
    'api:delete-idempotent': () =>
      typeof deleteFirst === 'boolean' &&
      deleteFirst === true &&
      typeof deleteSecond === 'boolean' &&
      deleteSecond === true
  });

  // deleteIfExists(): Validate informative first-delete and second-delete behavior.
  await kv.set('deleteIfExistsKey', { once: true });
  const deleteIfExistsFirst = await kv.deleteIfExists('deleteIfExistsKey');
  const deleteIfExistsSecond = await kv.deleteIfExists('deleteIfExistsKey');
  check(true, {
    'api:deleteIfExists-true': () =>
      typeof deleteIfExistsFirst === 'boolean' && deleteIfExistsFirst === true,
    'api:deleteIfExists-false': () =>
      typeof deleteIfExistsSecond === 'boolean' && deleteIfExistsSecond === false
  });

  // incrementBy(): Validate positive and negative deltas.
  const incrementPositive = await kv.incrementBy('counter:validated', 10);
  const incrementNegative = await kv.incrementBy('counter:validated', -3);
  check(true, {
    'api:incrementBy-positive': () =>
      typeof incrementPositive === 'number' && incrementPositive === 10,
    'api:incrementBy-negative': () =>
      typeof incrementNegative === 'number' && incrementNegative === 7
  });

  // compareAndSwap(): Validate boolean mismatch/success contract.
  const casBooleanKey = 'compareAndSwapBooleanKey';
  await kv.set(casBooleanKey, { version: 1 });
  const casBooleanMismatch = await kv.compareAndSwap(
    casBooleanKey,
    { version: 999 },
    { version: 2 }
  );
  const casBooleanSuccess = await kv.compareAndSwap(
    casBooleanKey,
    { version: 1 },
    { version: 2 }
  );
  check(true, {
    'api:compareAndSwap-mismatch': () => casBooleanMismatch === false,
    'api:compareAndSwap-success': () => casBooleanSuccess === true
  });

  // compareAndDelete(): Validate boolean mismatch/success contract.
  const cadBooleanKey = 'compareAndDeleteBooleanKey';
  await kv.set(cadBooleanKey, { state: 'active' });
  const cadBooleanMismatch = await kv.compareAndDelete(
    cadBooleanKey,
    { state: 'stale' }
  );
  const cadBooleanSuccess = await kv.compareAndDelete(
    cadBooleanKey,
    { state: 'active' }
  );
  check(true, {
    'api:compareAndDelete-mismatch': () => cadBooleanMismatch === false,
    'api:compareAndDelete-success': () => cadBooleanSuccess === true
  });

  // count(): Validate prefix-scoped and global counters.
  await kv.setMany({
    'count:alpha:1': { id: 1 },
    'count:alpha:2': { id: 2 },
    'count:beta:1': { id: 3 }
  });
  const countAlpha = await kv.count({ prefix: 'count:alpha:' });
  const countAll = await kv.count();
  check(true, {
    'api:count-prefix': () => typeof countAlpha === 'number' && countAlpha === 2,
    'api:count-total': () => typeof countAll === 'number' && countAll >= 3
  });

  // popRandom(): Validate returned entry shape and atomic removal.
  await kv.setMany({
    'pop:key:1': { id: 1, queue: 'api' },
    'pop:key:2': { id: 2, queue: 'api' }
  });
  const popped = await kv.popRandom({ prefix: 'pop:key:' });
  let poppedExistsAfterRemoval = false;
  if (popped) {
    poppedExistsAfterRemoval = await kv.exists(popped.key);
  }
  check(popped, {
    'api:popRandom-structure': () =>
      popped &&
      typeof popped.key === 'string' &&
      popped.key.startsWith('pop:key:') &&
      Object.prototype.hasOwnProperty.call(popped, 'value'),
    'api:popRandom-deletes-key': () =>
      popped && poppedExistsAfterRemoval === false
  });

  const poppedEmpty = await kv.popRandom({ prefix: 'missing:pop:key:' });
  check(true, {
    'api:popRandom-empty-null': () => poppedEmpty === null
  });

  await kv.setMany({
    'pop:many:key:1': { id: 1, queue: 'batch' },
    'pop:many:key:2': { id: 2, queue: 'batch' },
    'pop:many:key:3': { id: 3, queue: 'batch' }
  });
  const poppedMany = await kv.popRandomMany({
    prefix: 'pop:many:key:',
    count: 2
  });
  let popManyShape = Array.isArray(poppedMany) && poppedMany.length <= 2;
  let popManyRemoved = true;
  const poppedManySeen = new Set();
  for (const entry of poppedMany) {
    if (
      !entry ||
      typeof entry.key !== 'string' ||
      !entry.key.startsWith('pop:many:key:') ||
      !Object.prototype.hasOwnProperty.call(entry, 'value')
    ) {
      popManyShape = false;
    }

    if (poppedManySeen.has(entry.key)) {
      popManyShape = false;
    }
    poppedManySeen.add(entry.key);

    const stillExists = await kv.exists(entry.key);
    if (stillExists) {
      popManyRemoved = false;
    }
  }
  const poppedManyEmpty = await kv.popRandomMany({
    prefix: 'missing:pop:many:key:',
    count: 2
  });
  const popManyEmptyArray = Array.isArray(poppedManyEmpty) && poppedManyEmpty.length === 0;

  // claimRandom()/releaseClaim()/completeClaim(): Validate claim lifecycle payloads.
  await kv.setMany({
    'claim:key:1': { id: 1, status: 'queued' },
    'claim:key:2': { id: 2, status: 'queued' }
  });
  const claimOne = await kv.claimRandom({
    prefix: 'claim:key:',
    owner: 'api-output-validation',
    ttl: 30000
  });
  let releasedClaim = false;
  if (claimOne) {
    releasedClaim = await kv.releaseClaim(claimOne);
  }

  const claimTwo = await kv.claimRandom({
    prefix: 'claim:key:',
    owner: 'api-output-validation',
    ttl: 30000
  });
  let completedClaim = false;
  let completedKeyExists = false;
  if (claimTwo) {
    completedClaim = await kv.completeClaim(claimTwo, { deleteKey: false });
    completedKeyExists = await kv.exists(claimTwo.key);
  }
  check(true, {
    'api:claimRandom-structure': () =>
      claimOne &&
      typeof claimOne.id === 'string' &&
      typeof claimOne.key === 'string' &&
      typeof claimOne.token === 'number' &&
      typeof claimOne.expiresAt === 'number' &&
      typeof claimOne.entry === 'object' &&
      claimOne.entry.key === claimOne.key,
    'api:releaseClaim-success': () => claimOne && releasedClaim === true,
    'api:completeClaim-success': () => claimTwo && completedClaim === true,
    'api:completeClaim-deleteKey-false': () =>
      claimTwo && completedKeyExists === true
  });

  await kv.set('claim:key:exact', { id: 1, queue: 'claim-key' });
  const claimKeyResult = await kv.claimKey('claim:key:exact', {
    owner: 'api-output-validation',
    ttl: 30000
  });
  const claimKeyLiveNull = (await kv.claimKey('claim:key:exact', { ttl: 30000 })) === null;
  const claimKeyMissingNull = (await kv.claimKey('claim:key:missing', { ttl: 30000 })) === null;
  let claimKeyStructure = false;
  if (claimKeyResult !== null) {
    claimKeyStructure =
      typeof claimKeyResult.id === 'string' &&
      claimKeyResult.key === 'claim:key:exact' &&
      typeof claimKeyResult.token === 'number' &&
      typeof claimKeyResult.expiresAt === 'number' &&
      claimKeyResult.entry?.key === 'claim:key:exact';
    await kv.releaseClaim(claimKeyResult);
  }

  await kv.setMany({
    'claim:many:key:1': { id: 1, batch: true },
    'claim:many:key:2': { id: 2, batch: true },
    'claim:many:key:3': { id: 3, batch: true },
    'claim:many:key:4': { id: 4, batch: true }
  });
  const heldClaim = await kv.claimKey('claim:many:key:1', { ttl: 30000 });
  const claimMany = await kv.claimRandomMany({
    prefix: 'claim:many:key:',
    count: 3,
    owner: 'api-output-validation',
    ttl: 30000
  });
  let claimManyShape = Array.isArray(claimMany) && claimMany.length <= 3;
  const claimManySeen = new Set();
  for (const claim of claimMany) {
    if (
      !claim ||
      typeof claim.id !== 'string' ||
      typeof claim.key !== 'string' ||
      typeof claim.token !== 'number' ||
      typeof claim.expiresAt !== 'number' ||
      claim.entry?.key !== claim.key
    ) {
      claimManyShape = false;
    }
    if (claimManySeen.has(claim.key)) {
      claimManyShape = false;
    }
    if (heldClaim !== null && claim.key === heldClaim.key) {
      claimManyShape = false;
    }
    claimManySeen.add(claim.key);
    await kv.releaseClaim(claim);
  }
  if (heldClaim !== null) {
    await kv.releaseClaim(heldClaim);
  }
  const claimManyEmpty = await kv.claimRandomMany({
    prefix: 'missing:claim:many:key:',
    count: 2,
    ttl: 30000
  });
  const claimManyEmptyArray = Array.isArray(claimManyEmpty) && claimManyEmpty.length === 0;

  await kv.set('renew:key:1', { id: 1, queue: 'renew' });
  const renewClaimRef = await kv.claimKey('renew:key:1', { ttl: 200 });
  let renewSuccess = false;
  let renewTokenStable = false;
  let renewStaleFalse = false;
  let renewMissingFalse = false;
  if (renewClaimRef !== null) {
    renewSuccess = await kv.renewClaim(renewClaimRef, { ttl: 30000 });
    renewStaleFalse =
      (await kv.renewClaim(
        { id: renewClaimRef.id, key: renewClaimRef.key, token: renewClaimRef.token + 1 },
        { ttl: 30000 }
      )) === false;
    renewMissingFalse =
      (await kv.renewClaim(
        { id: 'missing', key: 'missing:renew:key', token: 1 },
        { ttl: 30000 }
      )) === false;
    renewTokenStable = await kv.releaseClaim(renewClaimRef);
  }

  await kv.set('renew:key:expired', { id: 2, queue: 'renew' });
  const expiringClaim = await kv.claimKey('renew:key:expired', { ttl: 5 });
  let renewExpiredFalse = false;
  if (expiringClaim !== null) {
    sleep(0.02);
    renewExpiredFalse = (await kv.renewClaim(expiringClaim, { ttl: 30000 })) === false;
  }

  check(true, {
    'api:claim-batch-methods': () =>
      claimKeyStructure &&
      claimKeyLiveNull &&
      claimKeyMissingNull &&
      claimManyShape &&
      claimManyEmptyArray &&
      renewSuccess &&
      renewStaleFalse &&
      renewMissingFalse &&
      renewExpiredFalse &&
      renewTokenStable &&
      popManyShape &&
      popManyRemoved &&
      popManyEmptyArray
  });

  // claimKeys(), releaseClaims(), completeClaims(), renewClaims(): validate batch lifecycle contracts.
  await kv.setMany({
    'api:claimKeys:free:1': { id: 1, role: 'free' },
    'api:claimKeys:free:2': { id: 2, role: 'free' },
    'api:claimKeys:busy': { id: 3, role: 'busy' }
  });

  const claimKeysBusyClaim = await kv.claimKey('api:claimKeys:busy', { ttl: 30000 });
  const claimKeysResult = await kv.claimKeys(
    ['api:claimKeys:free:1', 'api:claimKeys:busy', 'api:claimKeys:missing:1'],
    { owner: 'api-output-validation:claimKeys', ttl: 30000, allOrNothing: false }
  );
  const claimKeysResultFields = Object.keys(claimKeysResult);

  const claimKeysAllOrNothing = await kv.claimKeys(
    ['api:claimKeys:free:2', 'api:claimKeys:missing:2'],
    { ttl: 30000, allOrNothing: true }
  );
  let claimKeysRollbackReleased = false;
  const claimKeysRollbackProbe = await kv.claimKey('api:claimKeys:free:2', { ttl: 30000 });
  if (claimKeysRollbackProbe !== null) {
    claimKeysRollbackReleased = await kv.releaseClaim(claimKeysRollbackProbe);
  }

  const claimKeysEmptyResult = await kv.claimKeys([], { ttl: 30000 });

  for (const claim of claimKeysResult.claimed) {
    await kv.releaseClaim(claim);
  }
  if (claimKeysBusyClaim !== null) {
    await kv.releaseClaim(claimKeysBusyClaim);
  }

  await kv.set('api:releaseClaims:key:1', { id: 1 });
  const releaseClaimsClaim = await kv.claimKey('api:releaseClaims:key:1', { ttl: 30000 });
  let releaseClaimsResult = { attempted: 0, released: 0, failed: [] };
  if (releaseClaimsClaim !== null) {
    releaseClaimsResult = await kv.releaseClaims([releaseClaimsClaim, releaseClaimsClaim]);
  }
  const releaseClaimsEmptyResult = await kv.releaseClaims([]);

  await kv.set('api:completeClaims:keep', { id: 1 });
  const completeKeepClaim = await kv.claimKey('api:completeClaims:keep', { ttl: 30000 });
  let completeClaimsResult = { attempted: 0, completed: 0, failed: [] };
  let completeKeepExists = false;
  if (completeKeepClaim !== null) {
    completeClaimsResult = await kv.completeClaims([completeKeepClaim, completeKeepClaim], { deleteKey: false });
    completeKeepExists = await kv.exists('api:completeClaims:keep');
  }
  const completeClaimsEmptyResult = await kv.completeClaims([]);

  await kv.set('api:completeClaims:delete', { id: 2 });
  const completeDeleteClaim = await kv.claimKey('api:completeClaims:delete', { ttl: 30000 });
  let completeClaimsDefaultDelete = { attempted: 0, completed: 0, failed: [] };
  let completeDeleteExists = true;
  if (completeDeleteClaim !== null) {
    completeClaimsDefaultDelete = await kv.completeClaims([completeDeleteClaim]);
    completeDeleteExists = await kv.exists('api:completeClaims:delete');
  }

  await kv.set('api:renewClaims:key:1', { id: 1 });
  const renewClaimsClaim = await kv.claimKey('api:renewClaims:key:1', { ttl: 30000 });
  let renewClaimsResult = { attempted: 0, renewed: 0, failed: [] };
  if (renewClaimsClaim !== null) {
    renewClaimsResult = await kv.renewClaims(
      [
        renewClaimsClaim,
        { id: renewClaimsClaim.id, key: renewClaimsClaim.key, token: renewClaimsClaim.token + 1 }
      ],
      { ttl: 30000 }
    );
    await kv.releaseClaim(renewClaimsClaim);
  }
  const renewClaimsEmptyResult = await kv.renewClaims([], { ttl: 1000 });

  check(
    {
      claimKeysResult,
      claimKeysResultFields,
      claimKeysAllOrNothing,
      claimKeysEmptyResult,
      releaseClaimsResult,
      releaseClaimsEmptyResult,
      completeClaimsResult,
      completeClaimsDefaultDelete,
      completeClaimsEmptyResult,
      renewClaimsResult,
      renewClaimsEmptyResult
    },
    {
      'api:claimKeys-structure': (ctx) =>
        ctx.claimKeysResultFields.includes('claimed') &&
        ctx.claimKeysResultFields.includes('busy') &&
        ctx.claimKeysResultFields.includes('missing') &&
        ctx.claimKeysResultFields.length === 3 &&
        Array.isArray(ctx.claimKeysResult.claimed) &&
        Array.isArray(ctx.claimKeysResult.busy) &&
        Array.isArray(ctx.claimKeysResult.missing),
      'api:claimKeys-busy-missing': (ctx) =>
        ctx.claimKeysResult.claimed.length === 1 &&
        ctx.claimKeysResult.busy.length === 1 &&
        ctx.claimKeysResult.busy[0] === 'api:claimKeys:busy' &&
        ctx.claimKeysResult.missing.length === 1 &&
        ctx.claimKeysResult.missing[0] === 'api:claimKeys:missing:1',
      'api:claimKeys-all-or-nothing-rollback': (ctx) =>
        ctx.claimKeysAllOrNothing.claimed.length === 0 &&
        ctx.claimKeysAllOrNothing.busy.length === 0 &&
        ctx.claimKeysAllOrNothing.missing.length === 1 &&
        ctx.claimKeysAllOrNothing.missing[0] === 'api:claimKeys:missing:2' &&
        claimKeysRollbackReleased === true,
      'api:claimKeys-empty-array': (ctx) =>
        Array.isArray(ctx.claimKeysEmptyResult.claimed) &&
        ctx.claimKeysEmptyResult.claimed.length === 0 &&
        Array.isArray(ctx.claimKeysEmptyResult.busy) &&
        ctx.claimKeysEmptyResult.busy.length === 0 &&
        Array.isArray(ctx.claimKeysEmptyResult.missing) &&
        ctx.claimKeysEmptyResult.missing.length === 0,
      'api:releaseClaims-partial': (ctx) =>
        typeof ctx.releaseClaimsResult.attempted === 'number' &&
        ctx.releaseClaimsResult.attempted === 2 &&
        typeof ctx.releaseClaimsResult.released === 'number' &&
        ctx.releaseClaimsResult.released === 1 &&
        Array.isArray(ctx.releaseClaimsResult.failed) &&
        ctx.releaseClaimsResult.failed.length === 1,
      'api:releaseClaims-failure-shape': (ctx) => {
        const failure = ctx.releaseClaimsResult.failed[0];
        return (
          failure &&
          typeof failure.id === 'string' &&
          typeof failure.key === 'string' &&
          failure.name === 'ClaimNotUpdated' &&
          typeof failure.message === 'string' &&
          failure.message.length > 0
        );
      },
      'api:releaseClaims-empty-array': (ctx) =>
        ctx.releaseClaimsEmptyResult.attempted === 0 &&
        ctx.releaseClaimsEmptyResult.released === 0 &&
        Array.isArray(ctx.releaseClaimsEmptyResult.failed) &&
        ctx.releaseClaimsEmptyResult.failed.length === 0,
      'api:completeClaims-partial': (ctx) =>
        ctx.completeClaimsResult.attempted === 2 &&
        ctx.completeClaimsResult.completed === 1 &&
        Array.isArray(ctx.completeClaimsResult.failed) &&
        ctx.completeClaimsResult.failed.length === 1 &&
        ctx.completeClaimsResult.failed[0].name === 'ClaimNotUpdated' &&
        completeKeepExists === true,
      'api:completeClaims-default-delete': (ctx) =>
        ctx.completeClaimsDefaultDelete.attempted === 1 &&
        ctx.completeClaimsDefaultDelete.completed === 1 &&
        Array.isArray(ctx.completeClaimsDefaultDelete.failed) &&
        ctx.completeClaimsDefaultDelete.failed.length === 0 &&
        completeDeleteExists === false,
      'api:completeClaims-empty-array': (ctx) =>
        ctx.completeClaimsEmptyResult.attempted === 0 &&
        ctx.completeClaimsEmptyResult.completed === 0 &&
        Array.isArray(ctx.completeClaimsEmptyResult.failed) &&
        ctx.completeClaimsEmptyResult.failed.length === 0,
      'api:renewClaims-partial': (ctx) =>
        ctx.renewClaimsResult.attempted === 2 &&
        ctx.renewClaimsResult.renewed === 1 &&
        Array.isArray(ctx.renewClaimsResult.failed) &&
        ctx.renewClaimsResult.failed.length === 1,
      'api:renewClaims-failure-shape': (ctx) => {
        const failure = ctx.renewClaimsResult.failed[0];
        return (
          failure &&
          typeof failure.id === 'string' &&
          typeof failure.key === 'string' &&
          failure.name === 'ClaimNotUpdated' &&
          typeof failure.message === 'string' &&
          failure.message.length > 0
        );
      },
      'api:renewClaims-empty-array': (ctx) =>
        ctx.renewClaimsEmptyResult.attempted === 0 &&
        ctx.renewClaimsEmptyResult.renewed === 0 &&
        Array.isArray(ctx.renewClaimsEmptyResult.failed) &&
        ctx.renewClaimsEmptyResult.failed.length === 0
    }
  );

  // allocationStats(): validate prefix-scoped and full snapshot contracts.
  await kv.setMany({
    'api:allocation:user:1': { id: 1 },
    'api:allocation:user:2': { id: 2 },
    'api:allocation:user:3': { id: 3 }
  });
  const allocationLiveClaimA = await kv.claimKey('api:allocation:user:1', { ttl: 30000 });
  const allocationLiveClaimB = await kv.claimKey('api:allocation:user:2', { ttl: 30000 });

  const allocationStatsPrefix = await kv.allocationStats({ prefix: 'api:allocation:user:' });
  if (allocationLiveClaimA !== null) {
    await kv.releaseClaim(allocationLiveClaimA);
  }
  const allocationStatsAfterRelease = await kv.allocationStats({ prefix: 'api:allocation:user:' });
  const allocationStatsAll = await kv.allocationStats();

  if (allocationLiveClaimB !== null) {
    await kv.releaseClaim(allocationLiveClaimB);
  }

  check(
    { allocationStatsPrefix, allocationStatsAfterRelease, allocationStatsAll },
    {
      'api:allocationStats-structure': (ctx) =>
        typeof ctx.allocationStatsPrefix.prefix === 'string' &&
        typeof ctx.allocationStatsPrefix.total === 'number' &&
        typeof ctx.allocationStatsPrefix.claimable === 'number' &&
        typeof ctx.allocationStatsPrefix.claimedLive === 'number' &&
        typeof ctx.allocationStatsPrefix.claimedExpired === 'number' &&
        typeof ctx.allocationStatsPrefix.backend === 'string' &&
        typeof ctx.allocationStatsPrefix.trackKeys === 'boolean',
      'api:allocationStats-prefix-counts': (ctx) =>
        ctx.allocationStatsPrefix.prefix === 'api:allocation:user:' &&
        ctx.allocationStatsPrefix.total === 3 &&
        ctx.allocationStatsPrefix.claimable === 1 &&
        ctx.allocationStatsPrefix.claimedLive === 2 &&
        ctx.allocationStatsPrefix.claimedExpired === 0 &&
        ctx.allocationStatsAfterRelease.claimable === 2 &&
        ctx.allocationStatsAfterRelease.claimedLive === 1 &&
        ctx.allocationStatsAfterRelease.claimedExpired === 0,
      'api:allocationStats-all-snapshot': (ctx) =>
        ctx.allocationStatsAll.prefix === '' &&
        ctx.allocationStatsAll.total >= ctx.allocationStatsPrefix.total &&
        (ctx.allocationStatsAll.backend === 'memory' || ctx.allocationStatsAll.backend === 'disk')
    }
  );

  // rebuildKeyList(), stats(), reportStats(): Validate observability/lifecycle helpers.
  const rebuiltBeforeFlow = await kv.rebuildKeyList();
  check(rebuiltBeforeFlow, {
    'api:rebuildKeyList-boolean': () =>
      typeof rebuiltBeforeFlow === 'boolean' && rebuiltBeforeFlow === true
  });

  const stats = await kv.stats();
  const statsKeys = Object.keys(stats);
  check(stats, {
    'api:stats-structure': () =>
      statsKeys.includes('backend') &&
      statsKeys.includes('serialization') &&
      statsKeys.includes('trackKeys') &&
      statsKeys.includes('count') &&
      statsKeys.includes('claims'),
    'api:stats-claims-fields': () =>
      typeof stats.claims === 'object' &&
      typeof stats.claims.live === 'number' &&
      typeof stats.claims.expired === 'number'
  });

  await kv.reportStats();
  check(true, {
    'api:reportStats-void': () => true
  });

  // getOrSet(): Validate structure and behavior for existing vs new keys.
  // Verifies that the result object has correct camelCase field names (value, loaded)
  // and that the loaded flag correctly indicates whether the key existed.
  const existingResult = await kv.getOrSet('foo', 999);
  const existingKeys = Object.keys(existingResult);

  check(existingResult, {
    'api:getOrSet-structure': () =>
      existingKeys.includes('value') && existingKeys.includes('loaded') && existingKeys.length === 2,
    'api:getOrSet-existing': () =>
      existingResult.loaded === true && existingResult.value === 'bar'
  });

  // getOrSet() with array value: Verify arrays are marshalled correctly from Go to JS.
  const arrayGetOrSet = await kv.getOrSet('arrayKey', [999]);
  check(arrayGetOrSet, {
    'api:getOrSet-array-value': () =>
      arrayGetOrSet.loaded === true && Array.isArray(arrayGetOrSet.value) && arrayGetOrSet.value.length === 4
  });

  // getOrSet() with negative number: Verify negative integers are preserved during marshalling.
  const negativeGetOrSet = await kv.getOrSet('negativeKey', 999);
  check(negativeGetOrSet, {
    'api:getOrSet-negative-number': () =>
      negativeGetOrSet.loaded === true && typeof negativeGetOrSet.value === 'number' && negativeGetOrSet.value === -456
  });

  // getOrSet() with Unicode string: Verify emojis, Chinese, and Russian Cyrillic characters
  // are correctly marshalled and preserved through the Go-to-JS boundary.
  const unicodeGetOrSet = await kv.getOrSet('unicodeKey', 'default');
  check(unicodeGetOrSet, {
    'api:getOrSet-unicode-value': () =>
      unicodeGetOrSet.loaded === true &&
      typeof unicodeGetOrSet.value === 'string' &&
      unicodeGetOrSet.value.includes('🚀') &&
      unicodeGetOrSet.value.includes('世界') &&
      unicodeGetOrSet.value.includes('енот') &&
      unicodeGetOrSet.value.includes('котлеты')
  });

  // getOrSet() with boolean values: Verify true/false are preserved correctly.
  const boolTrueGetOrSet = await kv.getOrSet('boolTrueKey', false);
  check(boolTrueGetOrSet, {
    'api:getOrSet-boolean-true': () =>
      boolTrueGetOrSet.loaded === true && boolTrueGetOrSet.value === true
  });

  const boolFalseGetOrSet = await kv.getOrSet('boolFalseKey', true);
  check(boolFalseGetOrSet, {
    'api:getOrSet-boolean-false': () =>
      boolFalseGetOrSet.loaded === true && boolFalseGetOrSet.value === false
  });

  // getOrSet() with floating point number: Verify decimal numbers are preserved.
  const floatGetOrSet = await kv.getOrSet('floatKey', 999.99);
  check(floatGetOrSet, {
    'api:getOrSet-float-value': () =>
      floatGetOrSet.loaded === true && typeof floatGetOrSet.value === 'number' && floatGetOrSet.value === 3.14159
  });

  // getOrSet() with new key: Verify that non-existent keys return loaded=false
  // and the default value is correctly set and returned.
  const newKey = `newKey:${Date.now()}`;
  const newResult = await kv.getOrSet(newKey, 555);
  const newKeys = Object.keys(newResult);

  check(newResult, {
    'api:getOrSet-new': () =>
      newResult.loaded === false && newResult.value === 555,
    'api:getOrSet-new-structure': () =>
      newKeys.includes('value') && newKeys.includes('loaded')
  });

  // swap(): Validate structure and behavior for new vs existing keys.
  // Verifies that the result object has correct camelCase field names (previous, loaded)
  // and that previous value is null for new keys, or the old value for existing keys.
  const swapNewResult = await kv.swap('swapKey', 'first');
  const swapNewKeys = Object.keys(swapNewResult);

  check(swapNewResult, {
    'api:swap-structure': () =>
      swapNewKeys.includes('previous') && swapNewKeys.includes('loaded') && swapNewKeys.length === 2,
    'api:swap-new': () =>
      swapNewResult.loaded === false && swapNewResult.previous === null
  });

  // swap() with existing key: Verify that previous value is correctly returned
  // and the loaded flag indicates the key existed.
  const swapExistingResult = await kv.swap('swapKey', 'second');
  check(swapExistingResult, {
    'api:swap-existing': () =>
      swapExistingResult.loaded === true && swapExistingResult.previous === 'first'
  });

  // compareAndSwapDetailed(): Validate detailed payload shape for all branches.
  // Success branch should contain only (swapped, reason). Mismatch branch should
  // contain (swapped, reason, existed) and include current only when requested and
  // the key exists.
  const casDetailedKey = 'casDetailedKey';
  const casDetailedInitial = { version: 1, owner: 'seed' };
  await kv.set(casDetailedKey, casDetailedInitial);

  const casDetailedMismatchNoCurrent = await kv.compareAndSwapDetailed(
    casDetailedKey,
    { version: 999, owner: 'seed' },
    { version: 2, owner: 'writer-1' },
    { includeCurrentOnMismatch: false }
  );
  const casDetailedMismatchNoCurrentKeys = Object.keys(casDetailedMismatchNoCurrent);

  check(casDetailedMismatchNoCurrent, {
    'api:compareAndSwapDetailed-mismatch-no-current': () =>
      casDetailedMismatchNoCurrent.swapped === false &&
      casDetailedMismatchNoCurrent.reason === 'mismatch' &&
      casDetailedMismatchNoCurrent.existed === true &&
      casDetailedMismatchNoCurrentKeys.includes('swapped') &&
      casDetailedMismatchNoCurrentKeys.includes('reason') &&
      casDetailedMismatchNoCurrentKeys.includes('existed') &&
      casDetailedMismatchNoCurrentKeys.length === 3 &&
      !Object.prototype.hasOwnProperty.call(casDetailedMismatchNoCurrent, 'current')
  });

  const casDetailedMismatchWithCurrent = await kv.compareAndSwapDetailed(
    casDetailedKey,
    { version: 999, owner: 'seed' },
    { version: 2, owner: 'writer-2' },
    { includeCurrentOnMismatch: true }
  );
  const casDetailedMismatchWithCurrentKeys = Object.keys(casDetailedMismatchWithCurrent);

  check(casDetailedMismatchWithCurrent, {
    'api:compareAndSwapDetailed-mismatch-with-current': () =>
      casDetailedMismatchWithCurrent.swapped === false &&
      casDetailedMismatchWithCurrent.reason === 'mismatch' &&
      casDetailedMismatchWithCurrent.existed === true &&
      casDetailedMismatchWithCurrentKeys.includes('swapped') &&
      casDetailedMismatchWithCurrentKeys.includes('reason') &&
      casDetailedMismatchWithCurrentKeys.includes('existed') &&
      casDetailedMismatchWithCurrentKeys.includes('current') &&
      casDetailedMismatchWithCurrentKeys.length === 4 &&
      typeof casDetailedMismatchWithCurrent.current === 'object' &&
      casDetailedMismatchWithCurrent.current.version === 1 &&
      casDetailedMismatchWithCurrent.current.owner === 'seed'
  });

  const casDetailedMissingKey = await kv.compareAndSwapDetailed(
    'casDetailedMissingKey',
    { expected: true },
    { version: 1 },
    { includeCurrentOnMismatch: true }
  );
  const casDetailedMissingKeyKeys = Object.keys(casDetailedMissingKey);

  check(casDetailedMissingKey, {
    'api:compareAndSwapDetailed-missing-key': () =>
      casDetailedMissingKey.swapped === false &&
      casDetailedMissingKey.reason === 'mismatch' &&
      casDetailedMissingKey.existed === false &&
      casDetailedMissingKeyKeys.includes('swapped') &&
      casDetailedMissingKeyKeys.includes('reason') &&
      casDetailedMissingKeyKeys.includes('existed') &&
      casDetailedMissingKeyKeys.length === 3 &&
      !Object.prototype.hasOwnProperty.call(casDetailedMissingKey, 'current')
  });

  const casDetailedSuccess = await kv.compareAndSwapDetailed(
    casDetailedKey,
    casDetailedInitial,
    { version: 2, owner: 'writer-3' },
    { includeCurrentOnMismatch: true }
  );
  const casDetailedSuccessKeys = Object.keys(casDetailedSuccess);

  check(casDetailedSuccess, {
    'api:compareAndSwapDetailed-success-structure': () =>
      casDetailedSuccessKeys.includes('swapped') &&
      casDetailedSuccessKeys.includes('reason') &&
      casDetailedSuccessKeys.length === 2,
    'api:compareAndSwapDetailed-success-fields': () =>
      casDetailedSuccess.swapped === true &&
      casDetailedSuccess.reason === 'swapped' &&
      !Object.prototype.hasOwnProperty.call(casDetailedSuccess, 'current') &&
      !Object.prototype.hasOwnProperty.call(casDetailedSuccess, 'existed')
  });

  // oldValue=null in CAS means "only if absent", same as set-if-not-exists.
  const casDetailedNullAbsentSentinel = await kv.compareAndSwapDetailed(
    'casDetailedAbsentKey',
    null,
    { createdBy: 'compareAndSwapDetailed' },
    { includeCurrentOnMismatch: true }
  );

  check(casDetailedNullAbsentSentinel, {
    'api:compareAndSwapDetailed-null-absent-sentinel': () =>
      casDetailedNullAbsentSentinel.swapped === true &&
      casDetailedNullAbsentSentinel.reason === 'swapped' &&
      !Object.prototype.hasOwnProperty.call(casDetailedNullAbsentSentinel, 'current') &&
      !Object.prototype.hasOwnProperty.call(casDetailedNullAbsentSentinel, 'existed')
  });

  // setIfAbsent(): Validate boolean contract and first-writer-wins behavior.
  const setIfAbsentKey = 'setIfAbsentKey';
  const setIfAbsentFirst = await kv.setIfAbsent(setIfAbsentKey, { owner: 'writer-1' });
  const setIfAbsentSecond = await kv.setIfAbsent(setIfAbsentKey, { owner: 'writer-2' });
  const setIfAbsentStored = await kv.get(setIfAbsentKey);

  check(true, {
    'api:setIfAbsent-first-write': () =>
      typeof setIfAbsentFirst === 'boolean' && setIfAbsentFirst === true,
    'api:setIfAbsent-second-write': () =>
      typeof setIfAbsentSecond === 'boolean' && setIfAbsentSecond === false,
    'api:setIfAbsent-value-preserved': () =>
      typeof setIfAbsentStored === 'object' &&
      setIfAbsentStored.owner === 'writer-1'
  });

  // compareAndDeleteDetailed(): Validate detailed payload shape for all branches.
  // Success branch should contain only (deleted, reason). Mismatch branch should
  // contain (deleted, reason, existed) and include current only when requested and
  // the key exists.
  const cadDetailedKey = 'cadDetailedKey';
  const cadDetailedInitial = { state: 'active', version: 1 };
  await kv.set(cadDetailedKey, cadDetailedInitial);

  const cadDetailedMismatchNoCurrent = await kv.compareAndDeleteDetailed(
    cadDetailedKey,
    { state: 'stale', version: 1 },
    { includeCurrentOnMismatch: false }
  );
  const cadDetailedMismatchNoCurrentKeys = Object.keys(cadDetailedMismatchNoCurrent);

  check(cadDetailedMismatchNoCurrent, {
    'api:compareAndDeleteDetailed-mismatch-no-current': () =>
      cadDetailedMismatchNoCurrent.deleted === false &&
      cadDetailedMismatchNoCurrent.reason === 'mismatch' &&
      cadDetailedMismatchNoCurrent.existed === true &&
      cadDetailedMismatchNoCurrentKeys.includes('deleted') &&
      cadDetailedMismatchNoCurrentKeys.includes('reason') &&
      cadDetailedMismatchNoCurrentKeys.includes('existed') &&
      cadDetailedMismatchNoCurrentKeys.length === 3 &&
      !Object.prototype.hasOwnProperty.call(cadDetailedMismatchNoCurrent, 'current')
  });

  const cadDetailedMismatchWithCurrent = await kv.compareAndDeleteDetailed(
    cadDetailedKey,
    { state: 'stale', version: 1 },
    { includeCurrentOnMismatch: true }
  );
  const cadDetailedMismatchWithCurrentKeys = Object.keys(cadDetailedMismatchWithCurrent);

  check(cadDetailedMismatchWithCurrent, {
    'api:compareAndDeleteDetailed-mismatch-with-current': () =>
      cadDetailedMismatchWithCurrent.deleted === false &&
      cadDetailedMismatchWithCurrent.reason === 'mismatch' &&
      cadDetailedMismatchWithCurrent.existed === true &&
      cadDetailedMismatchWithCurrentKeys.includes('deleted') &&
      cadDetailedMismatchWithCurrentKeys.includes('reason') &&
      cadDetailedMismatchWithCurrentKeys.includes('existed') &&
      cadDetailedMismatchWithCurrentKeys.includes('current') &&
      cadDetailedMismatchWithCurrentKeys.length === 4 &&
      typeof cadDetailedMismatchWithCurrent.current === 'object' &&
      cadDetailedMismatchWithCurrent.current.state === 'active' &&
      cadDetailedMismatchWithCurrent.current.version === 1
  });

  const cadDetailedMissingKey = await kv.compareAndDeleteDetailed(
    'cadDetailedMissingKey',
    { expected: true },
    { includeCurrentOnMismatch: true }
  );
  const cadDetailedMissingKeyKeys = Object.keys(cadDetailedMissingKey);

  check(cadDetailedMissingKey, {
    'api:compareAndDeleteDetailed-missing-key': () =>
      cadDetailedMissingKey.deleted === false &&
      cadDetailedMissingKey.reason === 'mismatch' &&
      cadDetailedMissingKey.existed === false &&
      cadDetailedMissingKeyKeys.includes('deleted') &&
      cadDetailedMissingKeyKeys.includes('reason') &&
      cadDetailedMissingKeyKeys.includes('existed') &&
      cadDetailedMissingKeyKeys.length === 3 &&
      !Object.prototype.hasOwnProperty.call(cadDetailedMissingKey, 'current')
  });

  const cadDetailedSuccess = await kv.compareAndDeleteDetailed(
    cadDetailedKey,
    cadDetailedInitial,
    { includeCurrentOnMismatch: true }
  );
  const cadDetailedSuccessKeys = Object.keys(cadDetailedSuccess);

  check(cadDetailedSuccess, {
    'api:compareAndDeleteDetailed-success-structure': () =>
      cadDetailedSuccessKeys.includes('deleted') &&
      cadDetailedSuccessKeys.includes('reason') &&
      cadDetailedSuccessKeys.length === 2,
    'api:compareAndDeleteDetailed-success-fields': () =>
      cadDetailedSuccess.deleted === true &&
      cadDetailedSuccess.reason === 'deleted' &&
      !Object.prototype.hasOwnProperty.call(cadDetailedSuccess, 'current') &&
      !Object.prototype.hasOwnProperty.call(cadDetailedSuccess, 'existed')
  });

  // CAD treats null/undefined as normal value comparisons (not absent sentinel).
  const cadNullKey = 'cadDetailedNullKey';
  await kv.set(cadNullKey, null);
  const cadNullDelete = await kv.compareAndDeleteDetailed(
    cadNullKey,
    null,
    { includeCurrentOnMismatch: true }
  );

  check(cadNullDelete, {
    'api:compareAndDeleteDetailed-null-literal': () =>
      cadNullDelete.deleted === true &&
      cadNullDelete.reason === 'deleted' &&
      !Object.prototype.hasOwnProperty.call(cadNullDelete, 'current') &&
      !Object.prototype.hasOwnProperty.call(cadNullDelete, 'existed')
  });

  const cadUndefinedKey = 'cadDetailedUndefinedKey';
  await kv.set(cadUndefinedKey, null);
  const cadUndefinedDelete = await kv.compareAndDeleteDetailed(
    cadUndefinedKey,
    undefined,
    { includeCurrentOnMismatch: true }
  );

  check(cadUndefinedDelete, {
    'api:compareAndDeleteDetailed-undefined-literal': () =>
      cadUndefinedDelete.deleted === true &&
      cadUndefinedDelete.reason === 'deleted' &&
      !Object.prototype.hasOwnProperty.call(cadUndefinedDelete, 'current') &&
      !Object.prototype.hasOwnProperty.call(cadUndefinedDelete, 'existed')
  });

  await kv.setMany({
    'api:listKeys:user:1': { value: 1 },
    'api:listKeys:user:2': { value: 2 },
    'api:listKeys:order:1': { value: 3 }
  });

  const listKeysResult = await kv.listKeys({
    prefix: 'api:listKeys:user:',
    limit: 10
  });

  check(listKeysResult, {
    'api:listKeys:returns array': (result) => Array.isArray(result),
    'api:listKeys:matching count': (result) => result.length === 2,
    'api:listKeys:lexicographic order': (result) =>
      result[0] === 'api:listKeys:user:1' &&
      result[1] === 'api:listKeys:user:2'
  });

  await kv.setMany({
    'api:deleteByPrefix:tmp:1': { value: 1 },
    'api:deleteByPrefix:tmp:2': { value: 2 },
    'api:deleteByPrefix:tmp:3': { value: 3 },
    'api:deleteByPrefix:user:1': { value: 4 }
  });

  const deleteByPrefixFirst = await kv.deleteByPrefix({
    prefix: 'api:deleteByPrefix:tmp:',
    limit: 2
  });

  check(deleteByPrefixFirst, {
    'api:deleteByPrefix:first returns object': (result) =>
      result !== null && typeof result === 'object',
    'api:deleteByPrefix:first deleted limited count': (result) =>
      result.deleted === 2,
    'api:deleteByPrefix:first reports not done': (result) =>
      result.done === false
  });

  const deleteByPrefixSecond = await kv.deleteByPrefix({
    prefix: 'api:deleteByPrefix:tmp:',
    limit: 2
  });

  check(deleteByPrefixSecond, {
    'api:deleteByPrefix:second deletes remaining': (result) =>
      result.deleted === 1,
    'api:deleteByPrefix:second reports done': (result) =>
      result.done === true
  });

  await kv.setMany({
    'api:exportJSONL:user:1': { value: 1 },
    'api:exportJSONL:user:2': { value: 2 },
    'api:exportJSONL:order:1': { value: 3 }
  });

  const exportJSONLResult = await kv.exportJSONL({
    fileName: EXPORT_JSONL_PATH,
    prefix: 'api:exportJSONL:user:'
  });

  check(exportJSONLResult, {
    'api:exportJSONL:returns object': (result) =>
      result !== null && typeof result === 'object',
    'api:exportJSONL:exported count': (result) =>
      result.exported === 2,
    'api:exportJSONL:wrote bytes': (result) =>
      result.bytesWritten > 0
  });

  await kv.setMany({
    'api:importJSONL:user:1': { value: 1 },
    'api:importJSONL:user:2': { value: 2 }
  });

  await kv.exportJSONL({
    fileName: IMPORT_JSONL_PATH,
    prefix: 'api:importJSONL:'
  });

  await kv.deleteMany([
    'api:importJSONL:user:1',
    'api:importJSONL:user:2'
  ]);

  const importJSONLResult = await kv.importJSONL({
    fileName: IMPORT_JSONL_PATH,
    batchSize: 1
  });

  check(importJSONLResult, {
    'api:importJSONL:returns object': (result) =>
      result !== null && typeof result === 'object',
    'api:importJSONL:imported count': (result) =>
      result.imported === 2,
    'api:importJSONL:read bytes': (result) =>
      result.bytesRead > 0
  });

  const importedItems = await kv.getMany([
    'api:importJSONL:user:1',
    'api:importJSONL:user:2'
  ]);

  check(importedItems, {
    'api:importJSONL:first item exists': (items) =>
      items[0].exists === true && items[0].value.value === 1,
    'api:importJSONL:second item exists': (items) =>
      items[1].exists === true && items[1].value.value === 2
  });

  const importCSVResult = await kv.importCSV({
    fileName: IMPORT_CSV_PATH,
    keyColumn: 'id',
    hasHeader: true,
    batchSize: 1
  });
  const importCSVItems = await kv.getMany(['user:1', 'user:2']);

  await kv.deleteMany(['user:1', 'user:2']);
  const importCSVLimitedResult = await kv.importCSV({
    fileName: IMPORT_CSV_PATH,
    keyColumn: 'id',
    hasHeader: true,
    limit: 1,
    batchSize: 1000
  });
  const importCSVLimitedItems = await kv.getMany(['user:1', 'user:2']);

  const importCSVMissingFileError = await expectErrorName(
    () =>
      kv.importCSV({
        fileName: MISSING_CSV_PATH,
        keyColumn: 'id',
        hasHeader: true
      }),
    'SnapshotNotFoundError'
  );

  check(true, {
    'api:importCSV-contract': () =>
      importCSVResult !== null &&
      typeof importCSVResult === 'object' &&
      typeof importCSVResult.imported === 'number' &&
      importCSVResult.imported === 2 &&
      typeof importCSVResult.fileName === 'string' &&
      (importCSVResult.fileName === IMPORT_CSV_PATH || importCSVResult.fileName === 'examples/fixtures/users.csv') &&
      typeof importCSVResult.bytesRead === 'number' &&
      importCSVResult.bytesRead > 0 &&
      importCSVItems[0]?.exists === true &&
      importCSVItems[1]?.exists === true &&
      importCSVItems[0]?.value?.name === 'Alice' &&
      importCSVItems[1]?.value?.name === 'Bob' &&
      typeof importCSVItems[0]?.value?.email === 'string' &&
      importCSVLimitedResult.imported === 1 &&
      importCSVLimitedItems[0]?.exists === true &&
      importCSVLimitedItems[1]?.exists === false &&
      importCSVMissingFileError
  });

  // exportCSV(): validate summary shape, prefix/limit behavior, and malformed-value rejections.
  await kv.setMany({
    'api:exportCSV:response:1': { status: 200, requestId: 'r1', userId: 'u1', bodyHash: 'a' },
    'api:exportCSV:response:2': { status: 201, requestId: 'r2', userId: 'u2', bodyHash: 'b' },
    'api:exportCSV:other:1': { status: 500, requestId: 'r3', userId: 'u3', bodyHash: 'c' }
  });

  const exportCSVResult = await kv.exportCSV({
    fileName: EXPORT_CSV_PATH,
    prefix: 'api:exportCSV:response:',
    columns: ['status', 'requestId', 'userId', 'bodyHash'],
    includeKey: true
  });

  const exportCSVLimitedResult = await kv.exportCSV({
    fileName: EXPORT_CSV_LIMIT_PATH,
    prefix: 'api:exportCSV:response:',
    columns: ['status', 'requestId', 'userId', 'bodyHash'],
    includeKey: false,
    delimiter: ';',
    limit: 1
  });

  const exportCSVMissingColumnResult = await kv.exportCSV({
    fileName: EXPORT_CSV_MISSING_COLUMN_PATH,
    prefix: 'api:exportCSV:response:',
    columns: ['status', 'missingColumn']
  });

  await kv.set('api:exportCSV:bad:scalar', 'raw-string');
  const exportCSVNonObjectError = await expectErrorName(
    () =>
      kv.exportCSV({
        fileName: EXPORT_CSV_PATH,
        prefix: 'api:exportCSV:bad:',
        columns: ['status']
      }),
    'ValueParseError'
  );

  await kv.set('api:exportCSV:bad:nested', {
    status: 200,
    nested: { detail: 'not-flat' }
  });
  const exportCSVNestedError = await expectErrorName(
    () =>
      kv.exportCSV({
        fileName: EXPORT_CSV_PATH,
        prefix: 'api:exportCSV:bad:nested',
        columns: ['status', 'nested']
      }),
    'ValueParseError'
  );

  check(
    { exportCSVResult, exportCSVLimitedResult, exportCSVMissingColumnResult },
    {
      'api:exportCSV-structure': (ctx) =>
        ctx.exportCSVResult !== null &&
        typeof ctx.exportCSVResult === 'object' &&
        typeof ctx.exportCSVResult.exported === 'number' &&
        typeof ctx.exportCSVResult.fileName === 'string' &&
        typeof ctx.exportCSVResult.bytesWritten === 'number',
      'api:exportCSV-prefix-count': (ctx) =>
        ctx.exportCSVResult.exported === 2 &&
        ctx.exportCSVResult.fileName === EXPORT_CSV_PATH &&
        ctx.exportCSVResult.bytesWritten > 0,
      'api:exportCSV-limit': (ctx) =>
        ctx.exportCSVLimitedResult.exported === 1 &&
        ctx.exportCSVLimitedResult.fileName === EXPORT_CSV_LIMIT_PATH &&
        ctx.exportCSVLimitedResult.bytesWritten > 0,
      'api:exportCSV-missing-column-allowed': (ctx) =>
        ctx.exportCSVMissingColumnResult.exported === 2 &&
        ctx.exportCSVMissingColumnResult.bytesWritten > 0,
      'api:exportCSV-non-object-rejects': () => exportCSVNonObjectError,
      'api:exportCSV-nested-rejects': () => exportCSVNestedError
    }
  );

  // validateCSV()/validateJSONL(): validate preflight result contracts and malformed-content behavior.
  const validateCSVValid = await kv.validateCSV({
    fileName: IMPORT_CSV_PATH,
    keyColumn: 'id',
    hasHeader: true
  });
  const validateCSVWithoutKeyColumn = await kv.validateCSV({
    fileName: IMPORT_CSV_PATH,
    hasHeader: true
  });
  const validateCSVLimit = await kv.validateCSV({
    fileName: IMPORT_CSV_PATH,
    keyColumn: 'id',
    hasHeader: true,
    limit: 1
  });
  const validateCSVDuplicateHeader = await kv.validateCSV({
    fileName: INVALID_CSV_DUPLICATE_HEADER_PATH,
    keyColumn: 'id',
    hasHeader: true
  });
  const validateCSVEmptyKey = await kv.validateCSV({
    fileName: INVALID_CSV_EMPTY_KEY_PATH,
    keyColumn: 'id',
    hasHeader: true
  });
  const validateCSVSyntax = await kv.validateCSV({
    fileName: INVALID_CSV_SYNTAX_PATH,
    keyColumn: 'id',
    hasHeader: true
  });

  const validateJSONLValid = await kv.validateJSONL({
    fileName: IMPORT_JSONL_PATH
  });
  const validateJSONLLimit = await kv.validateJSONL({
    fileName: IMPORT_JSONL_PATH,
    limit: 1
  });
  const validateJSONLInvalidJSON = await kv.validateJSONL({
    fileName: INVALID_JSONL_MALFORMED_PATH
  });
  const validateJSONLBlankLine = await kv.validateJSONL({
    fileName: INVALID_JSONL_BLANK_LINE_PATH
  });
  const validateJSONLMissingKey = await kv.validateJSONL({
    fileName: INVALID_JSONL_MISSING_KEY_PATH
  });
  const validateCSVValidFirstError = getOptionalFirstError(validateCSVValid);
  const validateCSVWithoutKeyColumnFirstError = getOptionalFirstError(validateCSVWithoutKeyColumn);
  const validateCSVLimitFirstError = getOptionalFirstError(validateCSVLimit);
  const validateCSVDuplicateHeaderFirstError = getOptionalFirstError(validateCSVDuplicateHeader);
  const validateCSVEmptyKeyFirstError = getOptionalFirstError(validateCSVEmptyKey);
  const validateCSVSyntaxFirstError = getOptionalFirstError(validateCSVSyntax);
  const validateJSONLValidFirstError = getOptionalFirstError(validateJSONLValid);
  const validateJSONLLimitFirstError = getOptionalFirstError(validateJSONLLimit);
  const validateJSONLInvalidJSONFirstError = getOptionalFirstError(validateJSONLInvalidJSON);
  const validateJSONLBlankLineFirstError = getOptionalFirstError(validateJSONLBlankLine);
  const validateJSONLMissingKeyFirstError = getOptionalFirstError(validateJSONLMissingKey);

  check(
    {
      validateCSVValid,
      validateCSVWithoutKeyColumn,
      validateCSVLimit,
      validateCSVDuplicateHeader,
      validateCSVEmptyKey,
      validateCSVSyntax,
      validateCSVValidFirstError,
      validateCSVWithoutKeyColumnFirstError,
      validateCSVLimitFirstError,
      validateCSVDuplicateHeaderFirstError,
      validateCSVEmptyKeyFirstError,
      validateCSVSyntaxFirstError,
      validateJSONLValid,
      validateJSONLLimit,
      validateJSONLInvalidJSON,
      validateJSONLBlankLine,
      validateJSONLMissingKey,
      validateJSONLValidFirstError,
      validateJSONLLimitFirstError,
      validateJSONLInvalidJSONFirstError,
      validateJSONLBlankLineFirstError,
      validateJSONLMissingKeyFirstError
    },
    {
      'api:validateCSV-valid': (ctx) =>
        ctx.validateCSVValid.valid === true &&
        ctx.validateCSVValid.rows === 2 &&
        ctx.validateCSVValid.bytesRead > 0 &&
        ctx.validateCSVValid.checkedAll === true &&
        ctx.validateCSVValidFirstError === null,
      'api:validateCSV-without-keyColumn': (ctx) =>
        ctx.validateCSVWithoutKeyColumn.valid === true &&
        ctx.validateCSVWithoutKeyColumn.rows === 2 &&
        ctx.validateCSVWithoutKeyColumn.bytesRead > 0 &&
        ctx.validateCSVWithoutKeyColumn.checkedAll === true &&
        ctx.validateCSVWithoutKeyColumnFirstError === null,
      'api:validateCSV-limit': (ctx) =>
        ctx.validateCSVLimit.valid === true &&
        ctx.validateCSVLimit.rows === 1 &&
        ctx.validateCSVLimit.bytesRead > 0 &&
        ctx.validateCSVLimit.checkedAll === false &&
        ctx.validateCSVLimitFirstError === null,
      'api:validateCSV-duplicate-header-invalid': (ctx) =>
        ctx.validateCSVDuplicateHeader.valid === false &&
        typeof ctx.validateCSVDuplicateHeaderFirstError === 'object' &&
        ctx.validateCSVDuplicateHeaderFirstError !== null &&
        ctx.validateCSVDuplicateHeaderFirstError.name === 'ValueParseError',
      'api:validateCSV-empty-key-invalid': (ctx) =>
        ctx.validateCSVEmptyKey.valid === false &&
        typeof ctx.validateCSVEmptyKeyFirstError === 'object' &&
        ctx.validateCSVEmptyKeyFirstError !== null &&
        ctx.validateCSVEmptyKeyFirstError.name === 'ValueParseError',
      'api:validateCSV-syntax-invalid': (ctx) =>
        ctx.validateCSVSyntax.valid === false &&
        typeof ctx.validateCSVSyntaxFirstError === 'object' &&
        ctx.validateCSVSyntaxFirstError !== null &&
        ctx.validateCSVSyntaxFirstError.name === 'ValueParseError',
      'api:validateJSONL-valid': (ctx) =>
        ctx.validateJSONLValid.valid === true &&
        ctx.validateJSONLValid.records === 2 &&
        ctx.validateJSONLValid.bytesRead > 0 &&
        ctx.validateJSONLValid.checkedAll === true &&
        ctx.validateJSONLValidFirstError === null,
      'api:validateJSONL-limit': (ctx) =>
        ctx.validateJSONLLimit.valid === true &&
        ctx.validateJSONLLimit.records === 1 &&
        ctx.validateJSONLLimit.bytesRead > 0 &&
        ctx.validateJSONLLimit.checkedAll === false &&
        ctx.validateJSONLLimitFirstError === null,
      'api:validateJSONL-invalid-json': (ctx) =>
        ctx.validateJSONLInvalidJSON.valid === false &&
        typeof ctx.validateJSONLInvalidJSONFirstError === 'object' &&
        ctx.validateJSONLInvalidJSONFirstError !== null &&
        ctx.validateJSONLInvalidJSONFirstError.name === 'ValueParseError',
      'api:validateJSONL-blank-line': (ctx) =>
        ctx.validateJSONLBlankLine.valid === false &&
        typeof ctx.validateJSONLBlankLineFirstError === 'object' &&
        ctx.validateJSONLBlankLineFirstError !== null &&
        ctx.validateJSONLBlankLineFirstError.name === 'ValueParseError',
      'api:validateJSONL-missing-key': (ctx) =>
        ctx.validateJSONLMissingKey.valid === false &&
        typeof ctx.validateJSONLMissingKeyFirstError === 'object' &&
        ctx.validateJSONLMissingKeyFirstError !== null &&
        ctx.validateJSONLMissingKeyFirstError.name === 'ValueParseError'
    }
  );

  // list(): Validate that results are returned as an array of entry objects.
  // Each entry must have camelCase field names (key, value) and preserve all value types.
  const entries = await kv.list({});

  check(entries, {
    'api:list-structure': () => Array.isArray(entries)
  });

  if (entries.length > 0) {
    // Verify entry structure: each entry must have exactly two fields (key, value) in camelCase.
    const entryKeys = Object.keys(entries[0]);
    check(entries[0], {
      'api:list-entry-structure': () =>
        entryKeys.includes('key') && entryKeys.includes('value') && entryKeys.length === 2
    });

    // Verify null values are preserved correctly through marshalling.
    const nullEntry = entries.find((e) => e.key === 'nullKey');
    check(nullEntry, {
      'api:list-null-values': () => nullEntry && nullEntry.value === null
    });

    // Verify negative numbers are preserved as JavaScript numbers.
    const negativeEntry = entries.find((e) => e.key === 'negativeKey');
    check(negativeEntry, {
      'api:list-negative-numbers': () =>
        negativeEntry && typeof negativeEntry.value === 'number' && negativeEntry.value === -456
    });

    // Verify zero is preserved correctly (edge case for number marshalling).
    const zeroEntry = entries.find((e) => e.key === 'zeroKey');
    check(zeroEntry, {
      'api:list-zero-value': () =>
        zeroEntry && typeof zeroEntry.value === 'number' && zeroEntry.value === 0
    });

    // Verify arrays stored as values are correctly marshalled and preserved.
    const arrayEntry = entries.find((e) => e.key === 'arrayKey');
    check(arrayEntry, {
      'api:list-array-values': () =>
        arrayEntry && Array.isArray(arrayEntry.value) && arrayEntry.value.length === 4
    });

    // Verify deeply nested objects maintain their structure through marshalling.
    const deepNestedEntry = entries.find((e) => e.key === 'deepNestedKey');
    check(deepNestedEntry, {
      'api:list-deeply-nested': () =>
        deepNestedEntry &&
        typeof deepNestedEntry.value === 'object' &&
        deepNestedEntry.value.level1?.level2?.level3 === 'deep'
    });

    // Verify Unicode strings (emojis, Chinese, Russian Cyrillic) are preserved correctly.
    const unicodeEntry = entries.find((e) => e.key === 'unicodeKey');
    check(unicodeEntry, {
      'api:list-unicode-values': () =>
        unicodeEntry &&
        typeof unicodeEntry.value === 'string' &&
        unicodeEntry.value.includes('🚀') &&
        unicodeEntry.value.includes('世界') &&
        unicodeEntry.value.includes('енот') &&
        unicodeEntry.value.includes('котлеты')
    });

    // Verify special characters in strings are preserved correctly.
    const specialCharsEntry = entries.find((e) => e.key === 'specialCharsKey');
    check(specialCharsEntry, {
      'api:list-special-chars': () =>
        specialCharsEntry && typeof specialCharsEntry.value === 'string' && specialCharsEntry.value.includes('!@#')
    });

    // Verify boolean values are preserved correctly.
    const boolTrueEntry = entries.find((e) => e.key === 'boolTrueKey');
    check(boolTrueEntry, {
      'api:list-boolean-true': () =>
        boolTrueEntry && typeof boolTrueEntry.value === 'boolean' && boolTrueEntry.value === true
    });

    const boolFalseEntry = entries.find((e) => e.key === 'boolFalseKey');
    check(boolFalseEntry, {
      'api:list-boolean-false': () =>
        boolFalseEntry && typeof boolFalseEntry.value === 'boolean' && boolFalseEntry.value === false
    });

    // Verify floating point numbers are preserved correctly.
    const floatEntry = entries.find((e) => e.key === 'floatKey');
    check(floatEntry, {
      'api:list-float-value': () =>
        floatEntry && typeof floatEntry.value === 'number' && floatEntry.value === 3.14159
    });

    // Verify empty arrays are preserved correctly.
    const emptyArrayEntry = entries.find((e) => e.key === 'emptyArrayKey');
    check(emptyArrayEntry, {
      'api:list-empty-array': () =>
        emptyArrayEntry && Array.isArray(emptyArrayEntry.value) && emptyArrayEntry.value.length === 0
    });

    // Verify empty objects are preserved correctly.
    const emptyObjectEntry = entries.find((e) => e.key === 'emptyObjectKey');
    check(emptyObjectEntry, {
      'api:list-empty-object': () =>
        emptyObjectEntry &&
        typeof emptyObjectEntry.value === 'object' &&
        emptyObjectEntry.value !== null &&
        Object.keys(emptyObjectEntry.value).length === 0
    });
  }

  // randomKey(): Validate that empty results are returned as empty strings (not null or undefined).
  // When no keys match the prefix, the function should return an empty string.
  const randomKey = await kv.randomKey({ prefix: 'user:' });
  check(randomKey, {
    'api:randomKey-empty': () =>
      typeof randomKey === 'string' && (randomKey === '' || randomKey.startsWith('user:'))
  });

  // randomKeys(): Validate key-array shape and empty result behavior.
  const randomKeysResult = await kv.randomKeys({
    prefix: 'api:listKeys:user:',
    count: 2
  });
  check(randomKeysResult, {
    'api:randomKeys-is-array': (result) => Array.isArray(result),
    'api:randomKeys-key-types': (result) => result.every((key) => typeof key === 'string')
  });

  const emptyRandomKeysResult = await kv.randomKeys({
    prefix: 'missing:randomKeys:',
    count: 2
  });
  check(emptyRandomKeysResult, {
    'api:randomKeys-empty-is-array': (result) =>
      Array.isArray(result) && result.length === 0
  });

  // scan(): Validate pagination structure and type preservation.
  // Results must have camelCase field names (entries, cursor, done) with correct types.
  const scanResult = await kv.scan({ limit: 5 });
  const scanKeys = Object.keys(scanResult);

  check(scanResult, {
    'api:scan-structure': () =>
      scanKeys.includes('entries') &&
      scanKeys.includes('cursor') &&
      scanKeys.includes('done') &&
      scanKeys.length === 3
  });

  // Verify field types: entries must be an array, cursor a string, done a boolean.
  check(scanResult, {
    'api:scan-types': () =>
      Array.isArray(scanResult.entries) &&
      typeof scanResult.cursor === 'string' &&
      typeof scanResult.done === 'boolean'
  });

  if (scanResult.entries.length > 0) {
    // Verify entry structure matches list() format (key, value fields in camelCase).
    const scanEntryKeys = Object.keys(scanResult.entries[0]);
    check(scanResult.entries[0], {
      'api:scan-entry-structure': () =>
        scanEntryKeys.includes('key') && scanEntryKeys.includes('value') && scanEntryKeys.length === 2
    });

    // Scan through all pages to find test keys (may not be in first page due to limit).
    let nullEntryInScan = scanResult.entries.find((e) => e.key === 'nullKey');
    let arrayEntryInScan = scanResult.entries.find((e) => e.key === 'arrayKey');
    let emptyArrayEntryInScan = scanResult.entries.find((e) => e.key === 'emptyArrayKey');
    let emptyObjectEntryInScan = scanResult.entries.find((e) => e.key === 'emptyObjectKey');
    let boolTrueEntryInScan = scanResult.entries.find((e) => e.key === 'boolTrueKey');
    let floatEntryInScan = scanResult.entries.find((e) => e.key === 'floatKey');
    let currentCursor = scanResult.cursor;
    let isDone = scanResult.done;

    // Continue scanning if test keys not found and more pages exist.
    while (
      (!nullEntryInScan || !arrayEntryInScan || !emptyArrayEntryInScan || !emptyObjectEntryInScan ||
        !boolTrueEntryInScan || !floatEntryInScan) &&
      !isDone &&
      currentCursor
    ) {
      const nextPage = await kv.scan({ cursor: currentCursor, limit: 5 });
      nullEntryInScan = nullEntryInScan || nextPage.entries.find((e) => e.key === 'nullKey');
      arrayEntryInScan = arrayEntryInScan || nextPage.entries.find((e) => e.key === 'arrayKey');
      emptyArrayEntryInScan = emptyArrayEntryInScan || nextPage.entries.find((e) => e.key === 'emptyArrayKey');
      emptyObjectEntryInScan = emptyObjectEntryInScan || nextPage.entries.find((e) => e.key === 'emptyObjectKey');
      boolTrueEntryInScan = boolTrueEntryInScan || nextPage.entries.find((e) => e.key === 'boolTrueKey');
      floatEntryInScan = floatEntryInScan || nextPage.entries.find((e) => e.key === 'floatKey');
      currentCursor = nextPage.cursor;
      isDone = nextPage.done;
    }

    // Verify null values are preserved in scan results.
    check(nullEntryInScan, {
      'api:scan-null-values': () => nullEntryInScan && nullEntryInScan.value === null
    });

    // Verify arrays stored as values are correctly marshalled in scan results.
    check(arrayEntryInScan, {
      'api:scan-array-values': () =>
        arrayEntryInScan && Array.isArray(arrayEntryInScan.value) && arrayEntryInScan.value.length === 4
    });

    // Verify empty arrays are preserved in scan results.
    check(emptyArrayEntryInScan, {
      'api:scan-empty-array': () =>
        emptyArrayEntryInScan && Array.isArray(emptyArrayEntryInScan.value) && emptyArrayEntryInScan.value.length === 0
    });

    // Verify empty objects are preserved in scan results.
    check(emptyObjectEntryInScan, {
      'api:scan-empty-object': () =>
        emptyObjectEntryInScan &&
        typeof emptyObjectEntryInScan.value === 'object' &&
        emptyObjectEntryInScan.value !== null &&
        Object.keys(emptyObjectEntryInScan.value).length === 0
    });

    // Verify boolean values are preserved in scan results.
    check(boolTrueEntryInScan, {
      'api:scan-boolean-true': () =>
        boolTrueEntryInScan && typeof boolTrueEntryInScan.value === 'boolean' && boolTrueEntryInScan.value === true
    });

    // Verify floating point numbers are preserved in scan results.
    check(floatEntryInScan, {
      'api:scan-float-value': () =>
        floatEntryInScan && typeof floatEntryInScan.value === 'number' && floatEntryInScan.value === 3.14159
    });
  }

  // Verify pagination: cursor continuation should work correctly when more entries exist.
  if (!scanResult.done && scanResult.cursor) {
    const nextPageResult = await kv.scan({ cursor: scanResult.cursor });
    check(nextPageResult, {
      'api:scan-pagination': () =>
        nextPageResult.entries.length > 0 && (nextPageResult.done === true || nextPageResult.cursor !== '')
    });
  }

  // scanKeys(): Validate key-only pagination shape and field types.
  const scanKeysResult = await kv.scanKeys({ prefix: 'api:listKeys:user:', limit: 1 });
  const scanKeysResultFields = Object.keys(scanKeysResult);

  check(scanKeysResult, {
    'api:scanKeys-structure': () =>
      scanKeysResultFields.includes('keys') &&
      scanKeysResultFields.includes('cursor') &&
      scanKeysResultFields.includes('done') &&
      scanKeysResultFields.length === 3,
    'api:scanKeys-types': () =>
      Array.isArray(scanKeysResult.keys) &&
      typeof scanKeysResult.cursor === 'string' &&
      typeof scanKeysResult.done === 'boolean',
    'api:scanKeys-key-types': () =>
      scanKeysResult.keys.every((key) => typeof key === 'string')
  });

  if (!scanKeysResult.done && scanKeysResult.cursor) {
    const nextScanKeysPage = await kv.scanKeys({
      prefix: 'api:listKeys:user:',
      cursor: scanKeysResult.cursor,
      limit: 1
    });

    check(nextScanKeysPage, {
      'api:scanKeys-pagination': () =>
        Array.isArray(nextScanKeysPage.keys) &&
        typeof nextScanKeysPage.cursor === 'string' &&
        typeof nextScanKeysPage.done === 'boolean'
    });
  }

  // scan()/scanKeys(): Invalid cursor payloads must reject with InvalidCursorError.
  const garbageCursor = '%%%not-base64-cursor%%%';
  let scanInvalidCursorError = null;
  try {
    await kv.scan({
      prefix: 'api:listKeys:user:',
      cursor: garbageCursor,
      limit: 1
    });
  } catch (err) {
    scanInvalidCursorError = err;
  }

  let scanKeysInvalidCursorError = null;
  try {
    await kv.scanKeys({
      prefix: 'api:listKeys:user:',
      cursor: garbageCursor,
      limit: 1
    });
  } catch (err) {
    scanKeysInvalidCursorError = err;
  }

  check(true, {
    'api:scan-invalid-cursor-rejects': () =>
      scanInvalidCursorError !== null && scanInvalidCursorError.name === 'InvalidCursorError',
    'api:scanKeys-invalid-cursor-rejects': () =>
      scanKeysInvalidCursorError !== null && scanKeysInvalidCursorError.name === 'InvalidCursorError'
  });

  // backup(): Validate snapshot operation structure and int64 marshalling.
  // Verifies that backup results have correct camelCase field names and that int64
  // values (bytesWritten) are correctly converted to JavaScript numbers.
  const backupResult = await kv.backup({
    fileName: SNAPSHOT_PATH,
    allowConcurrentWrites: false
  });
  const backupKeys = Object.keys(backupResult);

  check(backupResult, {
    'api:backup-structure': () =>
      backupKeys.includes('totalEntries') &&
      backupKeys.includes('bytesWritten') &&
      backupKeys.includes('bestEffort') &&
      backupKeys.includes('warning') &&
      backupKeys.length === 4,
    'api:backup-fields': () =>
      typeof backupResult.totalEntries === 'number' &&
      typeof backupResult.bytesWritten === 'number' &&
      typeof backupResult.bestEffort === 'boolean' &&
      typeof backupResult.warning === 'string',
    'api:backup-int64-marshalling': () =>
      // Verify int64 (bytesWritten) is marshalled correctly as a number.
      // JavaScript numbers are IEEE 754 doubles, so values > 2^53 may lose precision,
      // but we verify the type is correct and value is reasonable.
      Number.isInteger(backupResult.bytesWritten) || backupResult.bytesWritten % 1 === 0
  });

  // Verify backup content matches expected entry count.
  check(backupResult, {
    'api:backup-content': () =>
      backupResult.totalEntries === entries.length &&
      backupResult.totalEntries >= 0 &&
      backupResult.bytesWritten >= 0
  });

  // Clear store to test restore functionality.
  await kv.clear();
  const sizeAfterClear = await kv.size();
  check(sizeAfterClear, {
    'api:clear': () => typeof sizeAfterClear === 'number' && sizeAfterClear === 0
  });

  // restore(): Validate restore operation structure and content.
  // Verifies that restore results have correct camelCase field names (totalEntries).
  const restoreResult = await kv.restore({ fileName: SNAPSHOT_PATH });
  const restoreKeys = Object.keys(restoreResult);

  check(restoreResult, {
    'api:restore-structure': () =>
      restoreKeys.includes('totalEntries') && restoreKeys.length === 1,
    'api:restore-fields': () => typeof restoreResult.totalEntries === 'number',
    'api:restore-content': () => restoreResult.totalEntries === entries.length
  });

  // Verify store size matches restored entry count.
  const sizeAfterRestore = await kv.size();
  check(sizeAfterRestore, {
    'api:restore-verification': () =>
      typeof sizeAfterRestore === 'number' && sizeAfterRestore === entries.length
  });

  // Verify restored values maintain correct types and structure after backup/restore cycle.
  // This ensures that complex types (arrays, negative numbers, Unicode, nested objects)
  // survive serialization, backup, and restore operations.
  const restoredArray = await kv.get('arrayKey');
  check(restoredArray, {
    'api:restore-array-preservation': () =>
      Array.isArray(restoredArray) && restoredArray.length === 4 && restoredArray[0] === 1
  });

  const restoredNegative = await kv.get('negativeKey');
  check(restoredNegative, {
    'api:restore-negative-number-preservation': () =>
      typeof restoredNegative === 'number' && restoredNegative === -456
  });

  const restoredUnicode = await kv.get('unicodeKey');
  check(restoredUnicode, {
    'api:restore-unicode-preservation': () =>
      typeof restoredUnicode === 'string' &&
      restoredUnicode.includes('🚀') &&
      restoredUnicode.includes('世界') &&
      restoredUnicode.includes('енот') &&
      restoredUnicode.includes('котлеты')
  });

  const restoredDeepNested = await kv.get('deepNestedKey');
  check(restoredDeepNested, {
    'api:restore-deeply-nested-preservation': () =>
      typeof restoredDeepNested === 'object' &&
      restoredDeepNested.level1?.level2?.level3 === 'deep'
  });

  // Verify boolean values survive backup/restore.
  const restoredBoolTrue = await kv.get('boolTrueKey');
  check(restoredBoolTrue, {
    'api:restore-boolean-true-preservation': () =>
      typeof restoredBoolTrue === 'boolean' && restoredBoolTrue === true
  });

  const restoredBoolFalse = await kv.get('boolFalseKey');
  check(restoredBoolFalse, {
    'api:restore-boolean-false-preservation': () =>
      typeof restoredBoolFalse === 'boolean' && restoredBoolFalse === false
  });

  // Verify floating point numbers survive backup/restore.
  const restoredFloat = await kv.get('floatKey');
  check(restoredFloat, {
    'api:restore-float-preservation': () =>
      typeof restoredFloat === 'number' && restoredFloat === 3.14159
  });

  // Verify empty arrays survive backup/restore.
  const restoredEmptyArray = await kv.get('emptyArrayKey');
  check(restoredEmptyArray, {
    'api:restore-empty-array-preservation': () =>
      Array.isArray(restoredEmptyArray) && restoredEmptyArray.length === 0
  });

  // Verify empty objects survive backup/restore.
  const restoredEmptyObject = await kv.get('emptyObjectKey');
  check(restoredEmptyObject, {
    'api:restore-empty-object-preservation': () =>
      typeof restoredEmptyObject === 'object' &&
      restoredEmptyObject !== null &&
      Object.keys(restoredEmptyObject).length === 0
  });

  const getMissingKeyError = await expectErrorName(
    () => kv.get('missing:key:error'),
    'KeyNotFoundError'
  );

  const validationGetEmptyKey = await expectErrorName(
    () => kv.get(''),
    'InvalidOptionsError'
  );
  const validationGetManyNonArray = await expectErrorName(
    () => kv.getMany('bad'),
    'InvalidOptionsError'
  );
  const validationGetManyNonStringItem = await expectErrorName(
    () => kv.getMany(['ok', 42]),
    'InvalidOptionsError'
  );
  const validationSetEmptyKey = await expectErrorName(
    () => kv.set('', 'x'),
    'InvalidOptionsError'
  );
  const validationSetManyNonObject = await expectErrorName(
    () => kv.setMany('bad'),
    'InvalidOptionsError'
  );
  const validationDeleteManyNonArray = await expectErrorName(
    () => kv.deleteMany('bad'),
    'InvalidOptionsError'
  );
  const validationDeleteManyEmptyKey = await expectErrorName(
    () => kv.deleteMany(['ok', '']),
    'InvalidOptionsError'
  );
  const validationDeleteByPrefixEmptyPrefix = await expectErrorName(
    () => kv.deleteByPrefix({ prefix: '', limit: 1 }),
    'InvalidOptionsError'
  );
  const validationDeleteByPrefixNonPositiveLimit = await expectErrorName(
    () => kv.deleteByPrefix({ prefix: 'x', limit: 0 }),
    'InvalidOptionsError'
  );
  const validationDeleteEmptyKey = await expectErrorName(
    () => kv.delete(''),
    'InvalidOptionsError'
  );
  const validationExistsEmptyKey = await expectErrorName(
    () => kv.exists(''),
    'InvalidOptionsError'
  );
  const validationIncrementByNonNumber = await expectErrorName(
    () => kv.incrementBy('counter:validated', 1.5),
    'ValueNumberRequiredError'
  );
  const validationGetOrSetEmptyKey = await expectErrorName(
    () => kv.getOrSet('', 1),
    'InvalidOptionsError'
  );
  const validationSwapEmptyKey = await expectErrorName(
    () => kv.swap('', 1),
    'InvalidOptionsError'
  );
  const validationCompareAndSwapEmptyKey = await expectErrorName(
    () => kv.compareAndSwap('', 1, 2),
    'InvalidOptionsError'
  );
  const validationCompareAndSwapDetailedBadOptions = await expectErrorName(
    () => kv.compareAndSwapDetailed('foo', 1, 2, 'bad'),
    'InvalidOptionsError'
  );
  const validationSetIfAbsentEmptyKey = await expectErrorName(
    () => kv.setIfAbsent('', 1),
    'InvalidOptionsError'
  );
  const validationDeleteIfExistsEmptyKey = await expectErrorName(
    () => kv.deleteIfExists(''),
    'InvalidOptionsError'
  );
  const validationCompareAndDeleteEmptyKey = await expectErrorName(
    () => kv.compareAndDelete('', 1),
    'InvalidOptionsError'
  );
  const validationCompareAndDeleteDetailedBadOptions = await expectErrorName(
    () => kv.compareAndDeleteDetailed('foo', 1, 'bad'),
    'InvalidOptionsError'
  );
  const validationScanBadOptions = await expectErrorName(
    () => kv.scan('bad'),
    'InvalidOptionsError'
  );
  const validationScanKeysBadOptions = await expectErrorName(
    () => kv.scanKeys('bad'),
    'InvalidOptionsError'
  );
  const validationListBadOptions = await expectErrorName(
    () => kv.list('bad'),
    'InvalidOptionsError'
  );
  const validationListKeysBadOptions = await expectErrorName(
    () => kv.listKeys('bad'),
    'InvalidOptionsError'
  );
  const validationCountBadOptions = await expectErrorName(
    () => kv.count('bad'),
    'InvalidOptionsError'
  );
  const validationRandomKeyBadOptions = await expectErrorName(
    () => kv.randomKey('bad'),
    'InvalidOptionsError'
  );
  const validationRandomKeysMissingCount = await expectErrorName(
    () => kv.randomKeys({ prefix: 'x' }),
    'InvalidOptionsError'
  );
  const validationRandomKeysBadCount = await expectErrorName(
    () => kv.randomKeys({ count: 0 }),
    'InvalidOptionsError'
  );
  const validationPopRandomBadOptions = await expectErrorName(
    () => kv.popRandom('bad'),
    'InvalidOptionsError'
  );
  const validationPopRandomManyBadOptions = await expectErrorName(
    () => kv.popRandomMany({ count: 0 }),
    'InvalidOptionsError'
  );
  const validationClaimRandomBadTTL = await expectErrorName(
    () => kv.claimRandom({ ttl: 0 }),
    'InvalidOptionsError'
  );
  const validationClaimKeyEmptyKey = await expectErrorName(
    () => kv.claimKey('', { ttl: 30000 }),
    'InvalidOptionsError'
  );
  const validationClaimRandomManyMissingCount = await expectErrorName(
    () => kv.claimRandomMany({ ttl: 30000 }),
    'InvalidOptionsError'
  );
  const validationReleaseClaimBadClaim = await expectErrorName(
    () => kv.releaseClaim('bad'),
    'InvalidOptionsError'
  );
  const validationCompleteClaimBadClaim = await expectErrorName(
    () => kv.completeClaim('bad'),
    'InvalidOptionsError'
  );
  const validationCompleteClaimBadOptions = await expectErrorName(
    () => kv.completeClaim({ id: 'x', key: 'k', token: 1 }, 'bad'),
    'InvalidOptionsError'
  );
  const validationRenewClaimBadOptions = await expectErrorName(
    () => kv.renewClaim({ id: 'x', key: 'k', token: 1 }, { ttl: 0 }),
    'InvalidOptionsError'
  );
  const validationBackupBadOptions = await expectErrorName(
    () => kv.backup('bad'),
    'InvalidOptionsError'
  );
  const validationRestoreBadOptions = await expectErrorName(
    () => kv.restore('bad'),
    'InvalidOptionsError'
  );
  const validationExportJSONLMissingFile = await expectErrorName(
    () => kv.exportJSONL({ prefix: 'x' }),
    'InvalidOptionsError'
  );
  const validationImportJSONLMissingFile = await expectErrorName(
    () => kv.importJSONL({}),
    'InvalidOptionsError'
  );
  const validationImportCSVMissingKeyColumn = await expectErrorName(
    () => kv.importCSV({ fileName: IMPORT_CSV_PATH }),
    'InvalidOptionsError'
  );
  const validationImportCSVBadDelimiter = await expectErrorName(
    () =>
      kv.importCSV({
        fileName: IMPORT_CSV_PATH,
        keyColumn: 'id',
        delimiter: ';;'
      }),
    'InvalidOptionsError'
  );
  const validationClaimKeysNonArray = await expectErrorName(
    () => kv.claimKeys('bad'),
    'InvalidOptionsError'
  );
  const validationClaimKeysDuplicate = await expectErrorName(
    () => kv.claimKeys(['dup:key', 'dup:key'], { ttl: 30000 }),
    'InvalidOptionsError'
  );
  const validationClaimKeysEmptyKey = await expectErrorName(
    () => kv.claimKeys(['ok:key', ''], { ttl: 30000 }),
    'InvalidOptionsError'
  );
  const validationClaimKeysBadTTL = await expectErrorName(
    () => kv.claimKeys(['ok:key'], { ttl: 0 }),
    'InvalidOptionsError'
  );
  const validationReleaseClaimsNonArray = await expectErrorName(
    () => kv.releaseClaims('bad'),
    'InvalidOptionsError'
  );
  const validationReleaseClaimsBadItem = await expectErrorName(
    () => kv.releaseClaims([{}]),
    'InvalidOptionsError'
  );
  const validationCompleteClaimsNonArray = await expectErrorName(
    () => kv.completeClaims('bad'),
    'InvalidOptionsError'
  );
  const validationCompleteClaimsBadOptions = await expectErrorName(
    () => kv.completeClaims([{ id: 'x', key: 'k', token: 1 }], 'bad'),
    'InvalidOptionsError'
  );
  const validationRenewClaimsMissingTTL = await expectErrorName(
    () => kv.renewClaims([{ id: 'x', key: 'k', token: 1 }], {}),
    'InvalidOptionsError'
  );
  const validationRenewClaimsBadClaims = await expectErrorName(
    () => kv.renewClaims('bad', { ttl: 30000 }),
    'InvalidOptionsError'
  );
  const validationAllocationStatsBadOptions = await expectErrorName(
    () => kv.allocationStats('bad'),
    'InvalidOptionsError'
  );
  const validationAllocationStatsBadPrefixType = await expectErrorName(
    () => kv.allocationStats({ prefix: 123 }),
    'InvalidOptionsError'
  );
  const validationExportCSVMissingColumns = await expectErrorName(
    () => kv.exportCSV({ fileName: EXPORT_CSV_PATH }),
    'InvalidOptionsError'
  );
  const validationExportCSVBadDelimiter = await expectErrorName(
    () =>
      kv.exportCSV({
        fileName: EXPORT_CSV_PATH,
        columns: ['status'],
        delimiter: ';;'
      }),
    'InvalidOptionsError'
  );
  const validationExportCSVBadFileName = await expectErrorName(
    () =>
      kv.exportCSV({
        fileName: '   ',
        columns: ['status']
      }),
    'InvalidOptionsError'
  );
  const validationValidateCSVMissingFileName = await expectErrorName(
    () => kv.validateCSV({}),
    'InvalidOptionsError'
  );
  const validationValidateCSVBadDelimiter = await expectErrorName(
    () =>
      kv.validateCSV({
        fileName: IMPORT_CSV_PATH,
        delimiter: ';;'
      }),
    'InvalidOptionsError'
  );
  const validationValidateCSVNonNumericKeyColumnNoHeader = await expectErrorName(
    () =>
      kv.validateCSV({
        fileName: IMPORT_CSV_PATH,
        hasHeader: false,
        keyColumn: 'id'
      }),
    'InvalidOptionsError'
  );
  const validationValidateCSVMissingFile = await expectErrorName(
    () =>
      kv.validateCSV({
        fileName: MISSING_CSV_PATH
      }),
    'SnapshotNotFoundError'
  );
  const validationValidateJSONLMissingFileName = await expectErrorName(
    () => kv.validateJSONL({}),
    'InvalidOptionsError'
  );
  const validationValidateJSONLMissingFile = await expectErrorName(
    () =>
      kv.validateJSONL({
        fileName: MISSING_JSONL_PATH
      }),
    'SnapshotNotFoundError'
  );

  check(true, {
    'api:input-validation-new-methods': () =>
      validationClaimKeysNonArray &&
      validationClaimKeysDuplicate &&
      validationClaimKeysEmptyKey &&
      validationClaimKeysBadTTL &&
      validationReleaseClaimsNonArray &&
      validationReleaseClaimsBadItem &&
      validationCompleteClaimsNonArray &&
      validationCompleteClaimsBadOptions &&
      validationRenewClaimsMissingTTL &&
      validationRenewClaimsBadClaims &&
      validationAllocationStatsBadOptions &&
      validationAllocationStatsBadPrefixType &&
      validationExportCSVMissingColumns &&
      validationExportCSVBadDelimiter &&
      validationExportCSVBadFileName &&
      validationValidateCSVMissingFileName &&
      validationValidateCSVBadDelimiter &&
      validationValidateCSVNonNumericKeyColumnNoHeader &&
      validationValidateCSVMissingFile &&
      validationValidateJSONLMissingFileName &&
      validationValidateJSONLMissingFile
  });

  check(true, {
    'api:input-validation-matrix': () =>
      getMissingKeyError &&
      validationGetEmptyKey &&
      validationGetManyNonArray &&
      validationGetManyNonStringItem &&
      validationSetEmptyKey &&
      validationSetManyNonObject &&
      validationDeleteManyNonArray &&
      validationDeleteManyEmptyKey &&
      validationDeleteByPrefixEmptyPrefix &&
      validationDeleteByPrefixNonPositiveLimit &&
      validationDeleteEmptyKey &&
      validationExistsEmptyKey &&
      validationIncrementByNonNumber &&
      validationGetOrSetEmptyKey &&
      validationSwapEmptyKey &&
      validationCompareAndSwapEmptyKey &&
      validationCompareAndSwapDetailedBadOptions &&
      validationSetIfAbsentEmptyKey &&
      validationDeleteIfExistsEmptyKey &&
      validationCompareAndDeleteEmptyKey &&
      validationCompareAndDeleteDetailedBadOptions &&
      validationScanBadOptions &&
      validationScanKeysBadOptions &&
      validationListBadOptions &&
      validationListKeysBadOptions &&
      validationCountBadOptions &&
      validationRandomKeyBadOptions &&
      validationRandomKeysMissingCount &&
      validationRandomKeysBadCount &&
      validationPopRandomBadOptions &&
      validationPopRandomManyBadOptions &&
      validationClaimRandomBadTTL &&
      validationClaimKeyEmptyKey &&
      validationClaimRandomManyMissingCount &&
      validationReleaseClaimBadClaim &&
      validationCompleteClaimBadClaim &&
      validationCompleteClaimBadOptions &&
      validationRenewClaimBadOptions &&
      validationBackupBadOptions &&
      validationRestoreBadOptions &&
      validationExportJSONLMissingFile &&
      validationImportJSONLMissingFile &&
      validationImportCSVMissingKeyColumn &&
      validationImportCSVBadDelimiter
  });

  // close(): Validate synchronous idempotent lifecycle close behavior.
  let closeIdempotent = true;
  try {
    kv.close();
    kv.close();
  } catch (err) {
    closeIdempotent = false;
  }

  check(closeIdempotent, {
    'api:close-idempotent': () => closeIdempotent
  });

  const postCloseRejects =
    (await expectErrorName(() => kv.get('foo'), 'StoreClosedError')) &&
    (await expectErrorName(() => kv.set('post:close:key', 'value'), 'StoreClosedError')) &&
    (await expectErrorName(() => kv.claimRandom({ prefix: 'claim:key:' }), 'StoreClosedError')) &&
    (await expectErrorName(
      () => kv.popRandomMany({ prefix: 'pop:many:key:', count: 1 }),
      'StoreClosedError'
    ));

  check(true, {
    'api:post-close-rejects': () => closeIdempotent && postCloseRejects
  });
}
