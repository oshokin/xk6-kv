import { check } from 'k6';
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
// - Query/coordination operations (list, scan, count, randomKey, popRandom,
//   claimRandom, releaseClaim, completeClaim).
// - Observability/lifecycle (rebuildKeyList, stats, reportStats, close).
// - Snapshot operations (backup, restore).

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'api-output-validation';

// Snapshot file path for backup/restore validation.
const SNAPSHOT_PATH = getSnapshotPath(TEST_NAME);

// JSONL file path for exportJSONL validation.
const EXPORT_JSONL_PATH = './tmp/api-output-validation-export.jsonl';

// JSONL file path for importJSONL validation.
const IMPORT_JSONL_PATH = './tmp/api-output-validation-import.jsonl';

// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME);

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
    'checks{api:scanKeys-structure}': ['rate>0.999'],
    'checks{api:scanKeys-types}': ['rate>0.999'],
    'checks{api:scanKeys-key-types}': ['rate>0.999'],
    'checks{api:scanKeys-pagination}': ['rate>0.999'],
    'checks{api:randomKey-empty}': ['rate>0.999'],
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
    'checks{api:restore-empty-object-preservation}': ['rate>0.999']
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
  const newResult = await kv.getOrSet('newKey', 555);
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
}
