import { check } from 'k6';
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
// ATOMIC OPERATIONS TESTED:
// - Atomic operations (getOrSet, swap).
// - Query operations (list, scan, randomKey).
// - Snapshot operations (backup, restore).

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'api-output-validation';

// Snapshot file path for backup/restore validation.
const SNAPSHOT_PATH = getSnapshotPath(TEST_NAME);

// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME);

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: 1,
  iterations: 1,
  thresholds: {
    'checks{api:getOrSet-structure}': ['rate>0.999'],
    'checks{api:getOrSet-existing}': ['rate>0.999'],
    'checks{api:getOrSet-new}': ['rate>0.999'],
    'checks{api:swap-structure}': ['rate>0.999'],
    'checks{api:swap-new}': ['rate>0.999'],
    'checks{api:swap-existing}': ['rate>0.999'],
    'checks{api:list-structure}': ['rate>0.999'],
    'checks{api:list-entry-structure}': ['rate>0.999'],
    'checks{api:list-null-values}': ['rate>0.999'],
    'checks{api:scan-structure}': ['rate>0.999'],
    'checks{api:scan-entry-structure}': ['rate>0.999'],
    'checks{api:scan-pagination}': ['rate>0.999'],
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

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv, TEST_NAME);

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
}
