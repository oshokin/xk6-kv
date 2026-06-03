import { check } from 'k6';
import { createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: PORTABLE JSONL SEED IMPORT
// =============================================================================
//
// This scenario models a team importing a portable JSONL seed into KV so
// environments can be hydrated consistently from versioned artifacts.
//
// REAL-WORLD PROBLEM SOLVED:
// Import jobs must be deterministic and safe to automate:
// - API returns a stable summary shape.
// - Existing keys are overwritten by imported records.
// - limit bounds imported entries.
// - invalid options reject with InvalidOptionsError.
// - missing files reject with SnapshotNotFoundError.
//
// METHODS TESTED:
// - setMany(): seed deterministic source records for fixture generation.
// - exportJSONL(): generate deterministic import fixtures used by this test.
// - importJSONL(): portable JSONL import with batch/limit variants.
// - getMany(), listKeys(): verify imported contents and limit behavior.

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'import-jsonl-portable-seed';

// JSONL artifact paths used for import variants.
const IMPORT_ALL_PATH = './tmp/import-jsonl-portable-seed-all.jsonl';
const IMPORT_LIMITED_PATH = './tmp/import-jsonl-portable-seed-limited.jsonl';

// Deterministic user keys reused across checks.
const USER_KEYS = ['importJSONL:user:1', 'importJSONL:user:2'];

// normalizeImportPath mirrors filepath.Clean("./x") behavior used in Go options parsing.
function normalizeImportPath(path) {
  return String(path).replace(/^\.\/+/, '');
}

// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME);

// options configures a deterministic single-run API contract check.
export const options = {
  vus: 1,
  iterations: 1,
  thresholds: {
    'checks{importJSONL:method-available}': ['rate==1'],
    'checks{importJSONL:summary-shape}': ['rate==1'],
    'checks{importJSONL:file-name-roundtrip}': ['rate==1'],
    'checks{importJSONL:imported-count}': ['rate==1'],
    'checks{importJSONL:bytes-read}': ['rate==1'],
    'checks{importJSONL:overwrites-existing}': ['rate==1'],
    'checks{importJSONL:limit-respected}': ['rate==1'],
    'checks{importJSONL:invalid-shape-rejects}': ['rate==1'],
    'checks{importJSONL:missing-fileName-rejects}': ['rate==1'],
    'checks{importJSONL:fractional-limit-rejects}': ['rate==1'],
    'checks{importJSONL:fractional-batch-size-rejects}': ['rate==1'],
    'checks{importJSONL:missing-file-rejects}': ['rate==1']
  }
};

// setup clears previous state before import checks start.
export const setup = createSetup(kv);

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv);

// importJSONLPortableSeed validates portable import summaries and input guards.
export default async function importJSONLPortableSeed() {
  await kv.clear();

  const hasImportJSONL = typeof kv.importJSONL === 'function';
  check(true, {
    'importJSONL:method-available': () => hasImportJSONL
  });

  if (!hasImportJSONL) {
    return;
  }

  await kv.setMany({
    'importJSONL:user:1': { id: 1, name: 'Alice' },
    'importJSONL:user:2': { id: 2, name: 'Bob' },
    'importJSONL:order:1': { id: 101, total: 42 }
  });

  await kv.exportJSONL({
    fileName: IMPORT_ALL_PATH,
    prefix: 'importJSONL:user:'
  });

  await kv.exportJSONL({
    fileName: IMPORT_LIMITED_PATH,
    prefix: 'importJSONL:user:'
  });

  await kv.deleteMany(['importJSONL:user:1', 'importJSONL:user:2', 'importJSONL:order:1']);
  await kv.set('importJSONL:user:1', { id: 1, name: 'Stale' });

  const allResult = await kv.importJSONL({
    fileName: IMPORT_ALL_PATH,
    batchSize: 1
  });

  const importedItems = await kv.getMany(USER_KEYS);

  check(allResult, {
    'importJSONL:summary-shape': (result) =>
      result !== null &&
      typeof result === 'object' &&
      typeof result.imported === 'number' &&
      typeof result.fileName === 'string' &&
      typeof result.bytesRead === 'number',
    'importJSONL:file-name-roundtrip': (result) =>
      normalizeImportPath(result.fileName) === normalizeImportPath(IMPORT_ALL_PATH),
    'importJSONL:imported-count': (result) =>
      result.imported === 2,
    'importJSONL:bytes-read': (result) =>
      result.bytesRead > 0
  });

  check(importedItems, {
    'importJSONL:overwrites-existing': (items) =>
      items.length === 2 &&
      items[0].exists === true &&
      items[1].exists === true &&
      items[0].value.name === 'Alice' &&
      items[1].value.name === 'Bob'
  });

  await kv.deleteMany(USER_KEYS);

  const limitedResult = await kv.importJSONL({
    fileName: IMPORT_LIMITED_PATH,
    limit: 1,
    batchSize: 1000
  });
  const limitedKeys = await kv.listKeys({ prefix: 'importJSONL:user:' });

  check(limitedResult, {
    'importJSONL:limit-respected': (result) =>
      result.imported === 1 &&
      normalizeImportPath(result.fileName) === normalizeImportPath(IMPORT_LIMITED_PATH) &&
      result.bytesRead > 0 &&
      limitedKeys.length === 1
  });

  let invalidShapeErr = null;
  try {
    await kv.importJSONL(null);
  } catch (err) {
    invalidShapeErr = err;
  }

  let missingFileNameErr = null;
  try {
    await kv.importJSONL({});
  } catch (err) {
    missingFileNameErr = err;
  }

  let fractionalLimitErr = null;
  try {
    await kv.importJSONL({
      fileName: IMPORT_ALL_PATH,
      limit: 1.5
    });
  } catch (err) {
    fractionalLimitErr = err;
  }

  let fractionalBatchSizeErr = null;
  try {
    await kv.importJSONL({
      fileName: IMPORT_ALL_PATH,
      batchSize: 1.5
    });
  } catch (err) {
    fractionalBatchSizeErr = err;
  }

  let missingFileErr = null;
  try {
    await kv.importJSONL({
      fileName: './tmp/import-jsonl-portable-seed-missing.jsonl'
    });
  } catch (err) {
    missingFileErr = err;
  }

  check(true, {
    'importJSONL:invalid-shape-rejects': () =>
      invalidShapeErr !== null && invalidShapeErr.name === 'InvalidOptionsError',
    'importJSONL:missing-fileName-rejects': () =>
      missingFileNameErr !== null && missingFileNameErr.name === 'InvalidOptionsError',
    'importJSONL:fractional-limit-rejects': () =>
      fractionalLimitErr !== null && fractionalLimitErr.name === 'InvalidOptionsError',
    'importJSONL:fractional-batch-size-rejects': () =>
      fractionalBatchSizeErr !== null && fractionalBatchSizeErr.name === 'InvalidOptionsError',
    'importJSONL:missing-file-rejects': () =>
      missingFileErr !== null && missingFileErr.name === 'SnapshotNotFoundError'
  });
}
