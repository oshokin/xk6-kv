import { check } from 'k6';
import { createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: PORTABLE CSV SEED IMPORT
// =============================================================================
//
// This scenario models a team importing a versioned CSV seed into KV to hydrate
// environments before claim-based test execution.
//
// REAL-WORLD PROBLEM SOLVED:
// Import jobs must be deterministic and safe to automate:
// - API returns a stable summary shape.
// - Existing keys are overwritten by imported rows.
// - limit bounds imported rows.
// - invalid options reject with InvalidOptionsError.
// - missing files reject with SnapshotNotFoundError.
//
// METHODS TESTED:
// - importCSV(): streaming CSV ingest with header/key-column mapping.
// - getMany(), listKeys(): verify imported contents and limit behavior.
// - claimRandomMany(), releaseClaim(): confirm imported rows are claimable.

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'import-csv-portable-seed';

// CSV artifact paths used for import variants.
const IMPORT_ALL_PATH = './examples/fixtures/users.csv';
const IMPORT_MISSING_PATH = './tmp/import-csv-portable-seed-missing.csv';

// Deterministic user keys reused across checks.
const USER_KEYS = ['user:1', 'user:2'];

// normalizeImportPath mirrors filepath.Clean("./x") behavior used in Go options parsing.
function normalizeImportPath(path) {
  return String(path).replace(/^\.\/+/, '');
}

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
    'checks{importCSV:method-available}': ['rate==1'],
    'checks{importCSV:summary-shape}': ['rate==1'],
    'checks{importCSV:file-name-roundtrip}': ['rate==1'],
    'checks{importCSV:imported-count}': ['rate==1'],
    'checks{importCSV:bytes-read}': ['rate==1'],
    'checks{importCSV:imports-records}': ['rate==1'],
    'checks{importCSV:claim-random-many}': ['rate==1'],
    'checks{importCSV:limit-respected}': ['rate==1'],
    'checks{importCSV:invalid-shape-rejects}': ['rate==1'],
    'checks{importCSV:missing-key-column-rejects}': ['rate==1'],
    'checks{importCSV:invalid-delimiter-rejects}': ['rate==1'],
    'checks{importCSV:fractional-limit-rejects}': ['rate==1'],
    'checks{importCSV:missing-file-rejects}': ['rate==1'],
  },
};

// setup clears previous state before import checks start.
export const setup = createSetup(kv);

// teardown closes stores.
export const teardown = createTeardown(kv, TEST_NAME);

// importCSVPortableSeed validates portable CSV import summaries and input guards.
export default async function importCSVPortableSeed() {
  await kv.clear();

  const hasImportCSV = typeof kv.importCSV === 'function';
  check(true, {
    'importCSV:method-available': () => hasImportCSV,
  });

  if (!hasImportCSV) {
    return;
  }

  const allResult = await kv.importCSV({
    fileName: IMPORT_ALL_PATH,
    keyColumn: 'id',
    hasHeader: true,
    batchSize: 1,
  });

  const importedItems = await kv.getMany(USER_KEYS);

  const claims = await kv.claimRandomMany({
    prefix: 'user:',
    count: 2,
    owner: 'import-csv-e2e',
    ttl: 30000,
  });

  for (const claim of claims) {
    await kv.releaseClaim(claim);
  }

  check(allResult, {
    'importCSV:summary-shape': (result) =>
      result !== null &&
      typeof result === 'object' &&
      typeof result.imported === 'number' &&
      typeof result.fileName === 'string' &&
      typeof result.bytesRead === 'number',
    'importCSV:file-name-roundtrip': (result) =>
      normalizeImportPath(result.fileName) === normalizeImportPath(IMPORT_ALL_PATH),
    'importCSV:imported-count': (result) => result.imported === 2,
    'importCSV:bytes-read': (result) => result.bytesRead > 0,
  });

  check(importedItems, {
    'importCSV:imports-records': (items) =>
      items.length === 2 &&
      items[0].exists === true &&
      items[1].exists === true &&
      items[0].value.name === 'Alice' &&
      items[1].value.name === 'Bob',
  });

  check(claims, {
    'importCSV:claim-random-many': (items) => items.length === 2,
  });

  await kv.clear();

  const limitedResult = await kv.importCSV({
    fileName: IMPORT_ALL_PATH,
    keyColumn: 'id',
    hasHeader: true,
    limit: 1,
    batchSize: 1000,
  });
  const limitedKeys = await kv.listKeys({ prefix: 'user:' });

  check(limitedResult, {
    'importCSV:limit-respected': (result) =>
      result.imported === 1 &&
      normalizeImportPath(result.fileName) === normalizeImportPath(IMPORT_ALL_PATH) &&
      result.bytesRead > 0 &&
      limitedKeys.length === 1,
  });

  let invalidShapeErr = null;
  try {
    await kv.importCSV(null);
  } catch (err) {
    invalidShapeErr = err;
  }

  let missingKeyColumnErr = null;
  try {
    await kv.importCSV({
      fileName: IMPORT_ALL_PATH,
    });
  } catch (err) {
    missingKeyColumnErr = err;
  }

  let invalidDelimiterErr = null;
  try {
    await kv.importCSV({
      fileName: IMPORT_ALL_PATH,
      keyColumn: 'id',
      delimiter: ';;',
    });
  } catch (err) {
    invalidDelimiterErr = err;
  }

  let fractionalLimitErr = null;
  try {
    await kv.importCSV({
      fileName: IMPORT_ALL_PATH,
      keyColumn: 'id',
      limit: 1.5,
    });
  } catch (err) {
    fractionalLimitErr = err;
  }

  let missingFileErr = null;
  try {
    await kv.importCSV({
      fileName: IMPORT_MISSING_PATH,
      keyColumn: 'id',
    });
  } catch (err) {
    missingFileErr = err;
  }

  check(true, {
    'importCSV:invalid-shape-rejects': () =>
      invalidShapeErr !== null && invalidShapeErr.name === 'InvalidOptionsError',
    'importCSV:missing-key-column-rejects': () =>
      missingKeyColumnErr !== null && missingKeyColumnErr.name === 'InvalidOptionsError',
    'importCSV:invalid-delimiter-rejects': () =>
      invalidDelimiterErr !== null && invalidDelimiterErr.name === 'InvalidOptionsError',
    'importCSV:fractional-limit-rejects': () =>
      fractionalLimitErr !== null && fractionalLimitErr.name === 'InvalidOptionsError',
    'importCSV:missing-file-rejects': () =>
      missingFileErr !== null && missingFileErr.name === 'SnapshotNotFoundError',
  });
}
