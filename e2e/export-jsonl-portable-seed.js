import { check } from 'k6';
import { createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: PORTABLE JSONL SEED EXPORT
// =============================================================================
//
// This scenario models a team exporting a stable subset of KV data into JSONL
// so it can be reused across environments (seed fixtures, smoke datasets,
// migration handoff files).
//
// REAL-WORLD PROBLEM SOLVED:
// Exported artifacts must be deterministic and safe to automate:
// - API returns a stable summary shape.
// - Prefix filtering exports only the target subset.
// - limit bounds exported entries.
// - empty prefix matches produce an empty artifact summary.
// - invalid options reject with InvalidOptionsError.
//
// METHODS TESTED:
// - setMany(): seed deterministic source keys.
// - exportJSONL(): portable JSONL export with prefix/limit variants.

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'export-jsonl-portable-seed';

// JSONL artifact paths used for export variants.
const EXPORT_ALL_PATH = './tmp/export-jsonl-portable-seed-all.jsonl';
const EXPORT_LIMITED_PATH = './tmp/export-jsonl-portable-seed-limited.jsonl';
const EXPORT_EMPTY_PATH = './tmp/export-jsonl-portable-seed-empty.jsonl';

// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME);

// options configures a deterministic single-run API contract check.
export const options = {
  vus: 1,
  iterations: 1,
  thresholds: {
    'checks{exportJSONL:method-available}': ['rate==1'],
    'checks{exportJSONL:summary-shape}': ['rate==1'],
    'checks{exportJSONL:file-name-roundtrip}': ['rate==1'],
    'checks{exportJSONL:prefix-exported-count}': ['rate==1'],
    'checks{exportJSONL:bytes-written}': ['rate==1'],
    'checks{exportJSONL:limit-respected}': ['rate==1'],
    'checks{exportJSONL:empty-prefix-count-zero}': ['rate==1'],
    'checks{exportJSONL:empty-prefix-empty-file}': ['rate==1'],
    'checks{exportJSONL:invalid-shape-rejects}': ['rate==1'],
    'checks{exportJSONL:missing-fileName-rejects}': ['rate==1'],
    'checks{exportJSONL:invalid-prefix-type-rejects}': ['rate==1'],
    'checks{exportJSONL:fractional-limit-rejects}': ['rate==1']
  }
};

// setup clears previous state before export checks start.
export const setup = createSetup(kv);

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv);

// exportJSONLPortableSeed validates portable export summaries and input guards.
export default async function exportJSONLPortableSeed() {
  await kv.clear();

  const hasExportJSONL = typeof kv.exportJSONL === 'function';
  check(true, {
    'exportJSONL:method-available': () => hasExportJSONL
  });

  if (!hasExportJSONL) {
    return;
  }

  await kv.setMany({
    'exportJSONL:user:1': { id: 1, name: 'Alice' },
    'exportJSONL:user:2': { id: 2, name: 'Bob' },
    'exportJSONL:order:1': { id: 101, total: 42 }
  });

  const allResult = await kv.exportJSONL({
    fileName: EXPORT_ALL_PATH,
    prefix: 'exportJSONL:user:'
  });

  const limitedResult = await kv.exportJSONL({
    fileName: EXPORT_LIMITED_PATH,
    prefix: 'exportJSONL:user:',
    limit: 1
  });

  const emptyResult = await kv.exportJSONL({
    fileName: EXPORT_EMPTY_PATH,
    prefix: 'exportJSONL:missing:'
  });

  let invalidShapeErr = null;
  try {
    await kv.exportJSONL(null);
  } catch (err) {
    invalidShapeErr = err;
  }

  let missingFileNameErr = null;
  try {
    await kv.exportJSONL({ prefix: 'exportJSONL:user:' });
  } catch (err) {
    missingFileNameErr = err;
  }

  let invalidPrefixTypeErr = null;
  try {
    await kv.exportJSONL({
      fileName: EXPORT_ALL_PATH,
      prefix: 123
    });
  } catch (err) {
    invalidPrefixTypeErr = err;
  }

  let fractionalLimitErr = null;
  try {
    await kv.exportJSONL({
      fileName: EXPORT_LIMITED_PATH,
      limit: 1.5
    });
  } catch (err) {
    fractionalLimitErr = err;
  }

  check(allResult, {
    'exportJSONL:summary-shape': (result) =>
      result !== null &&
      typeof result === 'object' &&
      typeof result.exported === 'number' &&
      typeof result.fileName === 'string' &&
      typeof result.bytesWritten === 'number',
    'exportJSONL:file-name-roundtrip': (result) =>
      result.fileName === EXPORT_ALL_PATH,
    'exportJSONL:prefix-exported-count': (result) =>
      result.exported === 2,
    'exportJSONL:bytes-written': (result) =>
      result.bytesWritten > 0
  });

  check(limitedResult, {
    'exportJSONL:limit-respected': (result) =>
      result.exported === 1 &&
      result.fileName === EXPORT_LIMITED_PATH &&
      result.bytesWritten > 0
  });

  check(emptyResult, {
    'exportJSONL:empty-prefix-count-zero': (result) =>
      result.exported === 0,
    'exportJSONL:empty-prefix-empty-file': (result) =>
      result.fileName === EXPORT_EMPTY_PATH && result.bytesWritten === 0
  });

  check(true, {
    'exportJSONL:invalid-shape-rejects': () =>
      invalidShapeErr !== null && invalidShapeErr.name === 'InvalidOptionsError',
    'exportJSONL:missing-fileName-rejects': () =>
      missingFileNameErr !== null && missingFileNameErr.name === 'InvalidOptionsError',
    'exportJSONL:invalid-prefix-type-rejects': () =>
      invalidPrefixTypeErr !== null && invalidPrefixTypeErr.name === 'InvalidOptionsError',
    'exportJSONL:fractional-limit-rejects': () =>
      fractionalLimitErr !== null && fractionalLimitErr.name === 'InvalidOptionsError'
  });
}
