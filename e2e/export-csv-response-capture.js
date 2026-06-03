import { check } from 'k6';
import { createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: RESPONSE CAPTURE EXPORT TO CSV
// =============================================================================
//
// This scenario models exporting captured response envelopes into CSV for
// external analytics/reporting pipelines after a test run.
//
// REAL-WORLD PROBLEM SOLVED:
// CSV export should be deterministic and automation-friendly:
// - API returns stable summary shape.
// - prefix filter isolates response-only keys.
// - limit bounds the exported record count.
// - invalid options reject with InvalidOptionsError.
//
// METHODS TESTED:
// - setMany(): seed deterministic response payloads.
// - exportCSV(): export with prefix/limit/includeKey/delimiter variants.
// - input guard checks for invalid shape and option fields.

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'export-csv-response-capture';

// CSV file path for exportCSV validation.
const EXPORT_PATH = './tmp/export-csv-response-capture.csv';

// CSV file path for exportCSV limit validation.
const EXPORT_LIMIT_PATH = './tmp/export-csv-response-capture-limited.csv';

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
    'checks{exportCSV:method-available}': ['rate==1'],
    'checks{exportCSV:summary-shape}': ['rate==1'],
    'checks{exportCSV:file-name-roundtrip}': ['rate==1'],
    'checks{exportCSV:prefix-exported-count}': ['rate==1'],
    'checks{exportCSV:bytes-written}': ['rate==1'],
    'checks{exportCSV:limit-respected}': ['rate==1'],
    'checks{exportCSV:full-content-roundtrip}': ['rate==1'],
    'checks{exportCSV:limited-content-roundtrip}': ['rate==1'],
    'checks{exportCSV:invalid-shape-rejects}': ['rate==1'],
    'checks{exportCSV:missing-columns-rejects}': ['rate==1'],
    'checks{exportCSV:invalid-delimiter-rejects}': ['rate==1'],
  },
};

// setup clears previous state before export checks start.
export const setup = createSetup(kv);

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv);

// exportCSVResponseCapture validates portable CSV export summaries and guards.
export default async function exportCSVResponseCapture() {
  await kv.clear();

  const hasExportCSV = typeof kv.exportCSV === 'function';
  check(true, {
    'exportCSV:method-available': () => hasExportCSV,
  });

  if (!hasExportCSV) {
    return;
  }

  await kv.setMany({
    'responses:1': { status: 200, requestId: 'r1', userId: 'u1', bodyHash: 'abc' },
    'responses:2': { status: 201, requestId: 'r2', userId: 'u2', bodyHash: 'def' },
    'other:1': { status: 500, requestId: 'r3', userId: 'u3', bodyHash: 'ghi' },
  });

  const exportResult = await kv.exportCSV({
    fileName: EXPORT_PATH,
    prefix: 'responses:',
    columns: ['status', 'requestId', 'userId', 'bodyHash'],
    includeKey: true,
  });

  const limitedResult = await kv.exportCSV({
    fileName: EXPORT_LIMIT_PATH,
    prefix: 'responses:',
    columns: ['status', 'requestId', 'userId', 'bodyHash'],
    includeKey: false,
    limit: 1,
    delimiter: ';',
  });

  await kv.clear();
  const fullRoundtrip = await kv.importCSV({
    fileName: EXPORT_PATH,
    keyColumn: 'key',
    hasHeader: true,
    delimiter: ',',
  });
  const roundtripResponse1 = await kv.get('responses:1');
  const roundtripResponse2 = await kv.get('responses:2');
  const roundtripOtherExists = await kv.exists('other:1');

  await kv.clear();
  let limitedRoundtrip = null;
  let limitedRoundtripErr = null;
  try {
    limitedRoundtrip = await kv.importCSV({
      fileName: EXPORT_LIMIT_PATH,
      keyColumn: 'status',
      hasHeader: true,
      delimiter: ';',
    });
  } catch (err) {
    limitedRoundtripErr = err;
  }

  const limitedKeys = limitedRoundtripErr === null ? await kv.listKeys() : [];
  const limitedValue = limitedKeys.length === 1 ? await kv.get(limitedKeys[0]) : null;

  let invalidShapeErr = null;
  try {
    await kv.exportCSV(null);
  } catch (err) {
    invalidShapeErr = err;
  }

  let missingColumnsErr = null;
  try {
    await kv.exportCSV({
      fileName: EXPORT_PATH,
    });
  } catch (err) {
    missingColumnsErr = err;
  }

  let invalidDelimiterErr = null;
  try {
    await kv.exportCSV({
      fileName: EXPORT_PATH,
      columns: ['status'],
      delimiter: ';;',
    });
  } catch (err) {
    invalidDelimiterErr = err;
  }

  check(exportResult, {
    'exportCSV:summary-shape': (result) =>
      result !== null &&
      typeof result === 'object' &&
      typeof result.exported === 'number' &&
      typeof result.fileName === 'string' &&
      typeof result.bytesWritten === 'number',
    'exportCSV:file-name-roundtrip': (result) => result.fileName === EXPORT_PATH,
    'exportCSV:prefix-exported-count': (result) => result.exported === 2,
    'exportCSV:bytes-written': (result) => result.bytesWritten > 0,
  });

  check(limitedResult, {
    'exportCSV:limit-respected': (result) =>
      result.exported === 1 &&
      result.fileName === EXPORT_LIMIT_PATH &&
      result.bytesWritten > 0,
    'exportCSV:limited-content-roundtrip': () =>
      limitedRoundtripErr === null &&
      limitedRoundtrip !== null &&
      limitedRoundtrip.imported === 1 &&
      limitedKeys.length === 1 &&
      limitedValue !== null &&
      typeof limitedValue.requestId === 'string' &&
      typeof limitedValue.userId === 'string' &&
      typeof limitedValue.bodyHash === 'string' &&
      !Object.prototype.hasOwnProperty.call(limitedValue, 'key'),
  });

  check(fullRoundtrip, {
    'exportCSV:full-content-roundtrip': (result) =>
      result !== null &&
      result.imported === 2 &&
      roundtripOtherExists === false &&
      roundtripResponse1 !== null &&
      roundtripResponse2 !== null &&
      roundtripResponse1.status === '200' &&
      roundtripResponse1.requestId === 'r1' &&
      roundtripResponse1.userId === 'u1' &&
      roundtripResponse1.bodyHash === 'abc' &&
      roundtripResponse2.status === '201' &&
      roundtripResponse2.requestId === 'r2' &&
      roundtripResponse2.userId === 'u2' &&
      roundtripResponse2.bodyHash === 'def',
  });

  check(true, {
    'exportCSV:invalid-shape-rejects': () =>
      invalidShapeErr !== null && invalidShapeErr.name === 'InvalidOptionsError',
    'exportCSV:missing-columns-rejects': () =>
      missingColumnsErr !== null && missingColumnsErr.name === 'InvalidOptionsError',
    'exportCSV:invalid-delimiter-rejects': () =>
      invalidDelimiterErr !== null && invalidDelimiterErr.name === 'InvalidOptionsError',
  });
}
