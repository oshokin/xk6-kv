import { check } from 'k6';
import exec from 'k6/execution';
import { VUS, ITERATIONS, createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: CONCURRENT SCAN + LIST EXPORTS
// =============================================================================
//
// Data engineering teams often run multiple paginated exports (scan + list) in
// parallel for different tenants. Each worker issues Promise.all batches so that
// scan() and list() results arrive at the same time, stressing the promise to JS
// conversion path that previously panicked with concurrent map writes.
//
// REAL-WORLD PROBLEMS UNCOVERED:
// - Cursor corruption when multiple scans start mid-keyspace.
// - Event-loop lockups converting hundreds of entries to JS objects in parallel.
// - Inconsistent list prefixes during overlapping bulk reads.
//
// ATOMIC OPERATIONS TESTED:
// - scan(): cursor-based pagination.
// - list(): prefix-limited enumerations.
// - scanKeys()/listKeys(): key-only pagination/lookup APIs.
// - set()/deleteIfExists(): concurrent mutations while reads are in flight.

// Invoice key prefix.
const INVOICE_PREFIX = __ENV.INVOICE_PREFIX || 'invoice:';

// Total number of invoice records seeded during setup.
const TOTAL_INVOICES = parseInt(__ENV.TOTAL_INVOICES || '2000', 10);

// Number of concurrent scan() operations per iteration.
const SCAN_CONCURRENCY = parseInt(__ENV.SCAN_CONCURRENCY || '6', 10);

// Number of concurrent list() operations per iteration.
const LIST_CONCURRENCY = parseInt(__ENV.LIST_CONCURRENCY || '6', 10);

// Number of concurrent scanKeys() operations per iteration.
const SCAN_KEYS_CONCURRENCY = parseInt(__ENV.SCAN_KEYS_CONCURRENCY || '6', 10);

// Number of concurrent listKeys() operations per iteration.
const LIST_KEYS_CONCURRENCY = parseInt(__ENV.LIST_KEYS_CONCURRENCY || '6', 10);

// Maximum entries returned per scan() page.
const SCAN_PAGE_SIZE = parseInt(__ENV.SCAN_PAGE_SIZE || '120', 10);

// Maximum entries returned per list() call (half of scan page size).
const LIST_PAGE_SIZE = Math.floor(SCAN_PAGE_SIZE / 2);

// Width of zero-padded invoice IDs (e.g., 6 = invoice:000042).
const KEY_PADDING = 6;

// Number of tenant shards for invoice distribution.
const TENANT_COUNT = 25;

// Prefix used for short-lived mutation keys during concurrency checks.
const MUTATION_PREFIX = __ENV.MUTATION_PREFIX || 'scan-mutation:';

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'scan-list-concurrency';

// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME);

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: VUS,
  iterations: ITERATIONS,
  thresholds: {
    'checks{scan:non-empty}': ['rate>0.999'],
    'checks{scan:cursor-shape}': ['rate>0.999'],
    'checks{scanKeys:non-empty}': ['rate>0.999'],
    'checks{scanKeys:cursor-shape}': ['rate>0.999'],
    'checks{scanKeys:key-only}': ['rate>0.999'],
    'checks{scanKeys:terminates-bounded}': ['rate>0.999'],
    'checks{list:non-empty}': ['rate>0.999'],
    'checks{listKeys:non-empty}': ['rate>0.999'],
    'checks{listKeys:key-only}': ['rate>0.999']
  }
};

// setup seeds a deterministic invoice dataset before contention begins.
export async function setup() {
  const standardSetup = createSetup(kv);
  await standardSetup();

  for (let i = 0; i < TOTAL_INVOICES; i += 1) {
    const paddedId = String(i).padStart(KEY_PADDING, '0');
    await kv.set(`${INVOICE_PREFIX}${paddedId}`, {
      invoiceId: paddedId,
      tenant: `tenant-${i % TENANT_COUNT}`,
      // Amount between 10-99.
      amount: 10 + (i % 90),
      createdAt: Date.now()
    });
  }
}

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv);

// collectPageStartCursors retrieves page-start cursors through public APIs so
// tests never rely on internal cursor encoding details.
async function collectPageStartCursors(fetchPage, maxPageIndex) {
  const pageStartCursors = [''];
  let cursor = '';

  for (let pageIndex = 0; pageIndex < maxPageIndex; pageIndex += 1) {
    const page = await fetchPage(cursor);

    if (page.done || page.cursor === '') {
      break;
    }

    pageStartCursors.push(page.cursor);
    cursor = page.cursor;
  }

  return pageStartCursors;
}

function isSortedLexicographic(values) {
  for (let i = 1; i < values.length; i += 1) {
    if (values[i - 1] > values[i]) {
      return false;
    }
  }
  return true;
}

async function scanKeysTerminates(prefix, limit, maxPages) {
  let cursor = '';

  for (let i = 0; i < maxPages; i += 1) {
    const page = await kv.scanKeys({ prefix, limit, cursor });
    if (page.done) {
      return true;
    }
    if (typeof page.cursor !== 'string' || page.cursor === '') {
      return false;
    }
    cursor = page.cursor;
  }

  return false;
}

// scanListConcurrency fires overlapping scan/list/scanKeys/listKeys requests
// while small writes are happening to validate concurrent pagination contracts.
export default async function scanListConcurrency() {
  const iteration = exec.scenario.iterationInTest;
  const scanStartPositions = Array.from(
    { length: SCAN_CONCURRENCY },
    (_, idx) => (iteration * SCAN_CONCURRENCY + idx) % TOTAL_INVOICES
  );
  const scanStartPageIndexes = scanStartPositions.map(
    (position) => Math.floor(position / SCAN_PAGE_SIZE)
  );
  const maxScanStartPageIndex = scanStartPageIndexes.reduce(
    (currentMax, pageIndex) => Math.max(currentMax, pageIndex),
    0
  );
  const pageStartCursors = await collectPageStartCursors(
    (cursor) => kv.scan({
      prefix: INVOICE_PREFIX,
      limit: SCAN_PAGE_SIZE,
      cursor
    }),
    maxScanStartPageIndex
  );

  const scanKeysStartPositions = Array.from(
    { length: SCAN_KEYS_CONCURRENCY },
    (_, idx) => (iteration * SCAN_KEYS_CONCURRENCY + idx) % TOTAL_INVOICES
  );
  const scanKeysStartPageIndexes = scanKeysStartPositions.map(
    (position) => Math.floor(position / SCAN_PAGE_SIZE)
  );
  const maxScanKeysStartPageIndex = scanKeysStartPageIndexes.reduce(
    (currentMax, pageIndex) => Math.max(currentMax, pageIndex),
    0
  );
  const scanKeysStartCursors = await collectPageStartCursors(
    (cursor) => kv.scanKeys({
      prefix: INVOICE_PREFIX,
      limit: SCAN_PAGE_SIZE,
      cursor
    }),
    maxScanKeysStartPageIndex
  );

  // Create concurrent scan tasks with different cursor positions.
  const scanTasks = Array.from({ length: SCAN_CONCURRENCY }, (_, idx) => {
    // Distribute scan starting pages across the keyspace.
    const startPageIndex = scanStartPageIndexes[idx];
    const cursor = pageStartCursors[startPageIndex] || '';

    return kv.scan({
      prefix: INVOICE_PREFIX,
      limit: SCAN_PAGE_SIZE,
      cursor
    }).then((page) => ({ type: 'scan', page }));
  });

  // Create concurrent scanKeys tasks with different cursor positions.
  const scanKeysTasks = Array.from({ length: SCAN_KEYS_CONCURRENCY }, (_, idx) => {
    const startPageIndex = scanKeysStartPageIndexes[idx];
    const cursor = scanKeysStartCursors[startPageIndex] || '';

    return kv.scanKeys({
      prefix: INVOICE_PREFIX,
      limit: SCAN_PAGE_SIZE,
      cursor
    }).then((page) => ({ type: 'scanKeys', page }));
  });

  // Create concurrent list tasks with different prefix shards.
  const listTasks = Array.from({ length: LIST_CONCURRENCY }, (_, idx) => {
    // Use first 2 digits of invoice ID as shard prefix.
    const invoiceNumber = (iteration * LIST_CONCURRENCY + idx) % TOTAL_INVOICES;
    const paddedNumber = String(invoiceNumber).padStart(KEY_PADDING, '0');
    const prefixShard = `${INVOICE_PREFIX}${paddedNumber.slice(0, 2)}`;

    return kv.list({
      prefix: prefixShard,
      limit: LIST_PAGE_SIZE
    }).then((entries) => ({ type: 'list', entries }));
  });

  // Create concurrent listKeys tasks with different prefix shards.
  const listKeysTasks = Array.from({ length: LIST_KEYS_CONCURRENCY }, (_, idx) => {
    const invoiceNumber = (iteration * LIST_KEYS_CONCURRENCY + idx) % TOTAL_INVOICES;
    const paddedNumber = String(invoiceNumber).padStart(KEY_PADDING, '0');
    const prefixShard = `${INVOICE_PREFIX}${paddedNumber.slice(0, 2)}`;

    return kv.listKeys({
      prefix: prefixShard,
      limit: LIST_PAGE_SIZE
    }).then((keys) => ({ type: 'listKeys', keys }));
  });

  // Add small concurrent mutations so reads and writes overlap.
  const mutationKey = `${MUTATION_PREFIX}${exec.vu.idInTest}:${iteration}`;
  const previousMutationKey = `${MUTATION_PREFIX}${exec.vu.idInTest}:${Math.max(iteration - 1, 0)}`;
  const mutationTasks = [
    kv.set(mutationKey, { iteration, vu: exec.vu.idInTest, ts: Date.now() }).then(() => ({ type: 'set' })),
    kv.deleteIfExists(previousMutationKey).then(() => ({ type: 'deleteIfExists' }))
  ];

  // Execute reads and writes concurrently.
  const outcomes = await Promise.all([
    ...scanTasks,
    ...scanKeysTasks,
    ...listTasks,
    ...listKeysTasks,
    ...mutationTasks
  ]);

  // Count total rows returned by scan operations.
  const scanOutcomes = outcomes.filter((result) => result.type === 'scan');
  const scanRows = scanOutcomes.reduce((sum, result) => sum + result.page.entries.length, 0);

  const scanKeysOutcomes = outcomes.filter((result) => result.type === 'scanKeys');
  const scanKeysRows = scanKeysOutcomes.reduce((sum, result) => sum + result.page.keys.length, 0);

  // Count total rows returned by list operations.
  const listRows = outcomes
    .filter((result) => result.type === 'list')
    .reduce((sum, result) => sum + result.entries.length, 0);
  const listKeysRows = outcomes
    .filter((result) => result.type === 'listKeys')
    .reduce((sum, result) => sum + result.keys.length, 0);

  // Validate bounded termination over a shard-sized prefix.
  const terminationSeed = String(iteration % TOTAL_INVOICES).padStart(KEY_PADDING, '0');
  const terminationPrefixLen = Math.min(KEY_PADDING, 5);
  const terminationPrefix = `${INVOICE_PREFIX}${terminationSeed.slice(0, terminationPrefixLen)}`;
  const scanTerminates = await scanKeysTerminates(terminationPrefix, Math.max(1, Math.floor(LIST_PAGE_SIZE / 2)), 12);

  const scanCursorShape = scanOutcomes.every((result) => (
    typeof result.page.cursor === 'string'
    && typeof result.page.done === 'boolean'
    && (!result.page.done || result.page.cursor === '')
    && isSortedLexicographic(result.page.entries.map((entry) => entry.key))
  ));
  const scanKeysCursorShape = scanKeysOutcomes.every((result) => (
    typeof result.page.cursor === 'string'
    && typeof result.page.done === 'boolean'
    && (!result.page.done || result.page.cursor === '')
    && isSortedLexicographic(result.page.keys)
  ));
  const scanKeysKeyOnly = scanKeysOutcomes.every((result) => (
    Array.isArray(result.page.keys)
    && !Object.prototype.hasOwnProperty.call(result.page, 'entries')
    && result.page.keys.every((key) => typeof key === 'string')
  ));
  const listKeysKeyOnly = outcomes
    .filter((result) => result.type === 'listKeys')
    .every((result) => (
      Array.isArray(result.keys)
      && result.keys.every((key) => typeof key === 'string')
      && isSortedLexicographic(result.keys)
    ));

  check(true, {
    'scan:non-empty': () => scanRows > 0,
    'scan:cursor-shape': () => scanCursorShape,
    'scanKeys:non-empty': () => scanKeysRows > 0,
    'scanKeys:cursor-shape': () => scanKeysCursorShape,
    'scanKeys:key-only': () => scanKeysKeyOnly,
    'scanKeys:terminates-bounded': () => scanTerminates,
    'list:non-empty': () => listRows > 0,
    'listKeys:non-empty': () => listKeysRows > 0,
    'listKeys:key-only': () => listKeysKeyOnly
  });
}
