import { check } from 'k6';
import exec from 'k6/execution';
import encoding from 'k6/encoding';
import { VUS, ITERATIONS, createKv, createTeardown } from './common.js';

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
// - set(): deterministic dataset seeding.

// Invoice key prefix.
const INVOICE_PREFIX = __ENV.INVOICE_PREFIX || 'invoice:';

// Total number of invoice records seeded during setup.
const TOTAL_INVOICES = parseInt(__ENV.TOTAL_INVOICES || '2000', 10);

// Number of concurrent scan() operations per iteration.
const SCAN_CONCURRENCY = parseInt(__ENV.SCAN_CONCURRENCY || '6', 10);

// Number of concurrent list() operations per iteration.
const LIST_CONCURRENCY = parseInt(__ENV.LIST_CONCURRENCY || '6', 10);

// Maximum entries returned per scan() page.
const SCAN_PAGE_SIZE = parseInt(__ENV.SCAN_PAGE_SIZE || '120', 10);

// Maximum entries returned per list() call (half of scan page size).
const LIST_PAGE_SIZE = Math.floor(SCAN_PAGE_SIZE / 2);

// Width of zero-padded invoice IDs (e.g., 6 = invoice:000042).
const KEY_PADDING = 6;

// Number of tenant shards for invoice distribution.
const TENANT_COUNT = 25;

// kv is the shared store client used throughout the scenario.
const kv = createKv();

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: VUS,
  iterations: ITERATIONS,
  thresholds: {
    'checks{scan:non-empty}': ['rate>0.999'],
    'checks{list:non-empty}': ['rate>0.999']
  }
};

// setup seeds a deterministic invoice dataset before contention begins.
export async function setup() {
  await kv.clear();

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

// encodeCursor mimics the cursor encoding performed by Scan().
// encodeCursor wraps encoding.b64encode for readability inside scan tasks.
function encodeCursor(key) {
  return encoding.b64encode(key, 'utf-8');
}

// scanListConcurrency fires overlapping scan() and list() requests (via
// Promise.all) to ensure Sobek handles concurrent pagination results.
export default async function scanListConcurrency() {
  const iteration = exec.scenario.iterationInTest;

  // Create concurrent scan tasks with different cursor positions.
  const scanTasks = Array.from({ length: SCAN_CONCURRENCY }, (_, idx) => {
    // Distribute scan starting points across the keyspace.
    const startPosition = (iteration * SCAN_CONCURRENCY + idx) % TOTAL_INVOICES;
    const startKey = `${INVOICE_PREFIX}${String(startPosition).padStart(KEY_PADDING, '0')}`;
    const cursor = idx === 0 ? '' : encodeCursor(startKey);

    return kv.scan({
      prefix: INVOICE_PREFIX,
      limit: SCAN_PAGE_SIZE,
      cursor
    }).then((page) => ({ type: 'scan', page }));
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

  // Execute all scan and list operations concurrently.
  const outcomes = await Promise.all([...scanTasks, ...listTasks]);

  // Count total rows returned by scan operations.
  const scanRows = outcomes
    .filter((result) => result.type === 'scan')
    .reduce((sum, result) => sum + result.page.entries.length, 0);

  // Count total rows returned by list operations.
  const listRows = outcomes
    .filter((result) => result.type === 'list')
    .reduce((sum, result) => sum + result.entries.length, 0);

  check(true, {
    'scan:non-empty': () => scanRows > 0,
    'list:non-empty': () => listRows > 0
  });
}
