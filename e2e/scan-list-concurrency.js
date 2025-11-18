import { check, sleep } from 'k6';
import exec from 'k6/execution';
import { openKv } from 'k6/x/kv';
import encoding from 'k6/encoding';

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

// SELECTED_BACKEND_NAME picks the backend we stress (memory or disk).
const SELECTED_BACKEND_NAME = __ENV.KV_BACKEND || 'memory';
// TRACK_KEYS_OVERRIDE mirrors other scenarios for OSTree toggling.
const TRACK_KEYS_OVERRIDE =
  typeof __ENV.KV_TRACK_KEYS === 'string' ? __ENV.KV_TRACK_KEYS.toLowerCase() : '';
const ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND =
  TRACK_KEYS_OVERRIDE === '' ? true : TRACK_KEYS_OVERRIDE === 'true';

const INVOICE_PREFIX = __ENV.INVOICE_PREFIX || 'invoice:';
const TOTAL_INVOICES = parseInt(__ENV.TOTAL_INVOICES || '2000', 10);
const SCAN_CONCURRENCY = parseInt(__ENV.SCAN_CONCURRENCY || '6', 10);
const LIST_CONCURRENCY = parseInt(__ENV.LIST_CONCURRENCY || '6', 10);
const PAGE_LIMIT = parseInt(__ENV.PAGE_LIMIT || '120', 10);
// Width (digits) used when padding invoice IDs.
const KEY_PAD_WIDTH = parseInt(__ENV.KEY_PAD_WIDTH || '6', 10);
// Number of tenant shards used when seeding invoices.
const TENANT_SHARDS = parseInt(__ENV.TENANT_SHARDS || '25', 10);
// Modulo applied to invoice amounts during seeding.
const AMOUNT_MODULO = parseInt(__ENV.AMOUNT_MODULO || '97', 10);
// Offset added to ensure all invoice amounts remain positive.
const AMOUNT_OFFSET = parseInt(__ENV.AMOUNT_OFFSET || '10', 10);
// Spread used to choose scan anchor points across the keyspace.
const SCAN_ANCHOR_SPREAD = parseInt(__ENV.SCAN_ANCHOR_SPREAD || '17', 10);
// Spread used to choose list prefixes across the keyspace.
const LIST_SEED_SPREAD = parseInt(__ENV.LIST_SEED_SPREAD || '37', 10);
// Maximum number of entries returned by each list() call.
const LIST_LIMIT = Math.floor(PAGE_LIMIT / 2);
// Base duration (seconds) each iteration sleeps after processing.
const BASE_IDLE_SLEEP_SECONDS = parseFloat(__ENV.BASE_IDLE_SLEEP_SECONDS || '0.02');
// Random jitter (seconds) added to the base idle sleep.
const IDLE_SLEEP_JITTER_SECONDS = parseFloat(
  __ENV.IDLE_SLEEP_JITTER_SECONDS || '0.01'
);
// Default number of VUs used by the scenario.
const DEFAULT_VUS = parseInt(__ENV.VUS || '40', 10);
// Default iteration count used by the scenario.
const DEFAULT_ITERATIONS = parseInt(__ENV.ITERATIONS || '400', 10);

// kv is the shared store client used throughout the scenario.
const kv = openKv(
  SELECTED_BACKEND_NAME === 'disk'
    ? { backend: 'disk', trackKeys: ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND }
    : { backend: 'memory', trackKeys: ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND }
);

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: DEFAULT_VUS,
  iterations: DEFAULT_ITERATIONS,
  thresholds: {
    'checks{scan:non-empty}': ['rate>0.999'],
    'checks{list:non-empty}': ['rate>0.999']
  }
};

// setup seeds a deterministic invoice dataset before contention begins.
export async function setup() {
  await kv.clear();

  for (let i = 0; i < TOTAL_INVOICES; i += 1) {
    const padded = String(i).padStart(KEY_PAD_WIDTH, '0');
    await kv.set(`${INVOICE_PREFIX}${padded}`, {
      invoiceId: padded,
      tenant: `tenant-${i % TENANT_SHARDS}`,
      amount: (i % AMOUNT_MODULO) + AMOUNT_OFFSET,
      createdAt: Date.now()
    });
  }
}

// teardown closes disk stores after the run.
export async function teardown() {
  if (SELECTED_BACKEND_NAME === 'disk') {
    kv.close();
  }
}

// encodeCursor mimics the cursor encoding performed by Scan().
// encodeCursor wraps encoding.b64encode for readability inside scan tasks.
function encodeCursor(key) {
  return encoding.b64encode(key, 'utf-8');
}

// scanListConcurrency fires overlapping scan() and list() requests (via
// Promise.all) to ensure Sobek handles concurrent pagination results.
export default async function scanListConcurrency() {
  const iteration = exec.scenario.iterationInTest;

  const scanTasks = Array.from({ length: SCAN_CONCURRENCY }, (_, idx) => {
    const anchor =
      (iteration * SCAN_CONCURRENCY + idx * SCAN_ANCHOR_SPREAD) % TOTAL_INVOICES;
    const anchorKey = `${INVOICE_PREFIX}${String(anchor).padStart(KEY_PAD_WIDTH, '0')}`;
    const cursor = idx === 0 ? '' : encodeCursor(anchorKey);

    return kv.scan({
      prefix: INVOICE_PREFIX,
      limit: PAGE_LIMIT,
      cursor
    }).then((page) => ({ type: 'scan', page }));
  });

  const listTasks = Array.from({ length: LIST_CONCURRENCY }, (_, idx) => {
    const shardSeed = String(
      (iteration * LIST_CONCURRENCY + idx * LIST_SEED_SPREAD) % TOTAL_INVOICES
    ).padStart(KEY_PAD_WIDTH, '0');
    const prefixShard = `${INVOICE_PREFIX}${shardSeed.slice(0, 2)}`;

    return kv.list({
      prefix: prefixShard,
      limit: LIST_LIMIT
    }).then((entries) => ({ type: 'list', entries }));
  });

  const outcomes = await Promise.all([...scanTasks, ...listTasks]);

  const scanRows = outcomes
    .filter((result) => result.type === 'scan')
    .reduce((sum, result) => sum + result.page.entries.length, 0);

  const listRows = outcomes
    .filter((result) => result.type === 'list')
    .reduce((sum, result) => sum + result.entries.length, 0);

  check(true, {
    'scan:non-empty': () => scanRows > 0,
    'list:non-empty': () => listRows > 0
  });

  // Simulate work long enough to keep the scenario under load.
  sleep(BASE_IDLE_SLEEP_SECONDS + Math.random() * IDLE_SLEEP_JITTER_SECONDS);
}

