import { check } from 'k6';
import exec from 'k6/execution';
import { VUS, ITERATIONS, createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: KEY-ONLY CURSOR PAGINATION
// =============================================================================
//
// This scenario validates scanKeys() as the key-only equivalent of scan():
// - paginate keys with cursor continuation
// - keep lexicographic ordering
// - hydrate selected keys with getMany()
//
// REAL-WORLD PROBLEM SOLVED:
// Large keyspaces need cheap key-only paging before selective hydration.
//
// OPERATIONS TESTED:
// - setMany(): seed deterministic key batches
// - scanKeys(): key-only cursor pagination
// - getMany(): hydrate keys returned by scanKeys()

// Prefix namespace for per-worker scan shards.
const SCAN_KEYS_PREFIX = __ENV.SCAN_KEYS_PREFIX || 'scan-keys:tenant:';

// Number of deterministic slots reused across iterations.
const SLOT_RANGE = parseInt(__ENV.SCAN_KEYS_SLOT_RANGE || '1000', 10);

// Maximum keys returned per scanKeys() page.
const PAGE_SIZE = parseInt(__ENV.SCAN_KEYS_PAGE_SIZE || '2', 10);

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'scan-keys-pagination';

const kv = createKv(TEST_NAME);

export const options = {
  vus: VUS,
  iterations: ITERATIONS,
  thresholds: {
    'checks{scanKeys:page-shape}': ['rate>0.999'],
    'checks{scanKeys:paginates-prefix}': ['rate>0.999'],
    'checks{scanKeys:hydrates-returned-keys}': ['rate>0.999']
  }
};

// setup clears previous state before scan loops start.
export const setup = createSetup(kv);

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv, TEST_NAME);

export default async function scanKeysPagination() {
  const vuId = exec.vu.idInTest;
  const iteration = exec.scenario.iterationInTest;
  const slot = iteration % SLOT_RANGE;

  const rootPrefix = `${SCAN_KEYS_PREFIX}${vuId}:${slot}:`;
  const userPrefix = `${rootPrefix}user:`;

  const user1 = `${userPrefix}1`;
  const user2 = `${userPrefix}2`;
  const user3 = `${userPrefix}3`;
  const order1 = `${rootPrefix}order:1`;

  await kv.setMany({
    [user1]: { id: 1 },
    [user2]: { id: 2 },
    [user3]: { id: 3 },
    [order1]: { id: 10 }
  });

  let cursor = '';
  const keys = [];

  do {
    const page = await kv.scanKeys({
      prefix: userPrefix,
      cursor,
      limit: PAGE_SIZE
    });

    check(page, {
      'scanKeys:page-shape': (result) =>
        result &&
        Array.isArray(result.keys) &&
        typeof result.cursor === 'string' &&
        typeof result.done === 'boolean'
    });

    keys.push(...page.keys);
    cursor = page.cursor;
  } while (cursor !== '');

  check(keys, {
    'scanKeys:paginates-prefix': (result) =>
      JSON.stringify(result) === JSON.stringify([user1, user2, user3])
  });

  const values = await kv.getMany(keys);
  check(values, {
    'scanKeys:hydrates-returned-keys': (items) =>
      Array.isArray(items) &&
      items.length === 3 &&
      items.every((item) => item && item.exists === true)
  });
}
