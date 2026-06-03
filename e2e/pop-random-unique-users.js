import { check } from 'k6';
import { VUS, ITERATIONS, createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: UNIQUE USER ALLOCATION VIA POP
// =============================================================================
//
// This test simulates high-concurrency user allocation where each worker/VU must
// receive a unique account exactly once. It's a common pattern in:
//
// - Login/load tests with one-time test users.
// - Voucher/code redemption workloads.
// - Batch job queues where each payload must be consumed once.
//
// REAL-WORLD PROBLEM SOLVED:
// If random allocation isn't atomic, two workers can receive the same user and
// produce duplicate actions (double checkout, duplicate login, dirty data).
//
// ATOMIC OPERATIONS TESTED:
// - popRandom(): atomically pick and remove one user.
// - incrementBy(): detect duplicate allocations by key.
// - exists(): verify popped key is physically removed.
//
// CONCURRENCY PATTERN:
// - Many VUs pop from the same pool simultaneously.
// - Pool exhaustion is expected and treated as a normal terminal state.
//
// PERFORMANCE CHARACTERISTICS:
// - High contention on a bounded pool.
// - Safety requirement: no duplicate successful allocations.

// Key prefix for allocatable users.
const USER_PREFIX = __ENV.USER_PREFIX || 'pop-random:user:';

// Number of users pre-seeded into the allocation pool.
const USER_POOL_SIZE = parseInt(__ENV.USER_POOL_SIZE || '500', 10);

// Prefix for allocation counters used to detect duplicates.
const ALLOC_COUNTER_PREFIX = __ENV.ALLOC_COUNTER_PREFIX || 'alloc-count:';

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'pop-random-unique-users';

// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME);

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: VUS,
  iterations: ITERATIONS,
  thresholds: {
    'checks{pop:unique}': ['rate>0.999'],
    'checks{pop:deleted}': ['rate>0.999'],
  },
};

// setup clears old keys and seeds deterministic user pool.
export async function setup() {
  const baseSetup = createSetup(kv);
  await baseSetup();

  for (let i = 0; i < USER_POOL_SIZE; i += 1) {
    await kv.set(`${USER_PREFIX}${i}`, {
      id: i,
      username: `user-${i}`,
    });
  }
}

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv);

// popRandomUniqueUsers atomically allocates unique users until the pool is exhausted,
// then treats exhaustion as a normal terminal state.
export default async function popRandomUniqueUsers() {
  const entry = await kv.popRandom({ prefix: USER_PREFIX });
  if (entry === null) {
    // Exhaustion is normal when iterations outnumber pool size.
    return;
  }

  const allocationCount = await kv.incrementBy(`${ALLOC_COUNTER_PREFIX}${entry.key}`, 1);
  const stillExists = await kv.exists(entry.key);

  check(entry, {
    'pop:unique': () => allocationCount === 1,
    'pop:deleted': () => stillExists === false,
  });
}
