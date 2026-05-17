import { check } from 'k6';
import exec from 'k6/execution';
import { VUS, ITERATIONS, createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: BATCH WORK QUEUE DRAIN WITH RESERVED ITEMS
// =============================================================================
//
// This test models batch workers draining a shared work queue with popRandomMany()
// while another component temporarily reserves one item with claimRandom().
//
// REAL-WORLD PROBLEM SOLVED:
// In mixed workloads (batch pop + lease reservations), batch consumers must:
// - respect configured batch bounds;
// - avoid touching currently reserved/live-claimed items;
// - delete popped items durably;
// - avoid duplicate processing across workers.
//
// ATOMIC OPERATIONS TESTED:
// - claimRandom(): reserve one in-flight item.
// - popRandomMany(): pop up to N free items in one call.
// - releaseClaim(): release reserved item after batch drain.
// - exists(), incrementBy(): verify delete semantics and duplicate safety.
//
// CONCURRENCY PATTERN:
// - Many VUs contend on one work prefix with bounded batch sizes.
// - Exhaustion is normal once the queue is drained.
//
// PERFORMANCE CHARACTERISTICS:
// - High contention batch allocation/deletion workload.
// - Claim-aware filtering exercised on every iteration.

// Prefix for work items consumed by batch workers.
const WORK_PREFIX = __ENV.WORK_PREFIX || 'batch-pop:work:';
// Prefix for non-target keys used to verify prefix isolation.
const RESERVED_PREFIX = __ENV.RESERVED_PREFIX || 'batch-pop:reserved:';
// Number of work items seeded for batch draining.
const WORK_POOL_SIZE = parseInt(__ENV.WORK_POOL_SIZE || '600', 10);
// Number of non-target keys seeded under RESERVED_PREFIX.
const RESERVED_POOL_SIZE = parseInt(__ENV.RESERVED_POOL_SIZE || '20', 10);
// Maximum batch size requested from popRandomMany().
const BATCH_SIZE = parseInt(__ENV.BATCH_SIZE || '8', 10);
// Lease duration used for temporary reserved work item.
const CLAIM_TTL_MS = parseInt(__ENV.CLAIM_TTL_MS || '15000', 10);
// Prefix for duplicate-detection counters.
const DUP_COUNTER_PREFIX = __ENV.DUP_COUNTER_PREFIX || 'batch-pop:dup-counter:';

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'batch-job-drain-pop-random-many';
// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME);

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: VUS,
  iterations: ITERATIONS,
  thresholds: {
    'checks{pop_many:attempted}': ['rate>0.99'],
    'checks{pop_many:batch-bounded}': ['rate>0.99'],
    'checks{pop_many:prefix-isolation}': ['rate>0.99'],
    'checks{pop_many:claimed-skip}': ['rate>0.99'],
    'checks{pop_many:deleted}': ['rate>0.99'],
    'checks{pop_many:no-duplicates}': ['rate>0.99'],
    'checks{pop_many:reserve-release}': ['rate>0.99'],
  },
};

// setup clears old keys and seeds deterministic work + reserved pools.
export async function setup() {
  const baseSetup = createSetup(kv);
  await baseSetup();

  for (let i = 0; i < WORK_POOL_SIZE; i += 1) {
    await kv.set(`${WORK_PREFIX}${i + 1}`, {
      id: i + 1,
      payload: `work-${i + 1}`,
      type: 'work',
    });
  }

  for (let i = 0; i < RESERVED_POOL_SIZE; i += 1) {
    await kv.set(`${RESERVED_PREFIX}${i + 1}`, {
      id: i + 1,
      payload: `reserved-${i + 1}`,
      type: 'reserved',
    });
  }
}

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv, TEST_NAME);

// batchJobDrainPopRandomMany drains work in bounded batches while respecting
// one temporarily reserved item claimed at the start of the iteration.
export default async function batchJobDrainPopRandomMany() {
  const reservedClaim = await kv.claimRandom({
    prefix: WORK_PREFIX,
    owner: `reserved:vu:${exec.vu.idInTest}`,
    ttl: CLAIM_TTL_MS,
  });

  const entries = await kv.popRandomMany({
    prefix: WORK_PREFIX,
    count: BATCH_SIZE,
  });

  check(true, {
    'pop_many:attempted': () => true,
  });

  if (entries.length === 0) {
    let reserveReleased = true;
    if (reservedClaim !== null) {
      reserveReleased = await kv.releaseClaim(reservedClaim);
    }

    check(true, {
      'pop_many:reserve-release': () => reserveReleased,
    });

    return;
  }

  let allFromWorkPrefix = true;
  let allDeleted = true;
  let skippedReservedClaim = true;
  let noDuplicates = true;
  const seenInBatch = new Set();

  for (const entry of entries) {
    if (!entry.key.startsWith(WORK_PREFIX)) {
      allFromWorkPrefix = false;
    }

    if (reservedClaim !== null && entry.key === reservedClaim.key) {
      skippedReservedClaim = false;
    }

    if (seenInBatch.has(entry.key)) {
      noDuplicates = false;
    } else {
      seenInBatch.add(entry.key);
    }

    const stillExists = await kv.exists(entry.key);
    if (stillExists) {
      allDeleted = false;
    }

    const duplicateCount = await kv.incrementBy(`${DUP_COUNTER_PREFIX}${entry.key}`, 1);
    if (duplicateCount !== 1) {
      noDuplicates = false;
    }
  }

  let reserveReleased = true;
  if (reservedClaim !== null) {
    reserveReleased = await kv.releaseClaim(reservedClaim);
  }

  let reservedPrefixIntact = true;
  if (RESERVED_POOL_SIZE > 0) {
    reservedPrefixIntact = await kv.exists(`${RESERVED_PREFIX}1`);
  }

  check(entries, {
    'pop_many:batch-bounded': (items) => items.length <= BATCH_SIZE,
    'pop_many:prefix-isolation': () => allFromWorkPrefix && reservedPrefixIntact,
    'pop_many:claimed-skip': () => reservedClaim === null || skippedReservedClaim,
    'pop_many:deleted': () => allDeleted,
    'pop_many:no-duplicates': () => noDuplicates,
    'pop_many:reserve-release': () => reserveReleased,
  });
}
