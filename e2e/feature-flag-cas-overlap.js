import { check } from 'k6';
import exec from 'k6/execution';
import { VUS, ITERATIONS, createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: ADMIN FEATURE FLAG ROLLOUT
// =============================================================================
//
// Multiple release engineers tweak the same feature flag rollout percentage via
// compareAndSwap(). They do not await each request sequentially; instead they
// launch Promise.all batches to reproduce admin tooling that fires updates in
// quick bursts.
//
// REAL-WORLD PROBLEMS UNCOVERED:
// - CAS starvation when promise resolution races, causing endless retries.
// - Partial updates where some admins think a change landed but it never did.
// - Regression back to concurrent map writes when resolve/reject runs off loop.
//
// ATOMIC OPERATIONS TESTED:
// - compareAndSwap(): update rollout + version atomically.
// - get(): read current flag snapshot before CAS.
//
// CONCURRENCY PATTERN:
// - Every iteration fires concurrent CAS attempts with alternating deltas (+1, -1).
// - Each call retries until CAS succeeds (testing extreme contention without backoff).
// - High retry limit compensates for lack of sleep/backoff in tight retry loop.

// Feature flag key that all admins compete to update.
const FLAG_KEY = __ENV.FLAG_KEY || 'feature:payments';

// Number of concurrent CAS operations per iteration (simulates admin dashboard batch updates).
const CONCURRENT_UPDATES = parseInt(__ENV.CONCURRENT_UPDATES || '4', 10);

// Maximum compareAndSwap retry attempts (very high to compensate for no backoff/sleep).
const MAX_CAS_RETRIES = parseInt(__ENV.MAX_CAS_RETRIES || '50000', 10);

// Rollout percentage range (0-99).
const ROLLOUT_MAX = 100;

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'feature-flag-cas-overlap';

// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME);

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: VUS,
  iterations: ITERATIONS,
  thresholds: {
    'checks{cas:all-succeeded}': ['rate>0.995']
  }
};

// setup seeds the baseline flag before CAS storms begin.
export async function setup() {
  const standardSetup = createSetup(kv);
  await standardSetup();

  await kv.set(FLAG_KEY, {
    rollout: 10,
    version: 0,
    updatedBy: 'bootstrap'
  });
}

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv, TEST_NAME);

// featureFlagCasOverlap launches Promise.all CAS operations and verifies they all
// succeeded (unique version numbers recorded).
export default async function featureFlagCasOverlap() {
  // Alternating deltas simulate different admins adjusting rollout percentages.
  const deltas = Array.from({ length: CONCURRENT_UPDATES }, (_, idx) =>
    idx % 2 === 0 ? 1 : -1
  );

  const versions = await Promise.all(deltas.map((delta) => applyRolloutShift(delta)));

  const uniqueVersions = new Set(versions);

  check(true, {
    'cas:all-succeeded': () => uniqueVersions.size === deltas.length
  });
}

// applyRolloutShift retries compareAndSwap until it succeeds or exhausts attempts.
async function applyRolloutShift(delta) {
  for (let attempt = 0; attempt < MAX_CAS_RETRIES; attempt += 1) {
    const snapshot = await kv.get(FLAG_KEY);
    const proposed = {
      ...snapshot,
      rollout: (snapshot.rollout + delta + ROLLOUT_MAX) % ROLLOUT_MAX,
      version: snapshot.version + 1,
      updatedBy: `vu-${exec.vu.idInInstance}`
    };

    const swapped = await kv.compareAndSwap(FLAG_KEY, snapshot, proposed);
    if (swapped) {
      return proposed.version;
    }
  }

  throw new Error('CAS did not converge within max attempts');
}
