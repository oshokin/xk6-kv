import { check } from 'k6';
import exec from 'k6/execution';
import { VUS, ITERATIONS, createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: ORPHANED RESOURCE CLAIMING
// =============================================================================
//
// Background workers claim stale jobs using deleteIfExists() while observers poll
// exists() to surface dashboards. Everyone fires Promise.all batches so the
// delete + exists promises complete concurrently.
//
// REAL-WORLD PROBLEMS UNCOVERED:
// - Multiple claimers succeeding at once (double processing).
// - Observers reading half-deleted state due to promise race conditions.
// - Event-loop storms when delete + exists both return at the same time.
//
// ATOMIC OPERATIONS TESTED:
// - set(): seed a job.
// - deleteIfExists(): claim the job once.
// - exists(): watch the key transition for monitoring.

// Prefix for orphaned resource keys.
const RESOURCE_PREFIX = __ENV.RESOURCE_PREFIX || 'orphan:';

// Number of orphaned resources in the initial pool.
const RESOURCE_POOL_SIZE = parseInt(__ENV.RESOURCE_POOL_SIZE || '1000', 10);

// Number of concurrent workers attempting to claim each resource via deleteIfExists().
const CONCURRENT_CLAIMERS = parseInt(__ENV.CONCURRENT_CLAIMERS || '12', 10);

// Number of concurrent observers polling exists() to monitor state transitions.
const CONCURRENT_WATCHERS = parseInt(__ENV.CONCURRENT_WATCHERS || '18', 10);

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'orphan-resource-claim';

// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME);

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: VUS,
  iterations: ITERATIONS,
  thresholds: {
    'checks{claim:single-winner}': ['rate>0.999'],
    'checks{claim:fully-removed}': ['rate>0.999']
  }
};

// setup seeds the pool of orphaned resources that workers will compete to claim.
export async function setup() {
  const standardSetup = createSetup(kv);
  await standardSetup();

  // Seed orphaned resources for workers to claim.
  for (let i = 0; i < RESOURCE_POOL_SIZE; i++) {
    const resourceKey = `${RESOURCE_PREFIX}${i}`;
    await kv.set(resourceKey, {
      id: resourceKey,
      createdAt: Date.now(),
      stale: true
    });
  }
}

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv, TEST_NAME);

// orphanResourceClaim simulates workers racing to claim orphaned resources from a pool.
// Multiple claimers try deleteIfExists() concurrently while observers monitor with exists().
export default async function orphanResourceClaim() {
  const iteration = exec.scenario.iterationInTest;

  // Pick a resource from the pool (wraps around).
  const resourceKey = `${RESOURCE_PREFIX}${iteration % RESOURCE_POOL_SIZE}`;

  // Try to read the resource first to verify it exists.
  let resourceSnapshot;
  try {
    resourceSnapshot = await kv.get(resourceKey);
  } catch (err) {
    // Resource already claimed by another worker.
    resourceSnapshot = null;
  }

  // Skip if already claimed.
  if (!resourceSnapshot) {
    return;
  }

  // Split watchers: half before delete, half after.
  const watchersBeforeCount = Math.ceil(CONCURRENT_WATCHERS / 2);
  const watchersAfterCount = CONCURRENT_WATCHERS - watchersBeforeCount;

  // Race: observers check existence while claimers try to delete.
  const midStage = await Promise.all([
    ...Array.from({ length: watchersBeforeCount }, () =>
      kv.exists(resourceKey).then((ok) => ({ type: 'watch-before', ok }))
    ),
    ...Array.from({ length: CONCURRENT_CLAIMERS }, () =>
      kv.deleteIfExists(resourceKey).then((ok) => ({ type: 'claim', ok }))
    )
  ]);

  // Additional observers after the deletion race.
  const afterStage = await Promise.all(
    Array.from({ length: watchersAfterCount }, () =>
      kv.exists(resourceKey).then((ok) => ({ type: 'watch-after', ok }))
    )
  );

  const outcomes = [...midStage, ...afterStage];

  // Count successful claims and watcher observations.
  const claimWins = outcomes.filter((result) => result.type === 'claim' && result.ok).length;
  const watchersBeforeTrue = outcomes.filter(
    (result) => result.type === 'watch-before' && result.ok
  ).length;
  const watchersAfterTrue = outcomes.filter(
    (result) => result.type === 'watch-after' && result.ok
  ).length;

  // Final verification: resource should be gone.
  const existsAfter = await kv.exists(resourceKey);

  check(true, {
    'claim:single-winner': () => claimWins === 1,
    'claim:fully-removed': () => !existsAfter,
    'claim:observers-saw-transition': () =>
      watchersBeforeTrue > 0 || watchersAfterTrue < watchersAfterCount
  });
}
