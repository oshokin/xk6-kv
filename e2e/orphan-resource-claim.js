import { check, sleep } from 'k6';
import exec from 'k6/execution';
import { openKv } from 'k6/x/kv';

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

// Selected backend (memory or disk) used for orphan resource state.
const SELECTED_BACKEND_NAME = __ENV.KV_BACKEND || 'memory';
// Enables in-memory key tracking when the backend is memory.
const TRACK_KEYS_OVERRIDE =
  typeof __ENV.KV_TRACK_KEYS === 'string' ? __ENV.KV_TRACK_KEYS.toLowerCase() : '';
const ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND =
  TRACK_KEYS_OVERRIDE === '' ? true : TRACK_KEYS_OVERRIDE === 'true';

// RESOURCE_PREFIX namespaces the synthetic jobs.
const RESOURCE_PREFIX = __ENV.RESOURCE_PREFIX || 'orphan:';
// CLAIMERS_PER_BATCH controls number of deleteIfExists contenders.
const CLAIMERS_PER_BATCH = parseInt(__ENV.CLAIMERS_PER_BATCH || '12', 10);
// WATCHERS_PER_BATCH determines how many exists() polls occur in parallel.
const WATCHERS_PER_BATCH = parseInt(__ENV.WATCHERS_PER_BATCH || '18', 10);
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

// Number of distinct orphan keys that cycle through the race.
const RESOURCE_RING_SIZE = parseInt(__ENV.RESOURCE_RING_SIZE || '400', 10);

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: DEFAULT_VUS,
  iterations: DEFAULT_ITERATIONS,
  thresholds: {
    'checks{claim:single-winner}': ['rate>0.999'],
    'checks{claim:fully-removed}': ['rate>0.999']
  }
};

// setup wipes older resources before the claiming races start.
export async function setup() {
  await kv.clear();
}

// teardown closes disk stores post-run.
export async function teardown() {
  if (SELECTED_BACKEND_NAME === 'disk') {
    kv.close();
  }
}

// orphanResourceClaim seeds a job, races watchers vs. claimers via Promise.all,
// and verifies exactly one worker deleted the resource.
export default async function orphanResourceClaim() {
  const iteration = exec.scenario.iterationInTest;
  const resourceKey = `${RESOURCE_PREFIX}${iteration % RESOURCE_RING_SIZE}`;

  await kv.set(resourceKey, {
    id: resourceKey,
    createdAt: Date.now(),
    payload: `workload-${exec.vu.idInInstance}`
  });

  const watchersBeforeCount = Math.ceil(WATCHERS_PER_BATCH / 2);
  const watchersAfterCount = WATCHERS_PER_BATCH - watchersBeforeCount;

  const midStage = await Promise.all([
    ...Array.from({ length: watchersBeforeCount }, () =>
      kv.exists(resourceKey).then((ok) => ({ type: 'watch-before', ok }))
    ),
    ...Array.from({ length: CLAIMERS_PER_BATCH }, () =>
      kv.deleteIfExists(resourceKey).then((ok) => ({ type: 'claim', ok }))
    )
  ]);

  const afterStage = await Promise.all(
    Array.from({ length: watchersAfterCount }, () =>
      kv.exists(resourceKey).then((ok) => ({ type: 'watch-after', ok }))
    )
  );

  const outcomes = [...midStage, ...afterStage];

  const claimWins = outcomes.filter((result) => result.type === 'claim' && result.ok).length;
  const watchersBeforeTrue = outcomes.filter(
    (result) => result.type === 'watch-before' && result.ok
  ).length;
  const watchersAfterTrue = outcomes.filter(
    (result) => result.type === 'watch-after' && result.ok
  ).length;

  const existsAfter = await kv.exists(resourceKey);

  check(true, {
    'claim:single-winner': () => claimWins === 1,
    'claim:fully-removed': () => !existsAfter,
    'claim:observers-saw-transition': () =>
      watchersBeforeTrue > 0 || watchersAfterTrue < watchersAfterCount
  });

  // Simulate work long enough to keep the scenario under load.
  sleep(BASE_IDLE_SLEEP_SECONDS + Math.random() * IDLE_SLEEP_JITTER_SECONDS);
}

