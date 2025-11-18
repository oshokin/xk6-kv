import { check, sleep } from 'k6';
import exec from 'k6/execution';
import { openKv } from 'k6/x/kv';

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
// - Every iteration fires ADMIN_UPDATES_PER_TICK concurrent CAS attempts with
//   alternating deltas (+5, -3, etc). Each call retries until CAS succeeds.

// Selected backend (memory or disk) used for feature-flag state.
const SELECTED_BACKEND_NAME = __ENV.KV_BACKEND || 'memory';
// Enables in-memory key tracking when the backend is memory.
const TRACK_KEYS_OVERRIDE =
  typeof __ENV.KV_TRACK_KEYS === 'string' ? __ENV.KV_TRACK_KEYS.toLowerCase() : '';
const ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND =
  TRACK_KEYS_OVERRIDE === '' ? true : TRACK_KEYS_OVERRIDE === 'true';

// FLAG_KEY identifies the shared feature flag under test.
const FLAG_KEY = __ENV.CAS_FLAG_KEY || 'feature:payments';
// ADMIN_UPDATES_PER_TICK controls how many CAS updates fire per iteration.
const DEFAULT_UPDATES_PER_TICK = SELECTED_BACKEND_NAME === 'disk' ? '4' : '6';
const ADMIN_UPDATES_PER_TICK = parseInt(
  __ENV.CAS_UPDATES_PER_TICK || DEFAULT_UPDATES_PER_TICK,
  10
);
// MAX_CAS_ATTEMPTS bounds the retry loop before surfacing an error.
const MAX_CAS_ATTEMPTS = parseInt(__ENV.CAS_MAX_ATTEMPTS || '400', 10);
// Positive delta applied to rollout percentage for alternating admins.
const POSITIVE_ROLLOUT_DELTA = parseInt(__ENV.POSITIVE_ROLLOUT_DELTA || '5', 10);
// Negative delta applied to rollout percentage for alternating admins.
const NEGATIVE_ROLLOUT_DELTA = parseInt(__ENV.NEGATIVE_ROLLOUT_DELTA || '-3', 10);
// Rollout range (0-100) used when normalizing percentages.
const ROLLOUT_RANGE = parseInt(__ENV.ROLLOUT_RANGE || '100', 10);
// Maximum jitter (seconds) added before retrying a CAS attempt.
const RETRY_JITTER_MAX_SECONDS = parseFloat(
  __ENV.RETRY_JITTER_MAX_SECONDS || '0.002'
);
// Maximum duration (seconds) of the exponential backoff window.
const BACKOFF_CAP_SECONDS = parseFloat(__ENV.BACKOFF_CAP_SECONDS || '0.01');
// Initial backoff duration (seconds) before applying the multiplier.
const BACKOFF_INITIAL_SECONDS = parseFloat(
  __ENV.BACKOFF_INITIAL_SECONDS || '0.0005'
);
// Multiplier applied to the backoff duration after each failed attempt.
const BACKOFF_MULTIPLIER = parseFloat(__ENV.BACKOFF_MULTIPLIER || '2');
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
    'checks{cas:all-succeeded}': ['rate>0.995']
  }
};

// setup seeds the baseline flag before CAS storms begin.
export async function setup() {
  await kv.clear();
  await kv.set(FLAG_KEY, {
    rollout: 10,
    version: 0,
    updatedBy: 'bootstrap'
  });
}

// teardown closes disk stores after the scenario completes.
export async function teardown() {
  if (SELECTED_BACKEND_NAME === 'disk') {
    kv.close();
  }
}

// applyRolloutShift retries compareAndSwap until it succeeds or exhausts attempts.
async function applyRolloutShift(delta) {
  for (let attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt += 1) {
    // Spread the load slightly between retries to reduce thundering herds.
    sleep(Math.random() * RETRY_JITTER_MAX_SECONDS);

    const snapshot = await kv.get(FLAG_KEY);
    const proposed = {
      ...snapshot,
      rollout: (snapshot.rollout + delta + ROLLOUT_RANGE) % ROLLOUT_RANGE,
      version: snapshot.version + 1,
      updatedBy: `vu-${exec.vu.idInInstance}`
    };

    const swapped = await kv.compareAndSwap(FLAG_KEY, snapshot, proposed);
    if (swapped) {
      return proposed.version;
    }

    // Exponential backoff with jitter to give other CAS attempts time to progress.
    const backoff =
      Math.min(BACKOFF_CAP_SECONDS, BACKOFF_INITIAL_SECONDS * BACKOFF_MULTIPLIER ** attempt) *
      Math.random();
    sleep(backoff);
  }

  throw new Error('CAS did not converge within max attempts');
}

// featureFlagCasOverlap launches Promise.all CAS operations and verifies they all
// succeeded (unique version numbers recorded).
export default async function featureFlagCasOverlap() {
  const deltas = Array.from({ length: ADMIN_UPDATES_PER_TICK }, (_, idx) =>
    idx % 2 === 0 ? POSITIVE_ROLLOUT_DELTA : NEGATIVE_ROLLOUT_DELTA
  );

  const versions = await Promise.all(deltas.map((delta) => applyRolloutShift(delta)));

  const uniqueVersions = new Set(versions);

  check(true, {
    'cas:all-succeeded': () => uniqueVersions.size === deltas.length
  });

  // Simulate work long enough to keep the scenario under load.
  sleep(BASE_IDLE_SLEEP_SECONDS + Math.random() * IDLE_SLEEP_JITTER_SECONDS);
}

