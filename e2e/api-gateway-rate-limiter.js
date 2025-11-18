import { check, sleep } from 'k6';
import exec from 'k6/execution';
import { openKv } from 'k6/x/kv';

// =============================================================================
// REAL-WORLD SCENARIO: GLOBAL API RATE LIMITER
// =============================================================================
//
// This test mirrors gateway clusters (CDNs, edge proxies, API gateways) that
// increment per-IP counters for every request. Hundreds of workers call
// incrementBy() concurrently without awaiting each operation individually; the
// results must remain strictly monotonic to avoid double-counting or request
// drops.
//
// REAL-WORLD PROBLEMS UNCOVERED:
// - Duplicate totals when increments resolve on different goroutines.
// - Lost increments if promise resolution races clobber values.
// - Non-contiguous sequences that break leak-detection logic.
//
// ATOMIC OPERATIONS TESTED:
// - incrementBy(): hot counter increments.
// - getOrSet(): bucket bootstrap before the flood.
//
// CONCURRENCY PATTERN:
// - Every VU fires Promise.all batches of incrementBy() calls against the same
//   counter bucket. Resolutions happen simultaneously, reproducing the
//   runAsyncWithStore panic signature unless promise resolution is serialized.
//
// PERFORMANCE CHARACTERISTICS:
// - Extremely short-lived operations issued in bursts.
// - Sensitive to ordering; failures are often silent unless explicitly checked.

// Selected backend (memory or disk) used for the rate-limiter state.
const SELECTED_BACKEND_NAME = __ENV.KV_BACKEND || 'memory';
// Enables in-memory key tracking when the backend is memory.
const TRACK_KEYS_OVERRIDE =
  typeof __ENV.KV_TRACK_KEYS === 'string' ? __ENV.KV_TRACK_KEYS.toLowerCase() : '';
const ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND =
  TRACK_KEYS_OVERRIDE === '' ? true : TRACK_KEYS_OVERRIDE === 'true';

// RATE_LIMIT_PREFIX namespaces per-IP counters.
const RATE_LIMIT_PREFIX = __ENV.RATE_LIMIT_PREFIX || 'ratelimit:ip:';
// IP_BUCKETS controls how many virtual IPs we round-robin through.
const IP_BUCKETS = parseInt(__ENV.IP_BUCKETS || '100', 10);
// INCREMENTS_PER_BATCH decides how many increments we launch concurrently.
const INCREMENTS_PER_BATCH = parseInt(__ENV.INCREMENTS_PER_BATCH || '32', 10);

// Default number of VUs used in the scenario.
const DEFAULT_VUS = parseInt(__ENV.VUS || '40', 10);
// Default iteration count used in the scenario.
const DEFAULT_ITERATIONS = parseInt(__ENV.ITERATIONS || '400', 10);
// Base duration (seconds) each iteration sleeps after processing.
const BASE_IDLE_SLEEP_SECONDS = parseFloat(__ENV.BASE_IDLE_SLEEP_SECONDS || '0.01');
// Random jitter (seconds) added to the base idle sleep.
const IDLE_SLEEP_JITTER_SECONDS = parseFloat(
  __ENV.IDLE_SLEEP_JITTER_SECONDS || '0.01'
);

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
    'checks{counter:unique}': ['rate>0.999'],
    'checks{counter:contiguous}': ['rate>0.999']
  }
};

// setup clears previous counters before the run starts.
export async function setup() {
  await kv.clear();
}

// teardown closes disk stores so repeated runs do not collide.
export async function teardown() {
  if (SELECTED_BACKEND_NAME === 'disk') {
    kv.close();
  }
}

// apiGatewayRateLimiter fires Promise.all batches of incrementBy calls and verifies
// the returned totals stay unique and contiguous despite heavy contention.
export default async function apiGatewayRateLimiter() {
  const iteration = exec.scenario.iterationInTest;
  const bucket = iteration % IP_BUCKETS;
  const counterKey = `${RATE_LIMIT_PREFIX}${bucket}`;

  await kv.getOrSet(counterKey, 0);

  const increments = await Promise.all(
    Array.from({ length: INCREMENTS_PER_BATCH }, () => kv.incrementBy(counterKey, 1))
  );

  const uniqueValues = new Set(increments);
  const max = Math.max(...increments);
  const min = Math.min(...increments);

  const isUnique = uniqueValues.size === increments.length;
  const isContiguous = max - min + 1 === increments.length;

  check(true, {
    'counter:unique': () => isUnique,
    'counter:contiguous': () => isContiguous
  });

  // Simulate work long enough to keep the scenario under load.
  sleep(BASE_IDLE_SLEEP_SECONDS + Math.random() * IDLE_SLEEP_JITTER_SECONDS);
}

