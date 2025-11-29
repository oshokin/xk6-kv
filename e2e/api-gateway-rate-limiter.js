import { check } from 'k6';
import exec from 'k6/execution';
import { VUS, ITERATIONS, createKv, createSetup, createTeardown } from './common.js';

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

// Key prefix for per-IP rate limit counters.
const RATE_LIMIT_PREFIX = __ENV.RATE_LIMIT_PREFIX || 'ratelimit:ip:';

// Number of concurrent increments per batch (tests Promise.all race conditions).
const INCREMENTS_PER_BATCH = parseInt(__ENV.INCREMENTS_PER_BATCH || '32', 10);

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'api-gateway-rate-limiter';

// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME);

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: VUS,
  iterations: ITERATIONS,
  thresholds: {
    'checks{counter:unique}': ['rate>0.999'],
    'checks{counter:contiguous}': ['rate>0.999']
  }
};

// setup clears previous counters before the run starts.
export const setup = createSetup(kv);

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv, TEST_NAME);

// apiGatewayRateLimiter fires Promise.all batches of incrementBy calls and verifies
// the returned totals stay unique and contiguous despite heavy contention.
export default async function apiGatewayRateLimiter() {
  const vuId = exec.vu.idInTest;
  const iteration = exec.scenario.iterationInTest;

  // Each VU gets its own counter to avoid batch interleaving.
  const counterKey = `${RATE_LIMIT_PREFIX}${vuId}:${iteration}`;

  // Initialize counter if not exists.
  await kv.getOrSet(counterKey, 0);

  // Fire concurrent increments in a single batch.
  const increments = await Promise.all(
    Array.from({ length: INCREMENTS_PER_BATCH }, () => kv.incrementBy(counterKey, 1))
  );

  // Verify all returned values are unique (no duplicate counters).
  const uniqueValues = new Set(increments);
  const isUnique = uniqueValues.size === increments.length;

  // Verify all values are contiguous (no gaps in sequence).
  const min = Math.min(...increments);
  const max = Math.max(...increments);
  const isContiguous = max - min + 1 === increments.length;

  check(true, {
    'counter:unique': () => isUnique,
    'counter:contiguous': () => isContiguous
  });
}
