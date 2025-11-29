import { check } from 'k6';
import exec from 'k6/execution';
import { VUS, ITERATIONS, createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: BLUE/GREEN CACHE INVALIDATION
// =============================================================================
//
// Multiple config builders rebuild the same cache entry (think CDN edge config
// or ML feature materialization) and publish the artifact with swap(). Every
// worker fires Promise.all swaps so the promise resolutions hammer Sobek at the
// same time.
//
// REAL-WORLD PROBLEMS UNCOVERED:
// - Duplicate cold starts where more than one worker believes it created the key.
// - Stale configs when swap() promises resolve out of order.
// - Event-loop contention translating Go payloads back to JS objects.
//
// ATOMIC OPERATIONS TESTED:
// - swap(): replace the cached payload atomically and return the previous
//   version to the caller.
// - delete(): clear the slot before the next simulation round.
//
// CONCURRENCY PATTERN:
// - Each iteration deletes the config key and immediately launches dozens of
//   swap() calls via Promise.all. Only one response should report loaded=false.

// Base cache key name (each VU gets its own namespaced shard).
const CACHE_KEY = __ENV.CACHE_KEY || 'cache:global-config';

// Number of concurrent swap() operations to fire in parallel (tests Promise.all contention).
const CONCURRENT_SWAPS = parseInt(__ENV.CONCURRENT_SWAPS || '24', 10);

// Number of region shards for synthetic config generation.
const REGION_COUNT = parseInt(__ENV.REGION_COUNT || '5', 10);

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'cache-blue-green-swap';

// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME);

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: VUS,
  iterations: ITERATIONS,
  thresholds: {
    'checks{swap:cold-start}': ['rate>0.999'],
    'checks{swap:hot-updates}': ['rate>0.999']
  }
};

// setup clears the cache slot so every batch starts cleanly.
export const setup = createSetup(kv);

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv, TEST_NAME);

// buildConfigPayload crafts a synthetic config blob for the current iterator.
function buildConfigPayload(iteration, idx) {
  return {
    version: `${iteration}.${idx}`,
    builtBy: exec.vu.idInInstance,
    checksum: `${iteration}-${idx}-${Date.now()}`,
    metadata: {
      region: `region-${idx % REGION_COUNT}`,
      traffic: Math.random()
    }
  };
}

// cacheBlueGreenSwap deletes the shared key, launches Promise.all swap() calls,
// and asserts only one builder performed a cold start while the rest observed
// hot updates.
export default async function cacheBlueGreenSwap() {
  const iteration = exec.scenario.iterationInTest;
  const shardKey = `${CACHE_KEY}:${exec.vu.idInInstance}`;

  // Start each shard fresh so exactly one builder sees loaded=false.
  await kv.delete(shardKey);

  // Generate payloads for all concurrent swaps.
  const payloads = Array.from({ length: CONCURRENT_SWAPS }, (_, idx) =>
    buildConfigPayload(iteration, idx)
  );

  // Fire all swaps concurrently via Promise.all.
  const results = await Promise.all(payloads.map((payload) => kv.swap(shardKey, payload)));

  // Exactly one swap should be a cold start (key didn't exist).
  const coldStarts = results.filter((result) => result.loaded === false);
  const hotSwaps = results.filter((result) => result.loaded === true);

  check(true, {
    'swap:cold-start': () => coldStarts.length === 1,
    'swap:hot-updates': () => hotSwaps.length === CONCURRENT_SWAPS - 1
  });

  // Verify the final config is readable.
  let latestConfig = null;
  try {
    latestConfig = await kv.get(shardKey);
  } catch (err) {
    latestConfig = null;
  }

  check(true, {
    'swap:latest-readable': () =>
      latestConfig != null && typeof latestConfig.version === 'string'
  });
}
