import { check, sleep } from 'k6';
import exec from 'k6/execution';
import { openKv } from 'k6/x/kv';

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

// Selected backend (memory or disk) used for cache storage.
const SELECTED_BACKEND_NAME = __ENV.KV_BACKEND || 'memory';
// Enables in-memory key tracking when the backend is memory.
const TRACK_KEYS_OVERRIDE =
  typeof __ENV.KV_TRACK_KEYS === 'string' ? __ENV.KV_TRACK_KEYS.toLowerCase() : '';
const ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND =
  TRACK_KEYS_OVERRIDE === '' ? true : TRACK_KEYS_OVERRIDE === 'true';

// CONFIG_KEY_BASE is the logical cache slot; each VU namespaces it with its id to
// keep iterations isolated while still hammering swap() via Promise.all.
const CONFIG_KEY_BASE = __ENV.CONFIG_KEY || 'cache:global-config';
// BUILDERS_PER_BATCH dictates how many swap() calls we launch concurrently.
const BUILDERS_PER_BATCH = parseInt(__ENV.CONFIG_BUILDERS || '24', 10);
// Number of regions used when synthesizing config payloads.
const REGION_SHARDS = parseInt(__ENV.REGION_SHARDS || '5', 10);
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
    'checks{swap:cold-start}': ['rate>0.999'],
    'checks{swap:hot-updates}': ['rate>0.999']
  }
};

// setup clears the cache slot so every batch starts cleanly.
export async function setup() {
  await kv.clear();
}

// teardown closes disk stores when the scenario completes.
export async function teardown() {
  if (SELECTED_BACKEND_NAME === 'disk') {
    kv.close();
  }
}

// buildConfigPayload crafts a synthetic config blob for the current iterator.
function buildConfigPayload(iteration, idx) {
  return {
    version: `${iteration}.${idx}`,
    builtBy: exec.vu.idInInstance,
    checksum: `${iteration}-${idx}-${Date.now()}`,
    metadata: {
      region: `region-${idx % REGION_SHARDS}`,
      traffic: Math.random()
    }
  };
}

// cacheBlueGreenSwap deletes the shared key, launches Promise.all swap() calls,
// and asserts only one builder performed a cold start while the rest observed
// hot updates.
export default async function cacheBlueGreenSwap() {
  const iteration = exec.scenario.iterationInTest;
  const shardKey = `${CONFIG_KEY_BASE}:${exec.vu.idInInstance}`;

  // Start each shard fresh so exactly one builder sees loaded=false.
  await kv.delete(shardKey);

  const payloads = Array.from({ length: BUILDERS_PER_BATCH }, (_, idx) =>
    buildConfigPayload(iteration, idx)
  );

  const results = await Promise.all(payloads.map((payload) => kv.swap(shardKey, payload)));

  const coldStarts = results.filter((result) => result.loaded === false);
  const hotSwaps = results.filter((result) => result.loaded === true);

  check(true, {
    'swap:cold-start': () => coldStarts.length === 1,
    'swap:hot-updates': () => hotSwaps.length === BUILDERS_PER_BATCH - 1
  });

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

  // Simulate work long enough to keep the scenario under load.
  sleep(BASE_IDLE_SLEEP_SECONDS + Math.random() * IDLE_SLEEP_JITTER_SECONDS);
}

