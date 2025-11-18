import { check, sleep } from 'k6';
import exec from 'k6/execution';
import { openKv } from 'k6/x/kv';

// =============================================================================
// REAL-WORLD SCENARIO: MICROSERVICE CACHE MANAGEMENT SYSTEM
// =============================================================================
//
// This test simulates a microservice architecture where multiple services need
// to maintain consistent cached data and invalidate related caches atomically.
// This is a critical pattern in:
//
// - E-commerce platforms (product catalog, inventory, pricing)
// - Social media platforms (user profiles, posts, relationships)
// - Banking systems (account balances, transaction history)
// - Content management systems (articles, media, metadata)
// - IoT platforms (device states, sensor data)
// - Gaming platforms (player stats, leaderboards)
//
// REAL-WORLD PROBLEM SOLVED:
// Multiple microservices updating related data simultaneously.
// Without proper cache management, you get:
// - Stale data served to users (wrong prices, outdated info)
// - Inconsistent state across services
// - Race conditions in cache updates
// - Performance degradation (cache misses, DB overload)
// - Data integrity issues (partial updates)
//
// ATOMIC OPERATIONS TESTED:
// - swap(): Atomically update cached data
// - compareAndSwap(): Update version numbers atomically
// - compareAndDelete(): Invalidate related caches deterministically
// - exists(): Verify cache health
// - list(): Monitor cache state
//
// CONCURRENCY PATTERN:
// - Multiple VUs represent different microservices
// - Each VU updates different cached entities
// - Deterministic key selection prevents false positives
// - Version-based invalidation prevents stale data
//
// PERFORMANCE CHARACTERISTICS:
// - High read/write ratio (cache hits vs updates)
// - Critical for data consistency and user experience
// - Must handle thousands of concurrent cache operations
// - Low latency impact (cache operations should be fast)

// Selected backend (memory or disk) used for cache data.
const SELECTED_BACKEND_NAME = __ENV.KV_BACKEND || 'memory';

// Enables in-memory key tracking when the backend is memory.
const TRACK_KEYS_OVERRIDE =
  typeof __ENV.KV_TRACK_KEYS === 'string' ? __ENV.KV_TRACK_KEYS.toLowerCase() : '';
const ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND =
  TRACK_KEYS_OVERRIDE === '' ? true : TRACK_KEYS_OVERRIDE === 'true';

// Total number of products seeded into the cache.
const PRODUCT_COUNT = parseInt(__ENV.PRODUCT_COUNT || '20', 10);

// Maximum CAS retries when bumping product versions.
const VERSION_RETRY_ATTEMPTS = parseInt(__ENV.VERSION_RETRY_ATTEMPTS || '10', 10);
// Price multiplier applied to initial seed data to avoid monotony.
const PRICE_MULTIPLIER = parseInt(__ENV.PRICE_MULTIPLIER || '137', 10);
// Price modulo used when seeding catalog entries.
const PRICE_MODULO = parseInt(__ENV.PRICE_MODULO || '1000', 10);
// Price jitter range applied during runtime updates.
const PRICE_JITTER_MODULO = parseInt(__ENV.PRICE_JITTER_MODULO || '1000', 10);
// Base sleep (seconds) between compareAndDelete retries.
const INVALIDATION_RETRY_SLEEP_SECONDS = parseFloat(
  __ENV.INVALIDATION_RETRY_SLEEP_SECONDS || '0.001'
);
// Random jitter (seconds) added to invalidation retry sleep.
const INVALIDATION_RETRY_JITTER_SECONDS = parseFloat(
  __ENV.INVALIDATION_RETRY_JITTER_SECONDS || '0.001'
);
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
// Limit used when sampling cache entries for health checks.
const CACHE_HEALTH_LIST_LIMIT = parseInt(__ENV.CACHE_HEALTH_LIST_LIMIT || '3', 10);
// Prefix applied to every cache key.
const CACHE_PREFIX = __ENV.CACHE_PREFIX || 'product:';
// Prefix used when formatting version identifiers.
const VERSION_PREFIX = __ENV.VERSION_PREFIX || 'v';

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
    'checks{check:cache_updated}': ['rate>0.999'],
    'checks{check:cache_invalidated}': ['rate>0.999'],
    'checks{check:cache_health}': ['rate>0.999'],
    'checks{check:cache_version}': ['rate>0.999']
  }
};

// setup seeds a deterministic product catalog so every run exercises the same
// data dependencies and version numbers.
export async function setup() {
  await kv.clear();

  for (let i = 1; i <= PRODUCT_COUNT; i++) {
    const productId = `${CACHE_PREFIX}${i}`;
    const version = `${VERSION_PREFIX}${i}`;

    await kv.set(`${productId}:data`, buildProductData(i, version));
    await kv.set(`${productId}:version`, version);
    await kv.set(`${productId}:related`, buildRelatedState(productId, version));
  }
}

// teardown closes BoltDB cleanly so later runs do not trip over open handles.
export async function teardown() {
  if (SELECTED_BACKEND_NAME === 'disk') {
    kv.close();
  }
}

// microserviceCacheManagementTest deterministically selects a product, updates its
// cache entry, bumps the version via CAS, invalidates related data, and rebuilds
// the dependency.
export default async function microserviceCacheManagementTest() {
  const vuId = exec.vu.idInTest;
  const iterationSeed = exec.scenario.iterationInTest;
  const productIndex = (iterationSeed % PRODUCT_COUNT) + 1;
  const productId = `${CACHE_PREFIX}${productIndex}`;
  const dataKey = `${productId}:data`;
  const versionKey = `${productId}:version`;
  const relatedKey = `${productId}:related`;

  const currentData = await kv.get(dataKey);

  const newData = {
    ...currentData,
    price: (currentData.price + iterationSeed) % PRICE_JITTER_MODULO,
    version: `${VERSION_PREFIX}${Date.now()}`,
    updatedAt: Date.now(),
    updatedBy: vuId
  };

  const { loaded } = await kv.swap(dataKey, newData);

  check(loaded, {
    cache_updated: () => loaded
  });

  const bumpedVersion = await compareAndSwapVersion(versionKey);

  check(Boolean(bumpedVersion), {
    cache_version: () => Boolean(bumpedVersion)
  });

  const invalidated = await invalidateRelatedCache(relatedKey);

  check(invalidated, {
    cache_invalidated: () => invalidated
  });

  await kv.set(relatedKey, buildRelatedState(productId, bumpedVersion || newData.version));

  const cacheEntries = await kv.list({ prefix: CACHE_PREFIX, limit: CACHE_HEALTH_LIST_LIMIT });

  for (const entry of cacheEntries) {
    if (entry.key.endsWith(':data')) {
      const exists = await kv.exists(entry.key);
      check(exists, {
        cache_health: () => exists
      });
    }
  }

  // Simulate work long enough to keep the scenario under load.
  sleep(BASE_IDLE_SLEEP_SECONDS + Math.random() * IDLE_SLEEP_JITTER_SECONDS);
}

// buildProductData produces deterministic product payloads so diffing caches
// is straightforward when debugging.
function buildProductData(id, version) {
  return {
    id,
    name: `Product ${id}`,
    price: (id * PRICE_MULTIPLIER) % PRICE_MODULO,
    version,
    cachedAt: Date.now()
  };
}

// buildRelatedState represents an edge cache (e.g., aggregated response) that
// needs invalidation whenever the base product changes.
function buildRelatedState(productId, version) {
  return {
    productId,
    cachedVersion: version,
    status: 'stale'
  };
}

// compareAndSwapVersion retries CAS until the version bump succeeds, ensuring
// we actually validate concurrency primitives instead of bailing silently.
async function compareAndSwapVersion(versionKey) {
  for (let attempt = 0; attempt < VERSION_RETRY_ATTEMPTS; attempt++) {
    let currentVersion;

    try {
      currentVersion = await kv.get(versionKey);
    } catch (err) {
      return null;
    }

    const nextVersion = bumpVersionLabel(currentVersion);
    const swapped = await kv.compareAndSwap(versionKey, currentVersion, nextVersion);

    if (swapped) {
      return nextVersion;
    }
  }

  return null;
}

// invalidateRelatedCache retries compareAndDelete to tolerate competing writers.
async function invalidateRelatedCache(relatedKey) {
  for (let attempt = 0; attempt < VERSION_RETRY_ATTEMPTS; attempt++) {
    let snapshot;

    try {
      snapshot = await kv.get(relatedKey);
    } catch (err) {
      // Already deleted/invalidated by another service; treat as success.
      return true;
    }

    const deleted = await kv.compareAndDelete(relatedKey, snapshot);
    if (deleted) {
      return true;
    }

    // Yield slightly before retrying so other writers can finish.
    sleep(
      INVALIDATION_RETRY_SLEEP_SECONDS +
        Math.random() * INVALIDATION_RETRY_JITTER_SECONDS
    );
  }

  return false;
}

// bumpVersionLabel normalizes version strings (v123 -> v124) so CAS inputs are
// consistent even if previous writers used different formatting.
function bumpVersionLabel(currentVersion) {
  const numeric = parseInt(String(currentVersion).replace(/\D/g, ''), 10) || 0;
  return `${VERSION_PREFIX}${numeric + 1}`;
}
