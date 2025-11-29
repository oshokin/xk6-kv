import { check } from 'k6';
import exec from 'k6/execution';
import { VUS, ITERATIONS, createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: MICROSERVICE CACHE MANAGEMENT SYSTEM
// =============================================================================
//
// This test simulates a microservice architecture where multiple services need
// to maintain consistent cached data and invalidate related caches atomically.
// This is a critical pattern in:
//
// - E-commerce platforms (product catalog, inventory, pricing).
// - Social media platforms (user profiles, posts, relationships).
// - Banking systems (account balances, transaction history).
// - Content management systems (articles, media, metadata).
// - IoT platforms (device states, sensor data).
// - Gaming platforms (player stats, leaderboards).
//
// REAL-WORLD PROBLEM SOLVED:
// Multiple microservices updating related data simultaneously.
// Without proper cache management, you get:
// - Stale data served to users (wrong prices, outdated info).
// - Inconsistent state across services.
// - Race conditions in cache updates.
// - Performance degradation (cache misses, DB overload).
// - Data integrity issues (partial updates).
//
// ATOMIC OPERATIONS TESTED:
// - swap(): Atomically update cached data.
// - compareAndSwap(): Update version numbers atomically.
// - compareAndDelete(): Invalidate related caches deterministically.
// - exists(): Verify cache health.
// - list(): Monitor cache state.
//
// CONCURRENCY PATTERN:
// - Multiple VUs represent different microservices.
// - Each VU updates different cached entities.
// - Deterministic key selection prevents false positives.
// - Version-based invalidation prevents stale data.
//
// PERFORMANCE CHARACTERISTICS:
// - High read/write ratio (cache hits vs updates).
// - Critical for data consistency and user experience.
// - Must handle thousands of concurrent cache operations.
// - Low latency impact (cache operations should be fast).

// Total number of products seeded into the cache.
const PRODUCT_COUNT = parseInt(__ENV.PRODUCT_COUNT || '20', 10);

// Maximum CAS retry attempts when updating product versions.
const MAX_VERSION_CAS_RETRIES = parseInt(__ENV.MAX_VERSION_CAS_RETRIES || '1000', 10);

// Number of cache entries sampled during health checks.
const HEALTH_CHECK_SAMPLE_SIZE = parseInt(__ENV.HEALTH_CHECK_SAMPLE_SIZE || '3', 10);

// Key prefix for all product cache entries.
const CACHE_PREFIX = 'product:';

// Prefix for version identifiers.
const VERSION_PREFIX = 'v';

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'microservice-cache-management';

// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME);

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: VUS,
  iterations: ITERATIONS,
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
  const standardSetup = createSetup(kv);
  await standardSetup();

  for (let i = 1; i <= PRODUCT_COUNT; i++) {
    const productId = `${CACHE_PREFIX}${i}`;
    const version = `${VERSION_PREFIX}${i}`;

    await kv.set(`${productId}:data`, buildProductData(i, version));
    await kv.set(`${productId}:version`, version);
    await kv.set(`${productId}:related`, buildRelatedState(productId, version));
  }
}

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv, TEST_NAME);

// microserviceCacheManagementTest deterministically selects a product, updates its
// cache entry, bumps the version via CAS, invalidates related data, and rebuilds
// the dependency.
export default async function microserviceCacheManagementTest() {
  const vuId = exec.vu.idInTest;
  const iteration = exec.scenario.iterationInTest;

  // Deterministic product selection (round-robin across products).
  const productIndex = (iteration % PRODUCT_COUNT) + 1;
  const productId = `${CACHE_PREFIX}${productIndex}`;
  const dataKey = `${productId}:data`;
  const versionKey = `${productId}:version`;
  const relatedKey = `${productId}:related`;

  // Get current cached data.
  const currentData = await kv.get(dataKey);

  // Update product data (simulating price change).
  const newData = {
    ...currentData,
    price: currentData.price + 1, // Simple increment instead of magic formula.
    version: `${VERSION_PREFIX}${Date.now()}`,
    updatedAt: Date.now(),
    updatedBy: vuId
  };

  // Atomically swap cache entry.
  const { loaded } = await kv.swap(dataKey, newData);

  check(loaded, {
    cache_updated: () => loaded
  });

  // Bump version number via CAS.
  const bumpedVersion = await compareAndSwapVersion(versionKey);

  check(Boolean(bumpedVersion), {
    cache_version: () => Boolean(bumpedVersion)
  });

  // Invalidate related cache (edge cache, aggregations, etc.).
  const invalidated = await invalidateRelatedCache(relatedKey);

  check(invalidated, {
    cache_invalidated: () => invalidated
  });

  // Rebuild related cache with new version.
  await kv.set(relatedKey, buildRelatedState(productId, bumpedVersion || newData.version));

  // Sample cache health.
  const cacheEntries = await kv.list({ prefix: CACHE_PREFIX, limit: HEALTH_CHECK_SAMPLE_SIZE });

  for (const entry of cacheEntries) {
    if (entry.key.endsWith(':data')) {
      const exists = await kv.exists(entry.key);
      check(exists, {
        cache_health: () => exists
      });
    }
  }
}

// buildProductData produces deterministic product payloads for testing.
function buildProductData(id, version) {
  return {
    id,
    name: `Product ${id}`,
    price: id * 100, // Simple price formula: $100, $200, $300, etc.
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
  for (let attempt = 0; attempt < MAX_VERSION_CAS_RETRIES; attempt++) {
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
  for (let attempt = 0; attempt < MAX_VERSION_CAS_RETRIES; attempt++) {
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
  }

  return false;
}

// bumpVersionLabel normalizes version strings (v123 -> v124) so CAS inputs are
// consistent even if previous writers used different formatting.
function bumpVersionLabel(currentVersion) {
  const numeric = parseInt(String(currentVersion).replace(/\D/g, ''), 10) || 0;
  return `${VERSION_PREFIX}${numeric + 1}`;
}
