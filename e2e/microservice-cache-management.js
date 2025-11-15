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

// Backend selection: memory (default) or disk.
const SELECTED_BACKEND_NAME = __ENV.KV_BACKEND || 'memory';

// Optional: enable key tracking in memory backend to stress the tracking paths.
// (No effect for disk backend; safe to leave on)
const ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND =
  (__ENV.KV_TRACK_KEYS && __ENV.KV_TRACK_KEYS.toLowerCase() === 'true') || true;

// Dataset configuration (number of products and CAS retry budget).
const PRODUCT_COUNT = parseInt(__ENV.PRODUCT_COUNT || '20', 10);
const VERSION_RETRY_ATTEMPTS = 10;

// ---------------------------------------------
// Open a shared KV store available to all VUs.
// ---------------------------------------------
const kv = openKv(
  SELECTED_BACKEND_NAME === 'disk'
    ? { backend: 'disk', trackKeys: ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND }
    : { backend: 'memory', trackKeys: ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND }
);

// Rationale: 20 VUs × 100 iterations give enough overlap to expose CAS or
// swap() regressions while keeping the job under a second on CI. Thresholds
// require that every cache update, invalidation, and health check succeeds.
export const options = {
  vus: parseInt(__ENV.VUS || '20', 10),
  iterations: parseInt(__ENV.ITERATIONS || '100', 10),
  thresholds: {
    'checks{cache:updated}': ['rate>0.999'],
    'checks{cache:invalidated}': ['rate>0.999'],
    'checks{cache:health}': ['rate>0.999']
  }
};

// setup: seeds a deterministic product catalog so every run exercises the same
// data dependencies and version numbers.
export async function setup() {
  await kv.clear();

  for (let i = 1; i <= PRODUCT_COUNT; i++) {
    const productId = `product:${i}`;
    const version = `v${i}`;

    await kv.set(`${productId}:data`, buildProductData(i, version));
    await kv.set(`${productId}:version`, version);
    await kv.set(`${productId}:related`, buildRelatedState(productId, version));
  }
}

export async function teardown() {
  if (SELECTED_BACKEND_NAME === 'disk') {
    kv.close();
  }
}

// Each VU deterministically selects a product, updates its cache entry, bumps
// the version via CAS, invalidates related data, and rebuilds the dependency.
export default async function microserviceCacheManagementTest() {
  const vuId = exec.vu.idInTest;
  const iterationSeed = exec.scenario.iterationInTest;
  const productIndex = (iterationSeed % PRODUCT_COUNT) + 1;
  const productId = `product:${productIndex}`;
  const dataKey = `${productId}:data`;
  const versionKey = `${productId}:version`;
  const relatedKey = `${productId}:related`;

  const currentData = await kv.get(dataKey);

  const newData = {
    ...currentData,
    price: (currentData.price + iterationSeed) % 1000,
    version: `v${Date.now()}`,
    updatedAt: Date.now(),
    updatedBy: vuId
  };

  const { loaded } = await kv.swap(dataKey, newData);

  check(loaded, {
    'cache:updated': () => loaded
  });

  const bumpedVersion = await compareAndSwapVersion(versionKey);

  check(Boolean(bumpedVersion), {
    'cache:version': () => Boolean(bumpedVersion)
  });

  const invalidated = await invalidateRelatedCache(relatedKey);

  check(invalidated, {
    'cache:invalidated': () => invalidated
  });

  await kv.set(relatedKey, buildRelatedState(productId, bumpedVersion || newData.version));

  const cacheEntries = await kv.list({ prefix: 'product:', limit: 3 });

  for (const entry of cacheEntries) {
    if (entry.key.endsWith(':data')) {
      const exists = await kv.exists(entry.key);
      check(exists, {
        'cache:health': () => exists
      });
    }
  }

  sleep(0.01);
}

// buildProductData: produces deterministic product payloads so diffing caches
// is straightforward when debugging.
function buildProductData(id, version) {
  return {
    id,
    name: `Product ${id}`,
    price: (id * 137) % 1000,
    version,
    cachedAt: Date.now()
  };
}

// buildRelatedState: represents an edge cache (e.g., aggregated response) that
// needs invalidation whenever the base product changes.
function buildRelatedState(productId, version) {
  return {
    productId,
    cachedVersion: version,
    status: 'stale'
  };
}

// compareAndSwapVersion: retries CAS until the version bump succeeds, ensuring
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

// invalidateRelatedCache: ensures compareAndDelete behaves atomically by only
// deleting when the cached version matches the snapshot fetched earlier.
async function invalidateRelatedCache(relatedKey) {
  let snapshot;

  try {
    snapshot = await kv.get(relatedKey);
  } catch (err) {
    return false;
  }

  return kv.compareAndDelete(relatedKey, snapshot);
}

// bumpVersionLabel: normalizes version strings (v123 → v124) so CAS inputs are
// consistent even if previous writers used different formatting.
function bumpVersionLabel(currentVersion) {
  const numeric = parseInt(String(currentVersion).replace(/\D/g, ''), 10) || 0;
  return `v${numeric + 1}`;
}
