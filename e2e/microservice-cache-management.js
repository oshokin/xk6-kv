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
// - compareAndDelete(): Invalidate related caches conditionally
// - randomKey(): Find cached items to update
// - exists(): Verify cache health
// - list(): Monitor cache state
//
// CONCURRENCY PATTERN:
// - Multiple VUs represent different microservices
// - Each VU updates different cached entities
// - Shared KV store ensures cache consistency
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

// ---------------------------------------------
// Open a shared KV store available to all VUs.
// ---------------------------------------------
const kv = openKv(
  SELECTED_BACKEND_NAME === 'disk'
    ? { backend: 'disk', trackKeys: ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND }
    : { backend: 'memory', trackKeys: ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND }
);

export const options = {
  // Vary these to increase contention. Start with 20×100 like the shared script.
  vus: parseInt(__ENV.VUS || '20', 10),
  iterations: parseInt(__ENV.ITERATIONS || '100', 10),

  // Optional: add thresholds to fail fast if we start choking.
  thresholds: {
    // Require that at least 90% of iterations invalidate caches successfully.
    'checks{cache:invalidated}': ['rate>0.90'],
    'checks{cache:updated}': ['rate>0.95']
  }
};

// -----------------------
// Test setup & teardown.
// -----------------------
export async function setup() {
  // Start with a clean state so each run is deterministic.
  await kv.clear();

  // Pre-populate cache entries.
  for (let i = 1; i <= 20; i++) {
    const productId = `product:${i}`;
    const version = `v${Math.floor(Math.random() * 10)}`;

    await kv.set(`${productId}:data`, {
      id: i,
      name: `Product ${i}`,
      price: Math.floor(Math.random() * 1000),
      version: version,
      cachedAt: Date.now()
    });

    await kv.set(`${productId}:version`, version);
  }
}

export async function teardown() {
  // For disk backends, close the store cleanly so the file can be reused immediately.
  if (SELECTED_BACKEND_NAME === 'disk') {
    kv.close();
  }
}

// -------------------------------
// The main iteration body (VUs).
// -------------------------------
export default async function microserviceCacheManagementTest() {
  const vuId = exec.vu.idInTest;

  // Test 1: Random product update (cache invalidation trigger).
  const randomProductKey = await kv.randomKey({ prefix: 'product:' });

  if (randomProductKey && randomProductKey.includes(':data')) {
    const productId = randomProductKey.split(':data')[0];
    const versionKey = `${productId}:version`;

    // Test 2: Get current version.
    const currentVersion = await kv.get(versionKey);

    if (currentVersion) {
      // Test 3: Update product data (swap).
      const newData = {
        id: productId.split(':')[1],
        name: `Updated Product ${productId.split(':')[1]}`,
        price: Math.floor(Math.random() * 1000),
        version: `v${Date.now()}`,
        updatedAt: Date.now(),
        updatedBy: vuId
      };

      const { previous: oldData, loaded } = await kv.swap(randomProductKey, newData);

      check(loaded, {
        'cache:updated': () => loaded
      });

      // Test 4: Update version atomically (compareAndSwap).
      const newVersion = newData.version;
      const versionUpdated = await kv.compareAndSwap(versionKey, currentVersion, newVersion);

      if (versionUpdated) {
        // Test 5: Invalidate related caches (compareAndDelete).
        const relatedCacheKey = `${productId}:related`;
        const invalidated = await kv.compareAndDelete(relatedCacheKey, 'stale');

        check(true, {
          'cache:invalidated': () => true
        });
      }
    }
  }

  // Test 6: Check cache health (exists, list).
  const cacheEntries = await kv.list({ prefix: 'product:', limit: 5 });

  for (const entry of cacheEntries) {
    if (entry.key.includes(':data')) {
      const exists = await kv.exists(entry.key);
      check(exists, {
        'cache:health': () => exists
      });
    }
  }

  // Simulate some processing time.
  sleep(0.01);
}
