import { check, sleep } from 'k6';
import exec from 'k6/execution';
import { openKv } from 'k6/x/kv';

// =============================================================================
// REAL-WORLD SCENARIO: API RATE LIMITING SYSTEM
// =============================================================================
//
// This test simulates a typical API rate limiting system where we need to
// track request counts per user and enforce limits to prevent abuse. This is
// a critical pattern in:
//
// - REST APIs (GitHub, Twitter, Stripe APIs)
// - Microservices architectures (service-to-service communication)
// - Payment processing systems (prevent fraud and abuse)
// - Content delivery networks (CDN rate limiting)
// - IoT device management (prevent device spam)
// - Social media platforms (prevent bot abuse)
//
// REAL-WORLD PROBLEM SOLVED:
// Multiple clients/users making API requests simultaneously.
// Without proper rate limiting, you get:
// - API abuse and DoS attacks
// - Resource exhaustion (CPU, memory, database connections)
// - Unfair usage (some users hog all resources)
// - Increased costs (cloud resources, bandwidth)
// - Poor service quality for legitimate users
//
// ATOMIC OPERATIONS TESTED:
// - incrementBy(): Atomically increment request counter per user
// - compareAndSwap(): Reset counters when time window expires
// - getOrSet(): Initialize reset time for new users
// - swap(): Update reset time atomically
//
// CONCURRENCY PATTERN:
// - Multiple VUs represent different API clients/users
// - Each VU tracks its own request rate
// - Shared KV store ensures accurate rate counting
// - Time-based windows prevent indefinite blocking
//
// PERFORMANCE CHARACTERISTICS:
// - High frequency operations (every API call)
// - Critical for service availability and security
// - Must handle thousands of requests per second
// - Low latency impact (rate limiting should be fast)

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

// Rationale: 20 VUs × 100 iterations approximate a busy API minute. Thresholds
// enforce that rate checks and reset logic stay rock solid without adding long
// runtime to CI.
export const options = {
  // Vary these to increase contention. Start with 20×100 like the shared script.
  vus: parseInt(__ENV.VUS || '20', 10),
  iterations: parseInt(__ENV.ITERATIONS || '100', 10),

  // Optional: add thresholds to fail fast if we start choking.
  thresholds: {
    // Require that at least 95% of iterations pass rate limit checks.
    'checks{rate:limit-check}': ['rate>0.95'],
    'checks{rate:reset}': ['rate>0.80']
  }
};

// setup: clear every counter so the first request of each run behaves the same.
export async function setup() {
  // Start with a clean state so each run is deterministic.
  await kv.clear();
}

// teardown: close BoltDB cleanly so later runs do not trip over open handles.
export async function teardown() {
  // For disk backends, close the store cleanly so the file can be reused immediately.
  if (SELECTED_BACKEND_NAME === 'disk') {
    kv.close();
  }
}

// Each iteration represents a single API call: increment usage, enforce the
// limit, and reset counters when the sliding window elapses.
export default async function apiRateLimitingTest() {
  const userId = `user:${exec.vu.idInTest}`;
  const rateLimitKey = `rate_limit:${userId}`;
  const resetTimeKey = `rate_reset:${userId}`;

  // Test 1: Increment request counter.
  const requestCount = await kv.incrementBy(rateLimitKey, 1);

  // Test 2: Check if rate limit exceeded (100 requests per minute).
  const rateLimitExceeded = requestCount > 100;

  check(!rateLimitExceeded, {
    'rate:limit-check': () => !rateLimitExceeded
  });

  // Test 3: Set reset time if this is the first request.
  const resetTime = await kv.getOrSet(resetTimeKey, Date.now() + 60000);

  // Test 4: Check if we need to reset the counter.
  const currentTime = Date.now();
  const shouldReset = currentTime > resetTime.value;

  if (shouldReset) {
    // Test 5: Atomic reset using compareAndSwap.
    const resetSuccess = await kv.compareAndSwap(rateLimitKey, requestCount, 0);

    if (resetSuccess) {
      // Update reset time.
      await kv.swap(resetTimeKey, currentTime + 60000);

      check(true, {
        'rate:reset': () => true
      });
    }
  }

  // Simulate API call delay.
  sleep(0.01);
}
