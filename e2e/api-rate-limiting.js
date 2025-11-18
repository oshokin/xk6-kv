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

// Selected backend (memory or disk) used for rate-limiting state.
const SELECTED_BACKEND_NAME = __ENV.KV_BACKEND || 'memory';

// Enables in-memory key tracking when the backend is memory.
const TRACK_KEYS_OVERRIDE =
  typeof __ENV.KV_TRACK_KEYS === 'string' ? __ENV.KV_TRACK_KEYS.toLowerCase() : '';
const ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND =
  TRACK_KEYS_OVERRIDE === '' ? true : TRACK_KEYS_OVERRIDE === 'true';

// Maximum number of requests allowed per time window.
const RATE_LIMIT_PER_WINDOW = parseInt(__ENV.RATE_LIMIT_PER_WINDOW || '100', 10);
// Duration of each rate limit window in milliseconds.
const RATE_WINDOW_MS = parseInt(__ENV.RATE_WINDOW_MS || '60000', 10);
// Base duration (seconds) each iteration sleeps after processing.
const BASE_IDLE_SLEEP_SECONDS = parseFloat(__ENV.BASE_IDLE_SLEEP_SECONDS || '0.02');
// Random jitter (seconds) added to the base idle sleep.
const IDLE_SLEEP_JITTER_SECONDS = parseFloat(__ENV.IDLE_SLEEP_JITTER_SECONDS || '0.01');
// Default number of VUs used in the scenario.
const DEFAULT_VUS = parseInt(__ENV.VUS || '40', 10);
// Default iteration count used in the scenario.
const DEFAULT_ITERATIONS = parseInt(__ENV.ITERATIONS || '400', 10);

// kv is the shared store client used throughout the scenario.
const kv = openKv(
  SELECTED_BACKEND_NAME === 'disk'
    ? { backend: 'disk', trackKeys: ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND }
    : { backend: 'memory', trackKeys: ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND }
);

// options configures the load profile and pass/fail thresholds.
export const options = {
  // Adjust via env vars to dial contention up or down for your workload.
  vus: DEFAULT_VUS,
  iterations: DEFAULT_ITERATIONS,

  // Optional: add thresholds to fail fast if we start choking.
  thresholds: {
    // Require that at least 95% of iterations pass rate limit checks.
    'checks{rate:limit-check}': ['rate>0.95'],
    'checks{rate:reset}': ['rate>0.80']
  }
};

// setup clears every counter so the first request of each run behaves the same.
export async function setup() {
  // Start with a clean state so each run is deterministic.
  await kv.clear();
}

// teardown closes BoltDB cleanly so later runs do not trip over open handles.
export async function teardown() {
  // For disk backends, close the store cleanly so the file can be reused immediately.
  if (SELECTED_BACKEND_NAME === 'disk') {
    kv.close();
  }
}

// apiRateLimitingTest represents a single API call: increment usage, enforce the
// limit, and reset counters when the sliding window elapses.
export default async function apiRateLimitingTest() {
  const userId = `user:${exec.vu.idInTest}`;
  const rateLimitKey = `rate_limit:${userId}`;
  const resetTimeKey = `rate_reset:${userId}`;

  // Test 1: Increment request counter.
  const requestCount = await kv.incrementBy(rateLimitKey, 1);

  // Test 2: Check if rate limit exceeded (100 requests per minute).
  const rateLimitExceeded = requestCount > RATE_LIMIT_PER_WINDOW;

  check(!rateLimitExceeded, {
    'rate:limit-check': () => !rateLimitExceeded
  });

  // Test 3: Set reset time if this is the first request.
  const resetTime = await kv.getOrSet(resetTimeKey, Date.now() + RATE_WINDOW_MS);

  // Test 4: Check if we need to reset the counter.
  const currentTime = Date.now();
  const shouldReset = currentTime > resetTime.value;

  if (shouldReset) {
    // Test 5: Atomic reset using compareAndSwap.
    const resetSuccess = await kv.compareAndSwap(rateLimitKey, requestCount, 0);

    if (resetSuccess) {
      // Update reset time.
      await kv.swap(resetTimeKey, currentTime + RATE_WINDOW_MS);

      check(true, {
        'rate:reset': () => true
      });
    }
  }

  // Simulate API call delay with jitter to keep promises alive on the event loop.
  sleep(BASE_IDLE_SLEEP_SECONDS + Math.random() * IDLE_SLEEP_JITTER_SECONDS);
}
