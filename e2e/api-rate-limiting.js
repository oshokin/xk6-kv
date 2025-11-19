import { check } from 'k6';
import exec from 'k6/execution';
import { VUS, ITERATIONS, createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: API RATE LIMITING SYSTEM
// =============================================================================
//
// This test simulates an API rate limiting system where we need to
// track request counts per user and atomically reset counters when limits
// are exceeded. This is a critical pattern in:
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
// - compareAndSwap(): Reset counters atomically when limit exceeded
// - get(): Read current counter value
//
// CONCURRENCY PATTERN:
// - Multiple VUs represent different API clients/users
// - Each VU increments its counter and resets when threshold reached
// - Shared KV store ensures accurate counting under contention
//
// PERFORMANCE CHARACTERISTICS:
// - High frequency operations (every API call)
// - Critical for service availability and security
// - Must handle thousands of requests per second
// - Low latency impact (rate limiting should be fast)

// Counter reset threshold - when counter reaches this value, trigger atomic reset via CAS.
const RESET_THRESHOLD = parseInt(__ENV.RESET_THRESHOLD || '50', 10);

// kv is the shared store client used throughout the scenario.
const kv = createKv();

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: VUS,
  iterations: ITERATIONS,
  thresholds: {
    'checks{counter:incremented}': ['rate>0.99'],
    'checks{counter:reset}': ['rate>0.80']
  }
};

// setup clears every counter so the first request of each run behaves the same.
export const setup = createSetup(kv);

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv);

// apiRateLimitingTest represents a single API call: increment usage counter,
// and reset when threshold is reached using compareAndSwap.
export default async function apiRateLimitingTest() {
  const userId = `user:${exec.vu.idInTest}`;
  const counterKey = `counter:${userId}`;

  // Increment request counter atomically.
  const count = await kv.incrementBy(counterKey, 1);

  check(count > 0, {
    'counter:incremented': () => count > 0
  });

  // When counter reaches threshold, try to reset it atomically.
  if (count >= RESET_THRESHOLD) {
    // Read current value.
    const currentCount = await kv.get(counterKey);

    // Only reset if still at or above threshold (might have been reset by another VU).
    if (currentCount >= RESET_THRESHOLD) {
      const resetSuccess = await kv.compareAndSwap(counterKey, currentCount, 0);

      check(true, {
        'counter:reset': () => resetSuccess
      });
    }
  }
}
