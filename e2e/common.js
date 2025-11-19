import { openKv } from 'k6/x/kv';

// Storage backend to use: 'memory' (default) or 'disk'.
export const BACKEND = __ENV.KV_BACKEND || 'memory';

// Enable key tracking for selected backend.
export const TRACK_KEYS = __ENV.KV_TRACK_KEYS !== 'false';

// Number of virtual users (concurrent test executors).
export const VUS = parseInt(__ENV.VUS || '40', 10);

// Total number of test iterations to execute.
export const ITERATIONS = parseInt(__ENV.ITERATIONS || '400', 10);

// Initialize KV store with standard configuration.
// Returns a configured KV store instance ready for use in tests.
export function createKv() {
  return openKv({
    backend: BACKEND,
    trackKeys: TRACK_KEYS
  });
}

// Create standard setup function that clears all keys before test.
export function createSetup(kv) {
  return async function setup() {
    await kv.clear();
  };
}

// Create standard teardown function that closes disk backend cleanly.
export function createTeardown(kv) {
  return async function teardown() {
    if (BACKEND === 'disk') {
      kv.close();
    }
  };
}
