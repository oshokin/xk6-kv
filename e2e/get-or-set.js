import { check, sleep } from 'k6';
import exec from 'k6/execution';
import { openKv } from 'k6/x/kv';

// The total number of synthetic "orders" we pretend to pull from a backend.
// We intentionally keep this configurable via env var to scale tests quickly.
const TOTAL_NUMBER_OF_SYNTHETIC_ALLOCATED_ORDERS =
  (__ENV.TOTAL_FAKE_ORDERS && parseInt(__ENV.TOTAL_FAKE_ORDERS, 10)) || 1000;

// We will try a few times to "claim" an order in case of a race.
const MAXIMUM_NUMBER_OF_RETRY_ATTEMPTS_TO_CLAIM_ORDER = 5;

// The wait time between retry attempts in milliseconds.
// We use a tiny sleep to yield in heavy contention scenarios.
const WAIT_TIME_BETWEEN_RETRY_ATTEMPTS_IN_MILLISECONDS = 50;

// We generate stable, readable order keys for kv.
// The same prefix is used across all VUs to test inter-VU coordination.
const PERSISTENT_ORDER_CLAIM_KEY_PREFIX = 'order-';

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

// ---------------------------------------------------------------
// Module-scoped synthetic "ALLOCATED" orders that every VU sees.
// ---------------------------------------------------------------
const SYNTHETIC_ALLOCATED_ORDER_ID_LIST = Array.from(
  { length: TOTAL_NUMBER_OF_SYNTHETIC_ALLOCATED_ORDERS },
  (_, i) => i + 1, // Use 1..N for friendlier logs
);

// -----------------
// k6 test options.
// -----------------
export const options = {
  // Vary these to increase contention. Start with 50×1000 like the shared script.
  vus: parseInt(__ENV.VUS || '50', 10),
  iterations: parseInt(__ENV.ITERATIONS || '1000', 10),

  // Optional: add thresholds to fail fast if we start choking.
  thresholds: {
    // Require that at least 95% of iterations claim an order successfully.
    'checks{type:order-claimed}': ['rate>0.95'],
  },
};

// -----------------------
// Test setup & teardown.
// -----------------------
export async function setup() {
  // Start with a clean state so each run is deterministic.
  // This clears only the kv store, not the local array of order IDs.
  await kv.clear();
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
export default async function mainScenarioIteration() {
  // Try to pick a single "ALLOCATED" order and atomically claim it via kv.getOrSet(...).
  // This is the concurrency hotspot we want to verify under load.
  const claimedOrderId = await claimOneAllocatedOrderOrThrow();

  // At this point, the order has been "claimed" for processing by this VU.
  // In a real test you'd call an HTTP API; here we just pretend to do something expensive.
  // We keep a tiny sleep to model some work and allow other VUs to progress.
  sleep(0.01);

  // Assert that we indeed claimed something. 
  // We use built-in k6 `check` instead of external `expect`.
  check(true, {
    'claimed order id is present': () => claimedOrderId !== undefined,
  }, { type: 'order-claimed' });

  // Optional noisy log - turn on with LOG_CLAIMS=true.
  if (__ENV.LOG_CLAIMS === 'true') {
    console.log(`[VU ${exec.vu.idInTest}] processing order id: ${claimedOrderId}`);
  }
}

// ----------------------------------------------------------
// Helper: atomically "claim" an unprocessed order using KV.
// ----------------------------------------------------------
async function claimOneAllocatedOrderOrThrow() {
  // Loop over the list of order IDs and
  // try to atomically "claim" the very first one whose key is absent.
  // That is: kv.getOrSet("order-<id>", "processed") returns {loaded:false}.

  return withRetry(async () => {
    let orderIdWeHaveClaimed;

    // Iterate over the SYNTHETIC "ALLOCATED" orders.
    for (let i = 0; i < SYNTHETIC_ALLOCATED_ORDER_ID_LIST.length; i++) {
      const currentIdCandidate =
        SYNTHETIC_ALLOCATED_ORDER_ID_LIST[i % SYNTHETIC_ALLOCATED_ORDER_ID_LIST.length];

      // The heart of the test: if the key is absent, this call will set it and return loaded=false,
      // giving us exclusive "claim" of this order ID across all concurrent VUs.
      const { loaded } = await kv.getOrSet(
        `${PERSISTENT_ORDER_CLAIM_KEY_PREFIX}${currentIdCandidate}`,
        'processed'
      );

      if (!loaded) {
        orderIdWeHaveClaimed = currentIdCandidate;
        break;
      }
    }

    if (!orderIdWeHaveClaimed) {
      throw new Error('No unprocessed orders found (all claimed already).');
    }

    return orderIdWeHaveClaimed;
  }, MAXIMUM_NUMBER_OF_RETRY_ATTEMPTS_TO_CLAIM_ORDER, WAIT_TIME_BETWEEN_RETRY_ATTEMPTS_IN_MILLISECONDS);
}

// -------------------------------
// Helper: generic retry wrapper.
// -------------------------------
async function withRetry(fn, maximumAttempts, intervalMs) {
  let lastObservedError;
  for (let attemptNumber = 0; attemptNumber < maximumAttempts; attemptNumber++) {
    try {
      return await fn();
    } catch (err) {
      lastObservedError = err;

      // Yield a tiny bit to avoid tight spin loops under contention.
      if (attemptNumber < maximumAttempts - 1) {
        sleep(intervalMs / 1000);
      }
    }
  }
  throw lastObservedError;
}
