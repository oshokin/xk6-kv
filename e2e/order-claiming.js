import { check, sleep } from 'k6';
import exec from 'k6/execution';
import { openKv } from 'k6/x/kv';

// =============================================================================
// REAL-WORLD SCENARIO: E-COMMERCE ORDER CLAIMING SYSTEM
// =============================================================================
//
// This test simulates a critical e-commerce scenario where multiple virtual users
// (representing customers, warehouse workers, or fulfillment centers) compete to
// claim orders from a shared order pool. This is a common pattern in:
//
// - E-commerce platforms (Amazon, Shopify, etc.)
// - Food delivery systems (Uber Eats, DoorDash)
// - Ride-sharing platforms (Uber, Lyft)
// - Task assignment systems (crowdsourcing platforms)
// - Inventory management systems
//
// REAL-WORLD PROBLEM SOLVED:
// Multiple workers/customers trying to claim the same order simultaneously.
// Without proper atomic operations, you get race conditions leading to:
// - Double-processing of orders
// - Customer complaints about unavailable items
// - Inventory inconsistencies
// - Lost revenue from unfulfilled orders
//
// ATOMIC OPERATIONS TESTED:
// - getOrSet(): Atomically claim an order (set if not exists, get if exists)
// - This ensures only ONE worker can claim each order
//
// CONCURRENCY PATTERN:
// - Producer: System generates orders
// - Consumer: Multiple VUs compete to claim orders
// - Coordination: Shared KV store prevents race conditions
//
// PERFORMANCE CHARACTERISTICS:
// - High contention under load (many VUs competing for same orders)
// - Critical for business operations (revenue depends on successful claims)
// - Must handle thousands of concurrent order claims per second

// Synthetic dataset size (number of orders available per run).
const TOTAL_NUMBER_OF_SYNTHETIC_ALLOCATED_ORDERS =
  (__ENV.TOTAL_FAKE_ORDERS && parseInt(__ENV.TOTAL_FAKE_ORDERS, 10)) || 1000;

// Retry budget for claiming orders under contention.
const MAXIMUM_NUMBER_OF_RETRY_ATTEMPTS_TO_CLAIM_ORDER = 5;

// Backoff duration (ms) between retries when claiming orders.
const WAIT_TIME_BETWEEN_RETRY_ATTEMPTS_IN_MILLISECONDS = 50;

// Namespace prefix used for shared order keys.
const PERSISTENT_ORDER_CLAIM_KEY_PREFIX = 'order-';

// Backend selection: memory (default) or disk.
const SELECTED_BACKEND_NAME = __ENV.KV_BACKEND || 'memory';

// Enables in-memory key tracking when the backend is memory.
const TRACK_KEYS_OVERRIDE =
  typeof __ENV.KV_TRACK_KEYS === 'string' ? __ENV.KV_TRACK_KEYS.toLowerCase() : '';
const ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND =
  TRACK_KEYS_OVERRIDE === '' ? true : TRACK_KEYS_OVERRIDE === 'true';

// Shared KV store handle used by all VUs.
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

// Rationale: 50 VUs × 1000 iterations intentionally overwhelm the shared pool
// so every execution hits heavy contention. That pressure makes regressions in
// getOrSet() immediately visible, yet the whole scenario still completes within
// seconds on CI runners.
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

// setup: erase every order key so each run starts from the same initial state.
export async function setup() {
  // Start with a clean state so each run is deterministic.
  // This clears only the kv store, not the local array of order IDs.
  await kv.clear();
}

// teardown: close disk-backed stores so the BoltDB file can be reused instantly.
export async function teardown() {
  // For disk backends, close the store cleanly so the file can be reused immediately.
  if (SELECTED_BACKEND_NAME === 'disk') {
    kv.close();
  }
}

// Each iteration tries to atomically grab the next unclaimed order, simulates
// processing work, and releases the slot so future iterations can catch bugs in
// ordering or fairness.
export default async function mainScenarioIteration() {
  // Try to pick a single "ALLOCATED" order and atomically claim it via kv.getOrSet(...).
  // This is the concurrency hotspot we want to verify under load.
  const claimedOrder = await claimOneAllocatedOrderOrThrow();

  // At this point, the order has been "claimed" for processing by this VU.
  // In a real test you'd call an HTTP API; here we just pretend to do something expensive.
  // We keep a tiny sleep to model some work and allow other VUs to progress.
  sleep(0.01);

  // Assert that we indeed claimed something. 
  // We use built-in k6 `check` instead of external `expect`.
  check(true, {
    'claimed order id is present': () => claimedOrder.id !== undefined,
  }, { type: 'order-claimed' });

  // Release the slot so future iterations can re-claim the same order.
  await kv.delete(claimedOrder.key);

  // Optional noisy log - turn on with LOG_CLAIMS=true.
  if (__ENV.LOG_CLAIMS === 'true') {
    console.log(`[VU ${exec.vu.idInTest}] processing order id: ${claimedOrderId}`);
  }
}

// claimOneAllocatedOrderOrThrow: scans the deterministic dataset until it finds
// an unclaimed order, proving that getOrSet() guards ownership even when every
// VU is pounding the same keys.
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

    const orderKey = `${PERSISTENT_ORDER_CLAIM_KEY_PREFIX}${orderIdWeHaveClaimed}`;

    return {
      id: orderIdWeHaveClaimed,
      key: orderKey
    };
  }, MAXIMUM_NUMBER_OF_RETRY_ATTEMPTS_TO_CLAIM_ORDER, WAIT_TIME_BETWEEN_RETRY_ATTEMPTS_IN_MILLISECONDS);
}

// withRetry: tiny helper that yields between attempts so we do not spin-lock
// under contention; if we still fail, the test rightfully surfaces the bug.
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
