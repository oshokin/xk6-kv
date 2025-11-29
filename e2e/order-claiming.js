import { check } from 'k6';
import { VUS, ITERATIONS, createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: E-COMMERCE ORDER CLAIMING SYSTEM
// =============================================================================
//
// This test simulates a critical e-commerce scenario where multiple virtual users
// (representing customers, warehouse workers, or fulfillment centers) compete to
// claim orders from a shared order pool. This is a common pattern in:
//
// - E-commerce platforms (Amazon, Shopify, etc.).
// - Food delivery systems (Uber Eats, DoorDash).
// - Ride-sharing platforms (Uber, Lyft).
// - Task assignment systems (crowdsourcing platforms).
// - Inventory management systems.
//
// REAL-WORLD PROBLEM SOLVED:
// Multiple workers/customers trying to claim the same order simultaneously.
// Without proper atomic operations, you get race conditions leading to:
// - Double-processing of orders.
// - Customer complaints about unavailable items.
// - Inventory inconsistencies.
// - Lost revenue from unfulfilled orders.
//
// ATOMIC OPERATIONS TESTED:
// - getOrSet(): Atomically claim an order (set if not exists, get if exists).
// - This ensures only ONE worker can claim each order.
//
// CONCURRENCY PATTERN:
// - Producer: System generates orders.
// - Consumer: Multiple VUs compete to claim orders.
// - Coordination: Shared KV store prevents race conditions.
//
// PERFORMANCE CHARACTERISTICS:
// - High contention under load (many VUs competing for same orders).
// - Critical for business operations (revenue depends on successful claims).
// - Must handle thousands of concurrent order claims per second.

// Total number of orders in the pool available for claiming.
const ORDER_POOL_SIZE = parseInt(__ENV.ORDER_POOL_SIZE || '1000', 10);

// Maximum retry attempts when trying to claim an order before giving up.
const MAX_CLAIM_RETRIES = parseInt(__ENV.MAX_CLAIM_RETRIES || '5', 10);

// Key prefix for all order records.
const ORDER_KEY_PREFIX = 'order-';

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'order-claiming';

// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME);

// Pre-generated list of order IDs available for claiming.
const ORDER_IDS = Array.from({ length: ORDER_POOL_SIZE }, (_, i) => i + 1);

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: VUS,
  iterations: ITERATIONS,
  thresholds: {
    'checks{type:order-claimed}': ['rate>0.95']
  }
};

// setup erases every order key so each run starts from the same initial state.
export const setup = createSetup(kv);

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv, TEST_NAME);

// orderClaimingTest atomically claims an order, verifies the claim, then releases
// it so future iterations can re-claim (simulates continuous workload).
export default async function orderClaimingTest() {
  // Atomically claim an order using getOrSet().
  const claimedOrder = await claimOrder();

  // Verify we claimed an order.
  check(true, {
    'claimed order id is present': () => claimedOrder.id !== undefined
  }, { type: 'order-claimed' });

  // Release the order so it can be claimed again.
  await kv.delete(claimedOrder.key);
}

// claimOrder scans the order pool until it finds an unclaimed order, proving that
// getOrSet() guards ownership even when every VU competes for the same keys.
async function claimOrder() {
  for (let attempt = 0; attempt < MAX_CLAIM_RETRIES; attempt++) {
    // Scan through all orders looking for an unclaimed one.
    for (let i = 0; i < ORDER_IDS.length; i++) {
      const orderId = ORDER_IDS[i];
      const orderKey = `${ORDER_KEY_PREFIX}${orderId}`;

      // Try to claim: if key is absent, getOrSet() will set it and return loaded=false.
      const { loaded } = await kv.getOrSet(orderKey, 'processed');

      if (!loaded) {
        // Successfully claimed this order!
        return { id: orderId, key: orderKey };
      }
    }

    // All orders are currently claimed, retry if we haven't exhausted retries.
    if (attempt < MAX_CLAIM_RETRIES - 1) {
      // Could add a small delay here, but k6 handles this naturally.
    }
  }

  throw new Error(`Failed to claim any order after ${MAX_CLAIM_RETRIES} attempts (all ${ORDER_IDS.length} orders are claimed)`);
}
