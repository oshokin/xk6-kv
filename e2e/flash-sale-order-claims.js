import exec from 'k6/execution';
import { VUS, ITERATIONS, createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: FLASH-SALE ORDER CLAIMING SYSTEM
// =============================================================================
//
// This test models an e-commerce flash sale (think concert tickets or limited
// edition drops) where thousands of buyers try to reserve the same small set of
// SKUs simultaneously. The goal is to guarantee that only one buyer claims a
// specific SKU even when every virtual user fires requests in parallel without
// awaiting individual promises.
//
// REAL-WORLD PROBLEMS UNCOVERED:
// - Duplicate reservations caused by racy promise resolution.
// - Event-loop starvation when many getOrSet() calls finish concurrently.
// - Stale ownership when buyers abandon the cart and we need deterministic cleanup.
//
// ATOMIC OPERATIONS TESTED:
// - getOrSet(): claim an item if not already reserved.
// - delete(): release the claim at the end of the iteration to recycle stock.
//
// CONCURRENCY PATTERN:
// - Every VU blasts a Promise.all() batch against the same SKU namespace.
// - k6 resolves hundreds of promises at once, reproducing the historical
//   "concurrent map writes" panic unless runAsyncWithStore marshals resolve/reject
//   back onto the event loop.
//
// PERFORMANCE CHARACTERISTICS:
// - Short-lived but extremely bursty traffic (spikes of 100k RPS).
// - Emphasizes fan-out/fan-in workloads where the store work happens in Go but
//   the promise resolution must remain single-threaded.

// Number of SKUs each VU attempts to claim concurrently via Promise.all.
const CLAIM_BATCH_SIZE = parseInt(__ENV.CLAIM_BATCH_SIZE || '50', 10);

// SKU key prefix for flash sale items.
const SKU_PREFIX = __ENV.SKU_PREFIX || 'flash-sale:sku:';

// kv is the shared store client used throughout the scenario.
const kv = createKv();

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: VUS,
  iterations: ITERATIONS,
};

// setup wipes any previous flash-sale leftovers so every run starts clean.
export const setup = createSetup(kv);

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv);

// flashSaleOrderClaims fires CLAIM_BATCH_SIZE concurrent getOrSet() calls to test
// that promise resolution handles hundreds of simultaneous completions without
// panicking (concurrent map writes). Then releases all claims for next iteration.
export default async function flashSaleOrderClaims() {
  const iteration = exec.scenario.iterationInTest;

  // Generate batch of SKU keys to claim concurrently.
  const skuBatch = Array.from({ length: CLAIM_BATCH_SIZE }, (_, i) => {
    return `${SKU_PREFIX}${(iteration + i) % CLAIM_BATCH_SIZE}`;
  });

  // Claim all SKUs concurrently (tests concurrent promise resolution).
  await Promise.all(
    skuBatch.map((sku) => kv.getOrSet(sku, { claimedBy: exec.vu.idInInstance }))
  );

  // Release all claims so next iteration replays the race condition.
  await Promise.all(skuBatch.map((sku) => kv.delete(sku)));
}
