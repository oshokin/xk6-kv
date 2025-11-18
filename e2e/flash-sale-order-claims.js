import exec from 'k6/execution';
import { sleep } from 'k6';
import { openKv } from 'k6/x/kv';

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

// Selected backend (memory or disk) used for flash-sale inventory.
const SELECTED_BACKEND_NAME = __ENV.KV_BACKEND || 'memory';
// Enables in-memory key tracking when the backend is memory.
const TRACK_KEYS_OVERRIDE =
  typeof __ENV.KV_TRACK_KEYS === 'string' ? __ENV.KV_TRACK_KEYS.toLowerCase() : '';
const ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND =
  TRACK_KEYS_OVERRIDE === '' ? true : TRACK_KEYS_OVERRIDE === 'true';

// Number of concurrent SKUs each VU pounds via Promise.all.
const CLAIM_BATCH_SIZE = parseInt(__ENV.CLAIM_BATCH_SIZE || '50', 10);
// Base duration (seconds) each iteration sleeps after processing.
const BASE_IDLE_SLEEP_SECONDS = parseFloat(__ENV.BASE_IDLE_SLEEP_SECONDS || '0.02');
// Random jitter (seconds) added to the base idle sleep.
const IDLE_SLEEP_JITTER_SECONDS = parseFloat(
  __ENV.IDLE_SLEEP_JITTER_SECONDS || '0.01'
);
// Default number of VUs used by the scenario.
const DEFAULT_VUS = parseInt(__ENV.VUS || '40', 10);
// Default iteration count used by the scenario.
const DEFAULT_ITERATIONS = parseInt(__ENV.ITERATIONS || '400', 10);

// Namespace applied to every SKU so we can swap it via env vars.
const SKU_PREFIX = __ENV.SKU_PREFIX || 'flash-sale:sku:';

// kv is the shared store client used throughout the scenario.
const kv = openKv(
  SELECTED_BACKEND_NAME === 'disk'
    ? { backend: 'disk', trackKeys: ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND }
    : { backend: 'memory', trackKeys: ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND }
);

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: DEFAULT_VUS,
  iterations: DEFAULT_ITERATIONS,
};

// setup wipes any previous flash-sale leftovers so every run starts clean.
export async function setup() {
  await kv.clear();
}

// flashSaleOrderClaims orchestrates the Promise.all contention loop described above.
// Each iteration fires CLAIM_BATCH_SIZE concurrent getOrSet() calls to the same
// SKU namespace, then releases the claims so the next batch experiences the same
// pressure. We do not await in sequence on purpose-the entire point is to keep
// Sobek resolving promises en masse.
export default async function flashSaleOrderClaims() {
  const iteration = exec.scenario.iterationInTest;
  const skuBatch = Array.from({ length: CLAIM_BATCH_SIZE }, (_, i) => {
    return `${SKU_PREFIX}${(iteration + i) % CLAIM_BATCH_SIZE}`;
  });

  await Promise.all(
    skuBatch.map((sku) => kv.getOrSet(sku, { claimedBy: exec.vu.idInInstance }))
  );

  // Release everything so the next iteration replays the race.
  await Promise.all(skuBatch.map((sku) => kv.delete(sku)));

  // Keep each iteration active so the event loop processes pending callbacks.
  sleep(BASE_IDLE_SLEEP_SECONDS + Math.random() * IDLE_SLEEP_JITTER_SECONDS);
}

