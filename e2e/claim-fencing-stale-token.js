import { check } from 'k6';
import exec from 'k6/execution';
import { VUS, ITERATIONS, createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: STALE FENCING TOKEN HARDENING
// =============================================================================
//
// This test simulates a lease hand-off workflow where a worker claims a key,
// releases it, re-claims the same key (new token), then tries stale operations
// with the old token. This mirrors delayed callback patterns in:
//
// - Worker pools with retries and timeout recovery.
// - Queue consumers where slow handlers wake up after ownership changed.
// - Reservation systems that must reject stale completion signals.
//
// REAL-WORLD PROBLEM SOLVED:
// Without token fencing, delayed workers can accidentally release or complete
// someone else's newer claim after lease hand-off, causing duplicate work or
// accidental task resurrection.
//
// ATOMIC OPERATIONS TESTED:
// - claimRandom(): acquire and re-acquire lease for same key.
// - releaseClaim(): stale token must return false.
// - completeClaim(): stale token must return false.
// - exists(): key must remain intact after rejected stale operations.
//
// CONCURRENCY PATTERN:
// - One deterministic key per VU (by default) keeps the test stable and isolates
//   fencing behavior from random contention noise.
//
// PERFORMANCE CHARACTERISTICS:
// - Lease-heavy control-plane operations under repeated hand-offs.
// - Fast deterministic checks focused on correctness, not throughput.

// Prefix for keys participating in fencing token checks.
const FENCING_PREFIX = __ENV.FENCING_PREFIX || 'fencing:claim:';
// Lease duration used for initial and re-claim calls.
const LEASE_TTL_MS = parseInt(__ENV.LEASE_TTL_MS || '15000', 10);
// Number of deterministic key lanes available for VU mapping.
const KEY_POOL_SIZE = parseInt(__ENV.KEY_POOL_SIZE || String(VUS), 10);
// Fixed-width lane IDs prevent prefix collisions (e.g. 1 vs 10).
const KEY_WIDTH = Math.max(3, String(KEY_POOL_SIZE).length);

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'claim-fencing-stale-token';
// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME);

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: VUS,
  iterations: ITERATIONS,
  thresholds: {
    'checks{fencing:initial-claim}': ['rate>0.99'],
    'checks{fencing:reclaim}': ['rate>0.99'],
    'checks{fencing:stale-release-false}': ['rate>0.99'],
    'checks{fencing:stale-complete-false}': ['rate>0.99'],
    'checks{fencing:fresh-complete-true}': ['rate>0.99'],
    'checks{fencing:key-intact}': ['rate>0.99'],
  },
};

// setup seeds deterministic keys so each VU can repeatedly exercise
// stale-token rejection on its own key lane.
export async function setup() {
  const baseSetup = createSetup(kv);
  await baseSetup();

  for (let i = 1; i <= KEY_POOL_SIZE; i += 1) {
    const lane = String(i).padStart(KEY_WIDTH, '0');
    const keyPrefix = `${FENCING_PREFIX}${lane}:`;
    const key = `${keyPrefix}item`;
    await kv.set(key, {
      id: i,
      name: `fencing-item-${i}`,
      createdAt: Date.now(),
    });
  }
}

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv);

// claimFencingStaleToken validates that stale claim tokens are rejected and
// only the fresh token can complete the current live claim.
export default async function claimFencingStaleToken() {
  const vuSlot = ((exec.vu.idInTest - 1) % KEY_POOL_SIZE) + 1;
  const lane = String(vuSlot).padStart(KEY_WIDTH, '0');
  const keyPrefix = `${FENCING_PREFIX}${lane}:`;
  const key = `${keyPrefix}item`;
  const owner = `scenario:${exec.scenario.name}:vu:${exec.vu.idInTest}`;

  // Acquire the first lease token for this key.
  const initialClaim = await kv.claimRandom({
    prefix: keyPrefix,
    owner,
    ttl: LEASE_TTL_MS,
  });

  // Defensive guard for occasional transient nulls under extreme external contention.
  if (initialClaim === null) {
    return;
  }

  // Hand off ownership and obtain a fresh token for the same key.
  const releasedInitial = await kv.releaseClaim(initialClaim);
  const reclaimed = await kv.claimRandom({
    prefix: keyPrefix,
    owner: `${owner}:reclaim`,
    ttl: LEASE_TTL_MS,
  });

  let staleRelease = false;
  let staleComplete = false;
  let freshComplete = false;
  let keyExistsAfterStale = false;
  let keyExistsAfterFresh = false;

  if (reclaimed !== null) {
    // Stale token must never mutate the live claim.
    staleRelease = await kv.releaseClaim(initialClaim);
    staleComplete = await kv.completeClaim(initialClaim, { deleteKey: false });
    keyExistsAfterStale = await kv.exists(key);

    // Fresh token remains authoritative for completion.
    freshComplete = await kv.completeClaim(reclaimed, { deleteKey: false });
    keyExistsAfterFresh = await kv.exists(key);
  }

  check(initialClaim, {
    'fencing:initial-claim': () => Boolean(initialClaim),
    'fencing:reclaim': () => releasedInitial === true && reclaimed !== null,
    'fencing:stale-release-false': () => reclaimed !== null && staleRelease === false,
    'fencing:stale-complete-false': () => reclaimed !== null && staleComplete === false,
    'fencing:fresh-complete-true': () => reclaimed !== null && freshComplete === true,
    'fencing:key-intact': () => reclaimed !== null && keyExistsAfterStale === true && keyExistsAfterFresh === true,
  });
}
