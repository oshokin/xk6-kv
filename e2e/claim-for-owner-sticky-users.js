import { check } from 'k6';
import { createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: STICKY OWNER CLAIM ALLOCATION
// =============================================================================
//
// This scenario validates sticky allocation semantics for claimForOwner().
// One logical owner should keep receiving the same live claim for a given
// prefix until the lease is released/completed/expired.
//
// REAL-WORLD PROBLEM SOLVED:
// Load tests often require stable identity binding (e.g. per-VU user/session):
// - same owner should not randomly switch users between iterations;
// - different owners should not collide on one live claim;
// - after release, owner should lazily rebind to a new live claim.
//
// METHODS TESTED:
// - claimForOwner(): sticky hit, prefix partitioning, release rebind.
// - releaseClaim(): binding invalidation trigger.
//
// EXECUTION MODEL:
// - Intentional 1x1 functional contract check.
// - Deterministic setup seeding; no concurrent reseed inside default().

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'claim-for-owner-sticky-users';

// Prefixes used to validate owner-stickiness partitioning.
const USERS_PREFIX = 'users:';
const ADMINS_PREFIX = 'admins:';

// Sticky owners used in the scenario.
const OWNER_A = 'scenario:checkout:vu:1';
const OWNER_B = 'scenario:checkout:vu:2';

// Lease duration used for claim allocations.
const CLAIM_TTL_MS = 60_000;

// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME, {
  metrics: { operations: true },
});

// options configures deterministic pass/fail checks.
export const options = {
  vus: 1,
  iterations: 1,
  thresholds: {
    'checks{claimForOwner:methods-available}': ['rate==1'],
    'checks{claimForOwner:sticky-hit}': ['rate==1'],
    'checks{claimForOwner:no-implicit-renew}': ['rate==1'],
    'checks{claimForOwner:different-owner-different-key}': ['rate==1'],
    'checks{claimForOwner:prefix-partitions-identity}': ['rate==1'],
    'checks{claimForOwner:release-invalidates-binding}': ['rate==1'],
    'checks{claimForOwner:empty-prefix-null}': ['rate==1'],
  },
};

// setup clears state once and seeds deterministic claim pools.
export async function setup() {
  const baseSetup = createSetup(kv);
  await baseSetup();

  await kv.setMany({
    'users:1': { id: 1, username: 'user-1', role: 'buyer' },
    'users:2': { id: 2, username: 'user-2', role: 'buyer' },
    'users:3': { id: 3, username: 'user-3', role: 'buyer' },
    'admins:1': { id: 11, username: 'admin-1', role: 'admin' },
  });
}

// teardown closes stores.
export const teardown = createTeardown(kv);

// claimForOwnerStickyUsers validates sticky claim identity and rebind behavior.
export default async function claimForOwnerStickyUsers() {
  const methodsAvailable =
    typeof kv.claimForOwner === 'function' &&
    typeof kv.releaseClaim === 'function';

  check(true, {
    'claimForOwner:methods-available': () => methodsAvailable,
  });

  if (!methodsAvailable) {
    return;
  }

  const ownerAFirst = await kv.claimForOwner({
    prefix: USERS_PREFIX,
    owner: OWNER_A,
    ttl: CLAIM_TTL_MS,
  });
  const ownerASecond = await kv.claimForOwner({
    prefix: USERS_PREFIX,
    owner: OWNER_A,
    ttl: CLAIM_TTL_MS * 2,
  });
  const ownerBClaim = await kv.claimForOwner({
    prefix: USERS_PREFIX,
    owner: OWNER_B,
    ttl: CLAIM_TTL_MS,
  });
  const ownerAAdminClaim = await kv.claimForOwner({
    prefix: ADMINS_PREFIX,
    owner: OWNER_A,
    ttl: CLAIM_TTL_MS,
  });
  const emptyClaim = await kv.claimForOwner({
    prefix: 'missing:claim-for-owner:',
    owner: 'scenario:missing:vu:1',
    ttl: CLAIM_TTL_MS,
  });

  let releasedOwnerA = false;
  let ownerARebound = null;

  if (ownerAFirst !== null) {
    releasedOwnerA = await kv.releaseClaim(ownerAFirst);
    ownerARebound = await kv.claimForOwner({
      prefix: USERS_PREFIX,
      owner: OWNER_A,
      ttl: CLAIM_TTL_MS,
    });
  }

  check(true, {
    'claimForOwner:sticky-hit': () =>
      ownerAFirst !== null &&
      ownerASecond !== null &&
      ownerAFirst.id === ownerASecond.id &&
      ownerAFirst.key === ownerASecond.key &&
      ownerAFirst.token === ownerASecond.token,
    'claimForOwner:no-implicit-renew': () =>
      ownerAFirst !== null &&
      ownerASecond !== null &&
      ownerAFirst.expiresAt === ownerASecond.expiresAt,
    'claimForOwner:different-owner-different-key': () =>
      ownerAFirst !== null &&
      ownerBClaim !== null &&
      ownerAFirst.key !== ownerBClaim.key &&
      ownerAFirst.id !== ownerBClaim.id,
    'claimForOwner:prefix-partitions-identity': () =>
      ownerAFirst !== null &&
      ownerAAdminClaim !== null &&
      ownerAFirst.key !== ownerAAdminClaim.key &&
      ownerAFirst.id !== ownerAAdminClaim.id,
    'claimForOwner:release-invalidates-binding': () =>
      ownerAFirst !== null &&
      releasedOwnerA === true &&
      ownerARebound !== null &&
      ownerAFirst.id !== ownerARebound.id &&
      ownerAFirst.token !== ownerARebound.token,
    'claimForOwner:empty-prefix-null': () => emptyClaim === null,
  });

  if (ownerBClaim !== null) {
    await kv.releaseClaim(ownerBClaim);
  }
  if (ownerAAdminClaim !== null) {
    await kv.releaseClaim(ownerAAdminClaim);
  }
  if (ownerARebound !== null) {
    await kv.releaseClaim(ownerARebound);
  }
}
