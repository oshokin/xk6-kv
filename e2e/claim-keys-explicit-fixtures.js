import { check } from 'k6';
import { createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: EXPLICIT FIXTURE RESERVATION
// =============================================================================
//
// This scenario models deterministic fixture reservation when tests need exact
// keys (for example admin + buyer accounts) instead of random allocation.
//
// REAL-WORLD PROBLEM SOLVED:
// Explicit-key allocation should be predictable and rollback-safe:
// - claimKeys() returns claimed/busy/missing partition.
// - busy keys are separated from missing keys.
// - allOrNothing=true is best-effort rollback for this call, not a transaction.
// - releaseClaims() can clean up the claimed set in one call.
//
// METHODS TESTED:
// - setMany(): seed deterministic fixture keys.
// - claimKey(): pre-seed a busy key and verify follow-up claimability.
// - claimKeys(): mixed claim and all-or-nothing rollback paths.
// - releaseClaim() / releaseClaims(): cleanup of held leases.

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'claim-keys-explicit-fixtures';

// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME, {
  metrics: {
    operations: true,
  },
});

// options configures a deterministic single-run API contract check.
export const options = {
  vus: 1,
  iterations: 1,
  thresholds: {
    'checks{claimKeys:method-available}': ['rate==1'],
    'checks{claimKeys:claimed-busy-missing}': ['rate==1'],
    'checks{claimKeys:all-or-nothing-rollback}': ['rate==1'],
    'checks{claimKeys:batch-release-cleanup}': ['rate==1'],
  },
};

// setup clears previous state before fixture checks start.
export const setup = createSetup(kv);

// teardown closes stores.
export const teardown = createTeardown(kv);

// claimKeysExplicitFixtures validates explicit fixture lease behavior.
export default async function claimKeysExplicitFixtures() {
  await kv.clear();

  const hasClaimKeys = typeof kv.claimKeys === 'function';
  check(true, {
    'claimKeys:method-available': () => hasClaimKeys,
  });

  if (!hasClaimKeys) {
    return;
  }

  await kv.setMany({
    'users:admin': { id: 'admin' },
    'users:buyer': { id: 'buyer' },
    'users:busy': { id: 'busy' },
  });

  const busyClaim = await kv.claimKey('users:busy', {
    owner: 'e2e:busy',
    ttl: 60000,
  });

  if (!busyClaim) {
    throw new Error('failed to seed busy claim');
  }

  const mixedResult = await kv.claimKeys(['users:admin', 'users:busy', 'users:missing'], {
    owner: 'e2e:mixed',
    ttl: 60000,
    allOrNothing: false,
  });

  const allOrNothingResult = await kv.claimKeys(['users:buyer', 'users:missing'], {
    owner: 'e2e:all-or-nothing',
    ttl: 60000,
    allOrNothing: true,
  });

  const followUpClaim = await kv.claimKey('users:buyer', {
    owner: 'e2e:follow-up',
    ttl: 60000,
  });

  check(mixedResult, {
    'claimKeys:claimed-busy-missing': (result) =>
      result.claimed.length === 1 &&
      result.busy.length === 1 &&
      result.busy[0] === 'users:busy' &&
      result.missing.length === 1 &&
      result.missing[0] === 'users:missing',
  });

  check(allOrNothingResult, {
    'claimKeys:all-or-nothing-rollback': (result) =>
      result.claimed.length === 0 &&
      result.busy.length === 0 &&
      result.missing.length === 1 &&
      result.missing[0] === 'users:missing',
  });

  const cleanupClaims = [...mixedResult.claimed];
  if (followUpClaim) {
    cleanupClaims.push(followUpClaim);
  }

  const releaseMixed = cleanupClaims.length > 0
    ? await kv.releaseClaims(cleanupClaims)
    : { released: 0, failed: [] };
  const releaseBusy = await kv.releaseClaim(busyClaim);

  check({ releaseMixed, releaseBusy, followUpClaim }, {
    'claimKeys:batch-release-cleanup': (ctx) =>
      ctx.releaseBusy === true &&
      ctx.followUpClaim !== null &&
      ctx.releaseMixed.released === cleanupClaims.length &&
      ctx.releaseMixed.failed.length === 0,
  });
}
