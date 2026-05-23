import { check } from 'k6';
import { createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: BATCH CLAIM LIFECYCLE CLEANUP
// =============================================================================
//
// This scenario models batch worker cleanup where scripts allocate leases in
// groups, then renew, complete, or release them in one call.
//
// REAL-WORLD PROBLEM SOLVED:
// Teams need partial-success lifecycle helpers with deterministic outcomes:
// - releaseClaims() returns batch cleanup status.
// - completeClaims() reports completed vs stale claims.
// - renewClaims() extends active leases in one call.
// - stale second release is surfaced as ClaimNotUpdated.
//
// METHODS TESTED:
// - setMany(): seed deterministic user pool.
// - claimRandomMany(): acquire a bounded claim batch.
// - renewClaims(): renew all acquired claims.
// - releaseClaims(): release subset + stale replay validation.
// - completeClaims(): complete remaining claim.
// - allocationStats(): verify post-lifecycle pool consistency.

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'batch-claim-lifecycle';

// Prefix for user pool allocation.
const PREFIX = 'users:';

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
    'checks{batchClaimLifecycle:methods-available}': ['rate==1'],
    'checks{batchClaimLifecycle:claim-random-many}': ['rate==1'],
    'checks{batchClaimLifecycle:release-partial-success}': ['rate==1'],
    'checks{batchClaimLifecycle:complete-success}': ['rate==1'],
    'checks{batchClaimLifecycle:release-stale-failure}': ['rate==1'],
    'checks{batchClaimLifecycle:allocation-consistent}': ['rate==1'],
  },
};

// setup clears previous state before claim lifecycle checks start.
export const setup = createSetup(kv);

// teardown closes stores.
export const teardown = createTeardown(kv, TEST_NAME);

// batchClaimLifecycle validates batch renewal/completion/release semantics.
export default async function batchClaimLifecycle() {
  await kv.clear();

  const methodsAvailable =
    typeof kv.releaseClaims === 'function' &&
    typeof kv.completeClaims === 'function' &&
    typeof kv.renewClaims === 'function';

  check(true, {
    'batchClaimLifecycle:methods-available': () => methodsAvailable,
  });

  if (!methodsAvailable) {
    return;
  }

  await kv.setMany({
    'users:1': { id: 1 },
    'users:2': { id: 2 },
    'users:3': { id: 3 },
    'users:4': { id: 4 },
    'users:5': { id: 5 },
  });

  const claims = await kv.claimRandomMany({
    prefix: PREFIX,
    count: 3,
    owner: 'e2e:batch-claim',
    ttl: 60000,
  });

  check(claims, {
    'batchClaimLifecycle:claim-random-many': (items) => Array.isArray(items) && items.length === 3,
  });

  if (claims.length !== 3) {
    return;
  }

  const renewResult = await kv.renewClaims(claims, { ttl: 45000 });
  const releaseResult = await kv.releaseClaims(claims.slice(0, 2));
  const completeResult = await kv.completeClaims([claims[2]], { deleteKey: true });
  const staleReleaseResult = await kv.releaseClaims([claims[2]]);

  check(releaseResult, {
    'batchClaimLifecycle:release-partial-success': (result) =>
      result.attempted === 2 &&
      result.released === 2 &&
      Array.isArray(result.failed) &&
      result.failed.length === 0,
  });

  check(completeResult, {
    'batchClaimLifecycle:complete-success': (result) =>
      result.attempted === 1 &&
      result.completed === 1 &&
      Array.isArray(result.failed) &&
      result.failed.length === 0,
  });

  check(staleReleaseResult, {
    'batchClaimLifecycle:release-stale-failure': (result) =>
      result.attempted === 1 &&
      result.released === 0 &&
      Array.isArray(result.failed) &&
      result.failed.length === 1 &&
      result.failed[0].name === 'ClaimNotUpdated',
  });

  if (typeof kv.allocationStats === 'function') {
    const stats = await kv.allocationStats({ prefix: PREFIX });
    check({ stats, renewResult }, {
      'batchClaimLifecycle:allocation-consistent': (ctx) =>
        ctx.renewResult.renewed === 3 &&
        ctx.stats.total === 4 &&
        ctx.stats.claimable === 4 &&
        ctx.stats.claimedLive === 0 &&
        ctx.stats.claimedExpired === 0,
    });
  } else {
    check(renewResult, {
      'batchClaimLifecycle:allocation-consistent': (result) => result.renewed === 3,
    });
  }
}
