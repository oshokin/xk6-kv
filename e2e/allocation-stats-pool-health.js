import { check } from 'k6';
import { createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: PREFIX POOL HEALTH DIAGNOSTICS
// =============================================================================
//
// This scenario models teams monitoring claim-based pools by business prefix
// (for example, "users:", "accounts:", "devices:") during long-running tests.
//
// REAL-WORLD PROBLEM SOLVED:
// Prefix-specific observability should be explicit and low-risk:
// - allocationStats() returns stable snapshot shape for dashboards/debugging.
// - claimable/live counters reflect claim lifecycle transitions.
// - backend and trackKeys flags are visible for diagnostics.
// - no high-cardinality prefix labels are introduced into metrics.
//
// METHODS TESTED:
// - setMany(): seed deterministic pool keys.
// - claimRandomMany(): acquire active claims.
// - releaseClaim(): return one lease to pool.
// - allocationStats(): verify before/after pool health snapshots.

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'allocation-stats-pool-health';

// Prefix used for pool health checks.
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
    'checks{allocationStats:method-available}': ['rate==1'],
    'checks{allocationStats:initial-shape}': ['rate==1'],
    'checks{allocationStats:initial-counts}': ['rate==1'],
    'checks{allocationStats:after-release-counts}': ['rate==1'],
    'checks{allocationStats:after-all-released}': ['rate==1'],
    'checks{allocationStats:backend-known}': ['rate==1'],
  },
};

// setup clears previous state before pool diagnostics start.
export const setup = createSetup(kv);

// teardown closes stores.
export const teardown = createTeardown(kv);

// allocationStatsPoolHealth validates prefix-scoped pool counters.
export default async function allocationStatsPoolHealth() {
  await kv.clear();

  const hasAllocationStats = typeof kv.allocationStats === 'function';
  check(true, {
    'allocationStats:method-available': () => hasAllocationStats,
  });

  if (!hasAllocationStats) {
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
    count: 2,
    owner: 'e2e:allocation-stats',
    ttl: 60000,
  });

  const first = await kv.allocationStats({ prefix: PREFIX });

  if (claims[0]) {
    await kv.releaseClaim(claims[0]);
  }

  const second = await kv.allocationStats({ prefix: PREFIX });

  if (claims[1]) {
    await kv.releaseClaim(claims[1]);
  }
  const third = await kv.allocationStats({ prefix: PREFIX });

  check(first, {
    'allocationStats:initial-shape': (stats) =>
      stats !== null &&
      typeof stats.prefix === 'string' &&
      typeof stats.total === 'number' &&
      typeof stats.claimable === 'number' &&
      typeof stats.claimedLive === 'number' &&
      typeof stats.claimedExpired === 'number' &&
      typeof stats.trackKeys === 'boolean',
    'allocationStats:initial-counts': (stats) =>
      stats.prefix === PREFIX &&
      stats.total === 5 &&
      stats.claimable === 3 &&
      stats.claimedLive === 2 &&
      stats.claimedExpired === 0,
    'allocationStats:backend-known': (stats) =>
      stats.backend === 'memory' || stats.backend === 'disk',
  });

  check(second, {
    'allocationStats:after-release-counts': (stats) =>
      stats.prefix === PREFIX &&
      stats.total === 5 &&
      stats.claimable === 4 &&
      stats.claimedLive === 1 &&
      stats.claimedExpired === 0,
  });

  // Keep e2e deterministic: verify live/release transitions only.
  // Expired-claim timing behavior is covered in Go unit tests by forcing
  // claim metadata expiration directly.
  check(third, {
    'allocationStats:after-all-released': (stats) =>
      stats.prefix === PREFIX &&
      stats.total === 5 &&
      stats.claimable === 5 &&
      stats.claimedLive === 0 &&
      stats.claimedExpired === 0,
  });
}
