import { check } from 'k6';
import exec from 'k6/execution';
import { VUS, ITERATIONS, createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: FEATURE FLAG DETAILED ROLLOUT CONTROL
// =============================================================================
//
// Release engineers update the same feature flag in bursts. They need enough
// context to differentiate a compare mismatch from a real infrastructure error
// without changing legacy compareAndSwap() behavior.
//
// REAL-WORLD PROBLEMS UNCOVERED:
// - Opaque CAS mismatches that are hard to debug during rollout incidents.
// - First-writer bootstrap coordination races during noisy admin bursts.
// - Regressions where detailed APIs leak "current" on successful writes.
//
// ATOMIC OPERATIONS TESTED:
// - compareAndSwapDetailed(): mismatch metadata + success payload shape.
// - compareAndSwap(): legacy behavior remains boolean-only.
// - setIfAbsent(): first-writer-wins bootstrap coordination.
//
// CONCURRENCY PATTERN:
// - Promise.all compareAndSwapDetailed() attempts share the same stale snapshot
//   to force exactly one winner, mirroring admin dashboards that fire bursts.
// - Parallel setIfAbsent() calls compete for the same bootstrap coordination key.
//
// PERFORMANCE CHARACTERISTICS:
// - Tight CAS retry surfaces without sleeps; stresses promise resolution under
//   overlapping admin updates.

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'feature-flag-cas-detailed-rollout';

// Feature flag key prefix so each VU exercises an isolated rollout document.
const FLAG_PREFIX = __ENV.FLAG_PREFIX || 'feature:payments:detailed:';

// Number of concurrent detailed CAS attempts (and bootstrap coordination attempts) per phase.
const CONCURRENT_UPDATES = parseInt(__ENV.CONCURRENT_UPDATES || '6', 10);

// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME);

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: VUS,
  iterations: ITERATIONS,
  thresholds: {
    'checks{op:casDetailed,phase:deterministic}': ['rate==1.0'],
    'checks{op:casDetailed,phase:contention}': ['rate>0.995'],
    'checks{op:setIfAbsent,phase:contention}': ['rate>0.995']
  }
};

// setup wipes previous flags so deterministic mismatch checks start from a seed.
export const setup = createSetup(kv);

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv, TEST_NAME);

// featureFlagCasDetailedRollout exercises compareAndSwapDetailed() shape invariants,
// a single-winner CAS storm from identical snapshots, and setIfAbsent() under
// contention while legacy compareAndSwap() stays boolean-only.
export default async function featureFlagCasDetailedRollout() {
  const iter = exec.scenario.iterationInTest;
  const key = `${FLAG_PREFIX}${exec.vu.idInInstance}:${iter}`;
  const seeded = {
    rollout: 10,
    version: 1,
    updatedBy: 'seed'
  };

  await kv.set(key, seeded);

  // Mismatch path without echoing the live document (smaller incident payloads).
  const mismatchNoCurrent = await kv.compareAndSwapDetailed(
    key,
    { rollout: 99, version: 1, updatedBy: 'seed' },
    { rollout: 50, version: 2, updatedBy: 'admin-0' },
    { includeCurrentOnMismatch: false }
  );

  // Mismatch path with includeCurrentOnMismatch for triage dashboards.
  const mismatchWithCurrent = await kv.compareAndSwapDetailed(
    key,
    { rollout: 99, version: 1, updatedBy: 'seed' },
    { rollout: 50, version: 2, updatedBy: 'admin-0' },
    { includeCurrentOnMismatch: true }
  );

  // Successful swap must omit "current" even when mismatch reporting would allow it.
  const snapshot = await kv.get(key);
  const proposal = {
    ...snapshot,
    rollout: (snapshot.rollout + 1) % 100,
    version: snapshot.version + 1,
    updatedBy: `vu-${exec.vu.idInInstance}`
  };
  const detailedSuccess = await kv.compareAndSwapDetailed(
    key,
    snapshot,
    proposal,
    { includeCurrentOnMismatch: true }
  );

  // Legacy compareAndSwap() stays a bare boolean (null expected is invalid here).
  const legacyMismatch = await kv.compareAndSwap(key, null, { rollout: 1 });

  check(true, {
    'casDetailed:deterministic': () =>
      mismatchNoCurrent.swapped === false &&
      mismatchNoCurrent.reason === 'mismatch' &&
      mismatchNoCurrent.existed === true &&
      !Object.prototype.hasOwnProperty.call(mismatchNoCurrent, 'current') &&
      mismatchWithCurrent.swapped === false &&
      mismatchWithCurrent.reason === 'mismatch' &&
      mismatchWithCurrent.existed === true &&
      Object.prototype.hasOwnProperty.call(mismatchWithCurrent, 'current') &&
      detailedSuccess.swapped === true &&
      detailedSuccess.reason === 'swapped' &&
      !Object.prototype.hasOwnProperty.call(detailedSuccess, 'current') &&
      legacyMismatch === false
  }, { op: 'casDetailed', phase: 'deterministic' });

  // Contention: every admin read the same snapshot; only one CAS may land.
  const contestedSnapshot = await kv.get(key);
  const contentionResults = await Promise.all(
    Array.from({ length: CONCURRENT_UPDATES }, (_, idx) =>
      kv.compareAndSwapDetailed(
        key,
        contestedSnapshot,
        {
          ...contestedSnapshot,
          version: contestedSnapshot.version + 1,
          updatedBy: `admin-${idx}`
        },
        { includeCurrentOnMismatch: true }
      )
    )
  );
  const contentionWinners = contentionResults.filter((result) => result.swapped).length;

  check(true, {
    'casDetailed:contention': () =>
      contentionWinners === 1 &&
      contentionResults.every((result) =>
        result.swapped || (result.reason === 'mismatch' && result.existed === true)
      )
  }, { op: 'casDetailed', phase: 'contention' });

  // Bootstrap coordination: parallel first-writer-wins creation for rollout keys.
  const bootstrapKey = `${key}:bootstrap-coordination`;
  const bootstrapAttempts = await Promise.all(
    Array.from({ length: CONCURRENT_UPDATES }, (_, idx) =>
      kv.setIfAbsent(bootstrapKey, `owner-${idx}`)
    )
  );
  const bootstrapWinners = bootstrapAttempts.filter(Boolean).length;

  check(true, {
    'setIfAbsent:contention': () => bootstrapWinners === 1
  }, { op: 'setIfAbsent', phase: 'contention' });
}
