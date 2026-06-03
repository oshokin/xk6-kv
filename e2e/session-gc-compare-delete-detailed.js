import { check } from 'k6';
import exec from 'k6/execution';
import { VUS, ITERATIONS, createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: SESSION GC WITH DETAILED DELETE DIAGNOSTICS
// =============================================================================
//
// Session cleanup workers race to remove expired sessions. During incidents,
// operators need clear mismatch diagnostics while preserving the old boolean API.
//
// REAL-WORLD PROBLEMS UNCOVERED:
// - Hard-to-triage compare-and-delete failures during bursty cleanup waves.
// - Confusion around null/undefined expected-value semantics.
// - Regressions where successful detailed deletes incorrectly include "current".
//
// ATOMIC OPERATIONS TESTED:
// - compareAndDeleteDetailed(): mismatch metadata + success payload shape.
// - compareAndDelete(): legacy boolean semantics (including null comparison).
// - set(), exists(): setup and post-condition checks.
//
// CONCURRENCY PATTERN:
// - Promise.all batches mirror noisy GC sweeps where many workers delete the same
//   session key and must observe deterministic winner/loser detailed results.
//
// PERFORMANCE CHARACTERISTICS:
// - Bursty compare-and-delete traffic with simultaneous promise resolution back
//   to the Sobek event loop.

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'session-gc-compare-delete-detailed';

// Prefix for session keys exercised by detailed compare-and-delete probes.
const SESSION_PREFIX = __ENV.SESSION_PREFIX || 'gc-session:detailed:';

// Number of concurrent workers racing compareAndDeleteDetailed() on the contention key.
const CONCURRENT_CLEANERS = parseInt(__ENV.CONCURRENT_CLEANERS || '12', 10);

// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME);

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: VUS,
  iterations: ITERATIONS,
  thresholds: {
    'checks{op:compareDeleteDetailed,phase:deterministic}': ['rate==1.0'],
    'checks{op:compareDeleteDetailed,phase:contention}': ['rate>0.995'],
    'checks{op:compareDelete,contract:null-semantics}': ['rate==1.0']
  }
};

// setup clears stale data before deterministic and contention phases run.
export const setup = createSetup(kv);

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv);

// sessionGcCompareDeleteDetailed validates compareAndDeleteDetailed() mismatch
// payloads, null/undefined contract parity with compareAndDelete(), and a
// single-winner contention race on the same session key.
export default async function sessionGcCompareDeleteDetailed() {
  const iter = exec.scenario.iterationInTest;
  const sessionKey = `${SESSION_PREFIX}${exec.vu.idInInstance}:${iter}`;
  const sessionPayload = {
    sessionId: sessionKey,
    owner: `user-${exec.vu.idInInstance}`,
    expiresAt: Date.now() + 30_000
  };

  await kv.set(sessionKey, sessionPayload);

  // Deterministic mismatch: operators opt out of shipping the live payload.
  const mismatchNoCurrent = await kv.compareAndDeleteDetailed(
    sessionKey,
    { ...sessionPayload, owner: 'other-owner' },
    { includeCurrentOnMismatch: false }
  );

  // Deterministic mismatch: includeCurrentOnMismatch surfaces the stored value.
  const mismatchWithCurrent = await kv.compareAndDeleteDetailed(
    sessionKey,
    { ...sessionPayload, owner: 'other-owner' },
    { includeCurrentOnMismatch: true }
  );

  // Successful delete must never echo "current" even when the option is enabled.
  const success = await kv.compareAndDeleteDetailed(
    sessionKey,
    sessionPayload,
    { includeCurrentOnMismatch: true }
  );
  const existsAfterSuccess = await kv.exists(sessionKey);

  check(true, {
    'compareDeleteDetailed:deterministic': () =>
      mismatchNoCurrent.deleted === false &&
      mismatchNoCurrent.reason === 'mismatch' &&
      mismatchNoCurrent.existed === true &&
      !Object.prototype.hasOwnProperty.call(mismatchNoCurrent, 'current') &&
      mismatchWithCurrent.deleted === false &&
      mismatchWithCurrent.reason === 'mismatch' &&
      mismatchWithCurrent.existed === true &&
      Object.prototype.hasOwnProperty.call(mismatchWithCurrent, 'current') &&
      success.deleted === true &&
      success.reason === 'deleted' &&
      !Object.prototype.hasOwnProperty.call(success, 'current') &&
      existsAfterSuccess === false
  }, { op: 'compareDeleteDetailed', phase: 'deterministic' });

  // Legacy boolean CAD plus detailed parity for null/undefined value compares.
  const nullKey = `${sessionKey}:null`;
  await kv.set(nullKey, null);
  const legacyNullDelete = await kv.compareAndDelete(nullKey, null);

  const undefinedKey = `${sessionKey}:undefined`;
  await kv.set(undefinedKey, null);
  const detailedUndefinedDelete = await kv.compareAndDeleteDetailed(
    undefinedKey,
    undefined,
    { includeCurrentOnMismatch: true }
  );

  const nonNullKey = `${sessionKey}:non-null`;
  await kv.set(nonNullKey, 'active');
  const detailedNullMismatch = await kv.compareAndDeleteDetailed(
    nonNullKey,
    null,
    { includeCurrentOnMismatch: true }
  );

  check(true, {
    'compareDelete:null-semantics': () =>
      legacyNullDelete === true &&
      detailedUndefinedDelete.deleted === true &&
      detailedNullMismatch.deleted === false &&
      detailedNullMismatch.reason === 'mismatch' &&
      detailedNullMismatch.existed === true &&
      Object.prototype.hasOwnProperty.call(detailedNullMismatch, 'current')
  }, { op: 'compareDelete', contract: 'null-semantics' });

  // Contention wave: exactly one detailed delete succeeds; losers report mismatch
  // against a key that no longer exists once the winner finishes.
  const contentionKey = `${sessionKey}:contention`;
  const contentionPayload = { id: contentionKey, status: 'expired' };
  await kv.set(contentionKey, contentionPayload);

  const contentionResults = await Promise.all(
    Array.from({ length: CONCURRENT_CLEANERS }, () =>
      kv.compareAndDeleteDetailed(contentionKey, contentionPayload, { includeCurrentOnMismatch: true })
    )
  );
  const winners = contentionResults.filter((result) => result.deleted).length;
  const gone = !(await kv.exists(contentionKey));

  check(true, {
    'compareDeleteDetailed:contention': () =>
      winners === 1 &&
      gone &&
      contentionResults.every((result) =>
        result.deleted || (result.reason === 'mismatch' && result.existed === false)
      )
  }, { op: 'compareDeleteDetailed', phase: 'contention' });
}
