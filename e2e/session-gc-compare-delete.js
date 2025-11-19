import { check } from 'k6';
import exec from 'k6/execution';
import { VUS, ITERATIONS, createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: SESSION GARBAGE COLLECTION
// =============================================================================
//
// Expired sessions must be removed exactly once. Multiple workers race to
// delete the same session document using compareAndDelete(), launching their
// attempts in parallel via Promise.all to mirror noisy GC sweeps.
//
// REAL-WORLD PROBLEMS UNCOVERED:
// - Double deletes when compareAndDelete resolves off-thread.
// - Orphaned sessions that survive GC because retries stall.
// - Event-loop starvation under heavy Promise.all deletes.
//
// ATOMIC OPERATIONS TESTED:
// - set(): seed the session for the round.
// - compareAndDelete(): remove only if the payload matches.
// - exists(): verify the cleanup stuck.

// Prefix for session keys being garbage collected.
const SESSION_PREFIX = __ENV.SESSION_PREFIX || 'gc-session:';

// Number of concurrent workers attempting to delete the same session via compareAndDelete().
const CONCURRENT_CLEANERS = parseInt(__ENV.CONCURRENT_CLEANERS || '20', 10);

// Size of the session key ring used to rotate cleanup workload.
const SESSION_KEY_POOL_SIZE = parseInt(__ENV.SESSION_KEY_POOL_SIZE || '500', 10);

// Session TTL in milliseconds (used in session payload metadata).
const SESSION_TTL_MS = parseInt(__ENV.SESSION_TTL_MS || '30000', 10);

// kv is the shared store client used throughout the scenario.
const kv = createKv();

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: VUS,
  iterations: ITERATIONS,
  thresholds: {
    'checks{compareDelete:single-winner}': ['rate>0.999'],
    'checks{compareDelete:gone}': ['rate>0.999']
  }
};

// setup clears old sessions before the GC storm.
export const setup = createSetup(kv);

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv);

// sessionGcCompareDelete seeds a session, runs Promise.all compareAndDelete calls,
// and verifies that exactly one worker succeeded and the session vanished.
export default async function sessionGcCompareDelete() {
  const iteration = exec.scenario.iterationInTest;
  const sessionKey = `${SESSION_PREFIX}${iteration % SESSION_KEY_POOL_SIZE}`;
  const sessionPayload = {
    sessionId: sessionKey,
    owner: `user-${exec.vu.idInInstance}`,
    expiresAt: Date.now() + SESSION_TTL_MS
  };

  // Create the session.
  await kv.set(sessionKey, sessionPayload);

  // Race: multiple workers try to delete the same session simultaneously.
  const deleteResults = await Promise.all(
    Array.from({ length: CONCURRENT_CLEANERS }, () =>
      kv.compareAndDelete(sessionKey, sessionPayload)
    )
  );

  const successes = deleteResults.filter(Boolean).length;
  const stillExists = await kv.exists(sessionKey);

  check(true, {
    'compareDelete:single-winner': () => successes === 1,
    'compareDelete:gone': () => !stillExists
  });
}
