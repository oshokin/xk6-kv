import { check, sleep } from 'k6';
import exec from 'k6/execution';
import { openKv } from 'k6/x/kv';

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

// Selected backend (memory or disk) used for the GC drill.
const SELECTED_BACKEND_NAME = __ENV.KV_BACKEND || 'memory';
// Enables in-memory key tracking when the backend is memory.
const TRACK_KEYS_OVERRIDE =
  typeof __ENV.KV_TRACK_KEYS === 'string' ? __ENV.KV_TRACK_KEYS.toLowerCase() : '';
const ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND =
  TRACK_KEYS_OVERRIDE === '' ? true : TRACK_KEYS_OVERRIDE === 'true';

// Prefix applied to seeded session keys.
const SESSION_PREFIX = __ENV.SESSION_PREFIX || 'gc-session:';
// Number of cleaners racing for each session key.
const CLEANERS_PER_BATCH = parseInt(__ENV.CLEANERS_PER_BATCH || '20', 10);
// Size of the session key ring used to rotate workload.
const SESSION_KEY_RING = parseInt(__ENV.SESSION_KEY_RING || '500', 10);
// TTL buffer (ms) recorded within each session payload.
const SESSION_TTL_BUFFER_MS = parseInt(__ENV.SESSION_TTL_BUFFER_MS || '30000', 10);
// Base duration (seconds) each iteration sleeps after processing.
const BASE_IDLE_SLEEP_SECONDS = parseFloat(__ENV.BASE_IDLE_SLEEP_SECONDS || '0.015');
// Random jitter (seconds) added to the base idle sleep.
const IDLE_SLEEP_JITTER_SECONDS = parseFloat(
  __ENV.IDLE_SLEEP_JITTER_SECONDS || '0.01'
);
// Default number of VUs used by the scenario.
const DEFAULT_VUS = parseInt(__ENV.VUS || '40', 10);
// Default iteration count used by the scenario.
const DEFAULT_ITERATIONS = parseInt(__ENV.ITERATIONS || '400', 10);

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
  thresholds: {
    'checks{compareDelete:single-winner}': ['rate>0.999'],
    'checks{compareDelete:gone}': ['rate>0.999']
  }
};

// setup clears old sessions before the GC storm.
export async function setup() {
  await kv.clear();
}

// teardown closes disk-backed stores after the scenario.
export async function teardown() {
  if (SELECTED_BACKEND_NAME === 'disk') {
    kv.close();
  }
}

// sessionGcCompareDelete seeds a session, runs Promise.all compareAndDelete calls,
// and verifies that exactly one worker succeeded and the session vanished.
export default async function sessionGcCompareDelete() {
  const iteration = exec.scenario.iterationInTest;
  const sessionKey = `${SESSION_PREFIX}${iteration % SESSION_KEY_RING}`;
  const sessionPayload = {
    sessionId: sessionKey,
    owner: `user-${exec.vu.idInInstance}`,
    expiresAt: Date.now() + SESSION_TTL_BUFFER_MS
  };

  await kv.set(sessionKey, sessionPayload);

  const deleteResults = await Promise.all(
    Array.from({ length: CLEANERS_PER_BATCH }, () => kv.compareAndDelete(sessionKey, sessionPayload))
  );

  const successes = deleteResults.filter(Boolean).length;

  const stillExists = await kv.exists(sessionKey);

  check(true, {
    'compareDelete:single-winner': () => successes === 1,
    'compareDelete:gone': () => !stillExists
  });

  // Simulate work long enough to keep the scenario under load.
  sleep(BASE_IDLE_SLEEP_SECONDS + Math.random() * IDLE_SLEEP_JITTER_SECONDS);
}

