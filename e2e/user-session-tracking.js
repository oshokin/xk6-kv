import { check, sleep } from 'k6';
import exec from 'k6/execution';
import { openKv } from 'k6/x/kv';

// =============================================================================
// REAL-WORLD SCENARIO: USER SESSION TRACKING SYSTEM
// =============================================================================
//
// This test simulates a web application session management system where we need
// to track user activity, page views, and manage session lifecycle. This is a
// common pattern in:
//
// - E-commerce websites (tracking user shopping sessions)
// - Social media platforms (tracking user engagement)
// - SaaS applications (tracking user activity for billing/analytics)
// - Content management systems (tracking editor sessions)
// - Banking applications (tracking user sessions for security)
//
// REAL-WORLD PROBLEM SOLVED:
// Multiple concurrent users accessing the same application simultaneously.
// Without proper session management, you get:
// - Lost user state between page loads
// - Inaccurate analytics and usage tracking
// - Security vulnerabilities (session hijacking)
// - Poor user experience (lost shopping carts, form data)
// - Billing/usage calculation errors
//
// ATOMIC OPERATIONS TESTED:
// - getOrSet(): Create session if not exists, get existing session
// - incrementBy(): Track page views atomically
// - exists(): Check if session is valid
// - deleteIfExists(): Clean up expired sessions
//
// CONCURRENCY PATTERN:
// - Multiple VUs represent concurrent users
// - Each VU manages its own session lifecycle
// - Shared KV store ensures session consistency
//
// PERFORMANCE CHARACTERISTICS:
// - High read/write ratio (many page views per session)
// - Critical for user experience and business analytics
// - Must handle thousands of concurrent sessions

// Selected backend (memory or disk) used to store sessions.
const SELECTED_BACKEND_NAME = __ENV.KV_BACKEND || 'memory';

// Enables in-memory key tracking when the backend is memory.
const TRACK_KEYS_OVERRIDE =
  typeof __ENV.KV_TRACK_KEYS === 'string' ? __ENV.KV_TRACK_KEYS.toLowerCase() : '';
const ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND =
  TRACK_KEYS_OVERRIDE === '' ? true : TRACK_KEYS_OVERRIDE === 'true';

// Session identifier randomization parameters.
const SESSION_ID_RANDOM_RANGE = parseInt(__ENV.SESSION_ID_RANDOM_RANGE || '1000', 10);
const CLEANUP_PROBABILITY = parseFloat(__ENV.CLEANUP_PROBABILITY || '0.1');
const BASE_IDLE_SLEEP_SECONDS = parseFloat(__ENV.BASE_IDLE_SLEEP_SECONDS || '0.02');
const IDLE_SLEEP_JITTER_SECONDS = parseFloat(
  __ENV.IDLE_SLEEP_JITTER_SECONDS || '0.01'
);
const DEFAULT_VUS = parseInt(__ENV.VUS || '40', 10);
const DEFAULT_ITERATIONS = parseInt(__ENV.ITERATIONS || '400', 10);

// kv is the shared store client used throughout the scenario.
const kv = openKv(
  SELECTED_BACKEND_NAME === 'disk'
    ? { backend: 'disk', trackKeys: ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND }
    : { backend: 'memory', trackKeys: ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND }
);

// options configures the load profile and pass/fail thresholds.
export const options = {
  // Adjust via env vars to dial contention up or down for your workload.
  vus: DEFAULT_VUS,
  iterations: DEFAULT_ITERATIONS,

  // Optional: add thresholds to fail fast if we start choking.
  thresholds: {
    // Require that at least 95% of iterations create sessions successfully.
    'checks{session:created}': ['rate>0.95'],
    'checks{session:activity}': ['rate>0.95'],
    'checks{session:cleanup}': ['rate>0.90']
  }
};

// setup purges all sessions so we can assert deterministic counters per run.
export async function setup() {
  // Start with a clean state so each run is deterministic.
  await kv.clear();
}

// teardown closes disk stores so repeated test runs on the same file stay fast.
export async function teardown() {
  // For disk backends, close the store cleanly so the file can be reused immediately.
  if (SELECTED_BACKEND_NAME === 'disk') {
    kv.close();
  }
}

// userSessionTrackingTest creates a session, tracks page views, performs existence
// checks, and occasionally triggers cleanup to cover the full lifecycle.
export default async function userSessionTrackingTest() {
  const userId = `user:${exec.vu.idInTest}:${Math.floor(
    Math.random() * SESSION_ID_RANDOM_RANGE
  )}`;
  const sessionId = `session:${userId}:${Date.now()}`;

  // Test 1: Create session (getOrSet).
  const { loaded: sessionExists } = await kv.getOrSet(sessionId, {
    userId: userId,
    createdAt: Date.now(),
    pageViews: 0
  });

  check(!sessionExists, {
    'session:created': () => !sessionExists
  });

  // Test 2: Track page views (incrementBy).
  const pageViews = await kv.incrementBy(`${sessionId}:page_views`, 1);

  check(pageViews > 0, {
    'session:activity': () => pageViews > 0
  });

  // Test 3: Update last activity.
  await kv.set(`${sessionId}:last_activity`, Date.now());

  // Test 4: Check session exists.
  const exists = await kv.exists(sessionId);
  check(exists, {
    'session:exists': () => exists
  });

  // Test 5: Simulate session cleanup (deleteIfExists).
  // Only cleanup 10% of sessions to avoid race conditions.
  if (Math.random() < CLEANUP_PROBABILITY) {
    const deleted = await kv.deleteIfExists(sessionId);
    check(deleted, {
      'session:cleanup': () => deleted
    });
  }

  // Simulate some work with jitter so each iteration stays active.
  sleep(BASE_IDLE_SLEEP_SECONDS + Math.random() * IDLE_SLEEP_JITTER_SECONDS);
}
