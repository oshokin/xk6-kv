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

// Backend selection: memory (default) or disk.
const SELECTED_BACKEND_NAME = __ENV.KV_BACKEND || 'memory';

// Optional: enable key tracking in memory backend to stress the tracking paths.
// (No effect for disk backend; safe to leave on)
const ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND =
  (__ENV.KV_TRACK_KEYS && __ENV.KV_TRACK_KEYS.toLowerCase() === 'true') || true;

// ---------------------------------------------
// Open a shared KV store available to all VUs.
// ---------------------------------------------
const kv = openKv(
  SELECTED_BACKEND_NAME === 'disk'
    ? { backend: 'disk', trackKeys: ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND }
    : { backend: 'memory', trackKeys: ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND }
);

export const options = {
  // Vary these to increase contention. Start with 20×100 like the shared script.
  vus: parseInt(__ENV.VUS || '20', 10),
  iterations: parseInt(__ENV.ITERATIONS || '100', 10),

  // Optional: add thresholds to fail fast if we start choking.
  thresholds: {
    // Require that at least 95% of iterations create sessions successfully.
    'checks{session:created}': ['rate>0.95'],
    'checks{session:activity}': ['rate>0.95'],
    'checks{session:cleanup}': ['rate>0.90']
  }
};

// -----------------------
// Test setup & teardown.
// -----------------------
export async function setup() {
  // Start with a clean state so each run is deterministic.
  await kv.clear();
}

export async function teardown() {
  // For disk backends, close the store cleanly so the file can be reused immediately.
  if (SELECTED_BACKEND_NAME === 'disk') {
    kv.close();
  }
}

// -------------------------------
// The main iteration body (VUs).
// -------------------------------
export default async function userSessionTrackingTest() {
  const userId = `user:${exec.vu.idInTest}:${Math.floor(Math.random() * 1000)}`;
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
  if (Math.random() < 0.1) {
    const deleted = await kv.deleteIfExists(sessionId);
    check(deleted, {
      'session:cleanup': () => deleted
    });
  }

  // Simulate some work.
  sleep(0.01);
}
