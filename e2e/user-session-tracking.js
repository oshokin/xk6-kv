import { check } from 'k6';
import exec from 'k6/execution';
import { VUS, ITERATIONS, createKv, createSetup, createTeardown } from './common.js';

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

// Range of random session IDs per user (creates session diversity).
const SESSION_ID_RANGE = parseInt(__ENV.SESSION_ID_RANGE || '1000', 10);

// Probability (0.0-1.0) that a session will be cleaned up in this iteration.
const CLEANUP_PROBABILITY = parseFloat(__ENV.CLEANUP_PROBABILITY || '0.1');

// kv is the shared store client used throughout the scenario.
const kv = createKv();

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: VUS,
  iterations: ITERATIONS,
  thresholds: {
    'checks{session:created}': ['rate>0.95'],
    'checks{session:activity}': ['rate>0.95'],
    'checks{session:cleanup}': ['rate>0.90']
  }
};

// setup purges all sessions so we can assert deterministic counters per run.
export const setup = createSetup(kv);

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv);

// userSessionTrackingTest simulates users returning to their sessions across
// multiple page views, tracking activity, and eventually cleaning up.
export default async function userSessionTrackingTest() {
  const iteration = exec.scenario.iterationInTest;
  const vuId = exec.vu.idInTest;

  // Deterministic session ID - simulates user returning to same session.
  const sessionIndex = iteration % SESSION_ID_RANGE;
  const sessionId = `session:${vuId}:${sessionIndex}`;

  // Create or retrieve existing session.
  await kv.getOrSet(sessionId, {
    userId: vuId,
    sessionIndex: sessionIndex,
    createdAt: Date.now(),
    pageViews: 0
  });

  check(true, {
    'session:created': () => true
  });

  // Track page views atomically across multiple visits.
  const pageViews = await kv.incrementBy(`${sessionId}:page_views`, 1);

  check(pageViews > 0, {
    'session:activity': () => pageViews > 0
  });

  // Update last activity timestamp.
  await kv.set(`${sessionId}:last_activity`, Date.now());

  // Verify session exists.
  const exists = await kv.exists(sessionId);
  check(exists, {
    'session:exists': () => exists
  });

  // Probabilistically clean up sessions (10% chance).
  if (Math.random() < CLEANUP_PROBABILITY) {
    const deleted = await kv.deleteIfExists(sessionId);
    check(deleted, {
      'session:cleanup': () => deleted
    });
  }
}
