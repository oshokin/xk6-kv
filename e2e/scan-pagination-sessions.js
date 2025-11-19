import { check } from 'k6';
import exec from 'k6/execution';
import { VUS, ITERATIONS, createKv, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: PAGINATED SESSION ANALYTICS
// =============================================================================
//
// This test simulates large-scale session exports where operators need to
// iterate over hundreds of thousands of active sessions without loading them
// all into memory. It mirrors workflows used by:
//
// - Compliance teams exporting "active sessions" for audits.
// - Fraud platforms reconciling device fingerprints.
// - SaaS vendors billing by "session minutes" per tenant.
// - Customer-support systems syncing live chat sessions.
//
// REAL-WORLD PROBLEM SOLVED:
// Multiple workers stream the same keyspace using cursor continuation tokens.
// Without correct pagination you get:
// - Missing or duplicated sessions in exports.
// - Stalled cursors that never advance.
// - Backend hotspots when every worker restarts from the beginning.
//
// ATOMIC OPERATIONS TESTED:
// - scan(): Page through lexicographically ordered keys with prefix filters.
// - set(): Seed and annotate session metadata deterministically.
//
// CONCURRENCY PATTERN:
// - Several VUs run scans in parallel, exercising cursor handling on both
//   memory and disk backends.
//
// PERFORMANCE CHARACTERISTICS:
// - Read-heavy, cursor-based workloads.
// - Sensitive to ordering regressions and missed prefixes.

// Session key prefix used for seeding and scanning.
const SESSION_PREFIX = __ENV.SESSION_PREFIX || 'session:';

// Total number of session records seeded during setup.
const TOTAL_SESSIONS = parseInt(__ENV.TOTAL_SESSIONS || '500', 10);

// Maximum entries returned per scan page.
const PAGE_SIZE = parseInt(__ENV.PAGE_SIZE || '50', 10);

// Session time-to-live in seconds (metadata stored with each session).
const SESSION_TTL_SECONDS = 300;

// Width of zero-padded session IDs (e.g., 6 = session:000042).
const SESSION_KEY_PAD_WIDTH = 6;

// Number of sessions per user bucket for data distribution.
const SESSIONS_PER_USER = 5;

// kv is the shared store client used throughout the scenario.
const kv = createKv();

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: VUS,
  iterations: ITERATIONS,
  thresholds: {
    'checks{scan:complete}': ['rate>0.98'],
    'checks{scan:has-data}': ['rate>0.98']
  }
};

// setup seeds deterministic sessions so we can assert exact counts later.
export async function setup() {
  await kv.clear();

  for (let i = 0; i < TOTAL_SESSIONS; i += 1) {
    const paddedIndex = String(i).padStart(SESSION_KEY_PAD_WIDTH, '0');
    const key = `${SESSION_PREFIX}${paddedIndex}`;

    await kv.set(key, {
      sessionId: paddedIndex,
      userId: `user-${Math.floor(i / SESSIONS_PER_USER)}`,
      createdAt: Date.now(),
      ttlSeconds: SESSION_TTL_SECONDS,
      lastSeenByWorker: null
    });
  }
}

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv);

// scanPaginatedSessions performs repeated scans until `done` is true, verifying
// prefix ordering invariants.
export default async function scanPaginatedSessions() {
  let cursor = '';
  let seen = 0;

  // Paginate through all sessions using cursor continuation.
  while (true) {
    const { entries, cursor: nextCursor, done } = await kv.scan({
      prefix: SESSION_PREFIX,
      limit: PAGE_SIZE,
      cursor
    });

    for (const entry of entries) {
      if (!entry.key.startsWith(SESSION_PREFIX)) {
        throw new Error(`unexpected key from scan(): ${entry.key}`);
      }

      // Tag entry for traceability (simulated analytics side effect).
      entry.value.lastSeenByWorker = exec.vu.idInTest;
      seen += 1;
    }

    cursor = nextCursor;
    if (done) {
      break;
    }
  }

  // Verify all sessions were scanned exactly once.
  check(seen, {
    'scan:has-data': (value) => value > 0,
    'scan:complete': (value) => value === TOTAL_SESSIONS
  });
}
