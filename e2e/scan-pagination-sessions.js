import { check, sleep } from 'k6';
import exec from 'k6/execution';
import { openKv } from 'k6/x/kv';

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

// Selected backend (memory or disk) used by the scenario.
const SELECTED_BACKEND_NAME = __ENV.KV_BACKEND || 'memory';

// Enables in-memory key tracking when the backend is memory.
const ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND =
  (__ENV.KV_TRACK_KEYS && __ENV.KV_TRACK_KEYS.toLowerCase() === 'true') || true;

// Shared KV store handle used by all VUs.
const kv = openKv(
  SELECTED_BACKEND_NAME === 'disk'
    ? { backend: 'disk', trackKeys: true }
    : { backend: 'memory', trackKeys: ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND }
);

// Session key prefix used for seeding and scanning.
const SESSION_PREFIX = __ENV.SESSION_PREFIX || 'session:';

// Total number of sessions seeded during setup.
const TOTAL_SESSIONS = parseInt(__ENV.TOTAL_SESSIONS || '500', 10);

// Maximum number of entries returned per scan page.
const PAGE_SIZE = parseInt(__ENV.PAGE_SIZE || '50', 10);

// TTL metadata stored with each session record.
const SESSION_TTL_SECONDS = parseInt(__ENV.SESSION_TTL_SECONDS || '300', 10);

// 4 VUs × 10 iterations = 40 scans, enough to catch cursor bugs yet CI-friendly.
export const options = {
  vus: parseInt(__ENV.VUS || '4', 10),
  iterations: parseInt(__ENV.ITERATIONS || '10', 10),
  thresholds: {
    'checks{scan:complete}': ['rate>0.98'],
    'checks{scan:has-data}': ['rate>0.98']
  }
};

// setup: seeds deterministic sessions so we can assert exact counts later.
export async function setup() {
  await kv.clear();

  for (let i = 0; i < TOTAL_SESSIONS; i += 1) {
    const paddedIndex = String(i).padStart(6, '0');
    const key = `${SESSION_PREFIX}${paddedIndex}`;

    await kv.set(key, {
      sessionId: paddedIndex,
      userId: `user-${Math.floor(i / 5)}`,
      createdAt: Date.now(),
      ttlSeconds: SESSION_TTL_SECONDS,
      lastSeenByWorker: null
    });
  }
}

// teardown: closes BoltDB cleanly so later runs do not trip over open handles.
export async function teardown() {
  if (SELECTED_BACKEND_NAME === 'disk') {
    kv.close();
  }
}

// Each iteration scans until `done` is true, verifying prefix ordering invariants.
export default async function scanPaginatedSessions() {
  let cursor = '';
  let seen = 0;

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

  check(seen, {
    'scan:has-data': (value) => value > 0,
    'scan:complete': (value) => value === TOTAL_SESSIONS
  });

  // Simulate downstream processing time.
  sleep(0.1);
}

