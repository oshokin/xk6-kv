import { check } from 'k6'
import exec from 'k6/execution'
import { VUS, ITERATIONS, createKv, createSetup, createTeardown } from './common.js'

// =============================================================================
// REAL-WORLD SCENARIO: DEFAULT LEASE WINDOW VALIDATION
// =============================================================================
//
// This test simulates a worker pool that claims random tasks from a shared queue.
// Workers intentionally omit `ttl` when calling claimRandom() and rely on the
// extension's default lease window. This mirrors common real-world patterns:
//
// - Background processors picking random jobs from a reusable pool.
// - Synthetic-user allocators handing out test accounts to VUs.
// - Reservation services where workers may crash and leases must expire safely.
//
// REAL-WORLD PROBLEM SOLVED:
// Teams often forget to pass ttl explicitly and assume defaults are stable.
// If defaults drift silently, workloads can become flaky:
// - Leases expire too fast -> duplicate work under load.
// - Leases live too long -> pool starvation.
// - Inconsistent defaults across versions -> migration regressions.
//
// ATOMIC OPERATIONS TESTED:
// - claimRandom(): allocate one random entry with omitted ttl.
// - completeClaim(): finish work and return entry to the pool.
//
// CONCURRENCY PATTERN:
// - Many VUs concurrently claim and complete entries from the same prefix.
// - Claims are completed with deleteKey=false so the pool stays reusable.
//
// PERFORMANCE CHARACTERISTICS:
// - Lease-heavy control-plane traffic under contention.
// - Fast per-iteration operations with strict timing expectations.

// Official default lease TTL for claimRandom() when ttl is omitted.
const DEFAULT_CLAIM_TTL_MS = 30000

// Allowed timing slack for scheduler/runtime delay when validating expiresAt.
const TTL_TOLERANCE_MS = parseInt(__ENV.TTL_TOLERANCE_MS || '5000', 10)

// Additional guard for minor clock boundary skew.
const TTL_UPPER_SKEW_MS = parseInt(__ENV.TTL_UPPER_SKEW_MS || '500', 10)

// Prefix for reusable work items in the claim pool.
const TASK_PREFIX = __ENV.TASK_PREFIX || 'claim-default:task:'

// Number of task keys pre-seeded into the reusable pool.
const TASK_POOL_SIZE = parseInt(__ENV.CLAIM_POOL_SIZE || '200', 10)

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'claim-random-default-ttl'

// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME)

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: VUS,
  iterations: ITERATIONS,
  thresholds: {
    'checks{claim:acquired}': ['rate>0.95'],
    'checks{claim:ttl-default}': ['rate>0.99'],
    'checks{claim:complete-ok}': ['rate>0.99'],
  },
}

// setup clears previous state and seeds a deterministic reusable task pool.
export async function setup() {
  const baseSetup = createSetup(kv)
  await baseSetup()

  await seedReusableTaskPool()
}

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv, TEST_NAME)

// claimRandomDefaultTTL claims one random task without explicit ttl and verifies
// that the returned lease duration uses the documented 30s default.
export default async function claimRandomDefaultTTL() {
  const claim = await kv.claimRandom({
    prefix: TASK_PREFIX,
    owner: `scenario:${exec.scenario.name}:vu:${exec.vu.idInTest}`,
    // ttl intentionally omitted to validate default behavior.
  })

  // Pool may be temporarily exhausted under contention while other VUs hold leases.
  if (claim === null) {
    return
  }

  // Measure observed lease window against the documented default.
  const leaseRemainingMs = claim.expiresAt - Date.now()
  const ttlLooksDefault = isDefaultTTLWindow(leaseRemainingMs)

  // Keep keys reusable so every iteration can exercise claimRandom().
  const completed = await kv.completeClaim(claim, { deleteKey: false })

  check(claim, {
    'claim:acquired': () => claim.key.startsWith(TASK_PREFIX),
    'claim:ttl-default': () => ttlLooksDefault,
    'claim:complete-ok': () => completed === true,
  })
}

// seedReusableTaskPool creates deterministic task entries for random claiming.
async function seedReusableTaskPool() {
  for (let i = 0; i < TASK_POOL_SIZE; i += 1) {
    await kv.set(`${TASK_PREFIX}${i}`, {
      id: i,
      payload: `task-${i}`,
      createdAt: Date.now(),
    })
  }
}

// isDefaultTTLWindow validates lease duration against default ttl plus tolerated drift.
function isDefaultTTLWindow(leaseRemainingMs) {
  const minExpected = DEFAULT_CLAIM_TTL_MS - TTL_TOLERANCE_MS
  const maxExpected = DEFAULT_CLAIM_TTL_MS + TTL_UPPER_SKEW_MS

  return leaseRemainingMs >= minExpected && leaseRemainingMs <= maxExpected
}
