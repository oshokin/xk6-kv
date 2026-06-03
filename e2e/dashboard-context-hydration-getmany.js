import { check } from 'k6';
import exec from 'k6/execution';
import { VUS, ITERATIONS, createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: DASHBOARD CONTEXT HYDRATION WITH BATCH READS
// =============================================================================
//
// This test models an API endpoint that must assemble user dashboard context in
// one request. Typical production examples:
//
// - SaaS control planes hydrating profile, plan, and consent data.
// - BFF/API gateway layers collapsing many key reads into one batch call.
// - Mobile/web backends loading mixed required/optional user attributes.
//
// REAL-WORLD PROBLEM SOLVED:
// Without a stable batch-read contract, hydration flows regress:
// - Wrong key/value mapping when order is not preserved.
// - Optional missing fields confused with explicit null fields.
// - Duplicate lookups returning inconsistent values.
//
// BATCH OPERATIONS TESTED:
// - getMany(): ordered per-key items with key/exists/value shape.
// - setMany(): deterministic seed writes before hydration reads.
// - deleteIfExists(): force optional key to missing state.
//
// CONCURRENCY PATTERN:
// - VUs hydrate isolated user namespaces repeatedly.
// - Each iteration rewrites a bounded shard to keep key growth stable.
//
// PERFORMANCE CHARACTERISTICS:
// - Read-heavy endpoint with small deterministic write prelude.
// - Sensitive to batch-read shape regressions under concurrent traffic.

// Prefix for user dashboard context keys.
const USER_CONTEXT_PREFIX = __ENV.USER_CONTEXT_PREFIX || 'dashboard:user:';

// Number of deterministic user slots reused across iterations.
const USER_SLOT_RANGE = parseInt(__ENV.USER_SLOT_RANGE || '1000', 10);

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'dashboard-context-hydration-getmany';

// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME);

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: VUS,
  iterations: ITERATIONS,
  thresholds: {
    'checks{getMany:shape}': ['rate>0.999'],
    'checks{getMany:order}': ['rate>0.999'],
    'checks{getMany:missing-vs-null}': ['rate>0.999'],
    'checks{getMany:duplicates}': ['rate>0.999'],
    'checks{getMany:hydration-ready}': ['rate>0.999']
  }
};

// setup clears previous state before hydration loops start.
export const setup = createSetup(kv);

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv);

// dashboardContextHydrationGetMany validates getMany() contract in a realistic
// endpoint-style hydration flow.
export default async function dashboardContextHydrationGetMany() {
  const vuId = exec.vu.idInTest;
  const iteration = exec.scenario.iterationInTest;
  const userSlot = iteration % USER_SLOT_RANGE;
  const userId = `${vuId}:${userSlot}`;

  const profileKey = `${USER_CONTEXT_PREFIX}${userId}:profile`;
  const planKey = `${USER_CONTEXT_PREFIX}${userId}:plan`;
  const consentKey = `${USER_CONTEXT_PREFIX}${userId}:consent`;
  const missingPreferencesKey = `${USER_CONTEXT_PREFIX}${userId}:preferences`;

  await kv.setMany({
    [profileKey]: {
      userId,
      locale: 'en-US',
      tier: 'pro'
    },
    [planKey]: {
      entitlements: ['dashboard', 'alerts'],
      refreshedAt: Date.now()
    },
    [consentKey]: null
  });

  // Optional preferences are intentionally absent for this user context.
  await kv.deleteIfExists(missingPreferencesKey);

  const requestedKeys = [
    profileKey,
    missingPreferencesKey,
    consentKey,
    profileKey,
    planKey
  ];
  const items = await kv.getMany(requestedKeys);

  check(items, {
    'getMany:shape': (result) =>
      Array.isArray(result) &&
      result.length === requestedKeys.length &&
      result.every(
        (item) =>
          item &&
          typeof item.key === 'string' &&
          typeof item.exists === 'boolean' &&
          Object.prototype.hasOwnProperty.call(item, 'value')
      ),
    'getMany:order': (result) =>
      result.every((item, idx) => item.key === requestedKeys[idx]),
    'getMany:missing-vs-null': (result) =>
      result[1].exists === false &&
      result[1].value === null &&
      result[2].exists === true &&
      result[2].value === null,
    'getMany:duplicates': (result) =>
      result[0].exists === true &&
      result[3].exists === true &&
      result[0].key === result[3].key &&
      result[0].value.userId === result[3].value.userId,
    'getMany:hydration-ready': (result) =>
      result[0].value.tier === 'pro' &&
      Array.isArray(result[4].value.entitlements) &&
      result[4].value.entitlements.includes('dashboard')
  });
}
