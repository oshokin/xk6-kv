import { check } from 'k6';
import exec from 'k6/execution';
import { VUS, ITERATIONS, createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: PER-TENANT ACTIVE JOB WINDOW COUNTING
// =============================================================================
//
// Many systems need fast "how many active items exist for this tenant?" checks:
// - queue workers enforcing tenant-specific in-flight limits
// - SaaS plans capping concurrent background jobs
// - moderation pipelines throttling expensive operations per customer
//
// This scenario keeps a rolling window of active job keys per tenant and
// validates kv.count(prefix) under concurrent load.
//
// OPERATIONS EXERCISED:
// - set(): append newly active jobs
// - delete(): evict stale jobs from the rolling window
// - count(prefix): compute active jobs per tenant without full materialization

// Maximum number of active jobs retained per tenant (rolling window size).
const ACTIVE_WINDOW = parseInt(__ENV.ACTIVE_WINDOW || '20', 10);

// Prefix namespace for tenant-scoped active jobs.
const TENANT_PREFIX = __ENV.TENANT_PREFIX || 'tenant:';

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'tenant-prefix-count-window';

// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME);

// Per-VU local state (isolated per runtime): rolling active keys.
let localSequence = 0;
const activeKeys = [];

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: VUS,
  iterations: ITERATIONS,
  thresholds: {
    'checks{count:window-size}': ['rate>0.99'],
    'checks{count:total-not-smaller}': ['rate>0.99']
  }
};

// setup resets store state before the scenario.
export const setup = createSetup(kv);

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv, TEST_NAME);

// tenantPrefixCountWindow simulates one worker maintaining a bounded set
// of active jobs for its tenant and reading cardinality via count(prefix).
export default async function tenantPrefixCountWindow() {
  const tenantId = exec.vu.idInTest;
  const prefix = `${TENANT_PREFIX}${tenantId}:job:`;

  const key = `${prefix}${String(localSequence).padStart(8, '0')}`;
  localSequence += 1;

  await kv.set(key, {
    tenantId,
    state: 'active',
    createdAt: Date.now()
  });
  activeKeys.push(key);

  // Keep only the last ACTIVE_WINDOW keys as active.
  if (activeKeys.length > ACTIVE_WINDOW) {
    const staleKey = activeKeys.shift();
    await kv.delete(staleKey);
  }

  const tenantCount = await kv.count(prefix);
  const globalCount = await kv.count();

  check(tenantCount, {
    'count:window-size': (value) => value === activeKeys.length
  });

  check(globalCount, {
    'count:total-not-smaller': (value) => value >= tenantCount
  });
}
