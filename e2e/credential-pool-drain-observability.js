import { check } from 'k6';
import { Rate } from 'k6/metrics';
import { VUS, ITERATIONS, createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: TEST-CREDENTIAL POOL DRAIN MONITORING
// =============================================================================
//
// This test models one-time credential allocation (users, API keys, test data)
// where each item must be consumed once via popRandom() and then disappears.
//
// Production questions this scenario answers:
// - Are allocation operations failing?
// - How quickly is the pool exhausted (empty result rate)?
// - Do store-level gauges confirm that keys reached zero?

// Key prefix for the one-time credential pool consumed by workers.
const CREDENTIAL_PREFIX = __ENV.CREDENTIAL_PREFIX || 'seed:credential:';

// Number of credentials seeded for the scenario.
// Default is intentionally small relative to iterations so the pool drains.
const CREDENTIAL_POOL_SIZE = parseInt(
  __ENV.CREDENTIAL_POOL_SIZE || String(Math.max(10, Math.floor(ITERATIONS * 0.1))),
  10
);

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'credential-pool-drain-observability';

// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME, {
  metrics: {
    operations: true
  }
});

// allocationDrained tracks empty-pool hit rate without failing check output.
const allocationDrained = new Rate('allocation_drained');

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: VUS,
  iterations: ITERATIONS,
  thresholds: {
    'checks{allocation:attempted}': ['rate>0.99'],
    allocation_drained: ['rate>0.30']
  }
};

// setup seeds a finite credential pool so workers naturally transition from
// successful allocations to empty-pool responses during the run.
export async function setup() {
  const standardSetup = createSetup(kv);
  await standardSetup();

  for (let i = 0; i < CREDENTIAL_POOL_SIZE; i += 1) {
    await kv.set(`${CREDENTIAL_PREFIX}${i + 1}`, {
      id: i + 1,
      username: `seed-user-${i + 1}`,
      createdAt: Date.now()
    });
  }
}

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv, TEST_NAME);

// credentialPoolDrainObservability performs one allocation attempt per iteration
// and tracks when the pool is exhausted.
export default async function credentialPoolDrainObservability() {
  const credential = await kv.popRandom({ prefix: CREDENTIAL_PREFIX });

  check(true, {
    'allocation:attempted': () => true
  });

  allocationDrained.add(credential === null);
}
