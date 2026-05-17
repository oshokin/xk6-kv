import { check } from 'k6';
import exec from 'k6/execution';
import { VUS, ITERATIONS, createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: SUBSCRIPTION RENEWAL LEASE WORKERS
// =============================================================================
//
// This test models renewal workers that claim subscriptions, perform billing
// logic, and release the lease so other workers can continue processing.
//
// Production questions this scenario answers:
// - Are claim/release operations failing under concurrency?
// - Do workers see empty lease pools unexpectedly?
// - Can we emit periodic health snapshots without failures?

// Key prefix for subscriptions processed by renewal workers.
const SUBSCRIPTION_PREFIX = __ENV.SUBSCRIPTION_PREFIX || 'renewal:subscription:';

// Number of subscriptions preloaded into the renewal queue.
const SUBSCRIPTION_POOL_SIZE = parseInt(__ENV.SUBSCRIPTION_POOL_SIZE || '300', 10);

// Lease duration for each claim attempt, in milliseconds.
const CLAIM_TTL_MS = parseInt(__ENV.CLAIM_TTL_MS || '15000', 10);

// Renewal duration applied to active leases during processing.
const RENEW_TTL_MS = parseInt(__ENV.RENEW_TTL_MS || '30000', 10);

// Maximum claim attempts per iteration before treating the queue as empty.
const CLAIM_ATTEMPTS = parseInt(__ENV.CLAIM_ATTEMPTS || '5', 10);

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'subscription-renewal-lease-observability';

// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME, {
  metrics: {
    operations: true
  }
});

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: VUS,
  iterations: ITERATIONS,
  thresholds: {
    'checks{renewal:claimed}': ['rate>0.99'],
    'checks{renewal:renewed}': ['rate>0.99'],
    'checks{renewal:released}': ['rate>0.99']
  }
};

// setup seeds subscriptions so renewal workers have deterministic shared input.
export async function setup() {
  const standardSetup = createSetup(kv);
  await standardSetup();

  for (let i = 0; i < SUBSCRIPTION_POOL_SIZE; i += 1) {
    await kv.set(`${SUBSCRIPTION_PREFIX}${i + 1}`, {
      subscriptionId: i + 1,
      plan: i % 2 === 0 ? 'pro' : 'starter',
      renewAt: Date.now() + 86400000
    });
  }
}

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv, TEST_NAME);

// subscriptionRenewalLeaseObservability claims one subscription lease and
// releases it back to the pool after simulated processing.
export default async function subscriptionRenewalLeaseObservability() {
  const claim = await claimLease(exec.vu.idInTest);

  check(Boolean(claim), {
    'renewal:claimed': () => Boolean(claim)
  });

  if (claim) {
    const renewed = await kv.renewClaim(claim, { ttl: RENEW_TTL_MS });
    check(renewed, {
      'renewal:renewed': () => renewed
    });

    const released = await kv.releaseClaim(claim);
    check(released, {
      'renewal:released': () => released
    });
  }
}

// claimLease retries claimRandom() a bounded number of times to model workers
// polling for available subscriptions under contention.
async function claimLease(vuId) {
  for (let attempt = 0; attempt < CLAIM_ATTEMPTS; attempt += 1) {
    const claim = await kv.claimRandom({
      prefix: SUBSCRIPTION_PREFIX,
      owner: `renewal-worker:${vuId}`,
      ttl: CLAIM_TTL_MS
    });

    if (claim !== null) {
      return claim;
    }
  }

  return null;
}
