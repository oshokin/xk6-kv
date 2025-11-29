import { check } from 'k6';
import exec from 'k6/execution';
import { VUS, ITERATIONS, createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: A/B TESTING & FEATURE FLAGS SYSTEM
// =============================================================================
//
// This test simulates a feature flag and A/B testing system where we need to
// manage feature toggles, track usage, and perform controlled rollouts.
// This is a critical pattern in:
//
// - E-commerce platforms (testing checkout flows, product recommendations).
// - Social media platforms (testing UI changes, algorithm updates).
// - SaaS applications (testing new features, pricing models).
// - Mobile applications (testing app features, UI layouts).
// - Content platforms (testing article layouts, recommendation engines).
// - Gaming platforms (testing game mechanics, monetization features).
//
// REAL-WORLD PROBLEM SOLVED:
// Multiple users accessing features with different configurations simultaneously.
// Without proper feature flag management, you get:
// - Inconsistent user experiences (some users see features, others don't).
// - Failed A/B tests (contaminated results).
// - Rollback difficulties (can't quickly disable problematic features).
// - Poor analytics (inaccurate usage tracking).
// - Revenue impact (broken features affecting conversions).
//
// ATOMIC OPERATIONS TESTED:
// - getOrSet(): Initialize feature flags with defaults.
// - incrementBy(): Track feature usage statistics.
// - compareAndSwap(): Update feature configurations atomically.
// - exists(): Verify feature flag availability.
// - list(): Monitor all feature flags.
//
// CONCURRENCY PATTERN:
// - Multiple VUs represent different users.
// - Each VU checks feature flags and tracks usage.
// - Shared KV store ensures consistent feature state.
// - Deterministic retry loops ensure every CAS succeeds or surfaces a bug.
//
// PERFORMANCE CHARACTERISTICS:
// - High read frequency (every user interaction).
// - Critical for user experience and business decisions.
// - Must handle thousands of concurrent feature checks.
// - Low latency impact (feature checks should be fast).

// Feature flags being tested in this scenario.
const FLAG_NAMES = ['new-checkout', 'dark-mode', 'premium-features', 'beta-search'];

// Maximum compareAndSwap retry attempts under heavy contention before giving up.
const MAX_CAS_RETRIES = parseInt(__ENV.MAX_CAS_RETRIES || '1000', 10);

// Number of user buckets for A/B test rollout (0-99 = 100 buckets for percentage-based rollouts).
const BUCKET_COUNT = 100;

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'ab-testing-feature-flags';

// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME);

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: VUS,
  iterations: ITERATIONS,
  thresholds: {
    'checks{check:flag_enabled}': ['rate>0.999'],
    'checks{check:flag_updated}': ['rate>0.999'],
    'checks{check:flag_monitoring}': ['rate>0.999'],
    'checks{check:flag_usage_tracked}': ['rate>0.999'],
    'checks{check:flag_exists}': ['rate>0.999']
  }
};

// setup creates a known flag set so monitoring checks can assert the exact
// number of entries returned by list().
export async function setup() {
  const standardSetup = createSetup(kv);
  await standardSetup();

  for (const flagName of FLAG_NAMES) {
    await kv.set(`flag:${flagName}`, buildDefaultFlag(flagName));
  }
}

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv, TEST_NAME);

// abTestingFeatureFlagsTest emulates a user checking gates, recording usage, and
// occasionally acting as an admin to change rollout percentages.
export default async function abTestingFeatureFlagsTest() {
  const vuId = exec.vu.idInTest;
  const iteration = exec.scenario.iterationInTest;

  // Deterministic flag selection.
  const flagName = FLAG_NAMES[iteration % FLAG_NAMES.length];
  const flagKey = `flag:${flagName}`;
  const userId = `user:${vuId}:${iteration}`;

  // Get or initialize flag.
  const { value: flag } = await kv.getOrSet(flagKey, buildDefaultFlag(flagName));

  check(Boolean(flag), {
    flag_enabled: () => Boolean(flag) && typeof flag.enabled === 'boolean'
  });

  // Determine if user is in rollout bucket (simple hash-based bucketing).
  const userBucket = (vuId + iteration) % BUCKET_COUNT;
  const shouldSeeFeature = flag.enabled && userBucket < flag.rollout;

  // Track usage if user sees the feature.
  if (shouldSeeFeature) {
    const usageKey = `usage:${flagName}:${userId}`;
    const usageCount = await kv.incrementBy(usageKey, 1);

    check(usageCount > 0, {
      flag_usage_tracked: () => usageCount > 0
    });
  }

  // Simulate admin updating rollout percentage (stress test CAS).
  const updated = await updateFlagRollout(flagKey, flag, vuId);

  check(updated, {
    flag_updated: () => updated
  });

  // Monitor all flags.
  const allFlags = await kv.list({ prefix: 'flag:', limit: FLAG_NAMES.length });

  check(allFlags.length === FLAG_NAMES.length, {
    flag_monitoring: () => allFlags.length === FLAG_NAMES.length
  });

  // Verify flag existence.
  const flagExists = await kv.exists(flagKey);

  check(flagExists, {
    flag_exists: () => flagExists
  });
}

// buildDefaultFlag constructs a baseline configuration for each feature 
// so getOrSet() has a deterministic payload to insert on first use.
function buildDefaultFlag(name) {
  return {
    name,
    enabled: name !== 'dark-mode',
    rollout: name === 'premium-features' ? 100 : 50,
    createdAt: Date.now(),
    updatedAt: Date.now()
  };
}

// updateFlagRollout is a CAS loop that keeps trying until it successfully applies
// an update, ensuring compareAndSwap() is truly tested under contention.
async function updateFlagRollout(flagKey, initialFlag, vuId) {
  let snapshot = initialFlag;

  for (let attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
    // Increment rollout by 1, wrapping at 100. 
    const proposed = {
      ...snapshot,
      rollout: (snapshot.rollout + 1) % BUCKET_COUNT,
      updatedAt: Date.now(),
      updatedBy: vuId
    };

    const updated = await kv.compareAndSwap(flagKey, snapshot, proposed);

    if (updated) {
      return true;
    }

    // CAS failed, re-read current state.
    try {
      snapshot = await kv.get(flagKey);
    } catch (err) {
      return false;
    }
  }

  return false;
}
