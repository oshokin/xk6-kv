import { check, sleep } from 'k6';
import exec from 'k6/execution';
import { openKv } from 'k6/x/kv';

// =============================================================================
// REAL-WORLD SCENARIO: A/B TESTING & FEATURE FLAGS SYSTEM
// =============================================================================
//
// This test simulates a feature flag and A/B testing system where we need to
// manage feature toggles, track usage, and perform controlled rollouts.
// This is a critical pattern in:
//
// - E-commerce platforms (testing checkout flows, product recommendations)
// - Social media platforms (testing UI changes, algorithm updates)
// - SaaS applications (testing new features, pricing models)
// - Mobile applications (testing app features, UI layouts)
// - Content platforms (testing article layouts, recommendation engines)
// - Gaming platforms (testing game mechanics, monetization features)
//
// REAL-WORLD PROBLEM SOLVED:
// Multiple users accessing features with different configurations simultaneously.
// Without proper feature flag management, you get:
// - Inconsistent user experiences (some users see features, others don't)
// - Failed A/B tests (contaminated results)
// - Rollback difficulties (can't quickly disable problematic features)
// - Poor analytics (inaccurate usage tracking)
// - Revenue impact (broken features affecting conversions)
//
// ATOMIC OPERATIONS TESTED:
// - getOrSet(): Initialize feature flags with defaults
// - incrementBy(): Track feature usage statistics
// - compareAndSwap(): Update feature configurations atomically
// - exists(): Verify feature flag availability
// - list(): Monitor all feature flags
//
// CONCURRENCY PATTERN:
// - Multiple VUs represent different users
// - Each VU checks feature flags and tracks usage
// - Shared KV store ensures consistent feature state
// - Deterministic retry loops ensure every CAS succeeds or surfaces a bug
//
// PERFORMANCE CHARACTERISTICS:
// - High read frequency (every user interaction)
// - Critical for user experience and business decisions
// - Must handle thousands of concurrent feature checks
// - Low latency impact (feature checks should be fast)

// Selected backend (memory or disk) for the scenario.
const SELECTED_BACKEND_NAME = __ENV.KV_BACKEND || 'memory';

// Enables in-memory key tracking when the backend is memory.
const TRACK_KEYS_OVERRIDE =
  typeof __ENV.KV_TRACK_KEYS === 'string' ? __ENV.KV_TRACK_KEYS.toLowerCase() : '';
const ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND =
  TRACK_KEYS_OVERRIDE === '' ? true : TRACK_KEYS_OVERRIDE === 'true';

// Static list of feature flags that the test exercises.
const FLAG_NAMES = ['new-checkout', 'dark-mode', 'premium-features', 'beta-search'];

// Maximum CAS retries before surfacing a flag update failure (env overridable).
const MAX_FLAG_UPDATE_ATTEMPTS = parseInt(__ENV.MAX_FLAG_UPDATE_ATTEMPTS || '1000', 10);
// Delay (in ms) between CAS retries to avoid overwhelming the event loop.
const FLAG_UPDATE_RETRY_DELAY_MS = parseFloat(__ENV.FLAG_UPDATE_RETRY_DELAY_MS || '5');
// Factor applied to VU identifiers when computing session segments.
const USER_SEGMENT_VU_FACTOR = parseInt(__ENV.USER_SEGMENT_VU_FACTOR || '31', 10);
// Factor applied to iteration counters when computing session segments.
const USER_SEGMENT_ITERATION_FACTOR = parseInt(
  __ENV.USER_SEGMENT_ITERATION_FACTOR || '17',
  10
);
// Modulo used to normalize session segments into percentage buckets.
const USER_SEGMENT_MODULO = parseInt(__ENV.USER_SEGMENT_MODULO || '100', 10);
// Rollout increment applied per admin update to emulate gradual rollouts.
const ROLLOUT_INCREMENT = parseInt(__ENV.ROLLOUT_INCREMENT || '7', 10);
// Base duration (seconds) each iteration sleeps after processing.
const BASE_IDLE_SLEEP_SECONDS = parseFloat(__ENV.BASE_IDLE_SLEEP_SECONDS || '0.02');
// Random jitter (seconds) added to the base idle sleep.
const IDLE_SLEEP_JITTER_SECONDS = parseFloat(
  __ENV.IDLE_SLEEP_JITTER_SECONDS || '0.01'
);
// Default number of virtual users for the scenario.
const DEFAULT_VUS = parseInt(__ENV.VUS || '40', 10);
// Default iteration count for the scenario.
const DEFAULT_ITERATIONS = parseInt(__ENV.ITERATIONS || '400', 10);

// kv is the shared store client used throughout the scenario.
const kv = openKv(
  SELECTED_BACKEND_NAME === 'disk'
    ? { backend: 'disk', trackKeys: ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND }
    : { backend: 'memory', trackKeys: ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND }
);

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: DEFAULT_VUS,
  iterations: DEFAULT_ITERATIONS,
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
  await kv.clear();

  for (const flagName of FLAG_NAMES) {
    await kv.set(`flag:${flagName}`, buildDefaultFlag(flagName));
  }
}

// teardown closes BoltDB cleanly so later runs do not trip over open handles.
export async function teardown() {
  if (SELECTED_BACKEND_NAME === 'disk') {
    kv.close();
  }
}

// abTestingFeatureFlagsTest emulates a user checking gates, recording usage, and
// occasionally acting as an admin to change rollout percentages.
export default async function abTestingFeatureFlagsTest() {
  const vuId = exec.vu.idInTest;
  const iterationSeed = exec.scenario.iterationInTest;
  const flagName = FLAG_NAMES[iterationSeed % FLAG_NAMES.length];
  const flagKey = `flag:${flagName}`;
  const userId = `user:${vuId}:${iterationSeed}`;

  const { value: flag } = await kv.getOrSet(flagKey, buildDefaultFlag(flagName));

  check(Boolean(flag), {
    flag_enabled: () => Boolean(flag) && typeof flag.enabled === 'boolean'
  });

  const userSegment =
    (vuId * USER_SEGMENT_VU_FACTOR + iterationSeed * USER_SEGMENT_ITERATION_FACTOR) %
    USER_SEGMENT_MODULO;
  const shouldSeeFeature = flag.enabled && userSegment < flag.rollout;

  if (shouldSeeFeature) {
    const usageKey = `usage:${flagName}:${userId}`;
    const usageCount = await kv.incrementBy(usageKey, 1);

    check(usageCount > 0, {
      flag_usage_tracked: () => usageCount > 0
    });
  }

  const updated = await updateFlagWithRetry(flagKey, flag, vuId);

  check(updated, {
    flag_updated: () => updated
  });

  const allFlags = await kv.list({ prefix: 'flag:', limit: FLAG_NAMES.length });

  check(allFlags.length === FLAG_NAMES.length, {
    flag_monitoring: () => allFlags.length === FLAG_NAMES.length
  });

  const flagExists = await kv.exists(flagKey);

  check(flagExists, {
    flag_exists: () => flagExists
  });

  // Simulate work long enough to keep the scenario under load.
  sleep(BASE_IDLE_SLEEP_SECONDS + Math.random() * IDLE_SLEEP_JITTER_SECONDS);
}

// buildDefaultFlag: baseline configuration for each feature so getOrSet() has a
// deterministic payload to insert on first use.
function buildDefaultFlag(name) {
  return {
    name,
    enabled: name !== 'dark-mode',
    rollout: name === 'premium-features' ? 100 : 50,
    createdAt: Date.now(),
    updatedAt: Date.now()
  };
}

// updateFlagWithRetry: CAS loop that keeps trying until it successfully applies
// an update, ensuring compareAndSwap() is truly tested under contention.
async function updateFlagWithRetry(flagKey, initialFlag, vuId) {
  let snapshot = initialFlag;

  for (let attempt = 0; attempt < MAX_FLAG_UPDATE_ATTEMPTS; attempt++) {
    const proposed = {
      ...snapshot,
      rollout: (snapshot.rollout + ROLLOUT_INCREMENT + USER_SEGMENT_MODULO) %
        USER_SEGMENT_MODULO,
      updatedAt: Date.now(),
      updatedBy: vuId
    };

    const updated = await kv.compareAndSwap(flagKey, snapshot, proposed);

    if (updated) {
      return true;
    }

    try {
      snapshot = await kv.get(flagKey);
    } catch (err) {
      return false;
    }

    // Yield briefly before retrying so other VUs can progress.
    sleep(FLAG_UPDATE_RETRY_DELAY_MS / 1000);
  }

  return false;
}
