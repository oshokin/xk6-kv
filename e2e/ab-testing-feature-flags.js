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

// Backend selection: memory (default) or disk.
const SELECTED_BACKEND_NAME = __ENV.KV_BACKEND || 'memory';

// Optional: enable key tracking in memory backend to stress the tracking paths.
// (No effect for disk backend; safe to leave on)
const ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND =
  (__ENV.KV_TRACK_KEYS && __ENV.KV_TRACK_KEYS.toLowerCase() === 'true') || true;

// Feature flag configuration (coverage set and CAS retries).
const FLAG_NAMES = ['new-checkout', 'dark-mode', 'premium-features', 'beta-search'];
const MAX_FLAG_UPDATE_ATTEMPTS = 10;

// ---------------------------------------------
// Open a shared KV store available to all VUs.
// ---------------------------------------------
const kv = openKv(
  SELECTED_BACKEND_NAME === 'disk'
    ? { backend: 'disk', trackKeys: ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND }
    : { backend: 'memory', trackKeys: ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND }
);

// Rationale: 20 VUs × 100 iterations mirror a busy rollout window. Thresholds
// require that flag reads, updates, and monitoring logic never regress under
// deterministic CAS contention.
export const options = {
  vus: parseInt(__ENV.VUS || '20', 10),
  iterations: parseInt(__ENV.ITERATIONS || '100', 10),
  thresholds: {
    'checks{flag:enabled}': ['rate>0.999'],
    'checks{flag:updated}': ['rate>0.999'],
    'checks{flag:monitoring}': ['rate>0.999']
  }
};

// setup: creates a known flag set so monitoring checks can assert the exact
// number of entries returned by list().
export async function setup() {
  await kv.clear();

  for (const flagName of FLAG_NAMES) {
    await kv.set(`flag:${flagName}`, buildDefaultFlag(flagName));
  }
}

export async function teardown() {
  if (SELECTED_BACKEND_NAME === 'disk') {
    kv.close();
  }
}

// Each iteration emulates a user checking feature gates, recording usage, and
// occasionally acting as an admin to change rollout percentages.
export default async function abTestingFeatureFlagsTest() {
  const vuId = exec.vu.idInTest;
  const iterationSeed = exec.scenario.iterationInTest;
  const flagName = FLAG_NAMES[iterationSeed % FLAG_NAMES.length];
  const flagKey = `flag:${flagName}`;
  const userId = `user:${vuId}:${iterationSeed}`;

  const { value: flag } = await kv.getOrSet(flagKey, buildDefaultFlag(flagName));

  check(Boolean(flag), {
    'flag:enabled': () => Boolean(flag) && typeof flag.enabled === 'boolean'
  });

  const userSegment = (vuId * 31 + iterationSeed * 17) % 100;
  const shouldSeeFeature = flag.enabled && userSegment < flag.rollout;

  if (shouldSeeFeature) {
    const usageKey = `usage:${flagName}:${userId}`;
    const usageCount = await kv.incrementBy(usageKey, 1);

    check(usageCount > 0, {
      'flag:usage-tracked': () => usageCount > 0
    });
  }

  const updated = await updateFlagWithRetry(flagKey, flag, vuId);

  check(updated, {
    'flag:updated': () => updated
  });

  const allFlags = await kv.list({ prefix: 'flag:', limit: FLAG_NAMES.length });

  check(allFlags.length === FLAG_NAMES.length, {
    'flag:monitoring': () => allFlags.length === FLAG_NAMES.length
  });

  const flagExists = await kv.exists(flagKey);

  check(flagExists, {
    'flag:exists': () => flagExists
  });

  sleep(0.01);
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
      rollout: (snapshot.rollout + 7) % 100,
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
  }

  return false;
}
