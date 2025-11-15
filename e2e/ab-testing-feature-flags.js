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
// - compareAndSwap(): Update feature configurations atomically
// - incrementBy(): Track feature usage statistics
// - exists(): Verify feature flag availability
// - list(): Monitor all feature flags
//
// CONCURRENCY PATTERN:
// - Multiple VUs represent different users
// - Each VU checks feature flags and tracks usage
// - Shared KV store ensures consistent feature state
// - Rollout percentages control feature visibility
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

// ---------------------------------------------
// Open a shared KV store available to all VUs.
// ---------------------------------------------
const kv = openKv(
  SELECTED_BACKEND_NAME === 'disk'
    ? { backend: 'disk', trackKeys: ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND }
    : { backend: 'memory', trackKeys: ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND }
);

export const options = {
  // Vary these to increase contention. Start with 20×100 like the shared script.
  vus: parseInt(__ENV.VUS || '20', 10),
  iterations: parseInt(__ENV.ITERATIONS || '100', 10),

  // Optional: add thresholds to fail fast if we start choking.
  thresholds: {
    // Require that at least 95% of iterations handle feature flags successfully.
    'checks{flag:enabled}': ['rate>0.95'],
    // NOTE: Some compareAndSwap failures are EXPECTED and VALIDATE correct behavior:
    // - compareAndSwap() may return false when another VU updates the flag first (race condition)
    // - This confirms that atomic operations correctly prevent concurrent modification conflicts.
    // - A 90% success rate is expected under high concurrency (20 VUs × 100 iterations).
    'checks{flag:updated}': ['rate>0.90']
  }
};

// -----------------------
// Test setup & teardown.
// -----------------------
export async function setup() {
  // Start with a clean state so each run is deterministic.
  await kv.clear();

  // Pre-populate some feature flags.
  const flags = [
    { name: 'new-checkout', enabled: true, rollout: 50 },
    { name: 'dark-mode', enabled: false, rollout: 0 },
    { name: 'premium-features', enabled: true, rollout: 100 },
    { name: 'beta-search', enabled: false, rollout: 25 }
  ];

  for (const flag of flags) {
    await kv.set(`flag:${flag.name}`, {
      ...flag,
      createdAt: Date.now(),
      updatedAt: Date.now()
    });
  }
}

export async function teardown() {
  // For disk backends, close the store cleanly so the file can be reused immediately.
  if (SELECTED_BACKEND_NAME === 'disk') {
    kv.close();
  }
}

// -------------------------------
// The main iteration body (VUs).
// -------------------------------
export default async function abTestingFeatureFlagsTest() {
  const vuId = exec.vu.idInTest;
  const userId = `user:${vuId}:${Math.floor(Math.random() * 1000)}`;

  // Test 1: Check feature flag (getOrSet for default).
  const flagName = 'new-checkout';
  const flagKey = `flag:${flagName}`;

  const { value: flag, loaded } = await kv.getOrSet(flagKey, {
    name: flagName,
    enabled: false,
    rollout: 0,
    createdAt: Date.now(),
    updatedAt: Date.now()
  });

  check(loaded, {
    'flag:enabled': () => loaded && flag.enabled
  });

  // Test 2: Check if user should see feature (rollout logic).
  const userHash = userId.split(':').pop() % 100;
  const shouldShowFeature = flag.enabled && userHash < flag.rollout;

  if (shouldShowFeature) {
    // Test 3: Track feature usage.
    const usageKey = `usage:${flagName}:${userId}`;
    const usageCount = await kv.incrementBy(usageKey, 1);

    check(usageCount > 0, {
      'flag:usage-tracked': () => usageCount > 0
    });
  }

  // Test 4: Admin updates flag (compareAndSwap).
  // NOTE: compareAndSwap() may return false if another VU updated the flag first.
  // This is EXPECTED behavior - it validates that atomic operations prevent concurrent modification conflicts.
  // Under high concurrency (20 VUs), some updates will fail, which is correct behavior.
  if (Math.random() < 0.1) { // 10% chance to update flag
    const currentFlag = await kv.get(flagKey);

    if (currentFlag) {
      const newRollout = Math.floor(Math.random() * 100);
      const updated = await kv.compareAndSwap(flagKey, currentFlag, {
        ...currentFlag,
        rollout: newRollout,
        updatedAt: Date.now(),
        updatedBy: vuId
      });

      check(updated, {
        'flag:updated': () => updated
      });
    }
  }

  // Test 5: List all flags (monitoring).
  const allFlags = await kv.list({ prefix: 'flag:', limit: 10 });

  check(allFlags.length > 0, {
    'flag:monitoring': () => allFlags.length > 0
  });

  // Test 6: Check flag existence.
  const flagExists = await kv.exists(flagKey);

  check(flagExists, {
    'flag:exists': () => flagExists
  });

  // Simulate feature usage.
  sleep(0.01);
}
