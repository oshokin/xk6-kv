import { check } from 'k6';
import exec from 'k6/execution';
import { VUS, ITERATIONS, createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: BULK PROFILE IMPORT WITH ALL-OR-NOTHING BATCH UPSERTS
// =============================================================================
//
// This test models a profile-sync pipeline where each worker refreshes a tenant
// cache in one logical batch write using setMany(). Typical production examples:
//
// - CRM/contact sync jobs refreshing tenant-scoped profile caches.
// - Identity providers projecting user metadata into service-local stores.
// - Recommendation systems updating user feature vectors in bounded batches.
//
// REAL-WORLD PROBLEM SOLVED:
// Without all-or-nothing batch writes, refresh flows can fail mid-import and drift:
// - Some users updated while others still show stale versions.
// - Metadata and payload rows can drift out of sync.
// - Retry logic can amplify data skew under contention.
//
// BATCH OPERATIONS TESTED:
// - setMany(): upsert a tenant batch with all-or-nothing validation/serialization behavior.
// - get(): verify representative entry payload shape after refresh.
// - count({ prefix }): verify tenant shard cardinality stays consistent.
//
// CONCURRENCY PATTERN:
// - Each VU owns one tenant namespace and repeatedly refreshes that shard.
// - Rewrites are bounded (same keys per tenant) to avoid unbounded key growth.
//
// PERFORMANCE CHARACTERISTICS:
// - Medium-sized batch writes under sustained concurrency.
// - Sensitive to serialization overhead and write lock contention.

// Key prefix for tenant profile shards.
const TENANT_PREFIX = __ENV.TENANT_PREFIX || 'bulk-import:tenant:';

// Number of profile rows written per tenant refresh.
const BATCH_USER_COUNT = parseInt(__ENV.BATCH_USER_COUNT || '8', 10);

// Number of digits used for profile key suffixes.
const PROFILE_KEY_PAD_WIDTH = parseInt(__ENV.PROFILE_KEY_PAD_WIDTH || '4', 10);

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'bulk-profile-import-setmany';

// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME);

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: VUS,
  iterations: ITERATIONS,
  thresholds: {
    'checks{import:setMany-structure}': ['rate>0.999'],
    'checks{import:setMany-written}': ['rate>0.999'],
    'checks{import:sample-profile}': ['rate>0.999'],
    'checks{import:tenant-count}': ['rate>0.999'],
    'checks{import:batch-metadata}': ['rate>0.999']
  }
};

// setup clears previous state before profile refresh loops start.
export const setup = createSetup(kv);

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv, TEST_NAME);

// bulkProfileImportSetMany refreshes one tenant shard each iteration.
export default async function bulkProfileImportSetMany() {
  const vuId = exec.vu.idInTest;
  const batchVersion = exec.scenario.iterationInTest + 1;
  const tenantPrefix = `${TENANT_PREFIX}${vuId}:`;

  const entries = buildTenantBatch(tenantPrefix, vuId, batchVersion);
  const expectedWritten = Object.keys(entries).length;
  const writeResult = await kv.setMany(entries);

  const sampleKey = `${tenantPrefix}profile:${String(0).padStart(PROFILE_KEY_PAD_WIDTH, '0')}`;
  const metadataKey = `${tenantPrefix}meta`;

  const sampleProfile = await kv.get(sampleKey);
  const metadata = await kv.get(metadataKey);
  const tenantCount = await kv.count({ prefix: tenantPrefix });

  check(writeResult, {
    'import:setMany-structure': () =>
      typeof writeResult === 'object' &&
      Object.keys(writeResult).length === 1 &&
      Object.prototype.hasOwnProperty.call(writeResult, 'written'),
    'import:setMany-written': () =>
      typeof writeResult.written === 'number' &&
      writeResult.written === expectedWritten
  });

  check(sampleProfile, {
    'import:sample-profile': () =>
      typeof sampleProfile === 'object' &&
      sampleProfile.tenantId === vuId &&
      sampleProfile.version === batchVersion &&
      typeof sampleProfile.email === 'string'
  });

  check(tenantCount, {
    'import:tenant-count': () =>
      typeof tenantCount === 'number' &&
      tenantCount === expectedWritten
  });

  check(metadata, {
    'import:batch-metadata': () =>
      typeof metadata === 'object' &&
      metadata.tenantId === vuId &&
      metadata.version === batchVersion &&
      metadata.userCount === BATCH_USER_COUNT
  });
}

// buildTenantBatch constructs one tenant profile batch plus shard metadata.
function buildTenantBatch(prefix, tenantId, batchVersion) {
  const entries = {};

  for (let i = 0; i < BATCH_USER_COUNT; i += 1) {
    const suffix = String(i).padStart(PROFILE_KEY_PAD_WIDTH, '0');
    entries[`${prefix}profile:${suffix}`] = {
      tenantId,
      profileId: i,
      version: batchVersion,
      email: `tenant${tenantId}-user${i}@example.com`,
      plan: i % 2 === 0 ? 'pro' : 'starter',
      tags: ['imported', `batch-${batchVersion}`]
    };
  }

  entries[`${prefix}meta`] = {
    tenantId,
    version: batchVersion,
    userCount: BATCH_USER_COUNT,
    importedAt: Date.now()
  };

  return entries;
}
