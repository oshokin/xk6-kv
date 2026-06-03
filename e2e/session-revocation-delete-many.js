import { check } from 'k6';
import exec from 'k6/execution';
import { VUS, ITERATIONS, createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: SESSION REVOCATION WITH EXPLICIT BULK DELETES
// =============================================================================
//
// This test models an auth/session service that revokes multiple explicit keys
// in one call. Typical production examples:
//
// - Logout-all flow invalidating access + refresh token pairs.
// - Security incident response revoking compromised session material.
// - Identity backends cleaning a bounded per-user auth shard.
//
// REAL-WORLD PROBLEM SOLVED:
// Without deterministic bulk-delete semantics, revocation flows drift:
// - Incorrect deleted/missing accounting in audit pipelines.
// - Duplicate revoke requests behaving inconsistently.
// - Guard keys accidentally removed by broad cleanup logic.
//
// BATCH OPERATIONS TESTED:
// - setMany(): seed one deterministic auth shard.
// - deleteMany(): explicit-key revocation with deleted/missing counts.
// - getMany(): verify removed vs preserved keys after revocation.
//
// CONCURRENCY PATTERN:
// - VUs repeatedly revoke keys inside isolated user slots.
// - Key growth stays bounded by reusing deterministic slot IDs.
//
// PERFORMANCE CHARACTERISTICS:
// - Small write prelude plus bounded bulk deletes per iteration.
// - Sensitive to deleteMany result-shape and duplicate-key semantics.

// Prefix for per-user auth shards.
const SESSION_PREFIX = __ENV.SESSION_PREFIX || 'session-revoke:user:';

// Number of deterministic slots reused across iterations.
const SESSION_SLOT_RANGE = parseInt(__ENV.SESSION_SLOT_RANGE || '1000', 10);

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'session-revocation-delete-many';

// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME);

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: VUS,
  iterations: ITERATIONS,
  thresholds: {
    'checks{deleteMany:result-shape}': ['rate>0.999'],
    'checks{deleteMany:deleted-missing-counts}': ['rate>0.999'],
    'checks{deleteMany:duplicates-count-as-missing}': ['rate>0.999'],
    'checks{deleteMany:removed-keys}': ['rate>0.999'],
    'checks{deleteMany:non-target-key-preserved}': ['rate>0.999'],
    'checks{deleteMany:remaining-count}': ['rate>0.999'],
    'checks{deleteMany:empty-batch}': ['rate>0.999'],
    'checks{deleteMany:empty-key-rejects}': ['rate>0.999']
  }
};

// setup clears previous state before revocation loops start.
export const setup = createSetup(kv);

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv);

// sessionRevocationDeleteMany validates explicit bulk-delete behavior.
export default async function sessionRevocationDeleteMany() {
  const vuId = exec.vu.idInTest;
  const iteration = exec.scenario.iterationInTest;
  const slot = iteration % SESSION_SLOT_RANGE;
  const shardPrefix = `${SESSION_PREFIX}${vuId}:${slot}:`;

  const accessKey = `${shardPrefix}access`;
  const refreshKey = `${shardPrefix}refresh`;
  const stateKey = `${shardPrefix}state`;
  const missingKey = `${shardPrefix}missing`;

  await kv.setMany({
    [accessKey]: { token: `a-${vuId}-${slot}` },
    [refreshKey]: { token: `r-${vuId}-${slot}` },
    [stateKey]: { status: 'active' }
  });

  const deleteResult = await kv.deleteMany([
    accessKey,
    missingKey,
    refreshKey,
    accessKey
  ]);

  const items = await kv.getMany([accessKey, refreshKey, stateKey]);
  const remainingCount = await kv.count({ prefix: shardPrefix });
  const emptyBatchResult = await kv.deleteMany([]);

  let emptyKeyErr = null;
  try {
    await kv.deleteMany(['']);
  } catch (err) {
    emptyKeyErr = err;
  }

  check(deleteResult, {
    'deleteMany:result-shape': (result) =>
      result &&
      typeof result === 'object' &&
      typeof result.deleted === 'number' &&
      typeof result.missing === 'number',
    'deleteMany:deleted-missing-counts': (result) =>
      result.deleted === 2 && result.missing === 2,
    'deleteMany:duplicates-count-as-missing': (result) =>
      result.deleted === 2 && result.missing === 2
  });

  check(items, {
    'deleteMany:removed-keys': (result) =>
      Array.isArray(result) &&
      result.length === 3 &&
      result[0].exists === false &&
      result[1].exists === false,
    'deleteMany:non-target-key-preserved': (result) =>
      result[2].exists === true &&
      result[2].value &&
      result[2].value.status === 'active'
  });

  check(remainingCount, {
    'deleteMany:remaining-count': (count) =>
      typeof count === 'number' && count === 1
  });

  check(emptyBatchResult, {
    'deleteMany:empty-batch': (result) =>
      result.deleted === 0 && result.missing === 0
  });

  check(true, {
    'deleteMany:empty-key-rejects': () =>
      emptyKeyErr !== null && emptyKeyErr.name === 'InvalidOptionsError'
  });
}
