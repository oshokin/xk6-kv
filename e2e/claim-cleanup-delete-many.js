import { check } from 'k6';
import exec from 'k6/execution';
import { VUS, ITERATIONS, createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: LEASED WORK ITEM CANCELLATION VIA BULK DELETE
// =============================================================================
//
// This test models a worker system where tasks can be leased (claimed) and then
// cancelled in bulk by an admin/control plane operation.
//
// REAL-WORLD PROBLEM SOLVED:
// If bulk delete does not clean claim metadata, workers can hold stale claims:
// - release/complete calls report incorrect outcomes,
// - task pools remain logically inconsistent,
// - cancellation observability is misleading.
//
// OPERATIONS TESTED:
// - setMany(): seed a small per-slot task shard.
// - claimRandom(): acquire one live claim.
// - deleteMany(): delete claimed + unclaimed keys in one request.
// - releaseClaim(): ensure stale claim is not releasable after deleteMany.

// Prefix for task keys used in lease/delete flows.
const TASK_PREFIX = __ENV.TASK_PREFIX || 'delete-many-claim:task:';

// Number of deterministic slots reused across iterations.
const TASK_SLOT_RANGE = parseInt(__ENV.TASK_SLOT_RANGE || '1000', 10);

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'claim-cleanup-delete-many';

// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME);

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: VUS,
  iterations: ITERATIONS,
  thresholds: {
    'checks{deleteMany-claim:claim-created}': ['rate>0.999'],
    'checks{deleteMany-claim:delete-counts}': ['rate>0.999'],
    'checks{deleteMany-claim:claim-cleaned}': ['rate>0.999'],
    'checks{deleteMany-claim:keys-removed}': ['rate>0.999']
  }
};

// setup clears previous state before cancellation loops start.
export const setup = createSetup(kv);

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv);

// claimCleanupDeleteMany validates claim lifecycle after bulk deletions.
export default async function claimCleanupDeleteMany() {
  const vuId = exec.vu.idInTest;
  const iteration = exec.scenario.iterationInTest;
  const slot = iteration % TASK_SLOT_RANGE;
  const shardPrefix = `${TASK_PREFIX}${vuId}:${slot}:`;

  const taskA = `${shardPrefix}a`;
  const taskB = `${shardPrefix}b`;
  const missingTask = `${shardPrefix}missing`;

  await kv.setMany({
    [taskA]: { payload: 'A' },
    [taskB]: { payload: 'B' }
  });

  const claim = await kv.claimRandom({
    prefix: shardPrefix,
    owner: `vu:${vuId}`,
    ttl: 60000
  });

  if (claim === null) {
    check(false, {
      'deleteMany-claim:claim-created': () => false
    });
    return;
  }

  const pairedKey = claim.key === taskA ? taskB : taskA;
  const deleteResult = await kv.deleteMany([claim.key, pairedKey, missingTask]);
  const released = await kv.releaseClaim(claim);
  const items = await kv.getMany([taskA, taskB]);

  check(claim, {
    'deleteMany-claim:claim-created': (value) =>
      value &&
      typeof value.id === 'string' &&
      typeof value.token === 'number' &&
      typeof value.key === 'string'
  });

  check(deleteResult, {
    'deleteMany-claim:delete-counts': (result) =>
      result &&
      result.deleted === 2 &&
      result.missing === 1
  });

  check(released, {
    'deleteMany-claim:claim-cleaned': (value) => value === false
  });

  check(items, {
    'deleteMany-claim:keys-removed': (result) =>
      Array.isArray(result) &&
      result.length === 2 &&
      result[0].exists === false &&
      result[1].exists === false
  });
}
