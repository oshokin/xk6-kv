import { check } from 'k6';
import exec from 'k6/execution';
import { VUS, ITERATIONS, createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: BOUNDED PREFIX CLEANUP WITH DELETEBYPREFIX
// =============================================================================
//
// This scenario models an operator cleanup task that removes temporary keys by
// prefix in bounded batches until the shard is empty.
//
// REAL-WORLD PROBLEM SOLVED:
// Prefix cleanup is destructive. The API must stay bounded and deterministic:
// - first batch deletes only up to limit;
// - done=false signals that another pass is required;
// - non-target keys in the same shard stay intact;
// - invalid input fails with InvalidOptionsError.
//
// OPERATIONS TESTED:
// - setMany(): seed deterministic shard fixtures.
// - deleteByPrefix(): bounded destructive prefix deletion.
// - getMany(): verify removed vs preserved keys.
// - count({ prefix }): verify target keyspace is fully drained.

// Prefix namespace for per-worker cleanup shards.
const CLEANUP_PREFIX = __ENV.DELETE_BY_PREFIX_PREFIX || 'delete-by-prefix-cleanup:tenant:';

// Number of deterministic slots reused across iterations.
const CLEANUP_SLOT_RANGE = parseInt(__ENV.DELETE_BY_PREFIX_SLOT_RANGE || '1000', 10);

// Bounded deletion size used by deleteByPrefix().
const DELETE_LIMIT = 2;

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'prefix-cleanup-delete-by-prefix';

// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME);

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: VUS,
  iterations: ITERATIONS,
  thresholds: {
    'checks{deleteByPrefix:first-shape}': ['rate>0.999'],
    'checks{deleteByPrefix:first-bounded-delete}': ['rate>0.999'],
    'checks{deleteByPrefix:first-not-done}': ['rate>0.999'],
    'checks{deleteByPrefix:second-finishes-prefix}': ['rate>0.999'],
    'checks{deleteByPrefix:target-keys-removed}': ['rate>0.999'],
    'checks{deleteByPrefix:non-target-preserved}': ['rate>0.999'],
    'checks{deleteByPrefix:remaining-target-count-zero}': ['rate>0.999'],
    'checks{deleteByPrefix:missing-prefix-noop}': ['rate>0.999'],
    'checks{deleteByPrefix:invalid-shape-rejects}': ['rate>0.999'],
    'checks{deleteByPrefix:empty-prefix-rejects}': ['rate>0.999'],
    'checks{deleteByPrefix:invalid-limit-rejects}': ['rate>0.999']
  }
};

// setup clears previous state before cleanup loops start.
export const setup = createSetup(kv);

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv, TEST_NAME);

// prefixCleanupDeleteByPrefix validates bounded prefix-delete behavior.
export default async function prefixCleanupDeleteByPrefix() {
  const vuId = exec.vu.idInTest;
  const iteration = exec.scenario.iterationInTest;
  const slot = iteration % CLEANUP_SLOT_RANGE;
  const shardPrefix = `${CLEANUP_PREFIX}${vuId}:${slot}:`;
  const targetPrefix = `${shardPrefix}tmp:`;

  const keyA = `${targetPrefix}a`;
  const keyB = `${targetPrefix}b`;
  const keyC = `${targetPrefix}c`;
  const keepKey = `${shardPrefix}keep`;

  await kv.setMany({
    [keyA]: { status: 'tmp', id: 'a' },
    [keyB]: { status: 'tmp', id: 'b' },
    [keyC]: { status: 'tmp', id: 'c' },
    [keepKey]: { status: 'keep', id: 'control' }
  });

  const first = await kv.deleteByPrefix({
    prefix: targetPrefix,
    limit: DELETE_LIMIT
  });

  const second = await kv.deleteByPrefix({
    prefix: targetPrefix,
    limit: DELETE_LIMIT
  });

  const itemsAfterDelete = await kv.getMany([keyA, keyB, keyC, keepKey]);
  const remainingTargetCount = await kv.count({ prefix: targetPrefix });
  const missingPrefixResult = await kv.deleteByPrefix({
    prefix: `${shardPrefix}missing:`,
    limit: DELETE_LIMIT
  });

  let invalidShapeErr = null;
  try {
    await kv.deleteByPrefix([]);
  } catch (err) {
    invalidShapeErr = err;
  }

  let emptyPrefixErr = null;
  try {
    await kv.deleteByPrefix({ prefix: '', limit: DELETE_LIMIT });
  } catch (err) {
    emptyPrefixErr = err;
  }

  let invalidLimitErr = null;
  try {
    await kv.deleteByPrefix({ prefix: targetPrefix, limit: 0 });
  } catch (err) {
    invalidLimitErr = err;
  }

  check(first, {
    'deleteByPrefix:first-shape': (result) =>
      result &&
      typeof result === 'object' &&
      typeof result.deleted === 'number' &&
      typeof result.done === 'boolean',
    'deleteByPrefix:first-bounded-delete': (result) =>
      result.deleted === DELETE_LIMIT,
    'deleteByPrefix:first-not-done': (result) =>
      result.done === false
  });

  check(second, {
    'deleteByPrefix:second-finishes-prefix': (result) =>
      result.deleted === 1 && result.done === true
  });

  check(itemsAfterDelete, {
    'deleteByPrefix:target-keys-removed': (items) =>
      Array.isArray(items) &&
      items.length === 4 &&
      items[0].exists === false &&
      items[1].exists === false &&
      items[2].exists === false,
    'deleteByPrefix:non-target-preserved': (items) =>
      items[3].exists === true &&
      items[3].value &&
      items[3].value.status === 'keep'
  });

  check(remainingTargetCount, {
    'deleteByPrefix:remaining-target-count-zero': (count) =>
      typeof count === 'number' && count === 0
  });

  check(missingPrefixResult, {
    'deleteByPrefix:missing-prefix-noop': (result) =>
      result.deleted === 0 && result.done === true
  });

  check(true, {
    'deleteByPrefix:invalid-shape-rejects': () =>
      invalidShapeErr !== null && invalidShapeErr.name === 'InvalidOptionsError',
    'deleteByPrefix:empty-prefix-rejects': () =>
      emptyPrefixErr !== null && emptyPrefixErr.name === 'InvalidOptionsError',
    'deleteByPrefix:invalid-limit-rejects': () =>
      invalidLimitErr !== null && invalidLimitErr.name === 'InvalidOptionsError'
  });
}
