import { check } from 'k6';
import exec from 'k6/execution';
import { VUS, ITERATIONS, createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: PREFIX PREFLIGHT BEFORE DESTRUCTIVE CLEANUP
// =============================================================================
//
// This scenario models a safety-first cleanup flow used by operators:
// 1) preview candidate keys by prefix using listKeys(),
// 2) optionally hydrate a small subset via getMany(),
// 3) delete only the previewed keys via deleteMany().
//
// REAL-WORLD PROBLEM SOLVED:
// Without a key-only preflight step, bulk cleanup jobs can remove unintended
// records. listKeys() provides deterministic, low-cost inspection before delete.
//
// OPERATIONS TESTED:
// - setMany(): seed one bounded shard per worker.
// - listKeys(): key-only preview with lexicographic ordering and limit.
// - getMany(): hydrate previewed keys for optional validation.
// - deleteMany(): delete only previewed keys.
// - count({ prefix }): verify remaining key cardinality after cleanup.

// Prefix namespace for cleanup shards.
const CLEANUP_PREFIX = __ENV.CLEANUP_PREFIX || 'list-keys-cleanup:tenant:';

// Number of deterministic slots reused across iterations.
const CLEANUP_SLOT_RANGE = parseInt(__ENV.CLEANUP_SLOT_RANGE || '1000', 10);

// Number of keys requested from preflight preview.
const PREVIEW_LIMIT = parseInt(__ENV.PREVIEW_LIMIT || '2', 10);

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'prefix-preflight-list-keys';

// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME);

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: VUS,
  iterations: ITERATIONS,
  thresholds: {
    'checks{listKeys:preview-shape}': ['rate>0.999'],
    'checks{listKeys:preview-order-limit}': ['rate>0.999'],
    'checks{listKeys:unlimited-full-prefix}': ['rate>0.999'],
    'checks{listKeys:preview-hydration}': ['rate>0.999'],
    'checks{listKeys:delete-previewed-only}': ['rate>0.999'],
    'checks{listKeys:non-target-preserved}': ['rate>0.999'],
    'checks{listKeys:remaining-prefix-count}': ['rate>0.999'],
    'checks{listKeys:invalid-shape-rejects}': ['rate>0.999']
  }
};

// setup clears previous state before preflight cleanup loops start.
export const setup = createSetup(kv);

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv);

// prefixPreflightListKeys validates safe preflight + targeted delete workflow.
export default async function prefixPreflightListKeys() {
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
    [keyA]: { status: 'candidate', id: 'a' },
    [keyB]: { status: 'candidate', id: 'b' },
    [keyC]: { status: 'candidate', id: 'c' },
    [keepKey]: { status: 'keep', id: 'control' }
  });

  const preview = await kv.listKeys({
    prefix: targetPrefix,
    limit: PREVIEW_LIMIT
  });
  const fullPreview = await kv.listKeys({
    prefix: targetPrefix,
    limit: 0
  });

  const previewItems = await kv.getMany(preview);
  const deleteResult = await kv.deleteMany(preview);
  const stateAfterDelete = await kv.getMany([keyA, keyB, keyC, keepKey]);
  const remainingTargetCount = await kv.count({ prefix: targetPrefix });

  let invalidShapeErr = null;
  try {
    await kv.listKeys([]);
  } catch (err) {
    invalidShapeErr = err;
  }

  check(preview, {
    'listKeys:preview-shape': (keys) =>
      Array.isArray(keys) &&
      keys.length === PREVIEW_LIMIT,
    'listKeys:preview-order-limit': (keys) =>
      keys[0] === keyA &&
      keys[1] === keyB
  });

  check(fullPreview, {
    'listKeys:unlimited-full-prefix': (keys) =>
      Array.isArray(keys) &&
      keys.length === 3 &&
      keys[0] === keyA &&
      keys[1] === keyB &&
      keys[2] === keyC
  });

  check(previewItems, {
    'listKeys:preview-hydration': (items) =>
      Array.isArray(items) &&
      items.length === PREVIEW_LIMIT &&
      items[0].exists === true &&
      items[1].exists === true
  });

  check(deleteResult, {
    'listKeys:delete-previewed-only': (result) =>
      result &&
      result.deleted === PREVIEW_LIMIT &&
      result.missing === 0
  });

  check(stateAfterDelete, {
    'listKeys:non-target-preserved': (items) =>
      Array.isArray(items) &&
      items.length === 4 &&
      items[0].exists === false &&
      items[1].exists === false &&
      items[2].exists === true &&
      items[3].exists === true &&
      items[3].value &&
      items[3].value.status === 'keep'
  });

  check(remainingTargetCount, {
    'listKeys:remaining-prefix-count': (count) =>
      typeof count === 'number' &&
      count === 1
  });

  check(true, {
    'listKeys:invalid-shape-rejects': () =>
      invalidShapeErr !== null &&
      invalidShapeErr.name === 'InvalidOptionsError'
  });
}
