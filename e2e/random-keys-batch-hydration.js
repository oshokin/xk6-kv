import { check } from 'k6';
import { createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: RANDOM KEY SAMPLING + BATCH HYDRATION
// =============================================================================
//
// This scenario models a backend flow that first samples random candidate keys
// and then hydrates full records via one batch read.
//
// REAL-WORLD PROBLEM SOLVED:
// randomKeys() is used as a key-only sampler and then chained with getMany():
// - unique sampling must respect prefix boundaries;
// - non-unique sampling must allow repeats;
// - getMany() must return entries in request order with exists/value shape;
// - invalid randomKeys() options must reject with InvalidOptionsError.
//
// METHODS TESTED:
// - randomKeys(): unique/non-unique sampling and options validation.
// - getMany(): hydration of sampled key set.
// - setMany(): deterministic seed writes.

// Prefix namespace for sampled user keys.
const RANDOM_KEYS_USER_PREFIX = __ENV.RANDOM_KEYS_USER_PREFIX || 'random-keys:user:';

// Prefix namespace for non-target control keys.
const RANDOM_KEYS_ORDER_PREFIX = __ENV.RANDOM_KEYS_ORDER_PREFIX || 'random-keys:order:';

// Prefix namespace with a single candidate key (deterministic non-unique repeats).
const RANDOM_KEYS_SINGLE_PREFIX = __ENV.RANDOM_KEYS_SINGLE_PREFIX || 'random-keys:single:';

// Number of user keys available for unique random sampling.
const USER_POOL_SIZE = parseInt(__ENV.RANDOM_KEYS_USER_POOL_SIZE || '120', 10);

// Requested unique sample size from the user pool.
const UNIQUE_SAMPLE_SIZE = parseInt(__ENV.RANDOM_KEYS_UNIQUE_SAMPLE_SIZE || '25', 10);

// Requested non-unique sample size from the single-candidate prefix.
const SINGLE_REPEAT_COUNT = parseInt(__ENV.RANDOM_KEYS_SINGLE_REPEAT_COUNT || '7', 10);

// Number of control (non-user) keys used to verify prefix isolation.
const ORDER_POOL_SIZE = parseInt(__ENV.RANDOM_KEYS_ORDER_POOL_SIZE || '40', 10);

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'random-keys-batch-hydration';

// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME);

// options configures a deterministic single-run API contract check.
export const options = {
  vus: 1,
  iterations: 1,
  thresholds: {
    'checks{randomKeys:unique-array-shape}': ['rate==1'],
    'checks{randomKeys:unique-count}': ['rate==1'],
    'checks{randomKeys:unique-prefix-subset}': ['rate==1'],
    'checks{randomKeys:unique-no-duplicates}': ['rate==1'],
    'checks{randomKeys:getMany-shape}': ['rate==1'],
    'checks{randomKeys:getMany-order}': ['rate==1'],
    'checks{randomKeys:getMany-exists}': ['rate==1'],
    'checks{randomKeys:getMany-values}': ['rate==1'],
    'checks{randomKeys:single-candidate-repeats}': ['rate==1'],
    'checks{randomKeys:empty-prefix-empty-array}': ['rate==1'],
    'checks{randomKeys:count-larger-than-available}': ['rate==1'],
    'checks{randomKeys:invalid-shape-rejects}': ['rate==1'],
    'checks{randomKeys:missing-count-rejects}': ['rate==1'],
    'checks{randomKeys:fractional-count-rejects}': ['rate==1'],
    'checks{randomKeys:non-positive-count-rejects}': ['rate==1']
  }
};

// setup clears previous state before random sampling checks start.
export const setup = createSetup(kv);

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv, TEST_NAME);

// randomKeysBatchHydration validates random key sampling contract and batch hydration flow.
export default async function randomKeysBatchHydration() {
  await kv.clear();

  const singleCandidateKey = `${RANDOM_KEYS_SINGLE_PREFIX}only`;
  const seed = {
    [singleCandidateKey]: {
      id: 'single',
      type: 'single'
    }
  };

  for (let i = 0; i < USER_POOL_SIZE; i += 1) {
    seed[`${RANDOM_KEYS_USER_PREFIX}${i}`] = {
      id: i,
      type: 'user',
      cohort: i % 5
    };
  }

  for (let i = 0; i < ORDER_POOL_SIZE; i += 1) {
    seed[`${RANDOM_KEYS_ORDER_PREFIX}${i}`] = {
      id: i,
      type: 'order'
    };
  }

  await kv.setMany(seed);

  const expectedUniqueCount = Math.min(UNIQUE_SAMPLE_SIZE, USER_POOL_SIZE);

  const uniqueKeys = await kv.randomKeys({
    prefix: RANDOM_KEYS_USER_PREFIX,
    count: UNIQUE_SAMPLE_SIZE,
    unique: true
  });

  const hydrated = await kv.getMany(uniqueKeys);

  const repeatedSingle = await kv.randomKeys({
    prefix: RANDOM_KEYS_SINGLE_PREFIX,
    count: SINGLE_REPEAT_COUNT,
    unique: false
  });

  const emptyKeys = await kv.randomKeys({
    prefix: 'random-keys:missing:',
    count: 10,
    unique: true
  });

  const allAvailable = await kv.randomKeys({
    prefix: RANDOM_KEYS_USER_PREFIX,
    count: USER_POOL_SIZE * 10,
    unique: true
  });

  let invalidShapeErr = null;
  try {
    await kv.randomKeys([]);
  } catch (err) {
    invalidShapeErr = err;
  }

  let missingCountErr = null;
  try {
    await kv.randomKeys({ prefix: RANDOM_KEYS_USER_PREFIX });
  } catch (err) {
    missingCountErr = err;
  }

  let fractionalCountErr = null;
  try {
    await kv.randomKeys({
      prefix: RANDOM_KEYS_USER_PREFIX,
      count: 1.5
    });
  } catch (err) {
    fractionalCountErr = err;
  }

  let nonPositiveCountErr = null;
  try {
    await kv.randomKeys({
      prefix: RANDOM_KEYS_USER_PREFIX,
      count: 0
    });
  } catch (err) {
    nonPositiveCountErr = err;
  }

  check(uniqueKeys, {
    'randomKeys:unique-array-shape': (keys) =>
      Array.isArray(keys) && keys.every((key) => typeof key === 'string'),
    'randomKeys:unique-count': (keys) =>
      keys.length === expectedUniqueCount,
    'randomKeys:unique-prefix-subset': (keys) =>
      keys.every((key) => key.startsWith(RANDOM_KEYS_USER_PREFIX)),
    'randomKeys:unique-no-duplicates': (keys) =>
      new Set(keys).size === keys.length
  });

  check(hydrated, {
    'randomKeys:getMany-shape': (items) =>
      Array.isArray(items) &&
      items.length === uniqueKeys.length &&
      items.every(
        (item) =>
          item &&
          typeof item.key === 'string' &&
          typeof item.exists === 'boolean' &&
          Object.prototype.hasOwnProperty.call(item, 'value')
      ),
    'randomKeys:getMany-order': (items) =>
      items.every((item, index) => item.key === uniqueKeys[index]),
    'randomKeys:getMany-exists': (items) =>
      items.every((item) => item.exists === true),
    'randomKeys:getMany-values': (items) =>
      items.every((item) => item.value && item.value.type === 'user')
  });

  check(repeatedSingle, {
    'randomKeys:single-candidate-repeats': (keys) =>
      Array.isArray(keys) &&
      keys.length === SINGLE_REPEAT_COUNT &&
      keys.every((key) => key === singleCandidateKey)
  });

  check(emptyKeys, {
    'randomKeys:empty-prefix-empty-array': (keys) =>
      Array.isArray(keys) && keys.length === 0
  });

  check(allAvailable, {
    'randomKeys:count-larger-than-available': (keys) =>
      Array.isArray(keys) &&
      keys.length === USER_POOL_SIZE &&
      new Set(keys).size === USER_POOL_SIZE &&
      keys.every((key) => key.startsWith(RANDOM_KEYS_USER_PREFIX))
  });

  check(true, {
    'randomKeys:invalid-shape-rejects': () =>
      invalidShapeErr !== null && invalidShapeErr.name === 'InvalidOptionsError',
    'randomKeys:missing-count-rejects': () =>
      missingCountErr !== null && missingCountErr.name === 'InvalidOptionsError',
    'randomKeys:fractional-count-rejects': () =>
      fractionalCountErr !== null && fractionalCountErr.name === 'InvalidOptionsError',
    'randomKeys:non-positive-count-rejects': () =>
      nonPositiveCountErr !== null && nonPositiveCountErr.name === 'InvalidOptionsError'
  });
}
