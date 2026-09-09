import { check } from 'k6';
import exec from 'k6/execution';

import {
  ITERATIONS,
  VUS,
  createKv,
  createSetup,
  createTeardown,
} from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: ORDERED EXCLUSIVE CLAIM CONSUMERS
// =============================================================================
//
// This scenario validates claimNext() as a recoverable queue allocator.
//
// - Keys are pre-seeded in lexicographic order.
// - Concurrent VUs claim one key each with shared-iterations.
// - setIfAbsent(seen:key) proves no duplicate delivery.
// - completeClaim(deleteKey=true) permanently consumes each claimed key.

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'claim-next-ordered-consumers';

// Prefix used for queue entries.
const JOB_PREFIX = 'jobs:';

// Prefix used for duplicate-detection markers.
const SEEN_PREFIX = 'seen:';

// Lease duration used for ordered claims.
const CLAIM_TTL_MS = 60_000;

// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME, {
  metrics: { operations: true },
});

// options configures pass/fail checks for ordered concurrent claiming.
export const options = {
  scenarios: {
    orderedConsumers: {
      executor: 'shared-iterations',
      vus: VUS,
      iterations: ITERATIONS,
      maxDuration: '2m',
    },
  },
  thresholds: {
    'checks{claimNext:allocated}': ['rate==1'],
    'checks{claimNext:unique}': ['rate==1'],
    'checks{claimNext:completed}': ['rate==1'],
  },
};

// setup clears state and seeds exactly ITERATIONS sortable queue items.
export async function setup() {
  const baseSetup = createSetup(kv);
  await baseSetup();

  const entries = {};

  for (let index = 1; index <= ITERATIONS; index += 1) {
    const key = `${JOB_PREFIX}${String(index).padStart(8, '0')}`;
    entries[key] = {
      id: index,
      status: 'queued',
    };
  }

  await kv.setMany(entries);
}

// teardown closes stores.
export const teardown = createTeardown(kv);

// claimNextOrderedConsumers validates ordered exclusive queue claiming.
export default async function claimNextOrderedConsumers() {
  const claim = await kv.claimNext({
    prefix: JOB_PREFIX,
    owner: `e2e:vu:${exec.vu.idInInstance}`,
    ttl: CLAIM_TTL_MS,
  });

  check(claim, {
    'claimNext:allocated': (value) => value !== null,
  });

  if (claim === null) {
    return;
  }

  const firstUse = await kv.setIfAbsent(`${SEEN_PREFIX}${claim.key}`, true);
  check(firstUse, {
    'claimNext:unique': (value) => value === true,
  });

  const completed = await kv.completeClaim(claim, { deleteKey: true });
  check(completed, {
    'claimNext:completed': (value) => value === true,
  });
}
