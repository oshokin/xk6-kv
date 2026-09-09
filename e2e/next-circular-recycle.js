import { check } from 'k6';

import {
  ITERATIONS,
  VUS,
  createKv,
} from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: REUSABLE SEARCH FIXTURE RECYCLING
// =============================================================================
//
// This scenario models a common load-testing setup where concurrent VUs execute
// more iterations than there are reusable fixture rows (for example, search
// queries imported from CSV/JSONL).
//
// nextCircular() should provide one shared process-local cursor per prefix:
// - keys are returned in lexicographic order;
// - after EOF, iteration wraps to the first key;
// - aggregate key counts follow exact full-cycle + remainder math.

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'next-circular-recycle';
// Prefix used for reusable dataset entries.
const DATA_PREFIX = 'circular:data:';
// Prefix used for per-entry consumption counters.
const COUNT_PREFIX = 'circular:count:';
// Size of the reusable fixture catalog.
const DATASET_SIZE = 5;

// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME);

// options configures shared-iterations load and validation thresholds.
export const options = {
  scenarios: {
    circularConsumers: {
      executor: 'shared-iterations',
      vus: VUS,
      iterations: ITERATIONS,
      maxDuration: '2m',
    },
  },
  thresholds: {
    'checks{nextCircular:non_null}': ['rate==1'],
    'checks{nextCircular:valid_key}': ['rate==1'],
  },
};

// setup clears state and seeds a small reusable fixture dataset.
export async function setup() {
  await kv.clear();

  const entries = {};
  for (let i = 1; i <= DATASET_SIZE; i += 1) {
    const key = `${DATA_PREFIX}${String(i).padStart(6, '0')}`;
    entries[key] = { ordinal: i };
  }

  await kv.setMany(entries);
}

// default consumes one reusable record and tracks aggregate distribution.
export default async function () {
  const entry = await kv.nextCircular({
    prefix: DATA_PREFIX,
  });

  check(entry, {
    'nextCircular:non_null': (value) => value !== null,
  });

  if (entry === null) {
    return;
  }

  check(entry, {
    'nextCircular:valid_key': (value) =>
      typeof value.key === 'string' &&
      value.key.startsWith(DATA_PREFIX) &&
      value.value !== null &&
      typeof value.value.ordinal === 'number',
  });

  await kv.incrementBy(`${COUNT_PREFIX}${entry.key}`, 1);
}

// teardown validates exact cycle distribution and closes the store.
export async function teardown() {
  try {
    const failures = [];
    const fullCycles = Math.floor(ITERATIONS / DATASET_SIZE);
    const remainder = ITERATIONS % DATASET_SIZE;

    for (let i = 1; i <= DATASET_SIZE; i += 1) {
      const dataKey = `${DATA_PREFIX}${String(i).padStart(6, '0')}`;
      const expected = fullCycles + (i <= remainder ? 1 : 0);
      const actual = await kv.get(`${COUNT_PREFIX}${dataKey}`);

      if (actual !== expected) {
        failures.push(`${dataKey}: expected ${expected}, got ${actual}`);
      }
    }

    if (failures.length > 0) {
      throw new Error(`nextCircular distribution mismatch: ${failures.join('; ')}`);
    }
  } finally {
    kv.close();
  }
}
