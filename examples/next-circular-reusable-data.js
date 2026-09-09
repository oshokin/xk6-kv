// Reusable circular test-data example.
//
// nextCircular() shares one process-local cursor between all VUs using the
// same exact prefix. Records are reusable and are NOT exclusively leased.
//
// For exclusive work items or credentials, use claimNext()/claimForOwner().

import { sleep } from 'k6';
import { openKv } from 'k6/x/kv';

export const options = {
  vus: 3,
  iterations: 12,
};

const PREFIX = 'search:';

const kv = openKv({
  backend: 'memory',
  trackKeys: true,
});

export async function setup() {
  await kv.clear();

  await kv.setMany({
    'search:0001': {
      query: 'laptop',
    },
    'search:0002': {
      query: 'headphones',
    },
    'search:0003': {
      query: 'monitor',
    },
  });
}

export default async function () {
  const entry = await kv.nextCircular({
    prefix: PREFIX,
  });

  if (entry === null) {
    throw new Error('search dataset is empty');
  }

  console.log(`circular key=${entry.key}, query=${entry.value.query}`);

  // Simulate using reusable data.
  sleep(0.05);
}

export function teardown() {
  kv.close();
}
