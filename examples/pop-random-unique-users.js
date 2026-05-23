// One-time user allocation example with popRandom.
//
// Covered methods: clear, set, popRandom.
// Each successful pop removes the key so the same user is not allocated twice.

import { openKv } from "k6/x/kv";

const USER_PREFIX = "user:";
const USER_POOL_SIZE = 10;

// Track keys for fast random selection.
const kv = openKv({
  backend: "memory",
  trackKeys: true,
});

export async function setup() {
  // Seed a reusable pool of unique users.
  await kv.clear();

  for (let i = 0; i < USER_POOL_SIZE; i += 1) {
    await kv.set(`${USER_PREFIX}${i}`, {
      id: i,
      username: `user-${i}`,
    });
  }
}

export default async function () {
  // popRandom returns null after the pool is exhausted.
  const entry = await kv.popRandom({ prefix: USER_PREFIX });
  if (entry === null) {
    console.log("no users left in the pool");
    return;
  }

  console.log(`allocated unique user: ${entry.key} -> ${JSON.stringify(entry.value)}`);
}

export function teardown() {
  kv.close();
}

