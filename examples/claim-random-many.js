// Batch claim allocation example.
//
// Covered methods: clear, setMany, claimRandomMany, completeClaim, releaseClaim.

import { openKv } from "k6/x/kv";

const kv = openKv({
  backend: "memory",
  trackKeys: true,
});

const USER_PREFIX = "user:";

export async function setup() {
  await kv.clear();
  await kv.setMany({
    "user:1": { id: 1, name: "Alice" },
    "user:2": { id: 2, name: "Bob" },
    "user:3": { id: 3, name: "Carol" },
    "user:4": { id: 4, name: "Dan" },
  });
}

export default async function () {
  const claims = await kv.claimRandomMany({
    prefix: USER_PREFIX,
    count: 2,
    owner: "example-worker",
    ttl: 30000,
  });

  if (claims.length === 0) {
    console.log("no free users available");
    return;
  }

  try {
    for (const claim of claims) {
      console.log(`claimed ${claim.key} token=${claim.token}`);
      await kv.completeClaim(claim, { deleteKey: false });
    }
  } catch (err) {
    for (const claim of claims) {
      await kv.releaseClaim(claim);
    }
    throw err;
  }
}

export function teardown() {
  kv.close();
}
