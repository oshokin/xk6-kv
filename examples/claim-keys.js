// Explicit fixture reservation example.
//
// Covered methods: clear, setMany, claimKeys, releaseClaims.

import { check } from "k6";
import { openKv } from "k6/x/kv";

const kv = openKv({
  backend: "memory",
  serialization: "json",
  trackKeys: true,
});

export const options = {
  vus: 1,
  iterations: 1,
};

export async function setup() {
  await kv.clear();
  await kv.setMany({
    "users:admin": { id: "admin", role: "admin" },
    "users:buyer": { id: "buyer", role: "buyer" },
  });
}

export default async function () {
  const result = await kv.claimKeys(["users:admin", "users:buyer"], {
    owner: "scenario:checkout",
    ttl: 60000,
    // best-effort cleanup helper; not a transaction primitive
    allOrNothing: true,
  });

  check(result, {
    "claimKeys claimed all requested fixtures": (r) =>
      r.claimed.length === 2 && r.busy.length === 0 && r.missing.length === 0,
  });

  if (result.claimed.length !== 2) {
    throw new Error(`Could not claim fixtures: ${JSON.stringify(result)}`);
  }

  const releaseResult = await kv.releaseClaims(result.claimed);
  check(releaseResult, {
    "releaseClaims cleaned claimed fixtures": (r) =>
      r.released === 2 && r.failed.length === 0,
  });
}

export function teardown() {
  kv.close();
}
