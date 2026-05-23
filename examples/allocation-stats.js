// allocationStats prefix diagnostics example.
//
// Covered methods: clear, setMany, claimRandomMany, releaseClaim, allocationStats.

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
    "users:1": { id: 1, role: "admin" },
    "users:2": { id: 2, role: "buyer" },
    "users:3": { id: 3, role: "buyer" },
    "users:4": { id: 4, role: "buyer" },
    "users:5": { id: 5, role: "support" },
  });
}

export default async function () {
  const claims = await kv.claimRandomMany({
    prefix: "users:",
    count: 2,
    owner: "example:allocation-stats",
    ttl: 60000,
  });

  const beforeRelease = await kv.allocationStats({ prefix: "users:" });
  console.log(`before release: ${JSON.stringify(beforeRelease)}`);

  if (claims[0]) {
    await kv.releaseClaim(claims[0]);
  }

  const afterRelease = await kv.allocationStats({ prefix: "users:" });
  console.log(`after release: ${JSON.stringify(afterRelease)}`);

  check(beforeRelease, {
    "allocationStats total users": (stats) => stats.total === 5,
    "allocationStats claimed live before release": (stats) => stats.claimedLive >= 1,
  });

  check(afterRelease, {
    "allocationStats claimable increases after release": (stats) =>
      stats.claimable >= beforeRelease.claimable,
  });

  for (let i = 1; i < claims.length; i += 1) {
    await kv.releaseClaim(claims[i]);
  }
}

export function teardown() {
  kv.close();
}
