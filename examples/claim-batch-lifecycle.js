// Batch claim lifecycle helpers example.
//
// Covered methods: clear, setMany, claimRandomMany, renewClaims, completeClaims, releaseClaims.
// Note: releaseClaims/completeClaims/renewClaims are sequential lifecycle helpers that
// return partial success summaries; they are not cross-claim transactions.
// Technical errors reject the promise and may occur after earlier batch items were applied.

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
    "users:1": { id: 1 },
    "users:2": { id: 2 },
    "users:3": { id: 3 },
    "users:4": { id: 4 },
  });
}

export default async function () {
  const claims = await kv.claimRandomMany({
    prefix: "users:",
    count: 3,
    owner: "example:claim-batch-lifecycle",
    ttl: 30000,
  });

  if (claims.length === 0) {
    return;
  }

  const renewResult = await kv.renewClaims(claims, { ttl: 45000 });
  const claimsToComplete = claims.slice(0, 2);
  const claimsToRelease = claims.slice(2);

  const completeResult = await kv.completeClaims(claimsToComplete, { deleteKey: false });
  const releaseResult = await kv.releaseClaims(claimsToRelease);

  check(renewResult, {
    "renewClaims renewed all active claims": (result) => result.renewed === claims.length,
  });

  check(completeResult, {
    "completeClaims completed selected claims": (result) =>
      result.completed === claimsToComplete.length && result.failed.length === 0,
  });

  check(releaseResult, {
    "releaseClaims released remaining claims": (result) =>
      result.released === claimsToRelease.length && result.failed.length === 0,
  });
}

export function teardown() {
  kv.close();
}
