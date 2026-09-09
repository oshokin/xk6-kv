// Sticky user allocation with claimForOwner().
//
// Covered methods: claimForOwner, renewClaim.
// The same exact (prefix, owner) pair gets the same live claim.

import exec from "k6/execution";
import { openKv } from "k6/x/kv";

const store = openKv({
  backend: "memory",
  trackKeys: true,
});

export async function setup() {
  await store.clear();
  await store.setMany({
    "users:1": { username: "alice", password: "alice-password" },
    "users:2": { username: "bob", password: "bob-password" },
    "users:3": { username: "carol", password: "carol-password" },
  });
}

function owner() {
  return `${exec.scenario.name}:vu:${exec.vu.idInInstance}`;
}

export default async function () {
  const sticky = await store.claimForOwner({
    prefix: "users:",
    owner: owner(),
    ttl: 5 * 60_000,
  });

  if (sticky === null) {
    throw new Error(`No free user available for ${owner()}`);
  }

  // claimForOwner() itself never renews a lease.
  // Renew explicitly if your test can exceed the original lease window.
  if (sticky.expiresAt - Date.now() < 60_000) {
    const renewed = await store.renewClaim(sticky, {
      ttl: 5 * 60_000,
    });

    if (!renewed) {
      throw new Error("Sticky claim expired before renewal");
    }
  }

  const credentials = sticky.entry.value;
  console.log(
    `owner=${owner()} user=${credentials.username} claim=${sticky.id} key=${sticky.key}`
  );
}

export function teardown() {
  store.close();
}
