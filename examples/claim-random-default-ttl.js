// Claim lease example with default TTL behavior.
//
// Covered methods: clear, set, claimRandom, completeClaim, releaseClaim.
// Omits ttlMs intentionally to demonstrate the built-in 30000ms default.

import { openKv } from "k6/x/kv";

const DEFAULT_TTL_MS = 30000;

// Track keys so random claiming is fast and deterministic for the demo.
const kv = openKv({
  backend: "memory",
  trackKeys: true,
});

export async function setup() {
  // Seed a small pool of claimable users.
  await kv.clear();

  await kv.set("user:1", { id: 1, name: "Alice" });
  await kv.set("user:2", { id: 2, name: "Bob" });
  await kv.set("user:3", { id: 3, name: "Carol" });
}

export default async function () {
  // TTL omitted on purpose; claimRandom default is 30000ms.
  const claim = await kv.claimRandom({ prefix: "user:" });
  if (claim === null) {
    return;
  }

  try {
    const leaseRemainingMs = claim.expiresAt - Date.now();
    console.log(
      `claimed=${claim.key} token=${claim.token} leaseRemainingMs=${leaseRemainingMs} expectedDefault=${DEFAULT_TTL_MS}`
    );

    // Success path: complete the claim.
    await kv.completeClaim(claim, { deleteKey: false });
  } catch (err) {
    // Failure path: release the claim so another worker can take it.
    await kv.releaseClaim(claim);
    throw err;
  }
}

export function teardown() {
  // Close once after the run.
  kv.close();
}

