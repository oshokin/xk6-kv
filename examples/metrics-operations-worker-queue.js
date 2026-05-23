// Worker queue example focused on operation metrics.
//
// Covered methods: clear, set, claimRandom, releaseClaim.
// Enable `metrics.operations` so each async KV call emits `xk6_kv_*` operation samples.

import { check } from "k6";
import { openKv } from "k6/x/kv";

const TASK_PREFIX = "ops-metrics:task:";
const TASK_COUNT = 20;

// Use memory trackKeys for fast random claiming and enable operation metric emission.
const kv = openKv({
  backend: "memory",
  trackKeys: true,
  metrics: {
    operations: true,
  },
});

export const options = {
  vus: 2,
  iterations: 20,
};

export async function setup() {
  // Start from a deterministic queue state.
  await kv.clear();

  for (let i = 0; i < TASK_COUNT; i += 1) {
    await kv.set(`${TASK_PREFIX}${i + 1}`, {
      id: i + 1,
      type: "email",
      state: "queued",
    });
  }
}

export default async function () {
  // Claim one queued task per iteration.
  const claim = await kv.claimRandom({
    prefix: TASK_PREFIX,
    owner: `vu:${__VU}`,
    ttl: 10000,
  });

  check(Boolean(claim), {
    "queue:claimed": () => Boolean(claim),
  });

  if (claim) {
    // Release it so the pool can be reused in subsequent iterations.
    const released = await kv.releaseClaim(claim);
    check(released, {
      "queue:released": () => released,
    });
  }
}

export function teardown() {
  kv.close();
}
