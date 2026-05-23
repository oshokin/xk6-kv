// State gauge example using stats/reportStats observability helpers.
//
// Covered methods: clear, set, incrementBy, reportStats, stats.
// Runs with trackKeys enabled so index health checks are meaningful.

import { check } from "k6";
import { openKv } from "k6/x/kv";

// Track keys to include index stats in reportStats()/stats() output.
const kv = openKv({
  backend: "memory",
  trackKeys: true,
});

export const options = {
  vus: 1,
  iterations: 5,
};

export async function setup() {
  // Seed baseline state so emitted gauges are non-empty from the first iteration.
  await kv.clear();
  await kv.set("stats:user:1", { name: "Alice" });
  await kv.set("stats:user:2", { name: "Bob" });
}

export default async function () {
  // Mutate a heartbeat key, then emit state gauges.
  await kv.incrementBy("stats:heartbeat", 1);
  await kv.reportStats();

  // Fetch an explicit snapshot for local assertions/debug output.
  const snapshot = await kv.stats();
  check(snapshot.count > 0, {
    "stats:count-positive": () => snapshot.count > 0,
  });

  console.log(
    `stats snapshot -> keys=${snapshot.count}, claimsLive=${snapshot.claims.live}, trackKeys=${snapshot.trackKeys}`
  );
}

export function teardown() {
  kv.close();
}
