// Batch destructive allocation example with popRandomMany.
//
// Covered methods: clear, setMany, popRandomMany.

import { openKv } from "k6/x/kv";

const kv = openKv({
  backend: "memory",
  trackKeys: true,
});

const JOB_PREFIX = "job:";

export async function setup() {
  await kv.clear();
  await kv.setMany({
    "job:1": { id: 1, payload: "alpha" },
    "job:2": { id: 2, payload: "beta" },
    "job:3": { id: 3, payload: "gamma" },
    "job:4": { id: 4, payload: "delta" },
  });
}

export default async function () {
  const popped = await kv.popRandomMany({
    prefix: JOB_PREFIX,
    count: 2,
  });

  if (popped.length === 0) {
    console.log("queue empty");
    return;
  }

  for (const entry of popped) {
    console.log(`popped ${entry.key}: ${JSON.stringify(entry.value)}`);
  }
}

export function teardown() {
  kv.close();
}
