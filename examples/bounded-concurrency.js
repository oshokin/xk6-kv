// Bounded concurrency pattern for large async KV write batches.
//
// Covered methods: clear, set, get.

import { openKv } from "k6/x/kv";
import { check } from "k6";

const kv = openKv({
  backend: "memory",
  serialization: "json",
});

const RECORDS = parseInt(__ENV.RECORDS || "500", 10);
const CONCURRENCY = parseInt(__ENV.CONCURRENCY || "32", 10);
const PREFIX = __ENV.PREFIX || "seed:";

async function mapLimit(items, limit, fn) {
  const width = Math.max(1, Math.min(limit, items.length || 1));
  const workers = Array.from({ length: width }, async (_, worker) => {
    for (let i = worker; i < items.length; i += width) {
      await fn(items[i], i);
    }
  });

  await Promise.all(workers);
}

function buildRecords() {
  return Array.from({ length: RECORDS }, (_, idx) => ({
    key: `${PREFIX}${idx}`,
    value: {
      index: idx,
      email: `user-${idx}@example.com`,
      active: idx % 2 === 0,
    },
  }));
}

export async function setup() {
  await kv.clear();
}

export default async function () {
  const records = buildRecords();

  await mapLimit(records, CONCURRENCY, async (entry) => {
    await kv.set(entry.key, entry.value);
  });

  const first = await kv.get(`${PREFIX}0`);
  const last = await kv.get(`${PREFIX}${RECORDS - 1}`);

  check({ first, last }, {
    "bounded writes persisted first key": (result) => result.first.index === 0,
    "bounded writes persisted last key": (result) => result.last.index === RECORDS - 1,
  });
}

export function teardown() {
  kv.close();
}
