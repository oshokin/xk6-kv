// Demonstrates cursor-based key pagination via kv.scanKeys().
// Prefer bounded scanKeys({ limit }) pages over listKeys() for large keyspaces.
//
// Covered methods: setMany, scanKeys, getMany.

import { check } from "k6";
import { openKv } from "k6/x/kv";

const kv = openKv({
  backend: "memory",
  serialization: "json",
  trackKeys: true,
});

export default async function () {
  await kv.setMany({
    "user:1": { name: "Alice" },
    "user:2": { name: "Bob" },
    "user:3": { name: "Carol" },
    "order:1": { total: 42 },
  });

  let cursor = "";
  const allKeys = [];

  do {
    const page = await kv.scanKeys({
      prefix: "user:",
      cursor,
      limit: 2,
    });

    allKeys.push(...page.keys);

    const users = await kv.getMany(page.keys);
    console.log(JSON.stringify({ keys: page.keys, users }));

    cursor = page.cursor;
  } while (cursor !== "");

  check(allKeys, {
    "scanKeys returned all matching keys": (keys) =>
      JSON.stringify(keys) === JSON.stringify(["user:1", "user:2", "user:3"]),
  });
}

export function teardown() {
  kv.close();
}
