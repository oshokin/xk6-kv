// Prefix counting example for lightweight cardinality checks.
//
// Covered methods: clear, set, count.
// Compares count({ prefix }) against count() without materializing entries.

import { check } from "k6";
import { openKv } from "k6/x/kv";

const kv = openKv({
  backend: "memory",
  trackKeys: true
});

export async function setup() {
  // Seed two user keys and one session key.
  await kv.clear();

  await kv.set("user:1", { name: "Alice" });
  await kv.set("user:2", { name: "Bob" });
  await kv.set("session:1", { active: true });
}

export default async function () {
  // Compare prefix-scoped counts with total key count.
  const usersCount = await kv.count({ prefix: "user:" });
  const sessionsCount = await kv.count({ prefix: "session:" });
  const totalCount = await kv.count();

  check(usersCount, {
    "count user prefix": (value) => value === 2
  });

  check(sessionsCount, {
    "count session prefix": (value) => value === 1
  });

  check(totalCount, {
    "count all keys": (value) => value === 3
  });
}

export function teardown() {
  kv.close();
}
