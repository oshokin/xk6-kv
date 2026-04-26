// Demonstrates kv.count(prefix) and kv.count() behavior.
// Useful for lightweight cardinality checks without materializing entries via list()/scan().

import { check } from "k6";
import { openKv } from "k6/x/kv";

const kv = openKv({
  backend: "memory",
  trackKeys: true
});

export async function setup() {
  await kv.clear();

  await kv.set("user:1", { name: "Alice" });
  await kv.set("user:2", { name: "Bob" });
  await kv.set("session:1", { active: true });
}

export default async function () {
  const usersCount = await kv.count("user:");
  const sessionsCount = await kv.count("session:");
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
