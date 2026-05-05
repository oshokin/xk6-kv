// listKeys key-only listing example.
//
// Covered methods: setMany, listKeys, getMany.

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
    "order:1": { total: 42 },
  });

  const keys = await kv.listKeys({
    prefix: "user:",
    limit: 100,
  });

  check(keys, {
    "listKeys returns only matching keys": (result) =>
      result.length === 2 &&
      result[0] === "user:1" &&
      result[1] === "user:2",
  });

  const users = await kv.getMany(keys);

  check(users, {
    "listKeys result can feed getMany": (items) =>
      items.length === 2 &&
      items[0].exists === true &&
      items[1].exists === true,
  });
}

export function teardown() {
  kv.close();
}
