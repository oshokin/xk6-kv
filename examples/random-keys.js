// Random batch selection of keys with optional prefix filtering.
// The function returns keys only; use getMany() to hydrate values.
//
// Covered methods: clear, setMany, randomKeys, getMany.

import { openKv } from "k6/x/kv";
import { check } from "k6";
import { expect } from "https://jslib.k6.io/k6-testing/0.5.0/index.js";

const kv = openKv({
  backend: "memory",
  serialization: "json",
  trackKeys: true,
});

export async function setup() {
  await kv.clear();
  await kv.setMany({
    "user:1": { id: 1, name: "Alice" },
    "user:2": { id: 2, name: "Bob" },
    "user:3": { id: 3, name: "Carol" },
    "order:1": { id: 10 },
  });
}

export default async function () {
  const keys = await kv.randomKeys({
    prefix: "user:",
    count: 2,
  });

  check(keys, {
    "randomKeys returns requested count": (result) => result.length === 2,
    "randomKeys returns matching prefix": (result) =>
      result.every((key) => key.startsWith("user:")),
  });

  expect(keys[0]).not.toEqual(keys[1]);

  const users = await kv.getMany(keys);
  check(users, {
    "getMany hydrates randomKeys": (result) =>
      Array.isArray(result) &&
      result.length === keys.length &&
      result.every((item) => item && item.exists === true),
  });
}
