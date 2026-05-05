// deleteByPrefix bounded prefix-delete example.
//
// Covered methods: setMany, listKeys, deleteByPrefix.

import { check } from "k6";
import { openKv } from "k6/x/kv";

const kv = openKv({
  backend: "memory",
  serialization: "json",
  trackKeys: true,
});

export default async function () {
  await kv.setMany({
    "tmp:1": { value: 1 },
    "tmp:2": { value: 2 },
    "tmp:3": { value: 3 },
    "user:1": { name: "Alice" },
  });

  const preview = await kv.listKeys({
    prefix: "tmp:",
    limit: 10,
  });

  check(preview, {
    "deleteByPrefix preview sees tmp keys": (keys) => keys.length === 3,
  });

  const first = await kv.deleteByPrefix({
    prefix: "tmp:",
    limit: 2,
  });

  check(first, {
    "deleteByPrefix deletes limited batch": (result) =>
      result.deleted === 2 && result.done === false,
  });

  const second = await kv.deleteByPrefix({
    prefix: "tmp:",
    limit: 2,
  });

  check(second, {
    "deleteByPrefix finishes remaining keys": (result) =>
      result.deleted === 1 && result.done === true,
  });

  const remaining = await kv.listKeys({
    prefix: "tmp:",
  });

  check(remaining, {
    "deleteByPrefix removed all tmp keys": (keys) => keys.length === 0,
  });
}

export function teardown() {
  kv.close();
}
