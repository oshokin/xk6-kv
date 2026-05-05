// deleteMany bulk-delete example.
//
// Covered methods: setMany, deleteMany, getMany.

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
  });

  const result = await kv.deleteMany([
    "tmp:1",
    "tmp:2",
    "tmp:missing",
  ]);

  check(result, {
    "deleteMany deleted existing keys": (r) => r.deleted === 2,
    "deleteMany counted missing keys": (r) => r.missing === 1,
  });

  const items = await kv.getMany(["tmp:1", "tmp:2"]);

  check(items, {
    "deleteMany removed keys": (values) =>
      values[0].exists === false &&
      values[1].exists === false,
  });
}

export function teardown() {
  kv.close();
}
