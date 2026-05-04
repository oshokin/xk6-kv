// getMany bulk-read example.
//
// Covered methods: setMany, getMany.

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
    "user:null": null,
  });

  const items = await kv.getMany([
    "user:1",
    "missing",
    "user:2",
    "user:null",
  ]);

  check(items, {
    "getMany preserves order": (result) =>
      result[0].exists === true &&
      result[0].value.name === "Alice" &&
      result[1].exists === false &&
      result[1].value === null &&
      result[2].exists === true &&
      result[2].value.name === "Bob" &&
      result[3].exists === true &&
      result[3].value === null,
  });
}

export function teardown() {
  kv.close();
}
