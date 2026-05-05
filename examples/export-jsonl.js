// exportJSONL portable export example.
//
// Covered methods: setMany, exportJSONL.

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

  const result = await kv.exportJSONL({
    fileName: "./exports/users.jsonl",
    prefix: "user:",
  });

  check(result, {
    "exportJSONL exported two users": (r) => r.exported === 2,
    "exportJSONL wrote bytes": (r) => r.bytesWritten > 0,
  });
}

export function teardown() {
  kv.close();
}
