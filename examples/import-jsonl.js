// importJSONL portable import example.
//
// Covered methods: importJSONL, getMany.

import { check } from "k6";
import { openKv } from "k6/x/kv";

const kv = openKv({
  backend: "memory",
  serialization: "json",
  trackKeys: true,
});

export default async function () {
  const result = await kv.importJSONL({
    fileName: "./examples/fixtures/users.jsonl",
    batchSize: 1000,
  });

  check(result, {
    "importJSONL imported records": (r) => r.imported === 2,
    "importJSONL read bytes": (r) => r.bytesRead > 0,
  });

  const items = await kv.getMany(["user:1", "user:2"]);

  check(items, {
    "importJSONL imported user:1": (rows) =>
      rows[0].exists === true && rows[0].value.name === "Alice",
    "importJSONL imported user:2": (rows) =>
      rows[1].exists === true && rows[1].value.name === "Bob",
  });
}

export function teardown() {
  kv.close();
}
