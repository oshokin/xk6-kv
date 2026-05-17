// importCSV streaming import example.
//
// Covered methods: importCSV, getMany.

import { check } from "k6";
import { openKv } from "k6/x/kv";

const kv = openKv({
  backend: "memory",
  serialization: "json",
  trackKeys: true,
});

export default async function () {
  const result = await kv.importCSV({
    fileName: "./examples/fixtures/users.csv",
    keyColumn: "id",
    hasHeader: true,
    batchSize: 1000,
  });

  check(result, {
    "importCSV imported rows": (r) => r.imported === 2,
    "importCSV read bytes": (r) => r.bytesRead > 0,
  });

  const items = await kv.getMany(["user:1", "user:2"]);
  check(items, {
    "importCSV imported user:1": (rows) =>
      rows[0].exists === true && rows[0].value.name === "Alice",
    "importCSV imported user:2": (rows) =>
      rows[1].exists === true && rows[1].value.name === "Bob",
  });
}

export function teardown() {
  kv.close();
}
