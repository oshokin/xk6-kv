// exportCSV portable export example.
//
// Covered methods: clear, setMany, exportCSV.
//
// exportCSV contract:
// - exports tabular object rows only;
// - each value must be a JSON object;
// - only top-level scalar fields are supported;
// - scalar stored row values are rejected;
// - nested object/array fields are rejected;
// - CSV cells are text, so scalar JSON types are stringified on roundtrip;
// - use exportJSONL for scalar rows or nested values.

import { check } from "k6";
import { openKv } from "k6/x/kv";

const kv = openKv({
  backend: "disk",
  path: "./.export-csv.kv",
  serialization: "json",
  trackKeys: true,
});

export const options = {
  vus: 1,
  iterations: 1,
};

export async function setup() {
  await kv.clear();
  await kv.setMany({
    // Good shape for exportCSV: object rows with top-level scalar fields.
    "responses:1": { status: 200, requestId: "r1", userId: "u1", bodyHash: "abc" },
    "responses:2": { status: 201, requestId: "r2", userId: "u2", bodyHash: "def" },
    "other:1": { status: 500, requestId: "r3", userId: "u3", bodyHash: "ghi" },
  });

  // Negative-shape examples (intentionally not executed in this script):
  // await kv.set("bad:1", { status: 200, body: { id: 123 } }); // nested object -> exportCSV rejects
  // await kv.set("bad:2", "raw body"); // scalar value -> exportCSV rejects
  // Use exportJSONL() for nested/scalar payloads.
}

export default async function () {}

export async function teardown() {
  const result = await kv.exportCSV({
    fileName: "./responses.csv",
    prefix: "responses:",
    columns: ["status", "requestId", "userId", "bodyHash"],
    includeKey: true,
  });

  check(result, {
    "exportCSV exported two response records": (r) => r.exported === 2,
    "exportCSV wrote bytes": (r) => r.bytesWritten > 0,
  });

  console.log(JSON.stringify(result));
  kv.close();
}
