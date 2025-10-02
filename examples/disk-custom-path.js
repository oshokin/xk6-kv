// Demonstrates configuring a custom file path for the persistent disk backend.
//
// Methods covered: clear, set, get, close.

import { openKv } from "k6/x/kv";
import { expect } from "https://jslib.k6.io/k6-testing/0.5.0/index.js";

// Choose a deterministic location for the BoltDB file. In real tests you might drive this from an env var.
const databasePath = `${__ENV.XK6_KV_PATH ?? "./custom-database.kv"}`;

const kv = openKv({
  backend: "disk",
  path: databasePath,
});

export async function setup() {
  // Clear the store so the example always starts from a clean slate.
  await kv.clear();
}

export default async function () {
  await kv.set("custom-path:key", "stored-on-disk");

  const value = await kv.get("custom-path:key");
  expect(value).toEqual("stored-on-disk");
}

export function teardown() {
  // Always close disk stores to release file handles.
  kv.close();
}
