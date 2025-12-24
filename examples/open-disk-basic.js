import { openKv } from "k6/x/kv";

// Default disk backend:
// - path: "./.k6.kv"
// - serialization: "json"
// - trackKeys: false
// - disk options: bbolt defaults (1s lock timeout, fsync on)
const kv = openKv({ backend: "disk" });

export default async function () {
  await kv.set("foo", "bar");
  const value = await kv.get("foo");
  console.log(`disk-basic value: ${value}`);
}

export async function teardown() {
  kv.close();
}
