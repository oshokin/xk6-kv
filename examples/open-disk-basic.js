import { openKv } from "k6/x/kv";

// Default disk backend:
// - path: "./.k6.kv"
// - serialization: "json"
// - trackKeys: false
// - disk options: bbolt defaults (1s lock timeout, fsync on)
const kv = openKv({ backend: "disk" });

export default function () {
  kv.set("foo", "bar");
  const v = kv.get("foo");
  console.log(`disk-basic value: ${v}`);
}

