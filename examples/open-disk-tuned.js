import { openKv } from "k6/x/kv";

// Disk backend with explicit bbolt tuning.
// Adjust for your durability/performance needs; everything here is optional.
const kv = openKv({
  backend: "disk",
  path: "./data/custom.kv",
  disk: {
    timeout: "2s",       // number=ms, string=Go duration (e.g. "1s", "500ms")
    noSync: false,       // keep fsync on commit (set true only for ephemeral runs)
    noGrowSync: false,
    noFreelistSync: false,
    preLoadFreelist: false,
    freelistType: "array", // or "map" for large/fragmented DBs
    readOnly: false,
    initialMmapSize: "64MB", // number=bytes; string supports SI ("MB") and IEC ("MiB"); 0 keeps default/no preallocation
    mlock: false,
  },
});

export default async function () {
  await kv.set("user:1", { name: "Alice" });
  const user = await kv.get("user:1");
  console.log(`disk-tuned user: ${JSON.stringify(user)}`);
}

export async function teardown() {
  kv.close();
}
