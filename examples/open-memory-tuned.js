import { openKv } from "k6/x/kv";

// Memory backend with explicit tuning.
// - shardCount: 32 (auto if <= 0; capped at 65536)
// - trackKeys: true for O(1) randomKey() without prefix
const kv = openKv({
  backend: "memory",
  memory: { shardCount: 32 },
  trackKeys: true,
});

export default async function () {
  await kv.set("counter", 0);
  const nextValue = await kv.incrementBy("counter", 1);
  console.log(`memory-tuned counter=${nextValue}`);
}
