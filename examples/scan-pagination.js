// Demonstrates cursor-based pagination via kv.scan().
// Useful when listing thousands of keys would exceed test memory.
//
// Covered method: scan.

import { check } from "k6";
import { openKv } from "k6/x/kv";

const kv = openKv({ backend: "memory" });

export async function setup() {
  await kv.clear();

  for (let i = 0; i < 200; i += 1) {
    const padded = String(i).padStart(4, "0");
    await kv.set(`session:${padded}`, { userId: `user-${i}` });
  }
}

export default async function () {
  let cursor = "";
  let total = 0;

  while (true) {
    const { entries, cursor: nextCursor, done } = await kv.scan({
      prefix: "session:",
      limit: 25,
      cursor
    });

    total += entries.length;

    if (done) {
      break;
    }

    cursor = nextCursor;
  }

  check(total, { "scanned all sessions": (count) => count === 200 });
}
