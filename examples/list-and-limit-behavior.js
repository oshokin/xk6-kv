// A focused example for list() behavior: sorted output, optional prefix filter, and hard limit.

import { openKv } from "k6/x/kv";
import { expect } from "https://jslib.k6.io/k6-testing/0.5.0/index.js";

const kv = openKv({ backend: "memory" });

export async function setup() {
  await kv.clear();
  await kv.set("a:1", "A1");
  await kv.set("a:2", "A2");
  await kv.set("b:1", "B1");
}

export default async function () {
  // Full list is lexicographically sorted by key.
  const fullList = await kv.list();
  expect(fullList.map(e => e.key)).toEqual(["a:1", "a:2", "b:1"]);

  // Prefix filter.
  const aOnly = await kv.list({ prefix: "a:" });
  expect(aOnly.map(e => e.key)).toEqual(["a:1", "a:2"]);

  // Limit reduces the number of returned entries from the beginning of the sorted slice.
  const limitedToOne = await kv.list({ limit: 1 });
  expect(limitedToOne).toHaveLength(1);
  expect(limitedToOne[0].key).toEqual("a:1");
}
