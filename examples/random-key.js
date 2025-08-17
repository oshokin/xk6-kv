import { openKv } from "k6/x/kv";
import { expect } from "https://jslib.k6.io/k6-testing/0.3.0/index.js";

const kv = openKv({
  backend: "memory",
  trackKeys: true,
});

export default async function () {
  await kv.clear();

  // Insert multiple keys
  await kv.set("a:1", "alpha");
  await kv.set("a:2", "algebra");
  await kv.set("b:1", "bravo");

  // No prefix
  const any = await kv.randomKey();
  expect(["a:1", "a:2", "b:1"]).toContain(any);

  // Prefix "a:"
  const akey = await kv.randomKey({ prefix: "a:" });
  expect(["a:1", "a:2"]).toContain(akey);

  // Empty set returns ""
  await kv.clear();
  
  const empty = await kv.randomKey({ prefix: "nope" });
  expect(empty).toEqual("");
}
