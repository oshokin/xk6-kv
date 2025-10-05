// Shows how to use getOrSet for "compute-if-absent" semantics and swap for
// "replace returning previous value" semantics.
//
// Covered methods: getOrSet, swap.

import { openKv } from "k6/x/kv";
import { expect } from "https://jslib.k6.io/k6-testing/0.5.0/index.js";

const kv = openKv({ backend: "memory" });

export async function setup() {
  await kv.clear();
}

export default async function () {
  // getOrSet on a missing key stores the provided value and returns loaded=false.
  const firstConfig = await kv.getOrSet("app:config", { theme: "dark", retries: 3 });
  expect(firstConfig.loaded).toEqual(false);
  expect(firstConfig.value).toEqual({ theme: "dark", retries: 3 });

  // getOrSet on an existing key returns the existing value and loaded=true; it does not overwrite.
  const secondConfig = await kv.getOrSet("app:config", { theme: "light" });
  expect(secondConfig.loaded).toEqual(true);
  expect(secondConfig.value).toEqual({ theme: "dark", retries: 3 });

  // swap replaces the value and returns { previous, loaded } where "loaded" indicates whether it existed.
  const firstSwap = await kv.swap("system:status", "initializing");
  expect(firstSwap.loaded).toEqual(false);
  expect(firstSwap.previous).toBeNull(); // previous is explicitly null when absent. :contentReference[oaicite:4]{index=4}

  const secondSwap = await kv.swap("system:status", "running");
  expect(secondSwap.loaded).toEqual(true);
  expect(secondSwap.previous).toEqual("initializing");
}
