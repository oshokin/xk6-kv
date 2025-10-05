// Producer/consumer/random consumer example.
//
// Covered methods: list, get, set, delete, exists, randomKey.

import { sleep } from "k6";
import { openKv } from "k6/x/kv";
import { expect } from "https://jslib.k6.io/k6-testing/0.5.0/index.js";

export let options = {
  scenarios: {
    producerScenario: {
      executor: "shared-iterations",
      vus: 1,
      iterations: 10,
      exec: "producer",
    },
    consumerScenario: {
      executor: "shared-iterations",
      vus: 1,
      iterations: 10,
      startTime: "5s",
      exec: "consumerFunction",
    },
    randomConsumerScenario: {
      executor: "shared-iterations",
      vus: 1,
      iterations: 10,
      startTime: "2s",
      exec: "randomConsumerFunction",
    },
  },
};

// Track keys to exercise fast random sampling.
const kv = openKv({
  backend: "memory",
  trackKeys: true,
});

export async function setup() {
  await kv.clear();
  await kv.set("latest-producer-id", 0);
}

export async function producerFunction() {
  // Read current sequence, then produce next token atomically via incrementBy.
  const nextId = await kv.incrementBy("latest-producer-id", 1);
  const producedKey = `token:${nextId}`;

  await kv.set(producedKey,
    {
      createdAt: Date.now(),
      id: nextId
    });
  console.log(`[producer] produced ${producedKey}`);

  // Let's simulate a delay between producing tokens.
  sleep(1);
}

export async function consumerFunction() {
  // Discover available tokens by prefix, then consume the lexicographically-first.
  const entries = await kv.list({ prefix: "token:" });
  if (entries.length > 0) {
    const firstTokenKey = entries[0].key;
    const tokenValue = await kv.get(firstTokenKey);
    expect(tokenValue).toBeDefined();

    await kv.delete(firstTokenKey);
    console.log(`[consumer] consumed ${firstTokenKey}`);
  } else {
    console.log("[consumer] nothing to consume right now");
  }

  // Let's simulate a delay between consuming tokens.
  sleep(1);
}

export async function randomConsumerFunction() {
  // Pick a random key (O(1) when in-memory keys are tracked).
  const ey = await kv.randomKey({ prefix: "token:" });
  if (key) {
    const tokenValue = await kv.get(key);
    expect(tokenValue).toBeDefined();

    await kv.delete(key);
    console.log(`[random-consumer] consumed ${key}`);
  } else {
    console.log("[random-consumer] no tokens available at this moment");
  }

  // Let's simulate a delay between consuming tokens.
  sleep(1);
}
