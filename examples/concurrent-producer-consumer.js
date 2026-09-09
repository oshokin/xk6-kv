// Concurrent producer/ordered-consumer example.
//
// Demonstrates an atomic recoverable work queue:
//
// producer:
//   incrementBy -> set
//
// multiple consumers:
//   claimNext -> process -> complete(deleteKey=true)
//
// processing failure:
//   releaseClaim
//
// Unlike list -> get -> delete, claimNext prevents two concurrent consumers
// from leasing the same live work item.

import { sleep } from "k6";
import exec from "k6/execution";
import { openKv } from "k6/x/kv";

export const options = {
  scenarios: {
    producerScenario: {
      executor: "shared-iterations",
      vus: 1,
      iterations: 20,
      exec: "producerFunction",
    },
    consumerScenario: {
      executor: "shared-iterations",
      vus: 4,
      iterations: 80,
      startTime: "500ms",
      exec: "consumerFunction",
    },
  },
};

const TOKEN_PREFIX = "token:";
const CLAIM_TTL_MS = 5_000;

const kv = openKv({
  backend: "memory",
  trackKeys: true,
});

export async function setup() {
  await kv.clear();
  await kv.set("latest-producer-id", 0);
}

export async function producerFunction() {
  const nextId = await kv.incrementBy("latest-producer-id", 1);

  // Zero-padding makes lexicographic order match numeric sequence order.
  const producedKey = `${TOKEN_PREFIX}${String(nextId).padStart(6, "0")}`;

  await kv.set(producedKey, {
    createdAt: Date.now(),
    id: nextId,
  });

  console.log(`[producer] produced ${producedKey}`);

  // Simulate work arriving over time.
  sleep(0.05);
}

export async function consumerFunction() {
  const owner = `consumer:vu:${exec.vu.idInInstance}`;

  const claim = await kv.claimNext({
    prefix: TOKEN_PREFIX,
    owner,
    ttl: CLAIM_TTL_MS,
  });

  if (claim === null) {
    console.log(`[consumer ${owner}] nothing available right now`);
    sleep(0.05);
    return;
  }

  let completed = false;

  try {
    const token = claim.entry.value;

    if (token === null || typeof token !== "object" || typeof token.id !== "number") {
      throw new Error(`invalid token payload for ${claim.key}`);
    }

    // Simulate processing.
    sleep(0.02);

    completed = await kv.completeClaim(claim, { deleteKey: true });
    if (!completed) {
      throw new Error(`claim ${claim.id} expired or became stale before completion`);
    }

    console.log(`[consumer ${owner}] consumed ${claim.key}`);
  } catch (err) {
    // If completion did not happen, return the work item to the queue
    // best-effort so another consumer can retry it.
    if (!completed) {
      const released = await kv.releaseClaim(claim);
      if (!released) {
        console.warn(`[consumer ${owner}] failed to release stale claim ${claim.id}`);
      }
    }

    throw err;
  }
}

export function teardown() {
  kv.close();
}
