import { openKv } from "k6/x/kv";

const USER_PREFIX = "user:";
const USER_POOL_SIZE = 10;

const kv = openKv({
  backend: "memory",
  trackKeys: true,
});

export async function setup() {
  await kv.clear();

  for (let i = 0; i < USER_POOL_SIZE; i += 1) {
    await kv.set(`${USER_PREFIX}${i}`, {
      id: i,
      username: `user-${i}`,
    });
  }
}

export default async function () {
  const entry = await kv.popRandom({ prefix: USER_PREFIX });
  if (entry === null) {
    console.log("no users left in the pool");
    return;
  }

  console.log(`allocated unique user: ${entry.key} -> ${JSON.stringify(entry.value)}`);
}

export function teardown() {
  kv.close();
}

