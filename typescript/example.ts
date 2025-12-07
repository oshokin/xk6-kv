// TypeScript usage example for xk6-kv.
// Demonstrates all methods and their variations.

import { openKv } from 'k6/x/kv';

// Open store in init context (outside default function).
const kv = openKv({
  backend: 'memory',        // Use 'disk' for persistent storage.
  serialization: 'json',    // Or 'string' for raw bytes.
  trackKeys: true,          // Enable O(1) randomKey() performance.
  memory: { shardCount: 0 } // Set to 1-65536 for manual control.
});

export default async function () {
  // Set values (creates or overwrites).
  await kv.set('counter', 0);
  await kv.set('user:1', { name: 'Alice', age: 30 });
  await kv.set('user:2', { name: 'Bob', age: 25 });
  await kv.set('config', { theme: 'dark', lang: 'en' });

  // Get values (throws if key doesn't exist).
  const counter = await kv.get('counter');
  const user1 = await kv.get('user:1');
  console.log(`Counter: ${counter}, User: ${user1.name}`);

  // Check if key exists.
  if (await kv.exists('user:1')) {
    console.log('User 1 exists');
  }

  // Delete key (always returns true).
  await kv.delete('user:2');

  // Get store size.
  const size = await kv.size();
  console.log(`Store has ${size} keys`);

  // Increment counter (creates if missing, treats as 0).
  const newCount = await kv.incrementBy('counter', 5);
  console.log(`Counter after +5: ${newCount}`);

  // Decrement with negative delta.
  await kv.incrementBy('counter', -2);

  // Get existing value or set default.
  const { value: cfg, loaded } = await kv.getOrSet('settings', { vol: 100 });
  if (loaded) {
    console.log('Settings already existed');
  } else {
    console.log('Settings just created');
  }

  // Swap value and get previous.
  const { previous, loaded: wasPresent } = await kv.swap('status', 'running');
  console.log(`Previous status: ${previous}, existed: ${wasPresent}`);

  // Compare-and-swap (CAS) - building block for locks.
  const casSuccess = await kv.compareAndSwap('lock', null, 'vu1');
  if (casSuccess) {
    console.log('Lock acquired!');
    await kv.compareAndSwap('lock', 'vu1', null);
  }

  // Delete only if exists (more informative than delete).
  const deleted = await kv.deleteIfExists('temp');
  console.log(`Temp deleted: ${deleted}`);

  // Compare-and-delete (conditional delete).
  await kv.set('job:123', 'failed');
  const cadSuccess = await kv.compareAndDelete('job:123', 'failed');
  if (cadSuccess) {
    console.log('Failed job removed');
  }

  // List all entries (or filtered by prefix/limit).
  const allUsers = await kv.list({ prefix: 'user:' });
  console.log(`Found ${allUsers.length} users`);
  for (const entry of allUsers) {
    console.log(`${entry.key} = ${JSON.stringify(entry.value)}`);
  }

  // List with limit.
  const first10 = await kv.list({ limit: 10 });

  // Scan with cursor-based pagination (for large datasets).
  let cursor = '';
  let pageNum = 1;
  while (true) {
    const { entries, cursor: nextCursor, done } = await kv.scan({
      prefix: 'user:',
      limit: 100,
      cursor
    });

    console.log(`Page ${pageNum}: ${entries.length} entries`);

    if (done) break;
    cursor = nextCursor;
    pageNum++;
  }

  // Random key selection.
  const anyKey = await kv.randomKey();
  if (anyKey) {
    console.log(`Random key: ${anyKey}`);
  }

  // Random key with prefix filter.
  const randomUser = await kv.randomKey({ prefix: 'user:' });
  if (randomUser) {
    console.log(`Random user: ${randomUser}`);
  }

  // Rebuild key index (only useful if trackKeys=true).
  // Typically called in setup() after external DB changes.
  const rebuilt = await kv.rebuildKeyList();
  console.log(`Key list rebuilt: ${rebuilt}`);

  // Clear all entries (for test isolation).
  await kv.clear();
}

export function teardown() {
  // Close store and release resources (synchronous).
  // Safe to call multiple times, reference-counted.
  kv.close();
}
