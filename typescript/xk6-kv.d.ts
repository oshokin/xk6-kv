// Type definitions for xk6-kv
// Project: https://github.com/oshokin/xk6-kv
// Definitions by: xk6-kv contributors

/**
 * xk6-kv provides a persistent key-value store for sharing state across
 * Virtual Users (VUs) during k6 load testing.
 *
 * @example
 * ```javascript
 * import { openKv } from 'k6/x/kv';
 *
 * const kv = openKv({ backend: 'memory' });
 *
 * export default async function() {
 *   await kv.set('counter', 0);
 *   const count = await kv.incrementBy('counter', 1);
 *   console.log(`Counter: ${count}`);
 * }
 * ```
 */
declare module 'k6/x/kv' {
  /**
   * Storage backend type.
   * - `"memory"`: Fast, ephemeral storage shared across VUs (lost between runs)
   * - `"disk"`: Persistent BoltDB-based storage (survives between runs)
   */
  export type Backend = 'memory' | 'disk';

  /**
   * Value serialization method.
   * - `"json"`: Values are JSON-encoded/decoded automatically (default)
   * - `"string"`: Values are stored as strings/bytes
   */
  export type Serialization = 'json' | 'string';

  /**
   * Options for opening a key-value store.
   * Must be called in the init context (outside default/setup/teardown functions).
   */
  export interface OpenKvOptions {
    /**
     * Storage backend to use.
     * @default "disk"
     */
    backend?: Backend;

    /**
     * Path to the BoltDB file (disk backend only).
     * Ignored when backend is "memory".
     * @default "./.k6.kv"
     */
    path?: string;

    /**
     * Value serialization method.
     * @default "json"
     */
    serialization?: Serialization;

    /**
     * Enable in-memory key indexing for O(1) randomKey() performance.
     * When enabled:
     * - randomKey() without prefix -> O(1)
     * - randomKey() with prefix -> O(log n)
     * When disabled:
     * - randomKey() uses two-pass scan (fine for small-to-medium sets)
     * @default false
     */
    trackKeys?: boolean;
  }

  /**
   * Options for listing entries.
   */
  export interface ListOptions {
    /**
     * Filter by key prefix. Only keys starting with this string are returned.
     */
    prefix?: string;

    /**
     * Maximum number of results to return.
     * If <= 0 or omitted, returns all matching entries.
     */
    limit?: number;
  }

  /**
   * Options for scanning entries with cursor-based pagination.
   */
  export interface ScanOptions {
    /**
     * Filter by key prefix. Only keys starting with this string are returned.
     */
    prefix?: string;

    /**
     * Maximum number of results per page.
     * If <= 0, returns all matching entries (behaves like list()).
     */
    limit?: number;

    /**
     * Opaque continuation token from the previous page.
     * - Pass empty string or omit to start a new scan.
     * - Pass the cursor from the previous ScanResult to continue.
     */
    cursor?: string;
  }

  /**
   * Options for random key selection.
   */
  export interface RandomKeyOptions {
    /**
     * Filter by key prefix. Only keys starting with this string are considered.
     */
    prefix?: string;
  }

  /**
   * Options for backing up a snapshot.
   */
  export interface BackupOptions {
    /**
     * Destination path for the Bolt snapshot file.
     * Required unless you intentionally want to overwrite the backend's live Bolt file.
     */
    fileName?: string;

    /**
     * When true, allow writes to proceed during backup (best-effort consistency).
     * When false (default), writes are blocked for strict point-in-time consistency.
     */
    allowConcurrentWrites?: boolean;
  }

  /**
   * Summary returned by backup().
   */
  export interface BackupSummary {
    /** Number of entries captured in the snapshot. */
    totalEntries: number;
    /** On-disk size of the snapshot file in bytes. */
    bytesWritten: number;
    /** True when AllowConcurrentWrites was enabled (best-effort snapshot). */
    bestEffort: boolean;
    /** Warning message describing best-effort semantics (present only when bestEffort=true). */
    warning?: string;
  }

  /**
   * Options for restoring a snapshot.
   */
  export interface RestoreOptions {
    /**
     * Path to the Bolt snapshot file produced by backup().
     * Required unless you intentionally want to restore from the backend's live Bolt file.
     */
    fileName?: string;

    /**
     * Maximum number of entries allowed during import.
     * Use to guard against unexpected snapshot sizes. <= 0 disables the guard.
     */
    maxEntries?: number;

    /**
     * Maximum total key/value payload (bytes) allowed during import.
     * <= 0 disables the guard.
     */
    maxBytes?: number;
  }

  /**
   * Summary returned by restore().
   */
  export interface RestoreSummary {
    /** Number of entries hydrated from the snapshot. */
    totalEntries: number;
  }

  /**
   * A single key-value entry.
   */
  export interface Entry {
    /** The entry key */
    key: string;
    /** The stored value (type depends on serialization) */
    value: any;
  }

  /**
   * Result of scan() operation with cursor-based pagination.
   */
  export interface ScanResult {
    /**
     * Page of key-value entries, sorted lexicographically by key.
     */
    entries: Entry[];

    /**
     * Opaque continuation token for the next page.
     * - Non-empty: more results available, pass this to next scan() call
     * - Empty: scan is complete
     */
    cursor: string;

    /**
     * True when the scan has reached the end (cursor is empty).
     */
    done: boolean;
  }

  /**
   * Result of getOrSet() operation.
   */
  export interface GetOrSetResult<T = any> {
    /**
     * The value stored at the key (either existing or newly set).
     */
    value: T;

    /**
     * True if the value was already present (get), false if it was just set.
     */
    loaded: boolean;
  }

  /**
   * Result of swap() operation.
   */
  export interface SwapResult<T = any> {
    /**
     * The previous value if key existed, null otherwise.
     */
    previous: T | null;

    /**
     * True if the key existed and previous contains its old value.
     * False if the key was created and previous is null.
     */
    loaded: boolean;
  }

  /**
   * Key-value store instance.
   * All methods are Promise-based except close() which is synchronous.
   */
  export interface KV {
    // ==================== Basic Operations ====================

    /**
     * Retrieves a value by key.
     *
     * @param key - The key to retrieve
     * @returns Promise that resolves to the stored value
     * @throws If key doesn't exist or database is not open
     *
     * @example
     * ```javascript
     * const value = await kv.get('myKey');
     * console.log(value);
     * ```
     */
    get(key: string): Promise<any>;

    /**
     * Sets a key-value pair.
     * Creates the key if absent, overwrites if present.
     *
     * @param key - The key to set
     * @param value - The value to store (must be JSON-serializable if serialization="json")
     * @returns Promise that resolves to the value that was set
     *
     * @example
     * ```javascript
     * await kv.set('user:1', { name: 'Alice', age: 30 });
     * ```
     */
    set(key: string, value: any): Promise<any>;

    /**
     * Removes a key-value pair.
     * Always resolves successfully, even if key didn't exist.
     *
     * @param key - The key to delete
     * @returns Promise that resolves to true
     *
     * @example
     * ```javascript
     * await kv.delete('tempKey');
     * ```
     */
    delete(key: string): Promise<boolean>;

    /**
     * Checks if a key exists.
     *
     * @param key - The key to check
     * @returns Promise that resolves to true if key exists, false otherwise
     *
     * @example
     * ```javascript
     * if (await kv.exists('config')) {
     *   console.log('Config exists');
     * }
     * ```
     */
    exists(key: string): Promise<boolean>;

    /**
     * Removes all entries from the store.
     *
     * Always resolves to `true` (and rejects on error) so callers can treat
     * the returned value as a simple success confirmation.
     *
     * @returns Promise that resolves to `true` when the operation completes
     *
     * @example
     * ```javascript
     * const ok = await kv.clear();
     * if (ok) {
     *   console.log("Store cleared");
     * }
     * ```
     */
    clear(): Promise<boolean>;

    /**
     * Returns the current number of keys in the store.
     *
     * @returns Promise that resolves to the number of keys
     *
     * @example
     * ```javascript
     * const size = await kv.size();
     * console.log(`Store contains ${size} keys`);
     * ```
     */
    size(): Promise<number>;

    // ==================== Atomic Operations ====================

    /**
     * Atomically increments a numeric value by delta.
     * If the key doesn't exist, it's treated as 0.
     *
     * @param key - The key to increment
     * @param delta - The amount to add (can be negative)
     * @returns Promise that resolves to the new value
     * @throws If existing value is not a number
     *
     * @example
     * ```javascript
     * await kv.incrementBy('counter', 5);  // Returns 5
     * await kv.incrementBy('counter', -2); // Returns 3
     * ```
     */
    incrementBy(key: string, delta: number): Promise<number>;

    /**
     * Atomically gets the existing value if present, or sets it if absent.
     *
     * @param key - The key to get or set
     * @param value - The value to set if key doesn't exist
     * @returns Promise that resolves to { value, loaded }
     *
     * @example
     * ```javascript
     * const { value, loaded } = await kv.getOrSet('config', { theme: 'dark' });
     * if (loaded) {
     *   console.log('Config already existed');
     * } else {
     *   console.log('Config was just created');
     * }
     * ```
     */
    getOrSet<T = any>(key: string, value: T): Promise<GetOrSetResult<T>>;

    /**
     * Atomically replaces the value at key with a new value.
     * Returns the previous value if key existed.
     *
     * @param key - The key to swap
     * @param value - The new value to set
     * @returns Promise that resolves to { previous, loaded }
     *
     * @example
     * ```javascript
     * const { previous, loaded } = await kv.swap('status', 'running');
     * if (loaded) {
     *   console.log(`Previous status: ${previous}`);
     * }
     * ```
     */
    swap<T = any>(key: string, value: T): Promise<SwapResult<T>>;

    /**
     * Atomically sets newValue only if current value equals oldValue.
     * This is the fundamental building block for implementing locks and
     * other synchronization primitives.
     *
     * Pass `null` (or leave undefined) for `oldValue` to express
     * "swap only if the key is currently absent" (set-if-not-exists semantics).
     *
     * @param key - The key to update
     * @param oldValue - The expected current value
     * @param newValue - The new value to set
     * @returns Promise that resolves to true if swap succeeded, false otherwise
     *
     * @example
     * ```javascript
     * // Implement a simple lock
     * const acquired = await kv.compareAndSwap('lock:resource', null, myId);
     * if (acquired) {
     *   // I acquired the lock
     * }
     * ```
     */
    compareAndSwap(key: string, oldValue: any, newValue: any): Promise<boolean>;

    /**
     * Deletes the key if it exists.
     * More informative than delete() which always returns true.
     *
     * @param key - The key to delete
     * @returns Promise that resolves to true if key was deleted, false if it didn't exist
     *
     * @example
     * ```javascript
     * const deleted = await kv.deleteIfExists('tempFile');
     * console.log(`File deleted: ${deleted}`);
     * ```
     */
    deleteIfExists(key: string): Promise<boolean>;

    /**
     * Atomically deletes the key only if current value equals oldValue.
     * Useful for "delete only if not processed" scenarios.
     *
     * @param key - The key to delete
     * @param oldValue - The expected current value
     * @returns Promise that resolves to true if key was deleted, false otherwise
     *
     * @example
     * ```javascript
     * const deleted = await kv.compareAndDelete('job:123', 'failed');
     * if (deleted) {
     *   console.log('Failed job removed');
     * }
     * ```
     */
    compareAndDelete(key: string, oldValue: any): Promise<boolean>;

    // ==================== Query Operations ====================

    /**
     * Streams entries using cursor-based pagination.
     * Keys are returned in lexicographic order.
     *
     * Use this when the keyspace is too large to materialize with list()
     * or when you need restart-safe pagination.
     *
     * @param options - Optional filters (prefix, limit, cursor)
     * @returns Promise that resolves to { entries, cursor, done }
     *
     * @example
     * ```javascript
     * let cursor = '';
     * while (true) {
     *   const { entries, cursor: nextCursor, done } = await kv.scan({
     *     prefix: 'user:',
     *     limit: 100,
     *     cursor
     *   });
     *
     *   for (const entry of entries) {
     *     console.log(entry.key, entry.value);
     *   }
     *
     *   if (done) break;
     *   cursor = nextCursor;
     * }
     * ```
     */
    scan(options?: ScanOptions): Promise<ScanResult>;

    /**
     * Returns key-value entries sorted lexicographically by key.
     * If limit <= 0 or omitted, returns all matching entries.
     *
     * @param options - Optional filters (prefix, limit)
     * @returns Promise that resolves to array of { key, value } objects
     *
     * @example
     * ```javascript
     * // List all entries
     * const all = await kv.list();
     *
     * // List with prefix
     * const users = await kv.list({ prefix: 'user:' });
     *
     * // List with limit
     * const first10 = await kv.list({ limit: 10 });
     * ```
     */
    list(options?: ListOptions): Promise<Entry[]>;

    /**
     * Returns a random key, optionally filtered by prefix.
     * Resolves to empty string ("") when no key matches (including empty store).
     * Never throws - always safe to call.
     *
     * Performance:
     * - trackKeys=true, no prefix -> O(1)
     * - trackKeys=true, with prefix -> O(log n)
     * - trackKeys=false -> two-pass scan
     *
     * @param options - Optional prefix filter
     * @returns Promise that resolves to a random key or ""
     *
     * @example
     * ```javascript
     * // Random key from entire store
     * const anyKey = await kv.randomKey();
     *
     * // Random key with prefix
     * const userKey = await kv.randomKey({ prefix: 'user:' });
     * if (userKey) {
     *   console.log(`Selected user: ${userKey}`);
     * }
     * ```
     */
    randomKey(options?: RandomKeyOptions): Promise<string>;

    /**
     * Rebuilds in-memory key indexes from the underlying store.
     * Only useful when trackKeys=true.
     *
     * - Memory backend: typically unnecessary unless manual corruption occurred
     * - Disk backend: index is automatically rebuilt at startup; call this
     *   only when the DB is mutated out-of-band during the run
     *
     * The operation is O(n) over keys, so prefer running it in setup()
     * or infrequently between stages - not every iteration.
     *
     * @returns Promise that resolves to true when finished
     *
     * @example
     * ```javascript
     * export async function setup() {
     *   await kv.rebuildKeyList();
     * }
     * ```
     */
    rebuildKeyList(): Promise<boolean>;

    // ==================== Snapshot Operations ====================

    /**
     * Streams the current store contents into a BoltDB snapshot on disk.
     * When the disk backend is asked to back up to its own DB path the call
     * succeeds but only returns metadata (no copy is produced).
     *
     * @param options - Destination path and concurrency mode.
     * @returns Promise that resolves to snapshot metadata.
     */
    backup(options?: BackupOptions): Promise<BackupSummary>;

    /**
     * Replaces the store contents with entries from a snapshot file created via backup().
     * Restoring from the disk backend's live DB path is treated as a no-op summary.
     *
     * @param options - Source snapshot path and optional safety limits.
     * @returns Promise that resolves to the number of entries imported.
     */
    restore(options?: RestoreOptions): Promise<RestoreSummary>;

    // ==================== Lifecycle ====================

    /**
     * Synchronously closes the underlying store and releases resources.
     * Should be called in teardown() when done with the store.
     *
     * - Disk backend: releases BoltDB file handle and in-memory indexes
     * - Memory backend: no-op (safe to call, does nothing)
     * - Safe to call multiple times
     * - Reference-counted: store closes only when last close() is called
     *
     * @example
     * ```javascript
     * export function teardown() {
     *   kv.close();
     * }
     * ```
     */
    close(): void;
  }

  /**
   * Opens a key-value store with the specified options.
   * Must be called in the init context (outside default/setup/teardown functions).
   *
   * The first successful call initializes the shared store for all VUs.
   * Later calls reuse the established store and ignore differing options.
   *
   * @param options - Storage and serialization configuration
   * @returns KV store instance
   *
   * @example
   * ```javascript
   * import { openKv } from 'k6/x/kv';
   *
   * // Default: disk backend with JSON serialization
   * const kv = openKv();
   *
   * // In-memory backend with key tracking
   * const kv = openKv({
   *   backend: 'memory',
   *   trackKeys: true
   * });
   *
   * // Disk backend with custom path
   * const kv = openKv({
   *   backend: 'disk',
   *   path: './my-test.db'
   * });
   * ```
   */
  export function openKv(options?: OpenKvOptions): KV;
}

