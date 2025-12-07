import { openKv } from 'k6/x/kv';
import file from 'k6/x/file';

// Storage backend to use: 'memory' (default) or 'disk'.
export const BACKEND = __ENV.KV_BACKEND || 'memory';

// Enable key tracking for selected backend.
export const TRACK_KEYS = __ENV.KV_TRACK_KEYS !== 'false';

// Number of virtual users (concurrent test executors).
export const VUS = parseInt(__ENV.VUS || '40', 10);

// Total number of test iterations to execute.
export const ITERATIONS = parseInt(__ENV.ITERATIONS || '400', 10);

// getTestPath returns the full path for test-specific files: .e2e-{test-name}.kv.
export function getTestPath(testName) {
  return `.e2e-${testName}.kv`;
}

// getSnapshotPath returns the path for snapshot files used in backup/restore operations.
// For disk backend, uses a different filename (.e2e-{test-name}.snapshot.kv) to avoid
// conflict with the database file (.e2e-{test-name}.kv).
// For memory backend, uses the standard test path since there's no persistent database file.
export function getSnapshotPath(testName) {
  return BACKEND === 'disk'
    ? `.e2e-${testName}.snapshot.kv`
    : getTestPath(testName);
}

// createKv initializes KV store with standard configuration.
// Returns a configured KV store instance ready for use in tests.
// testName: Optional test name for database path isolation (.e2e-{test-name}.kv).
export function createKv(testName) {
  return openKv({
    backend: BACKEND,
    path: BACKEND === 'disk' && testName ? getTestPath(testName) : undefined,
    trackKeys: TRACK_KEYS
  });
}

// createSetup returns standard setup function that clears all keys before test.
export function createSetup(kv) {
  return async function setup() {
    await kv.clear();
  };
}

// createTeardown returns standard teardown function that closes disk backend cleanly
// and removes test-specific database file. 
// Optionally removes snapshot file if snapshotPath is provided.
// testName: Test name for database path isolation.
// snapshotPath: Optional snapshot file path to clean up (if not provided, snapshot file is preserved).
export function createTeardown(kv, testName, snapshotPath) {
  return async function teardown() {
    if (BACKEND === 'disk') {
      kv.close();

      // Clean up test-specific database file if test name was provided.
      if (testName) {
        const dbPath = getTestPath(testName);
        try {
          file.deleteFile(dbPath);
        } catch (err) {
          // Ignore errors if file doesn't exist or is already deleted.
        }
      }

      // Clean up snapshot file if snapshot path was explicitly provided.
      if (snapshotPath) {
        try {
          file.deleteFile(snapshotPath);
        } catch (err) {
          // Ignore errors if file doesn't exist or is already deleted.
        }
      }
    }
  };
}
