import { openKv } from 'k6/x/kv';
import file from 'k6/x/file';

// Storage backend to use: 'memory' (default) or 'disk'.
export const BACKEND = __ENV.KV_BACKEND || 'memory';

// Optional serialization override: 'json' (default) or 'string'.
export const SERIALIZATION = __ENV.KV_SERIALIZATION;

// Enable key tracking for selected backend.
export const TRACK_KEYS = __ENV.KV_TRACK_KEYS !== 'false';

// Number of virtual users (concurrent test executors).
export const VUS = parseInt(__ENV.VUS || '40', 10);

// Total number of test iterations to execute.
export const ITERATIONS = parseInt(__ENV.ITERATIONS || '400', 10);

// E2E_ARTIFACT_DIR stores scenario-specific DB/snapshot artifacts.
export const E2E_ARTIFACT_DIR = './tmp/e2e';

// getTestPath returns the full path for test-specific files: ./tmp/e2e/e2e-{test-name}.kv.
export function getTestPath(testName) {
  return `${E2E_ARTIFACT_DIR}/e2e-${testName}.kv`;
}

// getSnapshotPath returns the path for snapshot files used in backup/restore operations.
// For disk backend, uses a different filename (./tmp/e2e/e2e-{test-name}.snapshot.kv)
// to avoid conflict with the database file (./tmp/e2e/e2e-{test-name}.kv).
// For memory backend, uses the standard test path since there's no persistent database file.
export function getSnapshotPath(testName) {
  return BACKEND === 'disk'
    ? `${E2E_ARTIFACT_DIR}/e2e-${testName}.snapshot.kv`
    : getTestPath(testName);
}

// createKv initializes KV store with standard configuration.
// Returns a configured KV store instance ready for use in tests.
// testName: Optional test name for database path isolation (./tmp/e2e/e2e-{test-name}.kv).
export function createKv(testName, overrides = {}) {
  const options = {
    backend: BACKEND,
    trackKeys: TRACK_KEYS
  };

  if (BACKEND === 'disk' && testName) {
    options.path = getTestPath(testName);
  }

  if (SERIALIZATION) {
    options.serialization = SERIALIZATION;
  }

  return openKv({
    ...options,
    ...overrides
  });
}

// createSetup returns standard setup function that clears all keys before test.
export function createSetup(kv) {
  return async function setup() {
    await kv.clear();
  };
}

// createTeardown returns standard teardown function that always closes KV handle
// and, for disk backend, removes test-specific database file.
// Optionally removes snapshot file if snapshotPath is provided.
// testName: Test name for database path isolation.
// snapshotPath: Optional snapshot file path to clean up (if not provided, snapshot file is preserved).
export function createTeardown(kv, testName, snapshotPath) {
  return async function teardown() {
    let closeError = null;
    try {
      kv.close();
    } catch (err) {
      console.error(`close failed: ${err && err.message ? err.message : err}`);
      closeError = err;
    }

    if (BACKEND === 'disk') {

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

    if (closeError) {
      throw closeError;
    }
  };
}
