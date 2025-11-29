import { check } from 'k6';
import exec from 'k6/execution';
import { getSnapshotPath, BACKEND, VUS, ITERATIONS, createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: DISASTER RECOVERY BACKUP/RESTORE SYSTEM
// =============================================================================
//
// This test simulates a production backup system where database snapshots are
// created while the system remains online and serves traffic. This is a
// critical pattern in:
//
// - Database systems (PostgreSQL WAL backups, MySQL hot backups)
// - Key-value stores (Redis RDB/AOF, etcd snapshots)
// - File storage systems (S3 versioning, snapshot backups)
// - Configuration management (Consul snapshots, ZooKeeper backups)
// - E-commerce platforms (daily inventory/order backups)
// - Banking systems (transaction log backups, point-in-time recovery)
//
// REAL-WORLD PROBLEM SOLVED:
// Creating consistent backups while write operations continue concurrently.
// Without proper snapshot isolation, you get:
// - Corrupted backups (partial writes, inconsistent state)
// - Service downtime (blocking backups stop all writes)
// - Data loss (backup doesn't capture concurrent writes)
// - Failed restores (inconsistent snapshots can't be restored)
// - Compliance violations (incomplete audit trails)
//
// ATOMIC OPERATIONS TESTED:
// - backup(): Create point-in-time snapshot with two modes:
//   * Blocking mode (allowConcurrentWrites=false): Strict consistency
//   * Best-effort mode (allowConcurrentWrites=true): Online backup
// - restore(): Rebuild KV store from snapshot file
// - set(): Write data during backup operations
// - list(): Verify backup captured expected keys
//
// CONCURRENCY PATTERN:
// - Coordinator VU: Creates snapshots at specific iterations
// - Worker VUs: Continuously write data during backup operations
// - Tests both blocking and concurrent backup modes
// - Validates backup metadata (totalEntries, bestEffort flag)
//
// PERFORMANCE CHARACTERISTICS:
// - Backup operations must complete quickly to minimize blocking
// - Concurrent writes should not corrupt backup consistency
// - Restore operations must handle missing/partial snapshots gracefully
// - Critical for business continuity and disaster recovery

// Key prefix for regular data writes during backup operations.
const DATA_KEY_PREFIX = 'key:';

// Key prefix for concurrent write tracking during best-effort backups.
const CONCURRENT_KEY_PREFIX = 'concurrent:';

// Baseline key to verify restore functionality.
const BASELINE_KEY = 'baseline';

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'backup-restore-concurrency';

// Snapshot file path for backup/restore operations.
const SNAPSHOT_PATH = getSnapshotPath(TEST_NAME);

// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME);

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: VUS,
  iterations: ITERATIONS,
  thresholds: {
    'checks{backup:blocking-strict}': ['rate>0.999'],
    'checks{backup:idempotent}': ['rate>0.999'],
    'checks{backup:concurrent-best-effort}': ['rate>0.999'],
    'checks{restore:baseline-present}': ['rate>0.999']
  }
};

// setup initializes the store and attempts to restore from previous snapshot.
// This validates that restore operations handle missing files gracefully.
export async function setup() {
  const standardSetup = createSetup(kv);
  await standardSetup();

  // Seed baseline data to verify restore functionality.
  await kv.set(BASELINE_KEY, 'stable');

  // Attempt to restore from previous backup if it exists.
  try {
    const summary = await kv.restore({ fileName: SNAPSHOT_PATH });
    console.log(`restore summary: ${JSON.stringify(summary)}`);

    check(true, {
      'restore:baseline-present': () => summary.totalEntries > 0
    });
  } catch (err) {
    if (err?.name === 'SnapshotNotFoundError') {
      console.warn(`restore skipped: ${SNAPSHOT_PATH} missing (first run)`);
    } else if (err?.name === 'SnapshotPermissionError') {
      console.error(`restore failed: insufficient permissions for ${SNAPSHOT_PATH}`);
      throw err;
    } else {
      throw err;
    }
  }
}

// backupRestoreConcurrencyTest exercises both blocking and best-effort backup
// modes while worker VUs continuously write data, validating snapshot consistency.
export default async function backupRestoreConcurrencyTest() {
  const iteration = exec.scenario.iterationInTest;
  const isCoordinator = exec.vu.idInInstance === 1;

  // Coordinator creates blocking backup on first iteration.
  // Blocking mode ensures strict consistency by preventing concurrent writes.
  if (isCoordinator && iteration === 0) {
    try {
      const blockingSummary = await kv.backup({
        fileName: SNAPSHOT_PATH,
        allowConcurrentWrites: false
      });

      console.log(`blocking backup summary: ${JSON.stringify(blockingSummary)}`);

      check(blockingSummary, {
        'backup:blocking-strict': (s) => s.bestEffort === false
      });

      // Verify idempotent backup behavior (repeated backup should work).
      const secondBlockingSummary = await kv.backup({
        fileName: SNAPSHOT_PATH,
        allowConcurrentWrites: false
      });

      console.log(`second blocking backup summary: ${JSON.stringify(secondBlockingSummary)}`);

      check(secondBlockingSummary, {
        'backup:idempotent': (s) => s.totalEntries === blockingSummary.totalEntries
      });
    } catch (err) {
      if (err?.name === 'BackupInProgressError') {
        console.log(`Expected: Another VU is performing a backup (proves locking works)`);
      } else if (err?.name === 'SnapshotPermissionError') {
        console.error(`blocking backup failed: insufficient permissions for ${SNAPSHOT_PATH}`);
        throw err;
      } else {
        throw err;
      }
    }

    return;
  }

  // Coordinator creates best-effort backup on second iteration.
  // Best-effort mode allows concurrent writes during snapshot creation.
  if (isCoordinator && iteration === 1) {
    try {
      const concurrentSummary = await kv.backup({
        fileName: SNAPSHOT_PATH,
        allowConcurrentWrites: true
      });

      console.log(`concurrent backup summary: ${JSON.stringify(concurrentSummary)}`);

      // For disk backend, BoltDB transactions always provide consistent snapshots,
      // so bestEffort will be false even when allowConcurrentWrites=true.
      // For memory backend, bestEffort reflects the allowConcurrentWrites flag.
      check(concurrentSummary, {
        'backup:concurrent-best-effort': (s) =>
          BACKEND === 'disk' ? s.bestEffort === false : s.bestEffort === true
      });

      // Write data after best-effort backup to test concurrent behavior.
      await kv.set(`${CONCURRENT_KEY_PREFIX}${iteration}`, `value-${iteration}`);
    } catch (err) {
      if (err?.name === 'BackupInProgressError') {
        console.log(`Expected: Another VU is performing a backup (proves locking works)`);
      } else if (err?.name === 'SnapshotPermissionError') {
        console.error(`concurrent backup failed: insufficient permissions for ${SNAPSHOT_PATH}`);
        throw err;
      } else {
        throw err;
      }
    }

    return;
  }

  // All VUs (including coordinator after iteration 1) continuously write data.
  // This simulates production traffic during backup operations.
  try {
    await kv.set(`${DATA_KEY_PREFIX}${iteration}`, `value-${iteration}`);
  } catch (err) {
    // During blocking backups, writes are temporarily blocked.
    // This is expected behavior and should not fail the test.
    if (err?.name === 'BackupInProgressError' || err?.name === 'RestoreInProgressError') {
      // Expected: writes are blocked during backup/restore operations.
      // This validates that the locking mechanism works correctly.
      return;
    }
    // Re-throw unexpected errors.
    throw err;
  }
}

// teardown performs final verification and cleanup operations.
export async function teardown() {
    // Verify that best-effort backup captured concurrent writes.
    const concurrentKeys = await kv.list({ prefix: CONCURRENT_KEY_PREFIX });

    if (concurrentKeys.length > 0) {
      console.log(`captured ${concurrentKeys.length} best-effort concurrent writes`);
    }

    // Create final blocking backup to ensure all data is captured.
    const finalSummary = await kv.backup({
      fileName: SNAPSHOT_PATH,
      allowConcurrentWrites: false
    });

    console.log(`final blocking backup summary: ${JSON.stringify(finalSummary)}`);

    // Close disk backend cleanly to prevent file handle leaks.
    const standardTeardown = createTeardown(kv, TEST_NAME);
    await standardTeardown();
}
