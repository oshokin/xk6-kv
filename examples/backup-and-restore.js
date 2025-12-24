// Demonstrates how to capture a bbolt snapshot via kv.backup() and later
// kv.restore() it to roll the dataset back to a known-good state.
//
// Highlights:
//   * Uses the disk backend with a dedicated DB file (not the default .k6.kv)
//     so we can safely create/destroy data during the example.
//   * Writes the snapshot to a different path from the live DB. When you point
//     backup()/restore() at the live DB path the DiskStore intentionally no-ops,
//     so always provide a distinct file.
//   * Shows strict consistency (AllowConcurrentWrites=false) so the snapshot is
//     a clean point-in-time image.
//   * Demonstrates error handling for common edge cases (missing files, concurrent backups).

import { openKv } from "k6/x/kv";
import { expect } from "https://jslib.k6.io/k6-testing/0.5.0/index.js";

const snapshotPath = "backup-and-restore.kv";

const kv = openKv({ backend: "memory" });

export async function setup() {
  // Attempt to restore from previous snapshot if it exists.
  try {
    const restoreSummary = await kv.restore({ fileName: snapshotPath });
    console.log(`Restored ${restoreSummary.totalEntries} entries from ${snapshotPath}`);
  } catch (err) {
    if (err?.name === "SnapshotNotFoundError") {
      // Expected on first run - snapshot doesn't exist yet.
      console.warn(`Restore skipped: ${snapshotPath} missing (first run)`);
    } else if (err?.name === "SnapshotPermissionError") {
      console.error(`Restore failed: insufficient permissions for ${snapshotPath}`);
      throw err;
    } else {
      // Unexpected error - rethrow to fail the test.
      throw err;
    }
  }

  // Seed initial data if store is empty.
  if ((await kv.size()) === 0) {
    await kv.set("user:1", { id: 1, name: "Ada" });
    await kv.set("user:2", { id: 2, name: "Grace" });
    console.log("Seeded initial data (2 users)");
  }
}

export default async function () {
  await kv.delete("user:1");
  await kv.set("user:3", { id: 3, name: "Linus" });
}

export async function teardown() {
  try {
    const backupSummary = await kv.backup({
      fileName: snapshotPath,
      allowConcurrentWrites: false,
    });

    expect(backupSummary.bestEffort).toEqual(false);
    console.log(
      `Snapshot saved to ${snapshotPath} (${backupSummary.totalEntries} entries, ${backupSummary.bytesWritten} bytes)`,
    );
  } catch (err) {
    if (err?.name === "BackupInProgressError") {
      // Another VU is already creating a backup - this is expected in concurrent tests.
      // The backup locking mechanism is working correctly.
      console.log("Backup skipped: Another operation in progress (locking works correctly)");
    } else if (err?.name === "SnapshotPermissionError") {
      console.error(`Backup failed: insufficient permissions for ${snapshotPath}`);
      throw err;
    } else {
      // Unexpected error - rethrow to fail the test.
      throw err;
    }
  }

  kv.close();
}
