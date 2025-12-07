package store

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkDiskStore_Backup: measures snapshot backup throughput across tracking
// and concurrency modes. For disk backend, bbolt transactions always provide
// consistent snapshots, so allowConcurrentWrites doesn't affect consistency but
// may still be tested for API completeness.
func BenchmarkDiskStore_Backup(b *testing.B) {
	const totalSeedKeys = 5_000

	scenarios := []struct {
		trackKeys             bool
		allowConcurrentWrites bool
	}{
		{trackKeys: true, allowConcurrentWrites: false},
		{trackKeys: true, allowConcurrentWrites: true},
		{trackKeys: false, allowConcurrentWrites: false},
		{trackKeys: false, allowConcurrentWrites: true},
	}

	for _, scenario := range scenarios {
		name := fmt.Sprintf("trackKeys=%v/allowConcurrent=%v", scenario.trackKeys, scenario.allowConcurrentWrites)

		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()

			store := newBenchmarkDiskStore(b, scenario.trackKeys, "disk-benchmark-backup-*.db")

			b.StopTimer()
			seedDiskStore(b, store, totalSeedKeys, "backup-")

			// Use unique filename per scenario to avoid conflicts in parallel execution.
			snapshotName := fmt.Sprintf(
				"disk-benchmark-backup-target-track-keys-%v-allow-concurrent-%v.kv",
				scenario.trackKeys,
				scenario.allowConcurrentWrites,
			)
			snapshotPath := filepath.Join(b.TempDir(), snapshotName)
			b.StartTimer()

			for b.Loop() {
				_, err := store.Backup(&BackupOptions{
					FileName:              snapshotPath,
					AllowConcurrentWrites: scenario.allowConcurrentWrites,
				})
				require.NoError(b, err)
			}
		})
	}
}

// BenchmarkDiskStore_Restore: measures snapshot restore throughput with and without
// key tracking. Tracking adds index rebuild overhead but enables faster RandomKey/List.
func BenchmarkDiskStore_Restore(b *testing.B) {
	const totalSeedKeys = 5_000

	for _, trackKeys := range []bool{true, false} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			b.ReportAllocs()

			source := newBenchmarkDiskStore(b, true, "disk-benchmark-restore-source-*.db")

			b.StopTimer()
			seedDiskStore(b, source, totalSeedKeys, "restore-")

			tempDir := b.TempDir()
			// Use unique filename per scenario to avoid conflicts in parallel execution.
			snapshotName := fmt.Sprintf("disk-benchmark-restore-source-track-keys-%v.kv", trackKeys)
			snapshotPath := filepath.Join(tempDir, snapshotName)

			_, err := source.Backup(&BackupOptions{FileName: snapshotPath})
			require.NoError(b, err)

			target := newBenchmarkDiskStore(b, trackKeys, "disk-benchmark-restore-target-*.db")

			b.StartTimer()

			for b.Loop() {
				_, err := target.Restore(&RestoreOptions{FileName: snapshotPath})
				require.NoError(b, err)
			}
		})
	}
}
