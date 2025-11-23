package store

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkMemoryStore_Backup: measures snapshot backup throughput across tracking
// and concurrency modes. AllowConcurrentWrites reduces lock contention but provides
// best-effort consistency instead of strict point-in-time semantics.
func BenchmarkMemoryStore_Backup(b *testing.B) {
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

			store := NewMemoryStore(scenario.trackKeys)

			b.StopTimer()
			seedMemoryStore(b, store, totalSeedKeys, "backup-")
			snapshotPath := filepath.Join(b.TempDir(), "backup.kv")
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

// BenchmarkMemoryStore_Restore: measures snapshot restore throughput with and without
// key tracking. Tracking adds index rebuild overhead but enables faster RandomKey/List.
func BenchmarkMemoryStore_Restore(b *testing.B) {
	const totalSeedKeys = 5_000

	for _, trackKeys := range []bool{true, false} {
		b.Run(fmt.Sprintf("trackKeys=%v", trackKeys), func(b *testing.B) {
			b.ReportAllocs()

			source := NewMemoryStore(true)

			b.StopTimer()
			seedMemoryStore(b, source, totalSeedKeys, "restore-")

			tempDir := b.TempDir()
			snapshotPath := filepath.Join(tempDir, "restore-src.kv")

			_, err := source.Backup(&BackupOptions{FileName: snapshotPath})
			require.NoError(b, err)

			target := NewMemoryStore(trackKeys)

			b.StartTimer()

			for b.Loop() {
				_, err := target.Restore(&RestoreOptions{FileName: snapshotPath})
				require.NoError(b, err)
			}
		})
	}
}
