package kv

import (
	"testing"

	"github.com/oshokin/xk6-kv/kv/store"
)

func TestOptionsEqual_MemoryIgnoresPath(t *testing.T) {
	t.Parallel()

	left := Options{
		Backend:       BackendMemory,
		Path:          "/tmp/ignored-a.db",
		Serialization: SerializationJSON,
		TrackKeys:     true,
	}
	right := Options{
		Backend:       BackendMemory,
		Path:          "/tmp/ignored-b.db",
		Serialization: SerializationJSON,
		TrackKeys:     true,
	}

	if !left.Equal(right) {
		t.Fatalf("memory backend must ignore path when comparing options")
	}
}

func TestOptionsEqual_MemoryIgnoresDiskOptions(t *testing.T) {
	t.Parallel()

	noSyncTrue := store.GetComparablePointer(true)

	left := Options{
		Backend:       BackendMemory,
		Serialization: SerializationJSON,
		TrackKeys:     true,
		DiskOptions: &DiskOptions{
			NoSync: noSyncTrue,
		},
	}
	right := Options{
		Backend:       BackendMemory,
		Serialization: SerializationJSON,
		TrackKeys:     true,
	}

	if !left.Equal(right) {
		t.Fatalf("memory backend must ignore disk options when comparing options")
	}
}

func TestOptionsEqual_DiskIgnoresMemoryOptions(t *testing.T) {
	t.Parallel()

	left := Options{
		Backend:       BackendDisk,
		Path:          "/tmp/kv.db",
		Serialization: SerializationJSON,
		TrackKeys:     true,
		MemoryOptions: &MemoryOptions{ShardCount: 64},
	}
	right := Options{
		Backend:       BackendDisk,
		Path:          "/tmp/kv.db",
		Serialization: SerializationJSON,
		TrackKeys:     true,
		MemoryOptions: &MemoryOptions{ShardCount: 0},
	}

	if !left.Equal(right) {
		t.Fatalf("disk backend must ignore memory options when comparing options")
	}
}
