package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// TestBuildBBoltOptions_NilConfig_UsesDefaults tests that when the disk config is nil,
// the default bbolt options are used.
func TestBuildBBoltOptions_NilConfig_UsesDefaults(t *testing.T) {
	t.Parallel()

	opts, err := buildBBoltOptions(nil)
	require.NoError(t, err)
	assert.Nil(t, opts, "nil config should defer to bbolt defaults")
}

// TestBuildBBoltOptions_AppliesDefaultsThenOverrides tests that the bbolt options are applied correctly.
func TestBuildBBoltOptions_AppliesDefaultsThenOverrides(t *testing.T) {
	t.Parallel()

	timeout := 250 * time.Millisecond
	noSync := true
	noGrowSync := true
	noFreelistSync := true
	preLoadFreelist := true
	freelistType := "map"
	readOnly := true
	initialMmapSize := 128 * 1024
	mlock := true
	cfg := &DiskConfig{
		Timeout:         &timeout,
		NoSync:          &noSync,
		NoGrowSync:      &noGrowSync,
		NoFreelistSync:  &noFreelistSync,
		PreLoadFreelist: &preLoadFreelist,
		FreelistType:    &freelistType,
		ReadOnly:        &readOnly,
		InitialMmapSize: &initialMmapSize,
		Mlock:           &mlock,
	}

	opts, err := buildBBoltOptions(cfg)
	require.NoError(t, err)
	require.NotNil(t, opts)

	// Defaults start from bolt.DefaultOptions, then our overrides.
	assert.Equal(t, timeout, opts.Timeout)
	assert.Equal(t, noSync, opts.NoSync)
	assert.Equal(t, noGrowSync, opts.NoGrowSync)
	assert.Equal(t, noFreelistSync, opts.NoFreelistSync)
	assert.Equal(t, preLoadFreelist, opts.PreLoadFreelist)
	assert.Equal(t, bolt.FreelistMapType, opts.FreelistType)
	assert.Equal(t, readOnly, opts.ReadOnly)
	assert.Equal(t, initialMmapSize, opts.InitialMmapSize)
	assert.Equal(t, mlock, opts.Mlock)
}
