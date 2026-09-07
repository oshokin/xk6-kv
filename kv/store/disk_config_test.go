package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestBuildBBoltOptions_NilConfig_UsesSafeLockTimeout(t *testing.T) {
	t.Parallel()

	opts, err := buildBBoltOptions(nil)
	require.NoError(t, err)
	require.NotNil(t, opts)

	assert.Equal(t, DefaultDiskStoreOpenTimeout, opts.Timeout)

	// Apart from the intentional lock-timeout override, keep the bbolt
	// defaults for the tuning knobs exposed by xk6-kv.
	assert.Equal(t, bolt.DefaultOptions.NoSync, opts.NoSync)
	assert.Equal(t, bolt.DefaultOptions.NoGrowSync, opts.NoGrowSync)
	assert.Equal(t, bolt.DefaultOptions.NoFreelistSync, opts.NoFreelistSync)
	assert.Equal(t, bolt.DefaultOptions.PreLoadFreelist, opts.PreLoadFreelist)
	assert.Equal(t, bolt.DefaultOptions.FreelistType, opts.FreelistType)
	assert.Equal(t, bolt.DefaultOptions.ReadOnly, opts.ReadOnly)
	assert.Equal(t, bolt.DefaultOptions.InitialMmapSize, opts.InitialMmapSize)
	assert.Equal(t, bolt.DefaultOptions.Mlock, opts.Mlock)
}

func TestBuildBBoltOptions_ExplicitZeroTimeoutDisablesFailFastDefault(t *testing.T) {
	t.Parallel()

	zero := time.Duration(0)

	opts, err := buildBBoltOptions(&DiskConfig{
		Timeout: &zero,
	})
	require.NoError(t, err)
	require.NotNil(t, opts)

	assert.Zero(t, opts.Timeout)
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
