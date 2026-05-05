package kv

import (
	"testing"

	"github.com/grafana/sobek"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/js/modulestest"
)

func TestImportListKeysOptions_Nullish(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	for _, value := range []sobek.Value{sobek.Undefined(), sobek.Null()} {
		opts, err := importListKeysOptions(rt, value)
		require.NoError(t, err)
		assert.Empty(t, opts.Prefix)
		assert.EqualValues(t, 0, opts.Limit)
	}
}

func TestImportListKeysOptions_NonObjectRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importListKeysOptions(rt, rt.ToValue([]any{}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
}

func TestImportListKeysOptions_ValidOptions(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	value := rt.ToValue(map[string]any{
		"prefix": "user:",
		"limit":  int64(10),
	})

	opts, err := importListKeysOptions(rt, value)
	require.NoError(t, err)
	assert.Equal(t, "user:", opts.Prefix)
	assert.EqualValues(t, 10, opts.Limit)
}

func TestImportListKeysOptions_InvalidPrefixRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importListKeysOptions(rt, rt.ToValue(map[string]any{
		"prefix": 123,
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
}

func TestImportListKeysOptions_FractionalLimitRejects(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	_, err := importListKeysOptions(rt, rt.ToValue(map[string]any{
		"limit": 1.5,
	}))
	require.Error(t, err)

	var kvErr *Error
	require.ErrorAs(t, err, &kvErr)
	assert.Equal(t, InvalidOptionsError, kvErr.Name)
}

func TestImportListKeysOptions_ZeroAndNegativeLimitAllowed(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	for _, limit := range []int64{0, -1} {
		opts, err := importListKeysOptions(rt, rt.ToValue(map[string]any{
			"limit": limit,
		}))
		require.NoError(t, err)
		assert.Equal(t, limit, opts.Limit)
	}
}
