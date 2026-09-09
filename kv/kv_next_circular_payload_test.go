package kv

import (
	"testing"

	"github.com/grafana/sobek"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/v2/js/modulestest"
)

func TestImportNextCircularOptions_NullishAndEmptyObject(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	for _, options := range []sobek.Value{
		sobek.Undefined(),
		sobek.Null(),
		rt.ToValue(map[string]any{}),
	} {
		parsed, err := importNextCircularOptions(rt, options)
		require.NoError(t, err)
		assert.Empty(t, parsed.Prefix)
	}
}

func TestImportNextCircularOptions_ParsesPrefix(t *testing.T) {
	t.Parallel()

	rt := modulestest.NewRuntime(t).VU.Runtime()

	parsed, err := importNextCircularOptions(rt, rt.ToValue(map[string]any{
		"prefix": "users:",
	}))
	require.NoError(t, err)
	assert.Equal(t, "users:", parsed.Prefix)
}

func TestImportNextCircularOptions_RejectsInvalidTypes(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		options any
	}{
		{
			name:    "options string",
			options: "bad",
		},
		{
			name:    "options array",
			options: []any{"bad"},
		},
		{
			name: "prefix number",
			options: map[string]any{
				"prefix": 1,
			},
		},
		{
			name: "prefix bool",
			options: map[string]any{
				"prefix": true,
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			rt := modulestest.NewRuntime(t).VU.Runtime()

			_, err := importNextCircularOptions(rt, rt.ToValue(testCase.options))
			requireInvalidOptionsError(t, err, "nextCircular")
		})
	}
}
