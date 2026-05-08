package kv

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oshokin/xk6-kv/kv/store"
)

func TestClassifyError_UnexpectedHeapTypeIsInternalStoreError(t *testing.T) {
	t.Parallel()

	err := fmt.Errorf("memory scan invariant: %w", store.ErrUnexpectedHeapType)

	classified := classifyError(err)
	require.Equal(t, InternalStoreError, classified.Name)
	require.Contains(t, classified.Message, store.ErrUnexpectedHeapType.Error())
}
