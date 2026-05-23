package kv

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oshokin/xk6-kv/kv/store"
)

// TestClassifyError_UnexpectedHeapTypeIsInternalStoreError verifies that classify error unexpected heap type is internal store error.
func TestClassifyError_UnexpectedHeapTypeIsInternalStoreError(t *testing.T) {
	t.Parallel()

	err := fmt.Errorf("memory scan invariant: %w", store.ErrUnexpectedHeapType)

	classified := classifyError(err)
	require.Equal(t, InternalStoreError, classified.Name)
	require.Contains(t, classified.Message, store.ErrUnexpectedHeapType.Error())
}

// TestClassifyError_ClaimCompletionFailedIsInternalStoreError verifies that classify error claim completion failed is internal store error.
func TestClassifyError_ClaimCompletionFailedIsInternalStoreError(t *testing.T) {
	t.Parallel()

	err := fmt.Errorf("serialized popRandom invariant: %w", store.ErrClaimCompletionFailed)

	classified := classifyError(err)
	require.Equal(t, InternalStoreError, classified.Name)
	require.Contains(t, classified.Message, store.ErrClaimCompletionFailed.Error())
}

// TestClassifyError_ContextCanceledIsOperationCanceledError verifies that classify error context canceled is operation canceled error.
func TestClassifyError_ContextCanceledIsOperationCanceledError(t *testing.T) {
	t.Parallel()

	err := fmt.Errorf("operation aborted: %w", context.Canceled)

	classified := classifyError(err)
	require.Equal(t, OperationCanceledError, classified.Name)
	require.Contains(t, classified.Message, "operation was canceled:")
	require.Contains(t, classified.Message, context.Canceled.Error())
}

// TestClassifyError_DeadlineExceededIsOperationCanceledError verifies that classify error deadline exceeded is operation canceled error.
func TestClassifyError_DeadlineExceededIsOperationCanceledError(t *testing.T) {
	t.Parallel()

	err := fmt.Errorf("operation deadline exceeded: %w", context.DeadlineExceeded)

	classified := classifyError(err)
	require.Equal(t, OperationCanceledError, classified.Name)
	require.Contains(t, classified.Message, "operation was canceled:")
	require.Contains(t, classified.Message, context.DeadlineExceeded.Error())
}
