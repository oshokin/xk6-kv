package store

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSampleUniqueOffsets_UniqueWithinRange(t *testing.T) {
	t.Parallel()

	offsets := sampleUniqueOffsets(100, 10)
	require.Len(t, offsets, 10)

	seen := make(map[int]struct{}, len(offsets))
	for _, offset := range offsets {
		assert.GreaterOrEqual(t, offset, 0)
		assert.Less(t, offset, 100)
		seen[offset] = struct{}{}
	}

	assert.Len(t, seen, len(offsets))
}

func TestSampleUniqueOffsets_CountGreaterThanTotal(t *testing.T) {
	t.Parallel()

	offsets := sampleUniqueOffsets(5, 10)
	require.Len(t, offsets, 5)

	seen := make(map[int]struct{}, len(offsets))
	for _, offset := range offsets {
		assert.GreaterOrEqual(t, offset, 0)
		assert.Less(t, offset, 5)
		seen[offset] = struct{}{}
	}

	assert.Len(t, seen, 5)
}

func TestSampleUniqueKeys_UniqueSubset(t *testing.T) {
	t.Parallel()

	keys := []string{
		"k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8", "k9", "k10",
	}

	result := sampleUniqueKeys(keys, 4)
	require.Len(t, result, 4)

	seen := make(map[string]struct{}, len(result))
	for _, key := range result {
		seen[key] = struct{}{}
	}

	assert.Len(t, seen, len(result))
}

func TestSampleUniqueKeys_SmallCount_ReturnsUniqueSubset(t *testing.T) {
	t.Parallel()

	const (
		totalKeys = 10_000
		count     = 10
	)

	keys := make([]string, 0, totalKeys)
	for i := range totalKeys {
		keys = append(keys, fmt.Sprintf("key:%05d", i))
	}

	result := sampleUniqueKeys(keys, count)
	require.Len(t, result, count)

	inputSet := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		inputSet[key] = struct{}{}
	}

	seen := make(map[string]struct{}, len(result))
	for _, key := range result {
		_, exists := inputSet[key]
		assert.True(t, exists, "sampled key must exist in source set")

		seen[key] = struct{}{}
	}

	assert.Len(t, seen, len(result))
}

func TestSampleUniqueKeys_CountLargerThanAvailableReturnsAllKeys(t *testing.T) {
	t.Parallel()

	keys := []string{"a", "b", "c"}
	result := sampleUniqueKeys(keys, 10)

	require.Len(t, result, len(keys))
	assert.ElementsMatch(t, keys, result)

	seen := make(map[string]struct{}, len(result))
	for _, key := range result {
		seen[key] = struct{}{}
	}

	assert.Len(t, seen, len(result))
}
