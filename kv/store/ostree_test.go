package store

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOSTree_InsertRankSelect verifies that:
// 1. Inserting a bag of (possibly duplicate) keys results in Len() equal to count of unique keys.
// 2. Kth(i) enumerates keys in strict sorted order (unique).
// 3. Rank(key) equals the index of that key in the sorted-unique sequence.
// 4. After deletions, Len(), Kth(), and order remain consistent.
func TestOSTree_InsertRankSelect(t *testing.T) {
	t.Parallel()

	originalKeys := []string{"a", "ab", "abc", "b", "ba", "z", "aa", "aaa"}

	tree := NewOSTree()
	for _, key := range originalKeys {
		tree.Insert(key)
	}

	sortedUniqueKeys := getUniqueValues(originalKeys)
	sort.Strings(sortedUniqueKeys)

	require.Equal(t,
		len(sortedUniqueKeys),
		tree.Len(),
		"Len() must equal unique key count",
	)

	// Verify Kth enumerates in-order and gather results to assert the entire sequence.
	collectedKeysInOrder := make([]string, 0, len(sortedUniqueKeys))

	for index := range sortedUniqueKeys {
		keyAtIndex, ok := tree.Kth(index)

		require.Truef(t, ok, "Kth(%d) must exist", index)
		assert.Equalf(t, sortedUniqueKeys[index], keyAtIndex, "Kth(%d) mismatch", index)

		collectedKeysInOrder = append(collectedKeysInOrder, keyAtIndex)
	}

	assert.Equal(t, sortedUniqueKeys, collectedKeysInOrder, "full in-order traversal mismatch")

	// Verify Rank aligns with positions in the sorted sequence.
	for index, key := range sortedUniqueKeys {
		assert.Equalf(t, index, tree.Rank(key), "Rank(%q) mismatch", key)
	}

	// Delete two keys and re-check invariants.
	tree.Delete("ab")
	tree.Delete("z")

	expectedKeysAfterDeletion := removeAll(sortedUniqueKeys, "ab", "z")
	require.Equal(t, len(expectedKeysAfterDeletion), tree.Len(), "Len() after delete mismatch")

	for index := range expectedKeysAfterDeletion {
		keyAtIndex, ok := tree.Kth(index)

		require.Truef(t, ok, "Kth(%d) must exist after deletions", index)
		assert.Equalf(t, expectedKeysAfterDeletion[index], keyAtIndex, "Kth(%d) mismatch after deletions", index)
	}
}

// TestOSTree_RangeBounds validates that RangeBounds(prefix) returns a half-open interval [l, r)
// such that the slice of Kth(l..r-1) are exactly the keys that start with "prefix".
// It checks several prefixes including empty, partial, full, and a non-existent prefix.
func TestOSTree_RangeBounds(t *testing.T) {
	t.Parallel()

	tree := NewOSTree()
	keysInserted := []string{"a", "aa", "ab", "aba", "ac", "b", "ba"}

	for _, key := range keysInserted {
		tree.Insert(key)
	}

	sort.Strings(keysInserted)

	assertRangeForPrefix := func(prefixUnderTest string) {
		leftBoundIndex, rightBoundIndex := tree.RangeBounds(prefixUnderTest)

		// Build the expected subset (all sorted keys with the prefix).
		expectedKeysWithPrefix := make([]string, 0, len(keysInserted))

		for _, key := range keysInserted {
			if !strings.HasPrefix(key, prefixUnderTest) {
				continue
			}

			expectedKeysWithPrefix = append(expectedKeysWithPrefix, key)
		}

		require.Equalf(t, len(expectedKeysWithPrefix), rightBoundIndex-leftBoundIndex,
			"Range size mismatch for prefix %q (interval [%d,%d))",
			prefixUnderTest, leftBoundIndex, rightBoundIndex,
		)

		// Verify Kth over [l, r) equals the expected list in order.
		for offset := range expectedKeysWithPrefix {
			keyAtIndex, ok := tree.Kth(leftBoundIndex + offset)

			require.Truef(t, ok, "Kth(%d) must exist for prefix %q", leftBoundIndex+offset, prefixUnderTest)
			assert.Equalf(t, expectedKeysWithPrefix[offset], keyAtIndex,
				"Range %q Kth(%d) mismatch", prefixUnderTest, leftBoundIndex+offset)
		}
	}

	assertRangeForPrefix("")
	assertRangeForPrefix("a")
	assertRangeForPrefix("ab")
	assertRangeForPrefix("aba")
	assertRangeForPrefix("ac")
	assertRangeForPrefix("b")

	// Non-existent prefix -> empty interval.
	assertRangeForPrefix("zz")
}

// TestNextPrefix checks nextPrefix() behavior around ASCII and 0xFF boundaries:
//   - "" -> "" (no next prefix for empty)
//   - "a" -> "b"; "a\xff" -> "b"
//   - "\xff" -> "" (overflow wraps to empty)
//   - "ab" -> "ac"; "ab\xff\xff" -> "ac"
func TestNextPrefix(t *testing.T) {
	t.Parallel()

	type testCase struct {
		inputString        string
		expectedNextPrefix string
	}

	testCases := []testCase{
		{inputString: "", expectedNextPrefix: ""},
		{inputString: "a", expectedNextPrefix: "b"},
		{inputString: "a\xff", expectedNextPrefix: "b"},
		{inputString: "\xff", expectedNextPrefix: ""},
		{inputString: "ab", expectedNextPrefix: "ac"},
		{inputString: "ab\xff\xff", expectedNextPrefix: "ac"},
	}

	for _, oneCase := range testCases {
		tc := oneCase

		t.Run(fmt.Sprintf("next(%q)->%q", tc.inputString, tc.expectedNextPrefix), func(t *testing.T) {
			t.Parallel()

			actual := nextPrefix(tc.inputString)
			assert.Equalf(t, tc.expectedNextPrefix, actual, "nextPrefix(%q) mismatch", tc.inputString)
		})
	}
}

// getUniqueValues returns a new slice that contains only the first occurrence
// of each distinct value from inputSlice, preserving the original encounter order.
func getUniqueValues[T comparable](inputSlice []T) []T {
	seenSet := make(map[T]struct{}, len(inputSlice))
	unique := make([]T, 0, len(inputSlice))

	for _, value := range inputSlice {
		if _, exists := seenSet[value]; exists {
			continue
		}

		seenSet[value] = struct{}{}

		unique = append(unique, value)
	}

	return unique
}

// removeAll returns a new slice that contains every element from sourceSlice
// except those present in elementsToDelete.
// The relative order of the retained elements is preserved.
func removeAll[T comparable](source []T, toDelete ...T) []T {
	deleteSet := make(map[T]struct{}, len(toDelete))
	for _, value := range toDelete {
		deleteSet[value] = struct{}{}
	}

	result := make([]T, 0, len(source))

	for _, value := range source {
		if _, shouldRemove := deleteSet[value]; shouldRemove {
			continue
		}

		result = append(result, value)
	}

	return result
}
