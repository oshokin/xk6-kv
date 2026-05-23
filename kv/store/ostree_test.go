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

// TestOSTree_BackwardCompatibleAlias verifies that the OSTree API is backward compatible
// with the original implementation.
func TestOSTree_BackwardCompatibleAlias(t *testing.T) {
	t.Parallel()

	tree := NewOSTree()
	tree.Insert("b")
	tree.Insert("a")
	tree.Insert("c")

	require.Equal(t, 3, tree.Len())

	key, ok := tree.Kth(0)
	require.True(t, ok)
	assert.Equal(t, "a", key)

	key, ok = tree.Kth(1)
	require.True(t, ok)
	assert.Equal(t, "b", key)

	key, ok = tree.Kth(2)
	require.True(t, ok)
	assert.Equal(t, "c", key)

	assert.Equal(t, 0, tree.Rank("a"))
	assert.Equal(t, 1, tree.Rank("b"))
	assert.Equal(t, 2, tree.Rank("c"))

	left, right := tree.RangeBounds("b")
	assert.Equal(t, 1, left)
	assert.Equal(t, 2, right)

	left, right = tree.RangeBounds("z")
	assert.Equal(t, 3, left)
	assert.Equal(t, 3, right)
}

// TestOSTreeOf_Metadata verifies that the OSTreeOf API can store metadata for each key.
func TestOSTreeOf_Metadata(t *testing.T) {
	t.Parallel()

	tree := NewOSTreeOf[*trackedDiskClaimRecord]()
	tree.InsertWithMeta("a", nil)
	tree.InsertWithMeta("b", nil)

	record := &trackedDiskClaimRecord{
		ID:  "claim-1",
		Key: "a",
	}

	updated := tree.UpdateMeta("a", func(_ *trackedDiskClaimRecord) (*trackedDiskClaimRecord, bool) {
		return record, false
	})
	require.True(t, updated)

	meta, ok := tree.Meta("a")
	require.True(t, ok)
	require.NotNil(t, meta)
	assert.Equal(t, "claim-1", meta.ID)
	assert.Equal(t, 2, tree.Len())
	assert.Equal(t, 1, tree.SelectableLen())
}

// TestOSTreeOf_KthSelectableDoesNotAffectKth verifies that the KthSelectable API does not affect the Kth API.
func TestOSTreeOf_KthSelectableDoesNotAffectKth(t *testing.T) {
	t.Parallel()

	tree := NewOSTreeOf[emptyOSTMeta]()
	tree.Insert("a")
	tree.Insert("b")
	tree.Insert("c")

	require.True(t, tree.SetSelectable("b", false))

	key, ok := tree.Kth(0)
	require.True(t, ok)
	assert.Equal(t, "a", key)
	key, ok = tree.Kth(1)
	require.True(t, ok)
	assert.Equal(t, "b", key)
	key, ok = tree.Kth(2)
	require.True(t, ok)
	assert.Equal(t, "c", key)

	key, ok = tree.KthSelectable(0)
	require.True(t, ok)
	assert.Equal(t, "a", key)
	key, ok = tree.KthSelectable(1)
	require.True(t, ok)
	assert.Equal(t, "c", key)

	_, ok = tree.KthSelectable(2)
	assert.False(t, ok)
}

// TestOSTreeOf_SelectableRangeBounds verifies that the SelectableRangeBounds API returns
// the correct range of selectable keys.
func TestOSTreeOf_SelectableRangeBounds(t *testing.T) {
	t.Parallel()

	tree := NewOSTreeOf[emptyOSTMeta]()
	tree.Insert("user:1")
	tree.Insert("user:2")
	tree.Insert("user:3")
	tree.Insert("order:1")

	require.True(t, tree.SetSelectable("user:2", false))

	left, right := tree.SelectableRangeBounds("user:")
	require.Equal(t, 2, right-left)

	key, ok := tree.KthSelectable(left)
	require.True(t, ok)
	assert.Equal(t, "user:1", key)

	key, ok = tree.KthSelectable(left + 1)
	require.True(t, ok)
	assert.Equal(t, "user:3", key)
}

// TestOSTreeOf_ClearMeta verifies that the ClearMeta API removes all metadata from the tree.
func TestOSTreeOf_ClearMeta(t *testing.T) {
	t.Parallel()

	tree := NewOSTreeOf[*trackedDiskClaimRecord]()
	tree.InsertWithMeta("a", nil)
	tree.InsertWithMeta("b", nil)
	tree.InsertWithMeta("c", nil)

	require.True(t, tree.UpdateMeta("a", func(_ *trackedDiskClaimRecord) (*trackedDiskClaimRecord, bool) {
		return &trackedDiskClaimRecord{ID: "claim-a"}, false
	}))
	require.True(t, tree.UpdateMeta("b", func(_ *trackedDiskClaimRecord) (*trackedDiskClaimRecord, bool) {
		return &trackedDiskClaimRecord{ID: "claim-b"}, false
	}))

	require.Equal(t, 1, tree.SelectableLen())

	tree.ClearMeta(nil)
	assert.Equal(t, tree.Len(), tree.SelectableLen())

	metaA, ok := tree.Meta("a")
	require.True(t, ok)
	assert.Nil(t, metaA)

	metaB, ok := tree.Meta("b")
	require.True(t, ok)
	assert.Nil(t, metaB)
}

// TestOSTreeOf_DeleteRemovesMetadata verifies that the Delete API removes metadata for a key.
func TestOSTreeOf_DeleteRemovesMetadata(t *testing.T) {
	t.Parallel()

	tree := NewOSTreeOf[*trackedDiskClaimRecord]()
	tree.InsertWithMeta("user:1", nil)

	require.True(t, tree.UpdateMeta("user:1", func(_ *trackedDiskClaimRecord) (*trackedDiskClaimRecord, bool) {
		return &trackedDiskClaimRecord{ID: "claim-1"}, false
	}))

	tree.Delete("user:1")

	_, ok := tree.Meta("user:1")
	assert.False(t, ok)
	assert.Equal(t, 0, tree.Len())
	assert.Equal(t, 0, tree.SelectableLen())
}

// TestOSTreeOf_WalkMetaPrefix verifies that os tree of walk meta prefix.
func TestOSTreeOf_WalkMetaPrefix(t *testing.T) {
	t.Parallel()

	tree := NewOSTreeOf[*trackedDiskClaimRecord]()
	tree.InsertWithMeta("order:1", nil)
	tree.InsertWithMeta("user:1", &trackedDiskClaimRecord{ID: "claim-user-1"})
	tree.InsertWithMeta("user:2", &trackedDiskClaimRecord{ID: "claim-user-2"})
	tree.InsertWithMeta("user:3", nil)

	collected := make([]string, 0)
	claimed := make([]string, 0)

	tree.WalkMetaPrefix("user:", func(key string, meta *trackedDiskClaimRecord) bool {
		collected = append(collected, key)
		if meta != nil {
			claimed = append(claimed, key)
		}

		return true
	})

	assert.Equal(t, []string{"user:1", "user:2", "user:3"}, collected)
	assert.Equal(t, []string{"user:1", "user:2"}, claimed)

	stopped := make([]string, 0, 1)

	tree.WalkMetaPrefix("user:", func(key string, _ *trackedDiskClaimRecord) bool {
		stopped = append(stopped, key)
		return false
	})
	assert.Equal(t, []string{"user:1"}, stopped)
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
