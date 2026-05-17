package store

import (
	"math/rand/v2"
	"slices"
)

type (
	// emptyOSTMeta is a placeholder type for OSTree metadata.
	emptyOSTMeta struct{}

	// OSTree is a tree of ordered statistics that stores metadata for each key.
	OSTree = OSTreeOf[emptyOSTMeta]

	// OSTreeOf is a tree of ordered statistics that stores metadata for each key.
	OSTreeOf[M any] struct {
		// root is the root node of the tree.
		root *ostNode[M]
	}

	// ostNode is a node in a randomized BST (treap) that stores subtree sizes
	// so we can do order-statistics operations (rank/select).
	ostNode[M any] struct {
		// key is the key of the node.
		key string

		// priority is the priority of the node.
		priority int

		// size is the number of all keys in this subtree.
		// It backs claim-agnostic APIs such as randomKey/randomKeys/scanKeys/listKeys.
		size int

		// selectable says whether this exact key may be returned by allocation APIs.
		// For ordinary keys it is true. For live claimed keys it is false.
		selectable bool

		// selectableSize is the number of selectable keys in this subtree.
		// It backs allocation APIs such as claimRandom/claimKey/popRandom.
		selectableSize int

		// meta stores typed per-key metadata without forcing the tree to understand it.
		meta M

		// left is the left child of the node.
		left *ostNode[M]

		// right is the right child of the node.
		right *ostNode[M]
	}
)

// NewOSTree creates and returns a new empty order-statistics tree.
func NewOSTree() *OSTree {
	return NewOSTreeOf[emptyOSTMeta]()
}

// NewOSTreeOf creates and returns a new empty typed order-statistics tree.
func NewOSTreeOf[M any]() *OSTreeOf[M] {
	return &OSTreeOf[M]{}
}

// Len returns the current number of keys stored in the tree.
func (t *OSTreeOf[M]) Len() int {
	return nodeSize(t.root)
}

// Insert adds a new key to the tree with a random priority.
// If the key already exists, this is a no-op.
func (t *OSTreeOf[M]) Insert(key string) {
	var zero M
	t.InsertWithMeta(key, zero)
}

// InsertWithMeta adds a new key and associates typed metadata with it.
// If the key already exists, this is a no-op and existing metadata is retained.
func (t *OSTreeOf[M]) InsertWithMeta(key string, meta M) {
	// Using global rand to maintain uniform distribution
	// of priorities for balanced tree structure.

	// #nosec G404 -- treap balancing does not require cryptographic randomness.
	priority := rand.Int()
	t.root = insert(t.root, key, priority, meta)
}

// Delete removes a key from the tree if it exists.
// If the key is not present, this is a no-op.
func (t *OSTreeOf[M]) Delete(key string) {
	t.root = deleteNode(t.root, key)
}

// Rank returns the number of keys strictly less than k.
func (t *OSTreeOf[M]) Rank(key string) int {
	return rankLess(t.root, key)
}

// Kth returns the k-th key (0-based) in lexicographic order.
func (t *OSTreeOf[M]) Kth(k int) (string, bool) {
	return kth(t.root, k)
}

// RangeBounds returns [start, end) indices for keys with prefix
// using rank operations for O(log n) performance.
func (t *OSTreeOf[M]) RangeBounds(prefix string) (int, int) {
	// Empty prefix covers entire tree.
	if prefix == "" {
		return 0, t.Len()
	}

	// First key >= prefix: rank counts keys < prefix, so left is the first index >= prefix.
	left := t.Rank(prefix)

	upper := nextPrefix(prefix)
	if upper == "" {
		// No upper bound: range extends to end of tree.
		// This happens when prefix ends with all 0xFF bytes.
		return left, t.Len()
	}

	// First key >= nextPrefix is the exclusive end.
	// All keys with prefix are in range [left, right).
	right := t.Rank(upper)

	return left, right
}

// SelectableLen returns the number of selectable keys in the tree.
func (t *OSTreeOf[M]) SelectableLen() int {
	return nodeSelectableSize(t.root)
}

// SelectableRank returns the rank of the first selectable key greater than or equal to the given key.
func (t *OSTreeOf[M]) SelectableRank(key string) int {
	return selectableRankLess(t.root, key)
}

// KthSelectable returns the k-th selectable key (0-based) in lexicographic order.
func (t *OSTreeOf[M]) KthSelectable(k int) (string, bool) {
	return kthSelectable(t.root, k)
}

// SelectableRangeBounds returns [start, end) indices for selectable keys with prefix
// using selectableRank operations for O(log n) performance.
func (t *OSTreeOf[M]) SelectableRangeBounds(prefix string) (int, int) {
	if prefix == "" {
		return 0, t.SelectableLen()
	}

	left := t.SelectableRank(prefix)

	upper := nextPrefix(prefix)
	if upper == "" {
		return left, t.SelectableLen()
	}

	right := t.SelectableRank(upper)

	return left, right
}

// Meta returns the metadata for the given key.
//
// If M is a pointer type, callers must treat the pointed value as protected by
// the same synchronization that protects the tree itself.
func (t *OSTreeOf[M]) Meta(key string) (M, bool) {
	node := findNode(t.root, key)
	if node == nil {
		var zero M
		return zero, false
	}

	return node.meta, true
}

// UpdateMeta updates the metadata for the given key.
func (t *OSTreeOf[M]) UpdateMeta(
	key string,
	update func(old M) (next M, selectable bool),
) bool {
	var updated bool

	t.root, updated = updateMeta(t.root, key, update)

	return updated
}

// SetSelectable sets the selectable flag for the given key.
func (t *OSTreeOf[M]) SetSelectable(key string, selectable bool) bool {
	return t.UpdateMeta(key, func(old M) (M, bool) {
		return old, selectable
	})
}

// ClearMeta clears metadata for every node and marks all nodes selectable.
func (t *OSTreeOf[M]) ClearMeta(zero M) {
	clearMeta(t.root, zero)
}

// WalkMeta walks the metadata for the given key.
func (t *OSTreeOf[M]) WalkMeta(fn func(key string, meta M)) {
	walkMeta(t.root, fn)
}

// nodeSize gracefully handles nil nodes to avoid nil checks in recursive functions.
// Returns 0 for nil nodes, which represents an empty subtree.
func nodeSize[M any](n *ostNode[M]) int {
	if n == nil {
		return 0
	}

	return n.size
}

// nodeSelectableSize gracefully handles nil nodes to avoid nil checks in recursive functions.
// Returns 0 for nil nodes, which represents an empty subtree.
func nodeSelectableSize[M any](n *ostNode[M]) int {
	if n == nil {
		return 0
	}

	return n.selectableSize
}

// pull recalculates subtree size after structural changes.
// Must be called after any rotation or child modification to maintain
// correct order-statistics. Size = 1 (self) + left subtree + right subtree.
func pull[M any](n *ostNode[M]) {
	if n == nil {
		return
	}

	n.size = 1 + nodeSize(n.left) + nodeSize(n.right)

	var selfSelectable int
	if n.selectable {
		selfSelectable = 1
	}

	n.selectableSize = nodeSelectableSize(n.left) + selfSelectable + nodeSelectableSize(n.right)
}

// rotateRight maintains BST order while fixing heap property.
// Returns new root of rotated subtree.
func rotateRight[M any](n *ostNode[M]) *ostNode[M] {
	l := n.left

	n.left = l.right
	l.right = n

	// Update sizes bottom-up: child n first (now has fewer descendants),
	// then parent l (gains n's original right subtree).
	// Must update child before parent because parent's size depends on child's size.
	pull(n)
	pull(l)

	return l
}

// rotateLeft is symmetric to rotateRight.
func rotateLeft[M any](n *ostNode[M]) *ostNode[M] {
	r := n.right

	n.right = r.left
	r.left = n

	// Update sizes bottom-up: n loses its right subtree, r gains n as left child.
	pull(n)
	pull(r)

	return r
}

// insert recursively finds insertion point then bubbles up using rotations
// to maintain treap properties (BST order + max-heap by priority).
// Returns new root of subtree.
//
// The random priority ensures O(log n) expected height through probabilistic balancing.
func insert[M any](n *ostNode[M], key string, priority int, meta M) *ostNode[M] {
	if n == nil {
		return &ostNode[M]{
			key:            key,
			priority:       priority,
			size:           1,
			selectable:     true,
			selectableSize: 1,
			meta:           meta,
		}
	}

	switch {
	case key < n.key:
		n.left = insert(n.left, key, priority, meta)

		// Fix heap property violation after recursive insertion.
		// If newly inserted node has higher priority, rotate up.
		if n.left.priority > n.priority {
			n = rotateRight(n)
		}
	case key > n.key:
		n.right = insert(n.right, key, priority, meta)

		// Right-child has higher priority: needs left rotation.
		if n.right.priority > n.priority {
			n = rotateLeft(n)
		}
	default:
		return n
	}

	// Update size after potential rotations.
	pull(n)

	return n
}

// deleteKey recursively locates target then uses rotations to
// demote it to leaf position for safe removal.
func deleteNode[M any](n *ostNode[M], key string) *ostNode[M] {
	if n == nil {
		return nil
	}

	switch {
	case key < n.key:
		n.left = deleteNode(n.left, key)
	case key > n.key:
		n.right = deleteNode(n.right, key)
	default:
		// Target found: handle 0/1 child cases directly.
		if n.left == nil {
			return n.right
		}

		if n.right == nil {
			return n.left
		}

		// Two children: use rotations to sink the target node down to a leaf position
		// while preserving the treap heap property (higher priority = closer to root).
		// Choose rotation direction based on which child has higher priority.
		// Rotate so the higher-priority child becomes the new root, then recurse.
		if n.left.priority > n.right.priority {
			n = rotateRight(n)
			// After right rotation, target moved to right subtree.
			// Continue deletion in the subtree where target now resides.
			n.right = deleteNode(n.right, key)
		} else {
			n = rotateLeft(n)
			// After left rotation, target moved to left subtree.
			// Continue deletion in the subtree where target now resides.
			n.left = deleteNode(n.left, key)
		}
	}

	// Size might have changed in subtree.
	pull(n)

	return n
}

// rankLess counts keys < k by leveraging BST ordering
// and stored subtree sizes for O(log n) efficiency.
//
// Key insight: at each node we can count entire subtrees in O(1)
// rather than visiting every node individually.
func rankLess[M any](n *ostNode[M], key string) int {
	if n == nil {
		return 0
	}

	// When key <= n.key: target is in left subtree or equals n.key.
	// We don't count n or the right subtree because they're >= key.
	// Recurse into left subtree to find keys strictly less than key.
	if key <= n.key {
		return rankLess(n.left, key)
	}

	// When key > n.key: count everything in left subtree + n itself,
	// then recurse into right subtree for additional matches.
	// This avoids visiting nodes we know are < key, leveraging BST ordering.
	return 1 + nodeSize(n.left) + rankLess(n.right, key)
}

// kth uses subtree sizes for O(log n) traversal.
// Returns zero-value + false for out-of-range indices.
func kth[M any](n *ostNode[M], k int) (string, bool) {
	if n == nil || k < 0 || k >= nodeSize(n) {
		return "", false
	}

	leftSize := nodeSize(n.left)

	switch {
	case k < leftSize:
		// Target is in left subtree: delegate without adjustment.
		// Index k in left subtree is the same as index k in full tree.
		return kth(n.left, k)
	case k == leftSize:
		// Current node is exactly the k-th in sorted order.
		// All left subtree nodes come before, so this node is at position leftSize.
		return n.key, true
	default:
		// Target is in right subtree: subtract left subtree size + current node
		// to convert global index k into a right-subtree-local index.
		// Right subtree starts at index (leftSize + 1), so subtract that offset.
		return kth(n.right, k-leftSize-1)
	}
}

func selectableRankLess[M any](n *ostNode[M], key string) int {
	if n == nil {
		return 0
	}

	if key <= n.key {
		return selectableRankLess(n.left, key)
	}

	selfSelectable := 0
	if n.selectable {
		selfSelectable = 1
	}

	return nodeSelectableSize(n.left) + selfSelectable + selectableRankLess(n.right, key)
}

func kthSelectable[M any](n *ostNode[M], k int) (string, bool) {
	if n == nil || k < 0 || k >= nodeSelectableSize(n) {
		return "", false
	}

	leftSize := nodeSelectableSize(n.left)
	if k < leftSize {
		return kthSelectable(n.left, k)
	}

	selfSelectable := 0
	if n.selectable {
		selfSelectable = 1
	}

	if k < leftSize+selfSelectable {
		return n.key, true
	}

	return kthSelectable(n.right, k-leftSize-selfSelectable)
}

func findNode[M any](n *ostNode[M], key string) *ostNode[M] {
	for n != nil {
		switch {
		case key < n.key:
			n = n.left
		case key > n.key:
			n = n.right
		default:
			return n
		}
	}

	return nil
}

func updateMeta[M any](
	n *ostNode[M],
	key string,
	update func(old M) (next M, selectable bool),
) (*ostNode[M], bool) {
	if n == nil {
		return nil, false
	}

	var updated bool

	switch {
	case key < n.key:
		n.left, updated = updateMeta(n.left, key, update)
	case key > n.key:
		n.right, updated = updateMeta(n.right, key, update)
	default:
		n.meta, n.selectable = update(n.meta)
		updated = true
	}

	if updated {
		pull(n)
	}

	return n, updated
}

func clearMeta[M any](n *ostNode[M], zero M) {
	if n == nil {
		return
	}

	n.meta = zero
	n.selectable = true

	clearMeta(n.left, zero)
	clearMeta(n.right, zero)

	pull(n)
}

func walkMeta[M any](n *ostNode[M], fn func(key string, meta M)) {
	if n == nil {
		return
	}

	walkMeta(n.left, fn)
	fn(n.key, n.meta)
	walkMeta(n.right, fn)
}

// nextPrefix computes the lexicographic successor of a prefix
// by incrementing the last byte with carry propagation.
//
// Examples:
//
//	"abc"     → "abd"      (simple increment)
//	"ab\xFF"  → "ac"       (carry propagates left)
//	"\xFF"    → ""         (all bytes maxed, no successor)
//
// Used to compute exclusive end bounds for prefix ranges.
func nextPrefix(prefix string) string {
	// Empty prefix has no natural successor.
	if prefix == "" {
		return ""
	}

	b := []byte(prefix)
	// Iterate right-to-left, looking for a byte we can increment.
	for i, v := range slices.Backward(b) {
		if v == 0xFF {
			// This byte is at max value: incrementing would overflow.
			// Continue left to find a byte we can increment (carry propagation).
			continue
		}

		// Found a byte < 0xFF: increment it and truncate everything after.
		// Example: "abc" → "abd", "ab\xFF" → "ac"
		// Truncation ensures we get the lexicographically smallest key > prefix.
		b[i]++

		return string(b[:i+1])
	}

	// All bytes 0xFF -> no upper bound (matches everything lexicographically greater).
	return ""
}
