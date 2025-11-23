package store

import (
	"math/rand/v2"
)

// ostNode is a node in a randomized BST (treap) that stores subtree sizes
// so we can do order-statistics operations (rank/select).
type ostNode struct {
	// key is the key of the node.
	key string
	// priority is the priority of the node.
	priority int
	// size is the size of the subtree including the node.
	size int
	// left is the left child of the node.
	left *ostNode
	// right is the right child of the node.
	right *ostNode
}

// nodeSize gracefully handles nil nodes to avoid nil checks in recursive functions.
func nodeSize(n *ostNode) int {
	if n == nil {
		return 0
	}

	return n.size
}

// pull recalculates subtree size after structural changes.
// Must be called after any rotation or child modification.
func pull(n *ostNode) {
	if n == nil {
		return
	}

	n.size = 1 + nodeSize(n.left) + nodeSize(n.right)
}

// rotateRight maintains BST order while fixing heap property.
// Returns new root of rotated subtree.
func rotateRight(n *ostNode) *ostNode {
	l := n.left

	n.left = l.right
	l.right = n

	// Update sizes bottom-up: child n first (now has fewer descendants),
	// then parent l (gains n's original right subtree).
	pull(n)
	pull(l)

	return l
}

// rotateLeft is symmetric to rotateRight.
func rotateLeft(n *ostNode) *ostNode {
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
func insert(n *ostNode, key string, prio int) *ostNode {
	if n == nil {
		return &ostNode{key: key, priority: prio, size: 1}
	}

	if key == n.key {
		// Key already exists: no duplicates allowed in this tree.
		// Treat as idempotent: return existing node unchanged.
		return n
	}

	if key < n.key {
		n.left = insert(n.left, key, prio)

		// Fix heap property violation after recursive insertion.
		// If newly inserted node has higher priority, rotate up.
		if n.left.priority > n.priority {
			n = rotateRight(n)
		}
	} else {
		n.right = insert(n.right, key, prio)

		// Right-child has higher priority: needs left rotation.
		if n.right.priority > n.priority {
			n = rotateLeft(n)
		}
	}

	// Update size after potential rotations.
	pull(n)

	return n
}

// deleteKey recursively locates target then uses rotations to
// demote it to leaf position for safe removal.
func deleteKey(n *ostNode, key string) *ostNode {
	if n == nil {
		return nil
	}

	switch {
	case key < n.key:
		n.left = deleteKey(n.left, key)
	case key > n.key:
		n.right = deleteKey(n.right, key)
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
		if n.left.priority > n.right.priority {
			n = rotateRight(n)
			// After right rotation, target moved to right subtree.
			n.right = deleteKey(n.right, key)
		} else {
			n = rotateLeft(n)
			// After left rotation, target moved to left subtree.
			n.left = deleteKey(n.left, key)
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
func rankLess(n *ostNode, k string) int {
	if n == nil {
		return 0
	}

	// When k <= n.key: target is in left subtree or equals n.key.
	// We don't count n or the right subtree because they're >= k.
	if k <= n.key {
		return rankLess(n.left, k)
	}

	// When k > n.key: count everything in left subtree + n itself,
	// then recurse into right subtree for additional matches.
	// This avoids visiting nodes we know are < k.
	return 1 + nodeSize(n.left) + rankLess(n.right, k)
}

// kth uses subtree sizes for O(log n) traversal.
// Returns zero-value + false for out-of-range indices.
func kth(n *ostNode, k int) (string, bool) {
	if n == nil || k < 0 || k >= nodeSize(n) {
		return "", false
	}

	leftSize := nodeSize(n.left)

	switch {
	case k < leftSize:
		// Target is in left subtree: delegate without adjustment.
		return kth(n.left, k)
	case k == leftSize:
		// Current node is exactly the k-th in sorted order.
		return n.key, true
	default:
		// Target is in right subtree: subtract left subtree size + current node
		// to convert global index k into a right-subtree-local index.
		return kth(n.right, k-leftSize-1)
	}
}

// OSTree is an order-statistics tree with string keys (lexicographic order).
type OSTree struct {
	// root is the root node of the tree.
	root *ostNode
}

// NewOSTree creates and returns a new empty order-statistics tree.
func NewOSTree() *OSTree {
	return new(OSTree)
}

// Len returns the current number of keys stored in the tree.
func (t *OSTree) Len() int {
	return nodeSize(t.root)
}

// Insert adds a new key to the tree with a random priority.
// If the key already exists, this is a no-op.
func (t *OSTree) Insert(key string) {
	// Using global rand to maintain uniform distribution
	// of priorities for balanced tree structure.
	p := rand.Int() //nolint:gosec // math/rand/v2 is safe.

	t.root = insert(t.root, key, p)
}

// Delete removes a key from the tree if it exists.
// If the key is not present, this is a no-op.
func (t *OSTree) Delete(key string) {
	t.root = deleteKey(t.root, key)
}

// Rank returns the number of keys strictly less than k.
func (t *OSTree) Rank(k string) int {
	return rankLess(t.root, k)
}

// Kth returns the k-th key (0-based) in lexicographic order.
func (t *OSTree) Kth(k int) (string, bool) {
	return kth(t.root, k)
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
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] == 0xFF {
			// This byte is at max value: incrementing would overflow.
			// Continue left to find a byte we can increment (carry propagation).
			continue
		}

		// Found a byte < 0xFF: increment it and truncate everything after.
		// Example: "abc" → "abd", "ab\xFF" → "ac"
		b[i]++

		return string(b[:i+1])
	}

	// All bytes 0xFF -> no upper bound (matches everything lexicographically greater).
	return ""
}

// RangeBounds returns [start, end) indices for keys with prefix
// using rank operations for O(log n) performance.
func (t *OSTree) RangeBounds(prefix string) (int, int) {
	// Empty prefix covers entire tree.
	if prefix == "" {
		return 0, t.Len()
	}

	// First key >= prefix.
	l := t.Rank(prefix)

	up := nextPrefix(prefix)
	if up == "" {
		// No upper bound: range extends to end of tree.
		return l, t.Len()
	}

	// First key >= nextPrefix is the exclusive end.
	r := t.Rank(up)

	return l, r
}
