package store

import (
	"math/rand/v2"
)

// ostNode is a node in a randomized BST (treap) that stores subtree sizes
// so we can do order-statistics operations (rank/select).
type ostNode struct {
	key      string
	priority int // Maintains max-heap property for BST structure
	size     int // Subtree size including self
	left     *ostNode
	right    *ostNode
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

	// Original parent must be updated first.
	pull(n)

	// Then new parent.
	pull(l)

	return l
}

// rotateLeft is symmetric to rotateRight.
func rotateLeft(n *ostNode) *ostNode {
	r := n.right

	n.right = r.left
	r.left = n

	// Update original parent (now child).
	pull(n)

	// Update new root.
	pull(r)

	return r
}

// insert recursively finds insertion point then bubbles up using rotations
// to maintain treap properties. Returns new root of subtree.
func insert(n *ostNode, key string, prio int) *ostNode {
	if n == nil {
		return &ostNode{key: key, priority: prio, size: 1}
	}

	if key == n.key {
		// no duplicates - treat as idempotent operation.
		return n
	}

	if key < n.key {
		n.left = insert(n.left, key, prio)

		// Fix heap property violation after recursive insertion.
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

		// Two children: rotate higher priority child up until
		// target becomes leaf (preserving treap properties).
		if n.left.priority > n.right.priority {
			n = rotateRight(n)
			// Target moved right, delete from right subtree.
			n.right = deleteKey(n.right, key)
		} else {
			n = rotateLeft(n)
			// Target moved left, delete from left subtree.
			n.left = deleteKey(n.left, key)
		}
	}

	// Size might have changed in subtree.
	pull(n)

	return n
}

// rankLess counts keys < k by leveraging BST ordering
// and stored subtree sizes for efficiency.
func rankLess(n *ostNode, k string) int {
	if n == nil {
		return 0
	}

	// When k is in left subtree: no need to count right subtree.
	if k <= n.key {
		return rankLess(n.left, k)
	}

	// When k is in right subtree: count left subtree + current node.
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
	case k < leftSize: // Target is in left subtree
		return kth(n.left, k)
	case k == leftSize: // Current node is exactly the kth
		return n.key, true
	default: // Target is in right subtree (adjust index)
		return kth(n.right, k-leftSize-1)
	}
}

// OSTree is an order-statistics tree with string keys (lexicographic order).
type OSTree struct {
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
func nextPrefix(prefix string) string {
	// Empty prefix has no natural successor.
	if prefix == "" {
		return ""
	}

	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] == 0xFF {
			// Cannot increment, propagate left.
			continue
		}

		// Increment first non-max byte.
		b[i]++

		// Truncate after incremented byte (lexicographic minimum).
		return string(b[:i+1])
	}

	// All bytes 0xFF -> no upper bound.
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
