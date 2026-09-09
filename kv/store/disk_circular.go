package store

// NextCircular returns the next reusable matching entry in lexicographic order
// and wraps to the beginning after the current prefix range is exhausted.
func (s *DiskStore) NextCircular(prefix string) (*Entry, error) {
	return nextCircularWithRegistry(s.circularCursors, prefix, s.Scan)
}
