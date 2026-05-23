package store

// validateNonEmptyKey rejects empty keys before store operations.
func validateNonEmptyKey(key string) error {
	if key == "" {
		return ErrKeyEmpty
	}

	return nil
}
