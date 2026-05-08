package store

func validateNonEmptyKey(key string) error {
	if key == "" {
		return ErrKeyEmpty
	}

	return nil
}
