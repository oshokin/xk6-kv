package store

// Store defines the operations for a key-value store.
//
// General notes:
//
//   - Keys are strings. Values are implementation-dependent and typically stored
//     as byte slices after serialization by a higher layer.
//   - Implementations SHOULD preserve lexicographic key ordering where it
//     matters (e.g., List()).
//   - Methods that return a boolean typically indicate whether the underlying
//     mutation took place (e.g., CompareAndSwap) or whether a prior value
//     existed (e.g., Swap's loaded flag).
//   - All methods SHOULD be safe for concurrent use.
//
// Error semantics:
//
//   - Methods return a non-nil error only in exceptional conditions (I/O errors,
//     serialization constraints at the storage layer, etc.).
//   - RandomKey(prefix) returns "" and a nil error when there are no matching
//     keys; this is an intentional, user-friendly behavior.
type Store interface {
	// Open ensures the store is ready to accept operations by initializing any
	// deferred resources (file handles, background workers, etc.).
	// It SHOULD be safe to call Open multiple times concurrently.
	Open() error

	// Get returns the current value stored under key.
	//
	// If the key does not exist, an error is returned.
	Get(key string) (any, error)

	// Set stores value under key, overwriting any existing value.
	// Returns an error only if the operation cannot be completed.
	Set(key string, value any) error

	// IncrementBy atomically adds delta to the numeric value stored at key and
	// returns the new value. If the key does not exist, implementations MAY
	// treat the current value as zero. An error is returned if the existing
	// value cannot be interpreted as a number or on storage failure.
	IncrementBy(key string, delta int64) (int64, error)

	// GetOrSet atomically ensures a value for key and returns the resulting
	// value along with a loaded flag.
	//
	// Semantics:
	//   - If key already exists: the stored value is returned as actual and
	//     loaded == true; the store is not modified.
	//   - If key does not exist: value is stored, actual == value and
	//     loaded == false.
	GetOrSet(key string, value any) (actual any, loaded bool, err error)

	// Swap atomically replaces the value under key with value and returns the
	// previous value and a loaded flag indicating whether the key existed.
	//
	// Semantics:
	//   - If the key existed: previous contains the old value, loaded == true.
	//   - If the key did not exist: previous is nil (implementation-specific),
	//     loaded == false, and key is created with value.
	Swap(key string, value any) (previous any, loaded bool, err error)

	// CompareAndSwap atomically replaces the value under key with new only if
	// the current value is equal to "oldValue".
	// Returns true if the swap occurred.
	//
	// Equality is implementation-defined and typically compares the stored
	// byte representation.
	// Returns false with nil error if the value did not
	// match; returns an error only on storage failures.
	CompareAndSwap(key string, oldValue any, newValue any) (bool, error)

	// Delete removes key from the store.
	// It is not an error to delete a non-existent key (the operation is a no-op).
	Delete(key string) error

	// Exists reports whether key is present in the store.
	Exists(key string) (bool, error)

	// DeleteIfExists deletes key only if it exists.
	// Returns true if the key was present and deleted; false if it did not exist.
	DeleteIfExists(key string) (bool, error)

	// CompareAndDelete deletes key only if the current value equals "oldValue".
	// Returns true if the key existed and was deleted.
	//
	// Equality is implementation-defined and typically compares the stored
	// byte representation.
	CompareAndDelete(key string, oldValue any) (bool, error)

	// Clear removes all keys and values from the store.
	Clear() error

	// Size returns the number of keys currently stored.
	Size() (int64, error)

	// Scan is a cursor-based iterator over keys, ordered lexicographically.
	// If prefix is non-empty, only keys starting with prefix are considered.
	// If afterKey is non-empty, scanning starts from the first key strictly greater than afterKey;
	// otherwise from the first key matching prefix.
	// If limit > 0, at most limit entries are returned.
	// If limit <= 0, all matching entries are returned until the end of the prefix range.
	// Returns a ScanPage with Entries and NextKey (empty when scan is complete).
	Scan(prefix, afterKey string, limit int64) (*ScanPage, error)

	// List returns key-value pairs whose keys start with prefix.
	// If limit > 0, at most limit entries are returned.
	// If limit <= 0, all matching entries are returned.
	// Implementations SHOULD return entries ordered by key in ascending lexicographic order.
	List(prefix string, limit int64) ([]Entry, error)

	// RandomKey returns a random key from the store. If prefix is non-empty,
	// the random selection is restricted to keys that start with prefix.
	//
	// If there are no matching keys (including an empty store),
	// RandomKey returns the empty string "" and a nil error.
	RandomKey(prefix string) (string, error)

	// RebuildKeyList rebuilds any in-memory key indexes maintained by the
	// implementation (e.g., after a crash or when optional indexing is enabled).
	// Implementations that do not maintain indexes MAY implement this as a no-op.
	RebuildKeyList() error

	// Close releases any resources held by the store (file handles, caches,
	// background workers, etc.). After Close returns, the Store should not be
	// used. Close SHOULD be idempotent.
	Close() error
}

// Entry represents a single key-value pair returned by List() or used by
// implementations internally.
type Entry struct {
	// Key is the string identifier for the stored value.
	Key string
	// Value holds the stored value in the implementation's native representation
	// (commonly a []byte produced/consumed by a higher-level serializer).
	Value any
}

// ScanPage represents a single page of results from Scan().
type ScanPage struct {
	// Entries is the page of key-value pairs.
	Entries []Entry

	// NextKey is the last key of this page when more entries are available
	// for the given prefix and limit. It is an empty string when the scan
	// has reached the end of the keyspace (for that prefix).
	NextKey string
}
