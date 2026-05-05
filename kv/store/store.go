package store

type (
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
	//nolint:interfacebloat // store is intentionally broad to keep backend parity behind one shared contract.
	Store interface {
		// Open ensures the store is ready to accept operations by initializing any
		// deferred resources (file handles, background workers, etc.).
		// It SHOULD be safe to call Open multiple times concurrently.
		Open() error

		// Get returns the current value stored under key.
		//
		// If the key does not exist, an error is returned.
		Get(key string) (any, error)

		// GetMany returns entries in the same order as keys.
		//
		// Missing keys are represented as nil entries at the corresponding indexes.
		// The method returns an error only when the read operation itself fails.
		GetMany(keys []string) ([]*Entry, error)

		// Set stores value under key, overwriting any existing value.
		// Returns an error only if the operation cannot be completed.
		Set(key string, value any) error

		// SetMany stores a batch of key/value pairs, overwriting existing keys.
		//
		// The argument intentionally uses []Entry (values), not []*Entry (pointers):
		// this keeps batch payloads contiguous and avoids per-item heap allocation
		// churn. See setmany_slice_shape_benchmark_test.go for measurements.
		//
		// Implementations SHOULD apply the whole batch or return an error.
		SetMany(entries []Entry) (int64, error)

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
		//
		// Special case: passing a nil oldValue means "swap only if the key is
		// currently absent", mirroring sync/atomic.CompareAndSwap semantics.
		CompareAndSwap(key string, oldValue any, newValue any) (bool, error)

		// CompareAndSwapDetailed performs CAS and returns a structured outcome that
		// distinguishes compare-mismatch from successful swaps.
		//
		// includeCurrentOnMismatch controls whether the current value is returned when
		// the compare fails and the key exists. Implementations MUST NOT return current
		// on successful swaps.
		CompareAndSwapDetailed(
			key string,
			oldValue any,
			newValue any,
			includeCurrentOnMismatch bool,
		) (*CompareAndSwapDetailedResult, error)

		// Delete removes key from the store.
		// It is not an error to delete a non-existent key (the operation is a no-op).
		Delete(key string) error

		// DeleteMany removes explicit keys and returns deletion statistics.
		//
		// Missing keys are not errors and are counted in the result.
		// Implementations should process duplicate keys in input order.
		DeleteMany(keys []string) (*DeleteManyResult, error)

		// DeleteByPrefix deletes up to limit keys whose names start with prefix.
		//
		// Prefix must be non-empty.
		// Limit must be positive.
		// Implementations must remove claim metadata for physically deleted keys.
		//
		// Done is true when no matching key remains after this call.
		DeleteByPrefix(prefix string, limit int64) (*DeleteByPrefixResult, error)

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

		// CompareAndDeleteDetailed performs compare-and-delete and returns a structured
		// outcome that distinguishes compare-mismatch from successful deletions.
		//
		// includeCurrentOnMismatch controls whether the current value is returned when
		// the compare fails and the key exists. Implementations MUST NOT return current
		// on successful deletes.
		CompareAndDeleteDetailed(
			key string,
			oldValue any,
			includeCurrentOnMismatch bool,
		) (*CompareAndDeleteDetailedResult, error)

		// Clear removes all keys and values from the store.
		Clear() error

		// Size returns the number of keys currently stored.
		Size() (int64, error)

		// Count returns the number of keys that start with prefix.
		// Count("") is equivalent to Size().
		Count(prefix string) (int64, error)

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

		// ListKeys returns keys whose names start with prefix.
		//
		// If limit > 0, at most limit keys are returned.
		// If limit <= 0, all matching keys are returned.
		// Implementations SHOULD return keys ordered by ascending lexicographic order.
		//
		// ListKeys is key-only and MUST NOT load, clone, serialize, or deserialize values.
		ListKeys(prefix string, limit int64) ([]string, error)

		// RandomKey returns a random key from the store. If prefix is non-empty,
		// the random selection is restricted to keys that start with prefix.
		//
		// If there are no matching keys (including an empty store),
		// RandomKey returns the empty string "" and a nil error.
		RandomKey(prefix string) (string, error)

		// PopRandom atomically selects and removes a random free matching entry.
		// If there are no free matching entries, it returns nil, nil.
		PopRandom(prefix string) (*Entry, error)

		// ClaimRandom atomically leases a random matching entry.
		// If no free (unclaimed or expired-claim) entry exists, it returns nil, nil.
		ClaimRandom(opts *ClaimOptions) (*EntryClaim, error)

		// ReleaseClaim releases a live claim.
		// It returns true only when id/key/token match a non-expired live claim.
		ReleaseClaim(ref *ClaimRef) (bool, error)

		// CompleteClaim completes a live claim.
		// When opts.DeleteKey is true, the underlying key is removed.
		CompleteClaim(ref *ClaimRef, opts *CompleteClaimOptions) (bool, error)

		// RebuildKeyList rebuilds any in-memory key indexes maintained by the
		// implementation (e.g., after a crash or when optional indexing is enabled).
		// Implementations that do not maintain indexes MAY implement this as a no-op.
		RebuildKeyList() error

		// Backup writes the store contents to an on-disk snapshot (bbolt file).
		// MemoryStore streams data into a fresh bbolt file, optionally allowing
		// best-effort concurrency. DiskStore copies its underlying bbolt file.
		// When DiskStore is asked to back up to its own DB path, a fast-path summary
		// is returned without copying (so callers can detect the noop).
		// Returns a summary detailing entry counts and snapshot metadata.
		Backup(opts *BackupOptions) (*BackupSummary, error)

		// Restore replaces the current store contents with entries from a snapshot created by Backup().
		// Implementations should treat this as destructive; the existing dataset is wiped before the
		// snapshot contents are applied. DiskStore treats "restore from live DB path"
		// as a noop summary so the caller can detect the mistake without destroying
		// the running database.
		// Returns a summary indicating how many entries were hydrated.
		Restore(opts *RestoreOptions) (*RestoreSummary, error)

		// Stats returns a diagnostic snapshot of the current store state.
		// The snapshot is intended for explicit observability calls and debugging.
		Stats() (*StatsSnapshot, error)

		// Close releases any resources held by the store (file handles, caches,
		// background workers, etc.). After Close returns, the Store should not be
		// used. Close SHOULD be idempotent.
		Close() error
	}

	// CompareAndSwapDetailedResult describes the outcome of CompareAndSwapDetailed.
	// Current is only valid when HasCurrent is true.
	CompareAndSwapDetailedResult struct {
		// Swapped reports whether the compare-and-swap mutation was applied.
		Swapped bool
		// Reason describes the compare result ("swapped" or "mismatch").
		Reason string
		// Existed reports whether the key existed at the compare decision point.
		Existed bool
		// Current holds the observed current value when HasCurrent is true.
		Current any
		// HasCurrent reports whether Current is populated.
		HasCurrent bool
	}

	// CompareAndDeleteDetailedResult describes the outcome of CompareAndDeleteDetailed.
	// Current is only valid when HasCurrent is true.
	CompareAndDeleteDetailedResult struct {
		// Deleted reports whether the compare-and-delete mutation was applied.
		Deleted bool
		// Reason describes the compare result ("deleted" or "mismatch").
		Reason string
		// Existed reports whether the key existed at the compare decision point.
		Existed bool
		// Current holds the observed current value when HasCurrent is true.
		Current any
		// HasCurrent reports whether Current is populated.
		HasCurrent bool
	}

	// Entry represents a single key-value pair returned by List() or used by
	// implementations internally.
	// Fields are tagged for JavaScript camelCase convention when exposed via Sobek.
	Entry struct {
		// Key is the string identifier for the stored value.
		Key string `js:"key"`
		// Value holds the stored value in the implementation's native representation
		// (commonly a []byte produced/consumed by a higher-level serializer).
		Value any `js:"value"`
	}

	// DeleteManyResult reports the outcome of DeleteMany().
	// Fields are tagged for JavaScript camelCase convention when exposed via Sobek.
	DeleteManyResult struct {
		// Deleted is the number of requested keys that existed and were deleted.
		Deleted int64 `js:"deleted"`
		// Missing is the number of requested keys that did not exist at deletion time.
		Missing int64 `js:"missing"`
	}

	// DeleteByPrefixResult describes the result of deleting keys by prefix.
	// Fields are tagged for JavaScript camelCase convention when exposed via Sobek.
	DeleteByPrefixResult struct {
		// Deleted is the number of keys physically deleted by this call.
		Deleted int64 `js:"deleted"`
		// Done is true when no matching keys remain after this call.
		Done bool `js:"done"`
	}

	// ScanPage represents a single page of results from Scan().
	// Fields are tagged for JavaScript camelCase convention when exposed via Sobek.
	ScanPage struct {
		// Entries is the page of key-value pairs.
		Entries []Entry `js:"entries"`

		// NextKey is the last key of this page when more entries are available
		// for the given prefix and limit. It is an empty string when the scan
		// has reached the end of the keyspace (for that prefix).
		NextKey string `js:"nextKey"`
	}
)

const (
	// CompareReasonMismatch indicates a compare condition did not match.
	CompareReasonMismatch = "mismatch"
	// CompareAndSwapReasonSwapped indicates compare-and-swap succeeded.
	CompareAndSwapReasonSwapped = "swapped"
	// CompareAndDeleteReasonDeleted indicates compare-and-delete succeeded.
	CompareAndDeleteReasonDeleted = "deleted"
)

// ShouldIncludeCurrent reports whether current should be exposed to API callers.
// current is visible only for mismatch on existing keys when requested by caller.
func (r *CompareAndSwapDetailedResult) ShouldIncludeCurrent() bool {
	if r == nil {
		return false
	}

	return !r.Swapped && r.Existed && r.HasCurrent
}

// ShouldIncludeCurrent reports whether current should be exposed to API callers.
// current is visible only for mismatch on existing keys when requested by caller.
func (r *CompareAndDeleteDetailedResult) ShouldIncludeCurrent() bool {
	if r == nil {
		return false
	}

	return !r.Deleted && r.Existed && r.HasCurrent
}
