package store

import (
	"fmt"
	"slices"
	"strconv"
)

const (
	// DefaultClaimTTLMs is the default lease duration used by ClaimRandom.
	DefaultClaimTTLMs int64 = 30_000
	// MaxClaimTTLMs bounds claim leases to avoid accidental forever leases and timestamp overflow.
	MaxClaimTTLMs int64 = 24 * 60 * 60 * 1000
	// MaxClaimOwnerBytes bounds optional diagnostic owner metadata stored with claims.
	MaxClaimOwnerBytes = 256
	// popRandomClaimOwner tags internal popRandom/popRandomMany claims.
	popRandomClaimOwner = "__xk6_kv_pop_random"
)

type (
	// EntryClaim describes an active lease for a store entry.
	// Fields are tagged for JavaScript camelCase convention when exposed via Sobek.
	EntryClaim struct {
		// ID is the unique claim identifier.
		ID string `js:"id"`
		// Key is the claimed user key.
		Key string `js:"key"`
		// Entry carries the claimed key/value snapshot.
		// Why value here is better:
		// Entry is required, not optional, value avoids nil checks.
		// Keeps JS shape stable (claim.entry always present).
		// Entry is small (key + value), copy cost is tiny.
		// Avoids extra allocation/indirection (*Entry gives no practical win here).
		// So this is not a "stack is endless" issue, it’s mostly API-shape + simplicity.
		Entry Entry `js:"entry"`
		// Owner optionally identifies the logical lease owner (diagnostics only).
		Owner string `js:"owner,omitempty"`
		// Token is a monotonically increasing fence token for stale-holder protection.
		Token int64 `js:"token"`
		// ExpiresAt is Unix epoch milliseconds when the lease expires.
		ExpiresAt int64 `js:"expiresAt"`
	}

	// ClaimRef identifies a claim for release/complete operations.
	ClaimRef struct {
		// ID is the unique claim identifier.
		ID string
		// Key is the claimed user key.
		Key string
		// Token is a monotonically increasing fence token for stale-holder protection.
		Token int64
	}

	// ClaimOptions configures ClaimRandom selection and lease behavior.
	ClaimOptions struct {
		// Prefix is the key prefix to filter for random selection.
		Prefix string
		// Owner is an optional logical owner identifier for diagnostics.
		Owner string
		// TTLMs is the lease duration in milliseconds.
		TTLMs int64
	}

	// CompleteClaimOptions configures CompleteClaim behavior.
	CompleteClaimOptions struct {
		// DeleteKey controls whether the underlying key is removed on completion.
		DeleteKey bool
	}

	// RenewClaimOptions configures lease renewal behavior.
	RenewClaimOptions struct {
		// TTLMs is the new lease duration in milliseconds.
		TTLMs int64
	}

	// ClaimManyOptions configures batch claim allocation behavior.
	ClaimManyOptions struct {
		// Prefix is the key prefix to filter random selection.
		Prefix string
		// Count is the maximum number of claims to allocate.
		Count int64
		// Owner is an optional logical owner identifier for diagnostics.
		Owner string
		// TTLMs is the lease duration in milliseconds for each claim.
		TTLMs int64
	}
)

// newEntryClaimFromBytes creates a new entry claim from bytes.
func newEntryClaimFromBytes(
	store *DiskStore,
	key string,
	value []byte,
	opts *ClaimOptions,
	now int64,
) *EntryClaim {
	token := store.claimToken.Add(1)

	return &EntryClaim{
		ID:  claimIDFromToken(token),
		Key: key,
		Entry: Entry{
			Key:   key,
			Value: slices.Clone(value),
		},
		Owner:     opts.Owner,
		Token:     token,
		ExpiresAt: now + opts.TTLMs,
	}
}

// claimIDFromToken creates a claim identifier from a token.
func claimIDFromToken(token int64) string {
	return "claim-" + strconv.FormatInt(token, 10)
}

// normalizeClaimOptions applies safe defaults to claim options.
func normalizeClaimOptions(opts *ClaimOptions) *ClaimOptions {
	if opts == nil {
		return &ClaimOptions{
			TTLMs: DefaultClaimTTLMs,
		}
	}

	normalized := &ClaimOptions{
		Prefix: opts.Prefix,
		Owner:  opts.Owner,
		TTLMs:  opts.TTLMs,
	}

	if normalized.TTLMs <= 0 {
		normalized.TTLMs = DefaultClaimTTLMs
	}

	return normalized
}

// validateClaimOptions rejects claim options that would make lease arithmetic unsafe.
func validateClaimOptions(opts *ClaimOptions) error {
	if opts.TTLMs > MaxClaimTTLMs {
		return fmt.Errorf(
			"%w: claim ttl must be less than or equal to %d ms",
			ErrKVOptionsInvalid,
			MaxClaimTTLMs,
		)
	}

	if len(opts.Owner) > MaxClaimOwnerBytes {
		return fmt.Errorf(
			"%w: claim owner must be less than or equal to %d bytes",
			ErrKVOptionsInvalid,
			MaxClaimOwnerBytes,
		)
	}

	return nil
}

// validateRenewClaimOptions validates explicit claim renewal options.
func validateRenewClaimOptions(opts *RenewClaimOptions) error {
	if opts == nil {
		return fmt.Errorf("%w: renewClaim options are required", ErrKVOptionsInvalid)
	}

	if opts.TTLMs <= 0 {
		return fmt.Errorf("%w: renewClaim ttl must be positive", ErrKVOptionsInvalid)
	}

	if opts.TTLMs > MaxClaimTTLMs {
		return fmt.Errorf(
			"%w: renewClaim ttl must be less than or equal to %d ms",
			ErrKVOptionsInvalid,
			MaxClaimTTLMs,
		)
	}

	return nil
}

// normalizeClaimManyOptions applies defaults for batch claim allocation behavior.
func normalizeClaimManyOptions(opts *ClaimManyOptions) *ClaimManyOptions {
	if opts == nil {
		return &ClaimManyOptions{
			Count: 0,
			TTLMs: DefaultClaimTTLMs,
		}
	}

	normalized := &ClaimManyOptions{
		Prefix: opts.Prefix,
		Count:  opts.Count,
		Owner:  opts.Owner,
		TTLMs:  opts.TTLMs,
	}

	if normalized.TTLMs <= 0 {
		normalized.TTLMs = DefaultClaimTTLMs
	}

	return normalized
}

// validateClaimManyOptions validates claim many options.
func validateClaimManyOptions(opts *ClaimManyOptions) error {
	if opts == nil {
		return fmt.Errorf("%w: claimRandomMany options are required", ErrKVOptionsInvalid)
	}

	if opts.Count <= 0 {
		return fmt.Errorf("%w: claimRandomMany count must be positive", ErrKVOptionsInvalid)
	}

	if opts.Count > MaxRandomKeysCount {
		return fmt.Errorf(
			"%w: claimRandomMany count must be less than or equal to %d",
			ErrKVOptionsInvalid,
			MaxRandomKeysCount,
		)
	}

	if err := validateClaimOptions(&ClaimOptions{
		Prefix: opts.Prefix,
		Owner:  opts.Owner,
		TTLMs:  opts.TTLMs,
	}); err != nil {
		return err
	}

	return nil
}

// normalizeCompleteClaimOptions applies defaults for completion behavior.
func normalizeCompleteClaimOptions(opts *CompleteClaimOptions) *CompleteClaimOptions {
	if opts == nil {
		return &CompleteClaimOptions{
			DeleteKey: true,
		}
	}

	return &CompleteClaimOptions{
		DeleteKey: opts.DeleteKey,
	}
}

// isValidClaimRef checks if a claim reference is valid.
func isValidClaimRef(ref *ClaimRef) bool {
	return ref != nil && ref.ID != "" && ref.Key != "" && ref.Token > 0
}

// Ref returns the release/complete reference for this claim.
func (c *EntryClaim) Ref() *ClaimRef {
	return &ClaimRef{
		ID:    c.ID,
		Key:   c.Key,
		Token: c.Token,
	}
}
