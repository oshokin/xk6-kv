package store

import "fmt"

const (
	// DefaultClaimTTLMs is the default lease duration used by ClaimRandom.
	DefaultClaimTTLMs int64 = 30_000
	// MaxClaimTTLMs bounds claim leases to avoid accidental forever leases and timestamp overflow.
	MaxClaimTTLMs int64 = 24 * 60 * 60 * 1000
	// MaxClaimOwnerBytes bounds optional diagnostic owner metadata stored with claims.
	MaxClaimOwnerBytes = 256
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
)

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

// Ref returns the release/complete reference for this claim.
func (c *EntryClaim) Ref() *ClaimRef {
	return &ClaimRef{
		ID:    c.ID,
		Key:   c.Key,
		Token: c.Token,
	}
}
