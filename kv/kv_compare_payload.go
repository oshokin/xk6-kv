package kv

import (
	"github.com/grafana/sobek"
	"go.k6.io/k6/js/common"

	"github.com/oshokin/xk6-kv/kv/store"
)

type (
	// compareAndSwapDetailedSuccessPayload is exported to JS for successful CAS.
	compareAndSwapDetailedSuccessPayload struct {
		// Swapped reports whether CAS mutation was applied.
		Swapped bool `js:"swapped"`
		// Reason describes the CAS outcome reason.
		Reason string `js:"reason"`
	}

	// compareAndSwapDetailedMismatchPayload is exported to JS for CAS mismatch.
	compareAndSwapDetailedMismatchPayload struct {
		// Swapped is false for mismatch payloads.
		Swapped bool `js:"swapped"`
		// Reason describes the CAS outcome reason.
		Reason string `js:"reason"`
		// Existed reports whether the key existed at compare time.
		Existed bool `js:"existed"`
	}

	// compareAndSwapDetailedMismatchWithCurrentPayload includes current on mismatch.
	compareAndSwapDetailedMismatchWithCurrentPayload struct {
		// Swapped is false for mismatch payloads.
		Swapped bool `js:"swapped"`
		// Reason describes the CAS outcome reason.
		Reason string `js:"reason"`
		// Existed reports whether the key existed at compare time.
		Existed bool `js:"existed"`
		// Current carries the observed current value on mismatch.
		Current any `js:"current"`
	}

	// compareAndDeleteDetailedSuccessPayload is exported to JS for successful CAD.
	compareAndDeleteDetailedSuccessPayload struct {
		// Deleted reports whether CAD mutation was applied.
		Deleted bool `js:"deleted"`
		// Reason describes the CAD outcome reason.
		Reason string `js:"reason"`
	}

	// compareAndDeleteDetailedMismatchPayload is exported to JS for CAD mismatch.
	compareAndDeleteDetailedMismatchPayload struct {
		// Deleted is false for mismatch payloads.
		Deleted bool `js:"deleted"`
		// Reason describes the CAD outcome reason.
		Reason string `js:"reason"`
		// Existed reports whether the key existed at compare time.
		Existed bool `js:"existed"`
	}

	// compareAndDeleteDetailedMismatchWithCurrentPayload includes current on mismatch.
	compareAndDeleteDetailedMismatchWithCurrentPayload struct {
		// Deleted is false for mismatch payloads.
		Deleted bool `js:"deleted"`
		// Reason describes the CAD outcome reason.
		Reason string `js:"reason"`
		// Existed reports whether the key existed at compare time.
		Existed bool `js:"existed"`
		// Current carries the observed current value on mismatch.
		Current any `js:"current"`
	}
)

// compareAndSwapDetailedPayload converts store CAS detailed result to JS payload.
func compareAndSwapDetailedPayload(detailed *store.CompareAndSwapDetailedResult) any {
	if detailed.Swapped {
		return compareAndSwapDetailedSuccessPayload{
			Swapped: true,
			Reason:  detailed.Reason,
		}
	}

	if detailed.ShouldIncludeCurrent() {
		return compareAndSwapDetailedMismatchWithCurrentPayload{
			Swapped: false,
			Reason:  detailed.Reason,
			Existed: detailed.Existed,
			Current: detailed.Current,
		}
	}

	return compareAndSwapDetailedMismatchPayload{
		Swapped: false,
		Reason:  detailed.Reason,
		Existed: detailed.Existed,
	}
}

// compareAndDeleteDetailedPayload converts store CAD detailed result to JS payload.
func compareAndDeleteDetailedPayload(detailed *store.CompareAndDeleteDetailedResult) any {
	if detailed.Deleted {
		return compareAndDeleteDetailedSuccessPayload{
			Deleted: true,
			Reason:  detailed.Reason,
		}
	}

	if detailed.ShouldIncludeCurrent() {
		return compareAndDeleteDetailedMismatchWithCurrentPayload{
			Deleted: false,
			Reason:  detailed.Reason,
			Existed: detailed.Existed,
			Current: detailed.Current,
		}
	}

	return compareAndDeleteDetailedMismatchPayload{
		Deleted: false,
		Reason:  detailed.Reason,
		Existed: detailed.Existed,
	}
}

// importIncludeCurrentOnMismatchOption reads { includeCurrentOnMismatch?: boolean }.
// Null/undefined/missing values default to false.
func importIncludeCurrentOnMismatchOption(rt *sobek.Runtime, options sobek.Value) bool {
	if common.IsNullish(options) {
		return false
	}

	include := options.ToObject(rt).Get("includeCurrentOnMismatch")
	if common.IsNullish(include) {
		return false
	}

	return include.ToBoolean()
}
