package kv

import (
	"fmt"
	"strconv"

	"github.com/grafana/sobek"
	"go.k6.io/k6/js/common"

	"github.com/oshokin/xk6-kv/kv/store"
)

type (
	// popRandomOptions holds parsed options for the corresponding KV method.
	popRandomOptions struct {
		// Prefix selects only keys that start with the given string.
		Prefix string `js:"prefix"`
	}

	// popRandomManyOptions holds parsed options for the corresponding KV method.
	popRandomManyOptions struct {
		// Prefix selects only keys that start with the given string.
		Prefix string `js:"prefix"`
		// Count is the number of keys or claims to allocate.
		Count int64 `js:"count"`
	}

	// claimRandomOptions holds parsed options for the corresponding KV method.
	claimRandomOptions struct {
		// Prefix selects only keys that start with the given string.
		Prefix string `js:"prefix"`
		// Owner is the claim owner identifier.
		Owner string `js:"owner"`
		// TTLMs is the claim lease duration in milliseconds.
		TTLMs int64 `js:"ttl"`
	}

	// claimKeyOptions holds parsed options for the corresponding KV method.
	claimKeyOptions struct {
		// Owner is the claim owner identifier.
		Owner string `js:"owner"`
		// TTLMs is the claim lease duration in milliseconds.
		TTLMs int64 `js:"ttl"`
	}

	// claimRandomManyOptions holds parsed options for the corresponding KV method.
	claimRandomManyOptions struct {
		// Prefix selects only keys that start with the given string.
		Prefix string `js:"prefix"`
		// Count is the number of keys or claims to allocate.
		Count int64 `js:"count"`
		// Owner is the claim owner identifier.
		Owner string `js:"owner"`
		// TTLMs is the claim lease duration in milliseconds.
		TTLMs int64 `js:"ttl"`
	}

	// claimKeysOptions holds parsed options for the corresponding KV method.
	claimKeysOptions struct {
		// Owner is the claim owner identifier.
		Owner string `js:"owner"`
		// TTLMs is the claim lease duration in milliseconds.
		TTLMs int64 `js:"ttl"`
		// AllOrNothing rolls back acquired claims and stops on first busy/missing key.
		AllOrNothing bool `js:"allOrNothing"`
	}

	// renewClaimOptions holds parsed options for the corresponding KV method.
	renewClaimOptions struct {
		// TTLMs is the claim lease duration in milliseconds.
		TTLMs int64 `js:"ttl"`
	}

	// claimRefPayload holds claim reference fields imported from JavaScript.
	claimRefPayload struct {
		// ID is the stable claim identifier.
		ID string `js:"id"`
		// Key is the store key associated with the claim or entry.
		Key string `js:"key"`
		// Token is the opaque claim token required for renew, release, or complete.
		Token int64 `js:"token"`
	}

	// completeClaimOptions holds parsed options for the corresponding KV method.
	completeClaimOptions struct {
		// DeleteKey removes the underlying key when completing a claim.
		DeleteKey bool `js:"deleteKey"`
	}

	// claimBatchFailure describes a single failed item in a batch claim operation.
	claimBatchFailure struct {
		// Index is the zero-based position of the failed item in the input batch.
		Index int64 `js:"index"`
		// ID is the stable claim identifier.
		ID string `js:"id"`
		// Key is the store key associated with the claim or entry.
		Key string `js:"key"`
		// Name is the machine-readable error name.
		Name string `js:"name"`
		// Message is a human-readable error description.
		Message string `js:"message"`
	}

	// releaseClaimsResult is the Go-side result returned by the corresponding KV operation.
	releaseClaimsResult struct {
		// Attempted is the number of batch items processed.
		Attempted int64 `js:"attempted"`
		// Released is the number of batch items that succeeded.
		Released int64 `js:"released"`
		// Failed lists per-item failures from a batch claim operation.
		Failed []claimBatchFailure `js:"failed"`
	}

	// completeClaimsResult is the Go-side result returned by the corresponding KV operation.
	completeClaimsResult struct {
		// Attempted is the number of batch items processed.
		Attempted int64 `js:"attempted"`
		// Completed is the number of batch items that succeeded.
		Completed int64 `js:"completed"`
		// Failed lists per-item failures from a batch claim operation.
		Failed []claimBatchFailure `js:"failed"`
	}

	// renewClaimsResult is the Go-side result returned by the corresponding KV operation.
	renewClaimsResult struct {
		// Attempted is the number of batch items processed.
		Attempted int64 `js:"attempted"`
		// Renewed is the number of batch items that succeeded.
		Renewed int64 `js:"renewed"`
		// Failed lists per-item failures from a batch claim operation.
		Failed []claimBatchFailure `js:"failed"`
	}

	// claimKeysResult is the Go-side result returned by the corresponding KV operation.
	claimKeysResult struct {
		// Claimed is the number of batch items that succeeded.
		Claimed []*store.EntryClaim `js:"claimed"`
		// Busy lists keys that were already claimed by another owner.
		Busy []string `js:"busy"`
		// Missing lists keys that were not present in the store.
		Missing []string `js:"missing"`
	}
)

// importPopRandomOptions parses Sobek options for the corresponding KV method.
func importPopRandomOptions(rt *sobek.Runtime, options sobek.Value) (*popRandomOptions, error) {
	if err := ensureOptionalObjectOptions("popRandom", options); err != nil {
		return nil, err
	}

	result := &popRandomOptions{}
	if common.IsNullish(options) {
		return result, nil
	}

	optionsObj := options.ToObject(rt)

	prefix, isSet, err := parseOptionalStringOption("popRandom", "prefix", optionsObj.Get("prefix"))
	if err != nil {
		return nil, err
	}

	if isSet {
		result.Prefix = prefix
	}

	return result, nil
}

// importPopRandomManyOptions parses Sobek options for the corresponding KV method.
func importPopRandomManyOptions(rt *sobek.Runtime, options sobek.Value) (*popRandomManyOptions, error) {
	result := &popRandomManyOptions{}

	if err := ensureOptionalObjectOptions("popRandomMany", options); err != nil {
		return nil, err
	}

	if common.IsNullish(options) {
		return nil, NewError(
			InvalidOptionsError,
			"popRandomMany count is required",
		)
	}

	optionsObj := options.ToObject(rt)

	prefix, isSet, err := parseOptionalStringOption("popRandomMany", "prefix", optionsObj.Get("prefix"))
	if err != nil {
		return nil, err
	}

	if isSet {
		result.Prefix = prefix
	}

	count, isSet, err := parseOptionalInt64Option("popRandomMany", "count", optionsObj.Get("count"))
	if err != nil {
		return nil, err
	}

	if !isSet {
		return nil, NewError(
			InvalidOptionsError,
			"popRandomMany count is required",
		)
	}

	if count <= 0 {
		return nil, NewError(
			InvalidOptionsError,
			"popRandomMany count must be a positive integer",
		)
	}

	if count > store.MaxRandomKeysCount {
		return nil, NewError(
			InvalidOptionsError,
			fmt.Sprintf("popRandomMany count must be less than or equal to %d", store.MaxRandomKeysCount),
		)
	}

	result.Count = count

	return result, nil
}

// importClaimRandomOptions parses Sobek options for the corresponding KV method.
func importClaimRandomOptions(rt *sobek.Runtime, options sobek.Value) (*claimRandomOptions, error) {
	if err := ensureOptionalObjectOptions("claimRandom", options); err != nil {
		return nil, err
	}

	result := &claimRandomOptions{
		TTLMs: store.DefaultClaimTTLMs,
	}
	if common.IsNullish(options) {
		return result, nil
	}

	optionsObj := options.ToObject(rt)

	prefix, isSet, err := parseOptionalStringOption("claimRandom", "prefix", optionsObj.Get("prefix"))
	if err != nil {
		return nil, err
	}

	if isSet {
		result.Prefix = prefix
	}

	owner, isSet, err := parseOptionalStringOption("claimRandom", "owner", optionsObj.Get("owner"))
	if err != nil {
		return nil, err
	}

	if isSet {
		if len(owner) > store.MaxClaimOwnerBytes {
			return nil, NewError(
				InvalidOptionsError,
				fmt.Sprintf(
					"claimRandom options.owner must be less than or equal to %d bytes",
					store.MaxClaimOwnerBytes,
				),
			)
		}

		result.Owner = owner
	}

	ttl, isSet, err := parseOptionalInt64Option("claimRandom", "ttl", optionsObj.Get("ttl"))
	if err != nil {
		return nil, err
	}

	if isSet {
		if ttl <= 0 {
			return nil, NewError(
				InvalidOptionsError,
				"claimRandom options.ttl must be a positive integer",
			)
		}

		if err := rejectIfAbove("claimRandom", "ttl", ttl, store.MaxClaimTTLMs); err != nil {
			return nil, err
		}

		result.TTLMs = ttl
	}

	return result, nil
}

// importClaimKeyOptions parses Sobek options for the corresponding KV method.
func importClaimKeyOptions(rt *sobek.Runtime, options sobek.Value) (*claimKeyOptions, error) {
	if err := ensureOptionalObjectOptions("claimKey", options); err != nil {
		return nil, err
	}

	result := &claimKeyOptions{
		TTLMs: store.DefaultClaimTTLMs,
	}
	if common.IsNullish(options) {
		return result, nil
	}

	optionsObj := options.ToObject(rt)

	owner, isSet, err := parseOptionalStringOption("claimKey", "owner", optionsObj.Get("owner"))
	if err != nil {
		return nil, err
	}

	if isSet {
		if len(owner) > store.MaxClaimOwnerBytes {
			return nil, NewError(
				InvalidOptionsError,
				fmt.Sprintf(
					"claimKey options.owner must be less than or equal to %d bytes",
					store.MaxClaimOwnerBytes,
				),
			)
		}

		result.Owner = owner
	}

	ttl, isSet, err := parseOptionalInt64Option("claimKey", "ttl", optionsObj.Get("ttl"))
	if err != nil {
		return nil, err
	}

	if isSet {
		if ttl <= 0 {
			return nil, NewError(
				InvalidOptionsError,
				"claimKey options.ttl must be a positive integer",
			)
		}

		if err := rejectIfAbove("claimKey", "ttl", ttl, store.MaxClaimTTLMs); err != nil {
			return nil, err
		}

		result.TTLMs = ttl
	}

	return result, nil
}

// importClaimRandomManyOptions parses Sobek options for the corresponding KV method.
func importClaimRandomManyOptions(rt *sobek.Runtime, options sobek.Value) (*claimRandomManyOptions, error) {
	result := &claimRandomManyOptions{
		TTLMs: store.DefaultClaimTTLMs,
	}

	if err := ensureOptionalObjectOptions("claimRandomMany", options); err != nil {
		return nil, err
	}

	if common.IsNullish(options) {
		return nil, NewError(
			InvalidOptionsError,
			"claimRandomMany count is required",
		)
	}

	optionsObj := options.ToObject(rt)

	prefix, isSet, err := parseOptionalStringOption("claimRandomMany", "prefix", optionsObj.Get("prefix"))
	if err != nil {
		return nil, err
	}

	if isSet {
		result.Prefix = prefix
	}

	count, err := parseRequiredClaimRandomManyCount(optionsObj)
	if err != nil {
		return nil, err
	}

	result.Count = count

	if err := applyOptionalClaimRandomManyClaimOptions(result, optionsObj); err != nil {
		return nil, err
	}

	return result, nil
}

// parseRequiredClaimRandomManyCount parses and validates a single options field.
func parseRequiredClaimRandomManyCount(optionsObj *sobek.Object) (int64, error) {
	count, isSet, err := parseOptionalInt64Option("claimRandomMany", "count", optionsObj.Get("count"))
	if err != nil {
		return 0, err
	}

	if !isSet {
		return 0, NewError(
			InvalidOptionsError,
			"claimRandomMany count is required",
		)
	}

	if count <= 0 {
		return 0, NewError(
			InvalidOptionsError,
			"claimRandomMany count must be a positive integer",
		)
	}

	if count > store.MaxRandomKeysCount {
		return 0, NewError(
			InvalidOptionsError,
			fmt.Sprintf("claimRandomMany count must be less than or equal to %d", store.MaxRandomKeysCount),
		)
	}

	return count, nil
}

// applyOptionalClaimRandomManyClaimOptions is an internal helper.
func applyOptionalClaimRandomManyClaimOptions(result *claimRandomManyOptions, optionsObj *sobek.Object) error {
	owner, isSet, err := parseOptionalStringOption("claimRandomMany", "owner", optionsObj.Get("owner"))
	if err != nil {
		return err
	}

	if isSet {
		if len(owner) > store.MaxClaimOwnerBytes {
			return NewError(
				InvalidOptionsError,
				fmt.Sprintf(
					"claimRandomMany options.owner must be less than or equal to %d bytes",
					store.MaxClaimOwnerBytes,
				),
			)
		}

		result.Owner = owner
	}

	ttl, isSet, err := parseOptionalInt64Option("claimRandomMany", "ttl", optionsObj.Get("ttl"))
	if err != nil {
		return err
	}

	if !isSet {
		return nil
	}

	if ttl <= 0 {
		return NewError(
			InvalidOptionsError,
			"claimRandomMany options.ttl must be a positive integer",
		)
	}

	if err := rejectIfAbove("claimRandomMany", "ttl", ttl, store.MaxClaimTTLMs); err != nil {
		return err
	}

	result.TTLMs = ttl

	return nil
}

// importClaimRefPayload imports claim or batch payload values from JavaScript.
func importClaimRefPayload(rt *sobek.Runtime, method string, claim sobek.Value) (*claimRefPayload, error) {
	if common.IsNullish(claim) {
		return nil, NewError(
			InvalidOptionsError,
			method+" claim must be an object; got null or undefined",
		)
	}

	claimObj := claim.ToObject(rt)

	id, err := parseRequiredNonEmptyStringArg(method, "claim.id", claimObj.Get("id"))
	if err != nil {
		return nil, err
	}

	key, err := parseRequiredNonEmptyStringArg(method, "claim.key", claimObj.Get("key"))
	if err != nil {
		return nil, err
	}

	tokenValue := claimObj.Get("token")
	if common.IsNullish(tokenValue) {
		return nil, NewError(
			InvalidOptionsError,
			method+" claim.token must be a number; got null or undefined",
		)
	}

	token, err := exportToInt64(tokenValue)
	if err != nil {
		return nil, NewError(
			InvalidOptionsError,
			fmt.Sprintf("%s claim.token must be a number: %v", method, err),
		)
	}

	if token <= 0 {
		return nil, NewError(
			InvalidOptionsError,
			fmt.Sprintf("%s claim.token must be a positive integer; got %d", method, token),
		)
	}

	return &claimRefPayload{
		ID:    id,
		Key:   key,
		Token: token,
	}, nil
}

// importClaimRefsPayload imports claim or batch payload values from JavaScript.
func importClaimRefsPayload(rt *sobek.Runtime, method string, claims sobek.Value) ([]*store.ClaimRef, error) {
	if common.IsNullish(claims) {
		return nil, NewError(
			InvalidOptionsError,
			method+" claims must be an array; got null or undefined",
		)
	}

	exported := claims.Export()

	if refs, handled, err := importNativeClaimRefsPayload(method, exported); handled || err != nil {
		return refs, err
	}

	var items []any

	switch typedItems := exported.(type) {
	case []any:
		items = typedItems
	case []map[string]any:
		items = make([]any, len(typedItems))
		for i := range typedItems {
			items[i] = typedItems[i]
		}
	default:
		return nil, NewError(
			InvalidOptionsError,
			fmt.Sprintf("%s claims must be an array; got %T", method, exported),
		)
	}

	refs := make([]*store.ClaimRef, 0, len(items))
	for i, item := range items {
		itemMethod := method + " claims[" + strconv.Itoa(i) + "]"

		payload, err := importClaimRefPayload(rt, itemMethod, rt.ToValue(item))
		if err != nil {
			return nil, err
		}

		refs = append(refs, &store.ClaimRef{
			ID:    payload.ID,
			Key:   payload.Key,
			Token: payload.Token,
		})
	}

	return refs, nil
}

// importNativeClaimRefsPayload imports claim or batch payload values from JavaScript.
func importNativeClaimRefsPayload(method string, exported any) ([]*store.ClaimRef, bool, error) {
	switch typedItems := exported.(type) {
	case []*store.ClaimRef:
		refs, err := importStoreClaimRefPointers(method, typedItems)

		return refs, true, err
	case []store.ClaimRef:
		refs, err := importStoreClaimRefValues(method, typedItems)

		return refs, true, err
	case []*store.EntryClaim:
		refs, err := importEntryClaimPointers(method, typedItems)

		return refs, true, err
	case []store.EntryClaim:
		refs, err := importEntryClaimValues(method, typedItems)

		return refs, true, err
	default:
		return nil, false, nil
	}
}

// importStoreClaimRefPointers is an internal helper.
func importStoreClaimRefPointers(method string, refsInput []*store.ClaimRef) ([]*store.ClaimRef, error) {
	refs := make([]*store.ClaimRef, 0, len(refsInput))

	for i, ref := range refsInput {
		itemMethod := claimRefItemMethod(method, i)
		if err := validateStoreClaimRef(itemMethod, ref); err != nil {
			return nil, err
		}

		refs = append(refs, cloneStoreClaimRef(ref))
	}

	return refs, nil
}

// importStoreClaimRefValues is an internal helper.
func importStoreClaimRefValues(method string, refsInput []store.ClaimRef) ([]*store.ClaimRef, error) {
	refs := make([]*store.ClaimRef, 0, len(refsInput))

	for i := range refsInput {
		ref := refsInput[i]
		if err := validateStoreClaimRef(claimRefItemMethod(method, i), &ref); err != nil {
			return nil, err
		}

		refs = append(refs, cloneStoreClaimRef(&ref))
	}

	return refs, nil
}

// importEntryClaimPointers is an internal helper.
func importEntryClaimPointers(method string, claimsInput []*store.EntryClaim) ([]*store.ClaimRef, error) {
	refs := make([]*store.ClaimRef, 0, len(claimsInput))

	for i, claim := range claimsInput {
		itemMethod := claimRefItemMethod(method, i)
		if claim == nil {
			return nil, validateStoreClaimRef(itemMethod, nil)
		}

		ref := claim.Ref()
		if err := validateStoreClaimRef(itemMethod, ref); err != nil {
			return nil, err
		}

		refs = append(refs, cloneStoreClaimRef(ref))
	}

	return refs, nil
}

// importEntryClaimValues is an internal helper.
func importEntryClaimValues(method string, claimsInput []store.EntryClaim) ([]*store.ClaimRef, error) {
	refs := make([]*store.ClaimRef, 0, len(claimsInput))

	for i := range claimsInput {
		ref := claimsInput[i].Ref()
		if err := validateStoreClaimRef(claimRefItemMethod(method, i), ref); err != nil {
			return nil, err
		}

		refs = append(refs, cloneStoreClaimRef(ref))
	}

	return refs, nil
}

// claimRefItemMethod is an internal helper.
func claimRefItemMethod(method string, index int) string {
	return method + " claims[" + strconv.Itoa(index) + "]"
}

// cloneStoreClaimRef is an internal helper.
func cloneStoreClaimRef(ref *store.ClaimRef) *store.ClaimRef {
	return &store.ClaimRef{
		ID:    ref.ID,
		Key:   ref.Key,
		Token: ref.Token,
	}
}

// validateStoreClaimRef validates user-supplied input.
func validateStoreClaimRef(method string, ref *store.ClaimRef) error {
	if ref == nil {
		return NewError(
			InvalidOptionsError,
			method+" claim must be an object; got null",
		)
	}

	if ref.ID == "" {
		return NewError(
			InvalidOptionsError,
			method+" claim.id must be a non-empty string",
		)
	}

	if ref.Key == "" {
		return NewError(
			InvalidOptionsError,
			method+" claim.key must be a non-empty string",
		)
	}

	if ref.Token <= 0 {
		return NewError(
			InvalidOptionsError,
			fmt.Sprintf("%s claim.token must be a positive integer; got %d", method, ref.Token),
		)
	}

	return nil
}

// importClaimKeysPayload imports claim or batch payload values from JavaScript.
func importClaimKeysPayload(_ *sobek.Runtime, keys sobek.Value) ([]string, error) {
	if common.IsNullish(keys) {
		return nil, NewError(
			InvalidOptionsError,
			"claimKeys keys must be an array; got null or undefined",
		)
	}

	exported := keys.Export()

	var keyItems []any

	switch typed := exported.(type) {
	case []any:
		keyItems = typed
	case []string:
		keyItems = make([]any, len(typed))
		for i := range typed {
			keyItems[i] = typed[i]
		}
	default:
		return nil, NewError(
			InvalidOptionsError,
			fmt.Sprintf("claimKeys keys must be an array; got %T", exported),
		)
	}

	parsed := make([]string, 0, len(keyItems))
	seen := make(map[string]struct{}, len(keyItems))

	for i, item := range keyItems {
		key, ok := item.(string)
		if !ok {
			return nil, NewError(
				InvalidOptionsError,
				fmt.Sprintf("claimKeys keys[%d] must be a string; got %T", i, item),
			)
		}

		if key == "" {
			return nil, NewError(
				InvalidOptionsError,
				fmt.Sprintf("claimKeys keys[%d] must be a non-empty string", i),
			)
		}

		if _, exists := seen[key]; exists {
			return nil, NewError(
				InvalidOptionsError,
				fmt.Sprintf("claimKeys keys contains duplicate key %q", key),
			)
		}

		seen[key] = struct{}{}
		parsed = append(parsed, key)
	}

	return parsed, nil
}

// importClaimKeysOptions parses Sobek options for the corresponding KV method.
func importClaimKeysOptions(rt *sobek.Runtime, options sobek.Value) (*claimKeysOptions, error) {
	if err := ensureOptionalObjectOptions("claimKeys", options); err != nil {
		return nil, err
	}

	result := &claimKeysOptions{
		TTLMs: store.DefaultClaimTTLMs,
	}
	if common.IsNullish(options) {
		return result, nil
	}

	optionsObj := options.ToObject(rt)

	owner, isSet, err := parseOptionalStringOption("claimKeys", "owner", optionsObj.Get("owner"))
	if err != nil {
		return nil, err
	}

	if isSet {
		if len(owner) > store.MaxClaimOwnerBytes {
			return nil, NewError(
				InvalidOptionsError,
				fmt.Sprintf("claimKeys options.owner must be less than or equal to %d bytes", store.MaxClaimOwnerBytes),
			)
		}

		result.Owner = owner
	}

	ttl, ttlSet, err := parseOptionalInt64Option("claimKeys", "ttl", optionsObj.Get("ttl"))
	if err != nil {
		return nil, err
	}

	if ttlSet {
		if ttl <= 0 {
			return nil, NewError(
				InvalidOptionsError,
				"claimKeys options.ttl must be a positive integer",
			)
		}

		if err := rejectIfAbove("claimKeys", "ttl", ttl, store.MaxClaimTTLMs); err != nil {
			return nil, err
		}

		result.TTLMs = ttl
	}

	allOrNothing, allOrNothingSet, err := parseOptionalBoolOption(
		"claimKeys",
		"allOrNothing",
		optionsObj.Get("allOrNothing"),
	)
	if err != nil {
		return nil, err
	}

	if allOrNothingSet {
		result.AllOrNothing = allOrNothing
	}

	return result, nil
}

// importCompleteClaimOptions parses Sobek options for the corresponding KV method.
func importCompleteClaimOptions(rt *sobek.Runtime, options sobek.Value) (*completeClaimOptions, error) {
	return importCompleteClaimOptionsWithMethod(rt, "completeClaim", options)
}

// importCompleteClaimsOptions parses Sobek options for the corresponding KV method.
func importCompleteClaimsOptions(rt *sobek.Runtime, options sobek.Value) (*completeClaimOptions, error) {
	return importCompleteClaimOptionsWithMethod(rt, "completeClaims", options)
}

// importCompleteClaimOptionsWithMethod is an internal helper.
func importCompleteClaimOptionsWithMethod(
	rt *sobek.Runtime,
	method string,
	options sobek.Value,
) (*completeClaimOptions, error) {
	if err := ensureOptionalObjectOptions(method, options); err != nil {
		return nil, err
	}

	result := &completeClaimOptions{
		DeleteKey: true,
	}
	if common.IsNullish(options) {
		return result, nil
	}

	optionsObj := options.ToObject(rt)

	deleteKey, isSet, err := parseOptionalBoolOption(method, "deleteKey", optionsObj.Get("deleteKey"))
	if err != nil {
		return nil, err
	}

	if isSet {
		result.DeleteKey = deleteKey
	}

	return result, nil
}

// importRenewClaimOptions parses Sobek options for the corresponding KV method.
func importRenewClaimOptions(rt *sobek.Runtime, options sobek.Value) (*renewClaimOptions, error) {
	return importRenewClaimOptionsWithMethod(rt, "renewClaim", options)
}

// importRenewClaimsOptions parses Sobek options for the corresponding KV method.
func importRenewClaimsOptions(rt *sobek.Runtime, options sobek.Value) (*renewClaimOptions, error) {
	return importRenewClaimOptionsWithMethod(rt, "renewClaims", options)
}

// importRenewClaimOptionsWithMethod is an internal helper.
func importRenewClaimOptionsWithMethod(
	rt *sobek.Runtime,
	method string,
	options sobek.Value,
) (*renewClaimOptions, error) {
	if err := ensureOptionalObjectOptions(method, options); err != nil {
		return nil, err
	}

	if common.IsNullish(options) {
		return nil, NewError(
			InvalidOptionsError,
			method+" options.ttl is required",
		)
	}

	optionsObj := options.ToObject(rt)

	ttl, ttlSet, err := parseOptionalInt64Option(method, "ttl", optionsObj.Get("ttl"))
	if err != nil {
		return nil, err
	}

	if !ttlSet {
		return nil, NewError(
			InvalidOptionsError,
			method+" options.ttl is required",
		)
	}

	if ttl <= 0 {
		return nil, NewError(
			InvalidOptionsError,
			method+" options.ttl must be a positive integer",
		)
	}

	if err := rejectIfAbove(method, "ttl", ttl, store.MaxClaimTTLMs); err != nil {
		return nil, err
	}

	return &renewClaimOptions{
		TTLMs: ttl,
	}, nil
}
