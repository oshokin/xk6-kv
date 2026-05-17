package kv

import (
	"fmt"

	"github.com/grafana/sobek"
	"go.k6.io/k6/js/common"

	"github.com/oshokin/xk6-kv/kv/store"
)

type (
	popRandomOptions struct {
		Prefix string `js:"prefix"`
	}

	popRandomManyOptions struct {
		Prefix string `js:"prefix"`
		Count  int64  `js:"count"`
	}

	claimRandomOptions struct {
		Prefix string `js:"prefix"`
		Owner  string `js:"owner"`
		TTLMs  int64  `js:"ttl"`
	}

	claimKeyOptions struct {
		Owner string `js:"owner"`
		TTLMs int64  `js:"ttl"`
	}

	claimRandomManyOptions struct {
		Prefix string `js:"prefix"`
		Count  int64  `js:"count"`
		Owner  string `js:"owner"`
		TTLMs  int64  `js:"ttl"`
	}

	renewClaimOptions struct {
		TTLMs int64 `js:"ttl"`
	}

	claimRefPayload struct {
		ID    string `js:"id"`
		Key   string `js:"key"`
		Token int64  `js:"token"`
	}

	completeClaimOptions struct {
		DeleteKey bool `js:"deleteKey"`
	}
)

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

func importCompleteClaimOptions(rt *sobek.Runtime, options sobek.Value) (*completeClaimOptions, error) {
	if err := ensureOptionalObjectOptions("completeClaim", options); err != nil {
		return nil, err
	}

	result := &completeClaimOptions{
		DeleteKey: true,
	}
	if common.IsNullish(options) {
		return result, nil
	}

	optionsObj := options.ToObject(rt)

	deleteKey, isSet, err := parseOptionalBoolOption("completeClaim", "deleteKey", optionsObj.Get("deleteKey"))
	if err != nil {
		return nil, err
	}

	if isSet {
		result.DeleteKey = deleteKey
	}

	return result, nil
}

func importRenewClaimOptions(rt *sobek.Runtime, options sobek.Value) (*renewClaimOptions, error) {
	if err := ensureOptionalObjectOptions("renewClaim", options); err != nil {
		return nil, err
	}

	if common.IsNullish(options) {
		return nil, NewError(
			InvalidOptionsError,
			"renewClaim options.ttl is required",
		)
	}

	optionsObj := options.ToObject(rt)

	ttl, ttlSet, err := parseOptionalInt64Option("renewClaim", "ttl", optionsObj.Get("ttl"))
	if err != nil {
		return nil, err
	}

	if !ttlSet {
		return nil, NewError(
			InvalidOptionsError,
			"renewClaim options.ttl is required",
		)
	}

	if ttl <= 0 {
		return nil, NewError(
			InvalidOptionsError,
			"renewClaim options.ttl must be a positive integer",
		)
	}

	if err := rejectIfAbove("renewClaim", "ttl", ttl, store.MaxClaimTTLMs); err != nil {
		return nil, err
	}

	return &renewClaimOptions{
		TTLMs: ttl,
	}, nil
}
