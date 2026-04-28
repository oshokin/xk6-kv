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

	claimRandomOptions struct {
		Prefix string `js:"prefix"`
		Owner  string `js:"owner"`
		TTLMs  int64  `js:"ttl"`
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

		result.TTLMs = ttl
	}

	return result, nil
}

func importClaimRefPayload(rt *sobek.Runtime, method string, claim sobek.Value) (*claimRefPayload, error) {
	if common.IsNullish(claim) {
		return nil, NewError(
			InvalidOptionsError,
			method+" claim must be an object; got null or undefined",
		)
	}

	claimObj := claim.ToObject(rt)

	id, err := parseRequiredStringArg(method, "claim.id", claimObj.Get("id"))
	if err != nil {
		return nil, err
	}

	key, err := parseRequiredStringArg(method, "claim.key", claimObj.Get("key"))
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
