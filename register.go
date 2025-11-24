package kv

import (
	"go.k6.io/k6/js/modules"

	"github.com/oshokin/xk6-kv/kv"
)

// init registers the kv module with the k6 runtime.
func init() {
	modules.Register("k6/x/kv", kv.New())
}
