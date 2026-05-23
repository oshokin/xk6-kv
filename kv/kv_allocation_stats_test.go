package kv

import (
	"testing"

	"go.k6.io/k6/js/modulestest"

	"github.com/oshokin/xk6-kv/kv/store"
)

// TestKVAsync_AllocationStats_ResolvesSnapshot verifies that kv async allocation stats resolves snapshot.
func TestKVAsync_AllocationStats_ResolvesSnapshot(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		__kv.setMany({
			"users:1": "u1",
			"users:2": "u2",
			"orders:1": "o10"
		})
			.then(() => __kv.claimKey("users:1", { ttl: 30000 }))
			.then((claim) => {
				if (!claim) {
					throw new Error("missing claim");
				}
				return __kv.allocationStats({ prefix: "users:" });
			})
			.then((stats) => {
				if (stats.prefix !== "users:") {
					throw new Error("wrong prefix");
				}
				if (stats.total !== 2) {
					throw new Error("wrong total");
				}
				if (stats.claimable !== 1) {
					throw new Error("wrong claimable");
				}
				if (stats.claimedLive !== 1) {
					throw new Error("wrong claimedLive");
				}
				if (stats.claimedExpired !== 0) {
					throw new Error("wrong claimedExpired");
				}
				if (stats.backend !== "memory") {
					throw new Error("wrong backend");
				}
				if (stats.trackKeys !== true) {
					throw new Error("wrong trackKeys");
				}
			});
	`)
}

// TestKVAsync_AllocationStats_InvalidOptionsRejectsPromise verifies that kv async allocation stats invalid options rejects its promise.
func TestKVAsync_AllocationStats_InvalidOptionsRejectsPromise(t *testing.T) {
	t.Parallel()

	runtime := modulestest.NewRuntime(t)
	kv := NewKV(runtime.VU, store.NewMemoryStore(&store.MemoryConfig{TrackKeys: true}))

	runKVScript(t, runtime, kv, `
		function expectInvalidOptions(promise, label) {
			return promise
				.then(() => {
					throw new Error(label + ": expected rejection");
				})
				.catch((err) => {
					if (!err || err.name !== "InvalidOptionsError") {
						throw new Error(label + ": unexpected error class: " + String(err && err.name));
					}
				});
		}

		Promise.all([
			expectInvalidOptions(__kv.allocationStats("bad"), "allocationStats.options"),
			expectInvalidOptions(__kv.allocationStats({ prefix: 1 }), "allocationStats.options.prefix")
		]);
	`)
}
