package kv

type (
	// getOrSetResult is the JS-facing result of getOrSet().
	getOrSetResult struct {
		// Value is the current or newly stored value.
		Value any `js:"value"`
		// Loaded reports whether the value already existed.
		Loaded bool `js:"loaded"`
	}

	// swapResult is the JS-facing result of swap().
	swapResult struct {
		// Previous is the value observed before swap.
		Previous any `js:"previous"`
		// Loaded reports whether a previous value existed.
		Loaded bool `js:"loaded"`
	}
)
