// Package store provides a pluggable, thread-safe key-value storage abstraction
// used by the xk6-kv module. It defines the Store interface and common data
// structures shared by memory- and disk-backed implementations.
//
// Implementations SHOULD be safe for concurrent use by multiple goroutines.
// Unless stated otherwise, methods operate atomically with respect to a single
// key (i.e., Set(), GetOrSet(), Swap(), CompareAndSwap(), Delete*, etc. do not
// interleave their critical sections for the same key).
package store
