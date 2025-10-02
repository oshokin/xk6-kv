// Package kv provides a key-value database shared across all VUs (virtual users).
//
// High-level behavior:
//   - The first call to openKv() lazily initializes a single, shared store for all VUs.
//   - The store is backed by either an in-memory or disk implementation and wrapped with a
//     serializer (JSON or string).
//     The first successful initialization determines the backend + serialization for the entire test run;
//     subsequent calls reuse the same store.
//   - For the disk backend, the data persists across runs; memory backend is ephemeral.
package kv
