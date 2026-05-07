package store

import (
	"container/heap"
	"fmt"
	"slices"
	"sort"
	"strings"
)

type (
	// trackedShardIterator is the iterator for the tracked shard.
	trackedShardIterator struct {
		// shard is the shard to iterate over.
		shard *memoryShard
		// prefix is the prefix to iterate over.
		prefix string
		// nextAfter is the key to start iterating from.
		nextAfter string
	}

	// untrackedShardIterator streams entries from shards that do not maintain
	// OST indexes by copying bounded batches while holding a read lock.
	// This avoids holding locks for the entire scan duration, improving concurrency.
	untrackedShardIterator struct {
		shard     *memoryShard
		prefix    string
		nextAfter string

		buffer    []Entry
		bufferIdx int
		exhausted bool
	}

	// shardIterator is the interface for the shard iterator.
	shardIterator interface {
		// Next returns the next entry in the iterator.
		Next() (Entry, bool)
	}

	// shardIteratorItem is the item for the shard iterator.
	shardIteratorItem struct {
		// iterator is being used to iterate over the shard.
		iterator shardIterator
		// entry is the current entry in the iterator.
		entry Entry
	}

	// shardIteratorHeap is the heap of shard iterators.
	//nolint:recvcheck // we need to use a pointer to the heap to avoid copying the heap.
	shardIteratorHeap []*shardIteratorItem

	// trackedKeyShardIterator is the key-only iterator for tracked shards.
	trackedKeyShardIterator struct {
		// shard is the shard to iterate over.
		shard *memoryShard
		// prefix is the prefix to iterate over.
		prefix string
		// nextAfter is the key to start iterating from.
		nextAfter string
	}

	// untrackedKeyShardIterator streams keys from shards that do not maintain
	// OST indexes by copying bounded batches while holding a read lock.
	untrackedKeyShardIterator struct {
		shard     *memoryShard
		prefix    string
		nextAfter string

		buffer    []string
		bufferIdx int
		exhausted bool
	}

	// keyShardIterator is the interface for key-only shard iterators.
	keyShardIterator interface {
		// Next returns the next key in the iterator.
		Next() (string, bool)
	}

	// keyShardIteratorItem is the item for the key-only iterator heap.
	keyShardIteratorItem struct {
		// iterator is being used to iterate over the shard.
		iterator keyShardIterator
		// key is the current key in the iterator.
		key string
	}

	// keyShardIteratorHeap is the heap of key-only iterators.
	//nolint:recvcheck // we need to use a pointer to the heap to avoid copying the heap.
	keyShardIteratorHeap []*keyShardIteratorItem
)

// untrackedIteratorBatchSize bounds how many entries we copy while holding
// a shard read lock in the untracked scan path.
// Larger batches reduce lock contention but increase memory usage per scan.
const untrackedIteratorBatchSize = 256

// Scan returns a page of key-value pairs ordered lexicographically.
func (s *MemoryStore) Scan(prefix, afterKey string, limit int64) (*ScanPage, error) {
	if !s.trackKeys {
		return s.scanWithoutTracking(prefix, afterKey, limit)
	}

	return s.scanWithTracking(prefix, afterKey, limit)
}

// ScanKeys returns a page of key names ordered lexicographically
// without cloning, deserializing, or returning values.
func (s *MemoryStore) ScanKeys(prefix, afterKey string, limit int64) (*KeyScanPage, error) {
	if !s.trackKeys {
		return s.scanKeysWithoutTracking(prefix, afterKey, limit)
	}

	return s.scanKeysWithTracking(prefix, afterKey, limit)
}

// Count returns the number of keys matching prefix.
// Count("") is equivalent to Size().
func (s *MemoryStore) Count(prefix string) (int64, error) {
	if prefix == "" {
		return s.Size()
	}

	var total int64

	for _, shard := range s.shards {
		shard.mu.RLock()

		if s.trackKeys && shard.ost != nil {
			left, right := shard.rangeBounds(prefix)
			total += int64(right - left)

			shard.mu.RUnlock()

			continue
		}

		for key := range shard.container {
			if strings.HasPrefix(key, prefix) {
				total++
			}
		}

		shard.mu.RUnlock()
	}

	return total, nil
}

// ListKeys returns keys matching prefix in ascending lexicographic order.
func (s *MemoryStore) ListKeys(prefix string, limit int64) ([]string, error) {
	page, err := s.ScanKeys(prefix, "", limit)
	if err != nil {
		return nil, err
	}

	return page.Keys, nil
}

// scanWithoutTracking streams entries via per-shard iterators without global read locks.
func (s *MemoryStore) scanWithoutTracking(prefix, afterKey string, limit int64) (*ScanPage, error) {
	iteratorHeap := make(shardIteratorHeap, 0, s.shardCount)

	for _, shard := range s.shards {
		iterator := newUntrackedShardIterator(shard, prefix, afterKey)
		if iterator == nil {
			continue
		}

		if entry, ok := iterator.Next(); ok {
			iteratorHeap = append(iteratorHeap, &shardIteratorItem{
				iterator: iterator,
				entry:    entry,
			})
		}
	}

	return s.mergeIteratorHeap(iteratorHeap, limit)
}

// scanWithTracking merges shards using OST indexes without global read locks.
func (s *MemoryStore) scanWithTracking(prefix, afterKey string, limit int64) (*ScanPage, error) {
	iteratorHeap := make(shardIteratorHeap, 0, s.shardCount)

	for _, shard := range s.shards {
		shard.mu.RLock()
		// We need to extract the OST length while holding the lock to avoid race conditions.
		hasTrackedKeys := shard.ost != nil && shard.ost.Len() > 0
		shard.mu.RUnlock()

		if !hasTrackedKeys {
			continue
		}

		iterator := &trackedShardIterator{
			shard:     shard,
			prefix:    prefix,
			nextAfter: afterKey,
		}

		if entry, ok := iterator.Next(); ok {
			iteratorHeap = append(iteratorHeap, &shardIteratorItem{
				iterator: iterator,
				entry:    entry,
			})
		}
	}

	return s.mergeIteratorHeap(iteratorHeap, limit)
}

// scanKeysWithoutTracking streams keys via per-shard iterators without global read locks.
func (s *MemoryStore) scanKeysWithoutTracking(prefix, afterKey string, limit int64) (*KeyScanPage, error) {
	iteratorHeap := make(keyShardIteratorHeap, 0, s.shardCount)

	for _, shard := range s.shards {
		iterator := newUntrackedKeyShardIterator(shard, prefix, afterKey)
		if iterator == nil {
			continue
		}

		if key, ok := iterator.Next(); ok {
			iteratorHeap = append(iteratorHeap, &keyShardIteratorItem{
				iterator: iterator,
				key:      key,
			})
		}
	}

	return s.mergeKeyIteratorHeap(iteratorHeap, limit)
}

// scanKeysWithTracking merges shards using OST indexes without global read locks.
func (s *MemoryStore) scanKeysWithTracking(prefix, afterKey string, limit int64) (*KeyScanPage, error) {
	iteratorHeap := make(keyShardIteratorHeap, 0, s.shardCount)

	for _, shard := range s.shards {
		shard.mu.RLock()
		// We need to extract the OST length while holding the lock to avoid race conditions.
		hasTrackedKeys := shard.ost != nil && shard.ost.Len() > 0
		shard.mu.RUnlock()

		if !hasTrackedKeys {
			continue
		}

		iterator := &trackedKeyShardIterator{
			shard:     shard,
			prefix:    prefix,
			nextAfter: afterKey,
		}

		if key, ok := iterator.Next(); ok {
			iteratorHeap = append(iteratorHeap, &keyShardIteratorItem{
				iterator: iterator,
				key:      key,
			})
		}
	}

	return s.mergeKeyIteratorHeap(iteratorHeap, limit)
}

// mergeIteratorHeap merges the iterator heap into a scan page.
func (s *MemoryStore) mergeIteratorHeap(iteratorHeap shardIteratorHeap, limit int64) (*ScanPage, error) {
	if len(iteratorHeap) == 0 {
		return new(ScanPage), nil
	}

	heap.Init(&iteratorHeap)

	var initialCapacity int
	if limit > 0 {
		initialCapacity = int(limit)
	}

	var (
		entries = make([]Entry, 0, initialCapacity)
		emitted int64
		lastKey string
		nextKey string
	)

	for iteratorHeap.Len() > 0 {
		popped := heap.Pop(&iteratorHeap)

		item, ok := popped.(*shardIteratorItem)
		if !ok {
			return nil, fmt.Errorf("%w: %T", ErrUnexpectedHeapType, popped)
		}

		entries = append(entries, item.entry)
		lastKey = item.entry.Key
		emitted++

		if limit > 0 && emitted >= limit {
			// Check if more entries exist: either in heap or from current iterator.
			hasMore := iteratorHeap.Len() > 0
			if !hasMore {
				// Peek at iterator to see if it has more entries without consuming them.
				if _, ok := item.iterator.Next(); ok {
					hasMore = true
				}
			}

			// Set NextKey to enable pagination continuation.
			if hasMore {
				nextKey = lastKey
			}

			break
		}

		if nextEntry, ok := item.iterator.Next(); ok {
			item.entry = nextEntry
			heap.Push(&iteratorHeap, item)
		}
	}

	return &ScanPage{
		Entries: entries,
		NextKey: nextKey,
	}, nil
}

// mergeKeyIteratorHeap merges the key iterator heap into a scan page.
func (s *MemoryStore) mergeKeyIteratorHeap(iteratorHeap keyShardIteratorHeap, limit int64) (*KeyScanPage, error) {
	if len(iteratorHeap) == 0 {
		return &KeyScanPage{Keys: []string{}}, nil
	}

	heap.Init(&iteratorHeap)

	var initialCapacity int
	if limit > 0 {
		initialCapacity = int(limit)
	}

	var (
		keys    = make([]string, 0, initialCapacity)
		emitted int64
		lastKey string
		nextKey string
	)

	for iteratorHeap.Len() > 0 {
		popped := heap.Pop(&iteratorHeap)

		item, ok := popped.(*keyShardIteratorItem)
		if !ok {
			return nil, fmt.Errorf("%w: %T", ErrUnexpectedHeapType, popped)
		}

		keys = append(keys, item.key)
		lastKey = item.key
		emitted++

		if limit > 0 && emitted >= limit {
			// Check if more keys exist: either in heap or from current iterator.
			hasMore := iteratorHeap.Len() > 0
			if !hasMore {
				// Peek at iterator to see if it has more keys without consuming them.
				if _, ok := item.iterator.Next(); ok {
					hasMore = true
				}
			}

			// Set NextKey to enable pagination continuation.
			if hasMore {
				nextKey = lastKey
			}

			break
		}

		if nextKeyValue, ok := item.iterator.Next(); ok {
			item.key = nextKeyValue
			heap.Push(&iteratorHeap, item)
		}
	}

	return &KeyScanPage{
		Keys:    keys,
		NextKey: nextKey,
	}, nil
}

// Len returns the length of the heap.
func (h shardIteratorHeap) Len() int {
	return len(h)
}

// Less returns true if the entry in the heap is less than the entry in the other heap.
func (h shardIteratorHeap) Less(i, j int) bool {
	return h[i].entry.Key < h[j].entry.Key
}

// Swap swaps the entries in the heap.
func (h shardIteratorHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

// Push pushes a new entry into the heap.
func (h *shardIteratorHeap) Push(x any) {
	item, ok := x.(*shardIteratorItem)
	if !ok {
		// container/heap interface doesn't allow us to return an error,
		// so we panic if the type is unexpected.
		panic(fmt.Errorf("%w: %T", ErrUnexpectedHeapType, x))
	}

	*h = append(*h, item)
}

// Pop pops the last entry from the heap.
// Note: container/heap expects Pop to remove the element at index len-1,
// not the root. The heap package handles reordering after Pop.
func (h *shardIteratorHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]

	return item
}

// Len returns the length of the key heap.
func (h keyShardIteratorHeap) Len() int {
	return len(h)
}

// Less returns true if the key in the heap is less than the key in the other heap.
func (h keyShardIteratorHeap) Less(i, j int) bool {
	return h[i].key < h[j].key
}

// Swap swaps the keys in the heap.
func (h keyShardIteratorHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

// Push pushes a new key item into the heap.
func (h *keyShardIteratorHeap) Push(x any) {
	item, ok := x.(*keyShardIteratorItem)
	if !ok {
		// container/heap interface doesn't allow us to return an error,
		// so we panic if the type is unexpected.
		panic(fmt.Errorf("%w: %T", ErrUnexpectedHeapType, x))
	}

	*h = append(*h, item)
}

// Pop pops the last key item from the heap.
// Note: container/heap expects Pop to remove the element at index len-1,
// not the root. The heap package handles reordering after Pop.
func (h *keyShardIteratorHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]

	return item
}

// Next returns the next entry in the iterator.
func (it *trackedShardIterator) Next() (Entry, bool) {
	entry, ok := it.nextEntry(it.nextAfter)
	if !ok {
		return Entry{}, false
	}

	it.nextAfter = entry.Key

	return entry, true
}

// nextEntry returns the next entry in the iterator.
func (it *trackedShardIterator) nextEntry(after string) (Entry, bool) {
	it.shard.mu.RLock()
	defer it.shard.mu.RUnlock()

	if it.shard.ost == nil || it.shard.ost.Len() == 0 {
		return Entry{}, false
	}

	l, r := it.shard.rangeBounds(it.prefix)
	if l >= r {
		return Entry{}, false
	}

	// Find the starting index accounting for afterKey constraint.
	start := it.shard.startIndex(l, r, after)
	if start >= r {
		return Entry{}, false
	}

	for i := start; i < r; i++ {
		// Use order-statistics tree to get key at position i in sorted order.
		key, ok := it.shard.ost.Kth(i)
		if !ok {
			continue
		}

		// Verify key still exists in container (may have been deleted concurrently).
		value, exists := it.shard.container[key]
		if !exists {
			continue
		}

		return Entry{
			Key:   key,
			Value: slices.Clone(value),
		}, true
	}

	return Entry{}, false
}

// Next returns the next key in the iterator.
func (it *trackedKeyShardIterator) Next() (string, bool) {
	key, ok := it.nextKey(it.nextAfter)
	if !ok {
		return "", false
	}

	it.nextAfter = key

	return key, true
}

// nextKey returns the next key in the iterator.
func (it *trackedKeyShardIterator) nextKey(after string) (string, bool) {
	it.shard.mu.RLock()
	defer it.shard.mu.RUnlock()

	if it.shard.ost == nil || it.shard.ost.Len() == 0 {
		return "", false
	}

	l, r := it.shard.rangeBounds(it.prefix)
	if l >= r {
		return "", false
	}

	// Find the starting index accounting for afterKey constraint.
	start := it.shard.startIndex(l, r, after)
	if start >= r {
		return "", false
	}

	for i := start; i < r; i++ {
		key, ok := it.shard.ost.Kth(i)
		if !ok {
			continue
		}

		// Defensive check: do not return stale-positive keys if index and map diverged.
		if _, exists := it.shard.container[key]; !exists {
			continue
		}

		return key, true
	}

	return "", false
}

// newUntrackedShardIterator creates a streaming iterator for shards without OSTs.
func newUntrackedShardIterator(shard *memoryShard, prefix, afterKey string) *untrackedShardIterator {
	iterator := &untrackedShardIterator{
		shard:     shard,
		prefix:    prefix,
		nextAfter: afterKey,
	}

	if !iterator.fillBuffer() {
		return nil
	}

	return iterator
}

// newUntrackedKeyShardIterator creates a streaming key-only iterator for shards without OSTs.
func newUntrackedKeyShardIterator(shard *memoryShard, prefix, afterKey string) *untrackedKeyShardIterator {
	iterator := &untrackedKeyShardIterator{
		shard:     shard,
		prefix:    prefix,
		nextAfter: afterKey,
	}

	if !iterator.fillBuffer() {
		return nil
	}

	return iterator
}

// Next returns the next key in key order.
func (it *untrackedKeyShardIterator) Next() (string, bool) {
	for {
		if it.bufferIdx < len(it.buffer) {
			key := it.buffer[it.bufferIdx]
			it.bufferIdx++
			it.nextAfter = key

			return key, true
		}

		if it.exhausted {
			return "", false
		}

		if !it.fillBuffer() {
			it.exhausted = true

			return "", false
		}
	}
}

// fillBuffer copies a bounded batch of keys while holding the shard read lock.
func (it *untrackedKeyShardIterator) fillBuffer() bool {
	shard := it.shard
	prefix := it.prefix
	after := it.nextAfter

	// Reuse existing buffer capacity if available, otherwise allocate new.
	buffer := it.buffer[:0]
	if cap(buffer) < untrackedIteratorBatchSize {
		buffer = make([]string, 0, untrackedIteratorBatchSize)
	}

	shard.mu.RLock()

	var (
		maxIndex    int
		hasMaxIndex bool
	)

	for key := range shard.container {
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			continue
		}

		if key <= after {
			continue
		}

		if len(buffer) < untrackedIteratorBatchSize {
			// Buffer not full: add key and track maximum key for replacement strategy.
			buffer = append(buffer, key)

			if !hasMaxIndex || buffer[maxIndex] < key {
				maxIndex = len(buffer) - 1
				hasMaxIndex = true
			}

			continue
		}

		// Buffer full: only replace if new key is smaller than current maximum.
		if hasMaxIndex && buffer[maxIndex] <= key {
			continue
		}

		// Replace maximum key to preserve the smallest lexicographic batch.
		buffer[maxIndex] = key

		// Recompute maximum key index after replacement.
		maxIndex = 0
		for i := 1; i < len(buffer); i++ {
			if buffer[i] > buffer[maxIndex] {
				maxIndex = i
			}
		}
	}

	shard.mu.RUnlock()

	if len(buffer) == 0 {
		it.buffer = it.buffer[:0]

		return false
	}

	sort.Strings(buffer)

	it.buffer = buffer
	it.bufferIdx = 0

	return true
}

// Next returns the next entry in key order.
func (it *untrackedShardIterator) Next() (Entry, bool) {
	for {
		if it.bufferIdx < len(it.buffer) {
			entry := it.buffer[it.bufferIdx]
			it.bufferIdx++
			it.nextAfter = entry.Key

			return entry, true
		}

		if it.exhausted {
			return Entry{}, false
		}

		if !it.fillBuffer() {
			it.exhausted = true

			return Entry{}, false
		}
	}
}

// fillBuffer copies a bounded batch of entries while holding the shard read lock.
func (it *untrackedShardIterator) fillBuffer() bool {
	shard := it.shard
	prefix := it.prefix
	after := it.nextAfter

	// Reuse existing buffer capacity if available, otherwise allocate new.
	buffer := it.buffer[:0]
	if cap(buffer) < untrackedIteratorBatchSize {
		buffer = make([]Entry, 0, untrackedIteratorBatchSize)
	}

	shard.mu.RLock()

	var (
		maxIndex    int
		hasMaxIndex bool
	)

	for k, v := range shard.container {
		if prefix != "" && !strings.HasPrefix(k, prefix) {
			continue
		}

		if k <= after {
			continue
		}

		if len(buffer) < untrackedIteratorBatchSize {
			// Buffer not full: add entry and track maximum key for replacement strategy.
			buffer = append(buffer, Entry{
				Key:   k,
				Value: slices.Clone(v),
			})

			if !hasMaxIndex || buffer[maxIndex].Key < k {
				maxIndex = len(buffer) - 1
				hasMaxIndex = true
			}

			continue
		}

		// Buffer full: only replace if the new key is smaller than current maximum.
		// This maintains a bounded lexicographic candidate set (smallest keys first),
		// not random reservoir sampling.
		if hasMaxIndex && buffer[maxIndex].Key <= k {
			continue
		}

		// Replace maximum key entry with smaller key to maintain lexicographic order.
		buffer[maxIndex] = Entry{
			Key:   k,
			Value: slices.Clone(v),
		}

		// Recompute maximum key index after replacement.
		maxIndex = 0
		for i := 1; i < len(buffer); i++ {
			if buffer[i].Key > buffer[maxIndex].Key {
				maxIndex = i
			}
		}
	}

	shard.mu.RUnlock()

	if len(buffer) == 0 {
		it.buffer = it.buffer[:0]

		return false
	}

	sort.Slice(buffer, func(i, j int) bool {
		return buffer[i].Key < buffer[j].Key
	})

	it.buffer = buffer
	it.bufferIdx = 0

	return true
}
