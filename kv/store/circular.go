package store

import "sync"

type (
	// circularCursorSlot serializes reusable circular iteration for one exact
	// prefix and stores the last successfully returned key.
	circularCursorSlot struct {
		mu      sync.Mutex
		lastKey string
	}

	// circularCursorRegistry stores process-local circular positions.
	//
	// One slot exists per exact prefix used with NextCircular.
	// Slots intentionally live for the Store lifetime: removing slots safely
	// under concurrent callers would require additional lifetime/reference
	// coordination that is not needed for stable load-test dataset prefixes.
	circularCursorRegistry struct {
		mu    sync.Mutex
		slots map[string]*circularCursorSlot
	}
)

// Lock ordering:
//
//   - registry.mu is used only to locate/create stable slot objects.
//   - NextCircular releases registry.mu before acquiring slot.mu.
//   - slot.mu may be held while Store.Scan executes.
//   - No Store operation may acquire registry.mu while already holding slot.mu.
//
// resetAll preserves slot identities and acquires registry.mu before slot.mu.
func (r *circularCursorRegistry) slot(prefix string) *circularCursorSlot {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.slots == nil {
		r.slots = make(map[string]*circularCursorSlot)
	}

	slot := r.slots[prefix]
	if slot != nil {
		return slot
	}

	slot = new(circularCursorSlot)
	r.slots[prefix] = slot

	return slot
}

func (r *circularCursorRegistry) resetAll() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, slot := range r.slots {
		slot.mu.Lock()
		slot.lastKey = ""
		slot.mu.Unlock()
	}
}

func nextCircularWithRegistry(
	registry *circularCursorRegistry,
	prefix string,
	scan func(prefix string, afterKey string, limit int64) (*ScanPage, error),
) (*Entry, error) {
	slot := registry.slot(prefix)

	slot.mu.Lock()
	defer slot.mu.Unlock()

	page, err := scan(prefix, slot.lastKey, 1)
	if err != nil {
		return nil, err
	}

	if len(page.Entries) == 0 && slot.lastKey != "" {
		page, err = scan(prefix, "", 1)
		if err != nil {
			return nil, err
		}
	}

	if len(page.Entries) == 0 {
		slot.lastKey = ""

		return nil, nil //nolint:nilnil // empty dataset is a normal result.
	}

	entry := page.Entries[0]
	slot.lastKey = entry.Key

	return &entry, nil
}
