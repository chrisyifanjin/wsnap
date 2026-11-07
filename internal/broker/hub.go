package broker

import "sync"

// Hub wakes long-polling sidecars when a request for their workspace
// appears. Informer event handlers call Notify; there is no polling, so an
// idle fleet of sidecars costs one open connection each and no CPU.
type Hub struct {
	mu      sync.Mutex
	waiters map[string]map[chan struct{}]struct{}
}

// HubKey identifies a workspace.
func HubKey(namespace, workspace string) string { return namespace + "/" + workspace }

// Subscribe returns a channel that receives a value when key is notified,
// and a function to unsubscribe.
func (h *Hub) Subscribe(key string) (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	h.mu.Lock()
	if h.waiters == nil {
		h.waiters = map[string]map[chan struct{}]struct{}{}
	}
	if h.waiters[key] == nil {
		h.waiters[key] = map[chan struct{}]struct{}{}
	}
	h.waiters[key][ch] = struct{}{}
	h.mu.Unlock()
	return ch, func() {
		h.mu.Lock()
		delete(h.waiters[key], ch)
		if len(h.waiters[key]) == 0 {
			delete(h.waiters, key)
		}
		h.mu.Unlock()
	}
}

// Notify wakes every subscriber of key without blocking.
func (h *Hub) Notify(key string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.waiters[key] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
