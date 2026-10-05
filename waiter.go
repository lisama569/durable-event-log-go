package eventlog

import "sync"

// Wait/notify support for consumers parked at the committed end.
//
// Notifications are edge-triggered: a waiter registers a channel, then
// re-checks the committed end under the store read lock, so no append (or
// compaction, which also moves the horizon) can be missed between the check
// and parking. There is no polling and no timed wakeups.
type waiterHub struct {
	mu     sync.Mutex
	waiter map[chan struct{}]struct{}
}

func newWaiterHub() *waiterHub {
	return &waiterHub{waiter: map[chan struct{}]struct{}{}}
}

// register starts an edge-triggered subscription. The returned channel is
// closed exactly once by the next notify. Pair with unregister when giving up
// (context timeout / cancel), and drain/ignore a possibly-already-closed
// channel after unregistering.
func (h *waiterHub) register() chan struct{} {
	ch := make(chan struct{})
	h.mu.Lock()
	h.waiter[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *waiterHub) unregister(ch chan struct{}) {
	h.mu.Lock()
	delete(h.waiter, ch)
	h.mu.Unlock()
}

// notify releases every currently parked waiter and starts a fresh epoch:
// waiters that arrive afterwards block until the next state change.
func (h *waiterHub) notify() {
	h.mu.Lock()
	pending := h.waiter
	h.waiter = map[chan struct{}]struct{}{}
	h.mu.Unlock()
	for ch := range pending {
		close(ch)
	}
}
