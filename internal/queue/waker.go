package queue

import "sync"

// Waker lets any number of goroutines wait for the next job notification.
// Wake closes the current channel and swaps in a fresh one, so every waiter
// holding the old one wakes at once.
type Waker struct {
	mu sync.Mutex
	ch chan struct{}
}

func NewWaker() *Waker {
	return &Waker{ch: make(chan struct{})}
}

// C returns a channel that closes on the next Wake. Take it before checking
// for work, or a wake between the check and the wait is missed.
func (w *Waker) C() <-chan struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.ch
}

func (w *Waker) Wake() {
	w.mu.Lock()
	close(w.ch)
	w.ch = make(chan struct{})
	w.mu.Unlock()
}
