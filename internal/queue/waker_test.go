package queue

import "testing"

func TestWakerWakesEveryWaiter(t *testing.T) {
	w := NewWaker()
	a, b := w.C(), w.C()
	w.Wake()
	for _, ch := range []<-chan struct{}{a, b} {
		select {
		case <-ch:
		default:
			t.Fatal("a waiter was not woken")
		}
	}
	select {
	case <-w.C():
		t.Fatal("a channel taken after Wake is already closed")
	default:
	}
}
