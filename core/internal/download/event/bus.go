package event

import (
	"sync"

	"github.com/tey-sun/suntey/core/internal/model"
)

// Bus is a non-blocking fan-out of progress/status events. Each subscriber has
// its own buffered channel; slow subscribers drop events (the UI polls task
// state from the store as a backstop, so dropped progress ticks are not fatal).
type Bus struct {
	mu   sync.Mutex
	subs map[chan model.Event]struct{}
}

func NewBus() *Bus {
	return &Bus{subs: map[chan model.Event]struct{}{}}
}

// Subscribe returns a receive channel and a cancel func that must be called.
func (b *Bus) Subscribe(buf int) (<-chan model.Event, func()) {
	ch := make(chan model.Event, buf)
	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()
	return ch, func() {
		b.mu.Lock()
		delete(b.subs, ch)
		b.mu.Unlock()
		close(ch)
	}
}

// Publish fans out to all subscribers, dropping on a full subscriber buffer.
func (b *Bus) Publish(e model.Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.subs {
		select {
		case ch <- e:
		default: // slow consumer: drop
		}
	}
}
