package forge

import "sync"

// EventBus publishes worker events to subscribers.
type EventBus interface {
	Publish(ev *Event)
}

// ChannelEventBus fans out events to Go channels (non-blocking send).
type ChannelEventBus struct {
	mu          sync.RWMutex
	subscribers []chan *Event
}

// NewChannelEventBus creates an empty bus (optional args reserved for future use).
func NewChannelEventBus(_ ...int) *ChannelEventBus {
	return &ChannelEventBus{}
}

// Subscribe registers a new subscriber channel with the given buffer size.
func (b *ChannelEventBus) Subscribe(bufSize int) <-chan *Event {
	ch := make(chan *Event, bufSize)
	b.mu.Lock()
	b.subscribers = append(b.subscribers, ch)
	b.mu.Unlock()
	return ch
}

// Publish delivers an event to all subscribers (drops if buffer full).
func (b *ChannelEventBus) Publish(ev *Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, ch := range b.subscribers {
		select {
		case ch <- ev:
		default:
		}
	}
}
