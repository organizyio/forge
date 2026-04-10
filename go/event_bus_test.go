package forge_test

import (
	"testing"

	forge "github.com/organizyio/forge/go"
)

func TestChannelEventBus_SubscribePublish(t *testing.T) {
	t.Parallel()
	bus := forge.NewChannelEventBus()
	ch := bus.Subscribe(2)
	ev := &forge.Event{Type: "t", JobID: "j1"}
	bus.Publish(ev)
	got := <-ch
	if got.Type != ev.Type || got.JobID != ev.JobID {
		t.Fatalf("got %+v", got)
	}
}

func TestChannelEventBus_DropsNewWhenBufferFull(t *testing.T) {
	t.Parallel()
	bus := forge.NewChannelEventBus()
	ch := bus.Subscribe(1)
	bus.Publish(&forge.Event{JobID: "first"})
	bus.Publish(&forge.Event{JobID: "second"})
	got := <-ch
	if got.JobID != "first" {
		t.Fatalf("buffer was full: incoming event dropped, want first still queued, got %q", got.JobID)
	}
}
