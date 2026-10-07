package forge

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/organizyio/forge/go/internal/codec"
)

func TestReliableRejectsOutOfOrderCommit(t *testing.T) {
	s := &ReliableSubscription{id: "delivery", events: make(chan *Event, 256), pending: make(map[string]map[uint64]int), sequence: make(map[string]uint64), committed: make(map[string]uint64)}
	first := &Event{DeliveryID: "delivery", JobID: "job", Sequence: 1, WireBytes: 100}
	second := &Event{DeliveryID: "delivery", JobID: "job", Sequence: 2, WireBytes: 100}
	if !s.accept(first) || !s.accept(second) {
		t.Fatal("admission failed")
	}
	if err := s.Commit(context.Background(), second); !errors.Is(err, ErrDeliveryInterrupted) {
		t.Fatalf("out of order acknowledgment: %v", err)
	}
	if count, bytes := s.Stats(); count != 2 || bytes != 200 {
		t.Fatalf("credit released before persistence: %d %d", count, bytes)
	}
	s.fail(ErrDeliveryInterrupted)
	if s.accept(first) {
		t.Fatal("closed stream admitted event")
	}
}

func TestStructuredMessagePackEnvelope(t *testing.T) {
	body, err := codec.Marshal(codec.FormatMsgpack, map[string]any{"id": "response", "ok": true, "payload": map[string]any{"alive": true}})
	if err != nil {
		t.Fatal(err)
	}
	c := &Conn{encoding: EncodingMsgpack}
	var response WireResponse
	if err = c.decodeEnvelope(body, &response); err != nil {
		t.Fatal(err)
	}
	if response.Payload == nil || string(*response.Payload) != `{"alive":true}` {
		t.Fatalf("payload: %v", response.Payload)
	}
}

func TestBestEffortUnsubscribeAndDropCount(t *testing.T) {
	bus := NewChannelEventBus()
	ctx, cancel := context.WithCancel(context.Background())
	events := bus.SubscribeContext(ctx, 1)
	bus.Publish(&Event{})
	bus.Publish(&Event{})
	if bus.DroppedEvents() != 1 {
		t.Fatal("missing overflow accounting")
	}
	cancel()
	timeout := time.After(time.Second)
	for {
		select {
		case _, open := <-events:
			if !open {
				bus.Publish(&Event{})
				return
			}
		case <-timeout:
			t.Fatal("unsubscribe blocked")
		}
	}
}
