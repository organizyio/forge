package forge

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/organizyio/forge/go/internal/codec"
	"github.com/organizyio/forge/go/internal/frame"
)

func TestConn_Call_JSON_roundtrip(t *testing.T) {
	t.Parallel()
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	c := &Conn{nc: client, encoding: EncodingJSON, closed: make(chan struct{})}
	go c.readLoop()
	t.Cleanup(func() { _ = c.Close() })

	done := make(chan struct{})
	go func() {
		defer close(done)
		echoPeer(t, server, EncodingJSON, 0)
	}()

	ctx := context.Background()
	resp, err := c.Call(ctx, "ping", map[string]string{"k": "v"})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.OK || resp.Payload == nil {
		t.Fatalf("resp: %+v", resp)
	}
	var body struct {
		Pong bool `json:"pong"`
	}
	if err := json.Unmarshal(*resp.Payload, &body); err != nil || !body.Pong {
		t.Fatalf("payload %s err %v", string(*resp.Payload), err)
	}
	_ = server.Close()
	<-done
}

func TestConn_Call_Msgpack_roundtrip(t *testing.T) {
	t.Parallel()
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	c := &Conn{nc: client, encoding: EncodingMsgpack, closed: make(chan struct{})}
	go c.readLoop()
	t.Cleanup(func() { _ = c.Close() })

	done := make(chan struct{})
	go func() {
		defer close(done)
		echoPeer(t, server, EncodingMsgpack, 0)
	}()

	resp, err := c.Call(context.Background(), "ping", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !resp.OK {
		t.Fatal("not OK")
	}
	_ = server.Close()
	<-done
}

func TestConn_Call_ContextCancel(t *testing.T) {
	t.Parallel()
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	c := &Conn{nc: client, encoding: EncodingJSON, closed: make(chan struct{})}
	go c.readLoop()
	t.Cleanup(func() { _ = c.Close() })

	done := make(chan struct{})
	go func() {
		defer close(done)
		echoPeer(t, server, EncodingJSON, 200*time.Millisecond)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.Call(ctx, "slow", nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v want Canceled", err)
	}
	_ = server.Close()
	<-done
}

func TestConn_OnEvent_JSON(t *testing.T) {
	t.Parallel()
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	evCh := make(chan *Event, 1)
	c := &Conn{nc: client, encoding: EncodingJSON, onEvent: func(ev *Event) { evCh <- ev }, closed: make(chan struct{})}
	go c.readLoop()
	t.Cleanup(func() { _ = c.Close() })

	go func() {
		ev := wireEvent{Type: "progress", JobID: "j1", Payload: json.RawMessage(`{"n":1}`)}
		body, err := codec.Marshal(codec.FormatJSON, ev)
		if err != nil {
			t.Error(err)
			return
		}
		if err := frame.Write(server, kindEvent, body); err != nil {
			t.Error(err)
		}
		_ = server.Close()
	}()

	select {
	case ev := <-evCh:
		if ev.Type != "progress" || ev.JobID != "j1" || string(ev.RawBody) != `{"n":1}` {
			t.Fatalf("event %+v", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for event")
	}
}

func TestConn_OnEvent_Msgpack(t *testing.T) {
	t.Parallel()
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	evCh := make(chan *Event, 1)
	c := &Conn{nc: client, encoding: EncodingMsgpack, onEvent: func(ev *Event) { evCh <- ev }, closed: make(chan struct{})}
	go c.readLoop()
	t.Cleanup(func() { _ = c.Close() })

	go func() {
		ev := wireEvent{Type: "x", JobID: "y", Payload: json.RawMessage(`{}`)}
		body, err := codec.Marshal(codec.FormatMsgpack, ev)
		if err != nil {
			t.Error(err)
			return
		}
		_ = frame.Write(server, kindEvent, body)
		_ = server.Close()
	}()

	select {
	case ev := <-evCh:
		if ev.Type != "x" || ev.JobID != "y" {
			t.Fatalf("event %+v", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
}

// echoPeer reads framed requests and replies with OK + small JSON body. Stops on read error.
func echoPeer(t *testing.T, server net.Conn, enc Encoding, delay time.Duration) {
	t.Helper()
	format := codec.Format(enc)
	for {
		kind, body, err := frame.Read(server)
		if err != nil {
			return
		}
		if kind != kindRequest {
			t.Errorf("unexpected frame kind %d", kind)
			return
		}
		var req WireRequest
		if err := codec.Unmarshal(format, body, &req); err != nil {
			return
		}
		if delay > 0 {
			time.Sleep(delay)
		}
		raw := json.RawMessage(`{"pong":true}`)
		resp := WireResponse{ID: req.ID, OK: true, Payload: &raw}
		out, err := codec.Marshal(format, resp)
		if err != nil {
			t.Errorf("marshal response: %v", err)
			return
		}
		if err := frame.Write(server, kindResponse, out); err != nil {
			return
		}
	}
}
