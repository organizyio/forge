package forge

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/organizyio/forge/go/internal/codec"
	"github.com/organizyio/forge/go/internal/frame"
)

const (
	kindRequest  uint8 = 1
	kindResponse uint8 = 2
	kindEvent    uint8 = 3
)

// WireRequest is a framed RPC request on the wire.
type WireRequest struct {
	ID     string `msgpack:"id"     json:"id"`
	Method string `msgpack:"method" json:"method"`
	Params any    `msgpack:"params" json:"params,omitempty"`
}

// WireResponse is a framed RPC response on the wire.
type WireResponse struct {
	ID      string           `msgpack:"id"      json:"id"`
	OK      bool             `msgpack:"ok"      json:"ok"`
	Error   *ErrorPayload    `msgpack:"error"   json:"error,omitempty"`
	Payload *json.RawMessage `msgpack:"payload" json:"payload,omitempty"`
}

type wireEvent struct {
	Type    string          `msgpack:"type"    json:"type"`
	JobID   string          `msgpack:"job_id"  json:"job_id"`
	Payload json.RawMessage `msgpack:"payload" json:"payload,omitempty"`
}

// Event is a push notification from a worker connection.
type Event struct {
	Type    string
	JobID   string
	RawBody json.RawMessage
}

// ErrorPayload carries a structured RPC error.
type ErrorPayload struct {
	Code    string `msgpack:"code"    json:"code"`
	Message string `msgpack:"message" json:"message"`
	Detail  string `msgpack:"detail"  json:"detail,omitempty"`
}

func (e *ErrorPayload) Error() string {
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

// Encoding selects MessagePack or JSON for the connection.
type Encoding int

const (
	// EncodingMsgpack uses MessagePack for payloads.
	EncodingMsgpack Encoding = iota
	// EncodingJSON uses JSON for payloads.
	EncodingJSON
)

func (e Encoding) codecFormat() codec.Format {
	return codec.Format(e)
}

// Conn is a framed bidirectional connection to a worker: Unix domain socket on
// Unix, or on Windows either a named pipe (address like \\.\pipe\Name or //./pipe/Name)
// or an AF_UNIX path when supported.
type Conn struct {
	nc       net.Conn
	mu       sync.Mutex
	encoding Encoding
	pending  sync.Map // map[string]chan *WireResponse
	onEvent  func(ev *Event)
	seq      atomic.Uint64
	closed   chan struct{}
}

// Dial opens a connection to a worker (see Conn) and starts the read loop.
func Dial(ctx context.Context, socketPath string, encoding Encoding, onEvent func(*Event)) (*Conn, error) {
	nc, err := dialWorker(ctx, socketPath)
	if err != nil {
		return nil, fmt.Errorf("dial worker %s: %w", socketPath, err)
	}

	c := &Conn{nc: nc, encoding: encoding, onEvent: onEvent, closed: make(chan struct{})}
	go c.readLoop()
	return c, nil
}

// Close shuts down the connection.
func (c *Conn) Close() error {
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
	return c.nc.Close()
}

// Call performs a request/response RPC over the connection.
func (c *Conn) Call(ctx context.Context, method string, params any) (*WireResponse, error) {
	id := fmt.Sprintf("req-%d", c.seq.Add(1))
	req := WireRequest{ID: id, Method: method, Params: params}
	payload, err := codec.Marshal(c.encoding.codecFormat(), req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	ch := make(chan *WireResponse, 1)
	c.pending.Store(id, ch)
	defer c.pending.Delete(id)

	if err := c.writeFrame(kindRequest, payload); err != nil {
		return nil, fmt.Errorf("write frame: %w", err)
	}

	select {
	case resp := <-ch:
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.closed:
		return nil, io.ErrClosedPipe
	}
}

func (c *Conn) writeFrame(kind uint8, payload []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return frame.Write(c.nc, kind, payload)
}

func (c *Conn) readLoop() {
	for {
		kind, body, err := frame.Read(c.nc)
		if err != nil {
			return
		}
		switch kind {
		case kindResponse:
			c.handleResponse(body)
		case kindEvent:
			c.handleEvent(body)
		}
	}
}

func (c *Conn) handleResponse(body []byte) {
	var resp WireResponse
	if err := codec.Unmarshal(c.encoding.codecFormat(), body, &resp); err != nil {
		return
	}
	if ch, ok := c.pending.Load(resp.ID); ok {
		ch.(chan *WireResponse) <- &resp
	}
}

func (c *Conn) handleEvent(body []byte) {
	if c.onEvent == nil {
		return
	}
	var ev wireEvent
	if err := codec.Unmarshal(c.encoding.codecFormat(), body, &ev); err != nil {
		return
	}
	go c.onEvent(&Event{Type: ev.Type, JobID: ev.JobID, RawBody: ev.Payload})
}
