package forge

import (
	"io"
	"net"
	"time"
)

// rwConn adapts a pair of half-duplex streams to net.Conn for framed IPC (stdio).
type rwConn struct {
	r io.ReadCloser
	w io.WriteCloser
}

func (c *rwConn) Read(b []byte) (int, error)  { return c.r.Read(b) }
func (c *rwConn) Write(b []byte) (int, error) { return c.w.Write(b) }
func (c *rwConn) Close() error {
	errR := c.r.Close()
	errW := c.w.Close()
	if errR != nil {
		return errR
	}
	return errW
}
func (c *rwConn) LocalAddr() net.Addr                { return pipeAddr("local") }
func (c *rwConn) RemoteAddr() net.Addr               { return pipeAddr("remote") }
func (c *rwConn) SetDeadline(t time.Time) error      { return nil }
func (c *rwConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *rwConn) SetWriteDeadline(t time.Time) error { return nil }

type pipeAddr string

func (a pipeAddr) Network() string { return "pipe" }
func (a pipeAddr) String() string  { return string(a) }

// DialReadWriter starts a Conn over arbitrary reader/writer (e.g. child stdio pipes).
func DialReadWriter(r io.ReadCloser, w io.WriteCloser, encoding Encoding, onEvent func(*Event)) *Conn {
	c := &Conn{
		nc:       &rwConn{r: r, w: w},
		encoding: encoding,
		onEvent:  onEvent,
		closed:   make(chan struct{}),
	}
	go c.readLoop()
	return c
}
