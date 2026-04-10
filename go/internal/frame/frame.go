// Package frame implements length-prefixed IPC framing for Forge connections.
package frame

import (
	"encoding/binary"
	"errors"
	"io"
)

// MaxPayload is the maximum allowed frame body size (64 MiB).
const MaxPayload = 64 * 1024 * 1024

// ErrPayloadTooLarge is returned when a frame declares a body larger than MaxPayload.
var ErrPayloadTooLarge = errors.New("frame: payload too large")

// Write writes a 5-byte header (big-endian length + kind) and payload to w.
func Write(w io.Writer, kind byte, payload []byte) error {
	var hdr [5]byte
	binary.BigEndian.PutUint32(hdr[:4], uint32(len(payload)))
	hdr[4] = kind
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

// Read reads one frame from r and returns the kind byte and body.
func Read(r io.Reader) (kind byte, body []byte, err error) {
	hdr := make([]byte, 5)
	if _, err = io.ReadFull(r, hdr); err != nil {
		return 0, nil, err
	}
	length := binary.BigEndian.Uint32(hdr[:4])
	kind = hdr[4]
	if length > MaxPayload {
		return 0, nil, ErrPayloadTooLarge
	}
	body = make([]byte, length)
	if _, err = io.ReadFull(r, body); err != nil {
		return 0, nil, err
	}
	return kind, body, nil
}
