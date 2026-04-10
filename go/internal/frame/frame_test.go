package frame

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func TestWriteRead_Roundtrip(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		kind    byte
		payload []byte
	}{
		{"empty", 7, nil},
		{"small", 2, []byte("hello")},
		{"binary", 1, []byte{0, 255, 1, 2, 3}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			if err := Write(&buf, tt.kind, tt.payload); err != nil {
				t.Fatalf("Write: %v", err)
			}
			kind, body, err := Read(&buf)
			if err != nil {
				t.Fatalf("Read: %v", err)
			}
			if kind != tt.kind {
				t.Fatalf("kind: got %d want %d", kind, tt.kind)
			}
			if !bytes.Equal(body, tt.payload) {
				t.Fatalf("body: got %q want %q", body, tt.payload)
			}
		})
	}
}

func TestRead_PayloadTooLarge(t *testing.T) {
	t.Parallel()
	hdr := []byte{0xff, 0xff, 0xff, 0xff, 9} // length MaxUint32, kind 9
	_, _, err := Read(bytes.NewReader(hdr))
	if !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("got %v want ErrPayloadTooLarge", err)
	}
	// MaxPayload + 1 in header (without sending body)
	b := make([]byte, 5)
	binary.BigEndian.PutUint32(b, uint32(MaxPayload+1))
	b[4] = 1
	_, _, err = Read(bytes.NewReader(b))
	if !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("got %v want ErrPayloadTooLarge", err)
	}
}

func TestRead_MaxPayload(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates MaxPayload bytes")
	}
	t.Parallel()
	payload := make([]byte, MaxPayload)
	payload[0] = 0xab
	payload[len(payload)-1] = 0xcd
	var buf bytes.Buffer
	if err := Write(&buf, 3, payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	kind, body, err := Read(&buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if kind != 3 {
		t.Fatalf("kind: %d", kind)
	}
	if len(body) != MaxPayload || body[0] != 0xab || body[len(body)-1] != 0xcd {
		t.Fatal("body mismatch")
	}
}

func TestRead_TruncatedHeader(t *testing.T) {
	t.Parallel()
	_, _, err := Read(bytes.NewReader([]byte{1, 2, 3}))
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRead_TruncatedBody(t *testing.T) {
	t.Parallel()
	// length 10, kind 1, but only 3 body bytes
	r := bytes.NewReader([]byte{0, 0, 0, 10, 1, 1, 2, 3})
	_, _, err := Read(r)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("got %v want unexpected EOF", err)
	}
}

func TestWrite_HeaderWriteError(t *testing.T) {
	t.Parallel()
	w := &failWriter{failAfter: 0}
	err := Write(w, 1, []byte("x"))
	if err == nil || err.Error() != "boom" {
		t.Fatalf("got %v", err)
	}
}

func TestWrite_PayloadWriteError(t *testing.T) {
	t.Parallel()
	w := &failWriter{failAfter: 1}
	err := Write(w, 1, []byte("x"))
	if err == nil || err.Error() != "boom" {
		t.Fatalf("got %v", err)
	}
}

type failWriter struct {
	n         int
	failAfter int
}

func (w *failWriter) Write(p []byte) (int, error) {
	if w.n == w.failAfter {
		return 0, errors.New("boom")
	}
	w.n++
	return len(p), nil
}
