package forge

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
)

// ErrDeliveryInterrupted requires a new scan generation, never silent continuation.
var ErrDeliveryInterrupted = errors.New("reliable worker delivery interrupted")

// ErrReliableUnsupported means an older worker cannot supply authoritative events.
var ErrReliableUnsupported = errors.New("worker lacks reliable_events_v1; upgrade worker")

// ReliableSubscription is one connection's bounded authoritative event stream.
// Commit acknowledges durable consumer persistence, not merely channel receipt.
type ReliableSubscription struct {
	conn         *Conn
	id           string
	events       chan *Event
	mu           sync.Mutex
	pending      map[string]map[uint64]int
	sequence     map[string]uint64
	committed    map[string]uint64
	commitMu     sync.Mutex
	count, bytes int
	err          error
}

// ConfigureReliable negotiates reliable_events_v1 before any scan is started.
func (c *Conn) ConfigureReliable(ctx context.Context) (*ReliableSubscription, error) {
	response, err := c.Call(ctx, "capabilities", nil)
	if err != nil {
		return nil, err
	}
	var caps struct {
		Features []string `json:"features"`
	}
	if !response.OK || response.Payload == nil || json.Unmarshal(*response.Payload, &caps) != nil {
		return nil, ErrReliableUnsupported
	}
	supported := false
	for _, f := range caps.Features {
		if f == "reliable_events_v1" {
			supported = true
		}
	}
	if !supported {
		return nil, ErrReliableUnsupported
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return nil, err
	}
	s := &ReliableSubscription{conn: c, id: hex.EncodeToString(random[:]), events: make(chan *Event, 256), pending: make(map[string]map[uint64]int), sequence: make(map[string]uint64), committed: make(map[string]uint64)}
	c.reliableMu.Lock()
	if c.reliable != nil {
		c.reliableMu.Unlock()
		return nil, errors.New("reliable delivery already configured")
	}
	c.reliable = s
	c.reliableMu.Unlock()
	result, err := c.Call(ctx, "configure_event_delivery", map[string]string{"delivery_id": s.id})
	if err != nil || !result.OK {
		s.fail(ErrDeliveryInterrupted)
		_ = c.Close()
		if err != nil {
			return nil, err
		}
		return nil, ErrDeliveryInterrupted
	}
	return s, nil
}

// Events returns the ordered authoritative stream. Closed streams require Err inspection.
func (s *ReliableSubscription) Events() <-chan *Event { return s.events }

// Err returns the stream failure, including connection termination.
func (s *ReliableSubscription) Err() error { s.mu.Lock(); defer s.mu.Unlock(); return s.err }

// Close unsubscribes by closing the connection; scans cannot continue on a detached reliable stream.
func (s *ReliableSubscription) Close() error { return s.conn.Close() }
func (s *ReliableSubscription) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.err = err
		close(s.events)
	}
}
func (s *ReliableSubscription) accept(ev *Event) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return false
	}
	if (s.sequence[ev.JobID] == 0 && len(s.sequence) >= 4096) || ev.DeliveryID != s.id || ev.Sequence != s.sequence[ev.JobID]+1 || len(ev.RawBody) > 1024*1024 || s.count >= 256 || s.bytes+ev.WireBytes > 16*1024*1024 {
		return false
	}
	select {
	case s.events <- ev:
		s.sequence[ev.JobID] = ev.Sequence
		if s.pending[ev.JobID] == nil {
			s.pending[ev.JobID] = make(map[uint64]int)
		}
		s.pending[ev.JobID][ev.Sequence] = ev.WireBytes
		s.count++
		s.bytes += ev.WireBytes
		return true
	default:
		return false
	}
}

// Commit releases cumulative delivery credit only after the caller's database commit.
func (s *ReliableSubscription) Commit(ctx context.Context, ev *Event) error {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	s.mu.Lock()
	valid := s.err == nil && ev.DeliveryID == s.id && ev.Sequence > 0 && ev.Sequence == s.committed[ev.JobID]+1 && ev.Sequence <= s.sequence[ev.JobID]
	s.mu.Unlock()
	if !valid {
		return ErrDeliveryInterrupted
	}
	// Persistence has completed. Release local admission before the peer can
	// observe the acknowledgment and immediately replenish its credit window.
	s.mu.Lock()
	s.committed[ev.JobID] = ev.Sequence
	for seq, size := range s.pending[ev.JobID] {
		if seq <= ev.Sequence {
			delete(s.pending[ev.JobID], seq)
			s.count--
			s.bytes -= size
		}
	}
	s.mu.Unlock()
	result, err := s.conn.Call(ctx, "ack_events", map[string]any{"delivery_id": s.id, "job_id": ev.JobID, "through_sequence": ev.Sequence})
	if err != nil || !result.OK {
		_ = s.conn.Close()
		if err != nil {
			return err
		}
		return ErrDeliveryInterrupted
	}
	return nil
}

// Stats reports outstanding consumer credits, including events already dequeued.
func (s *ReliableSubscription) Stats() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count, s.bytes
}
