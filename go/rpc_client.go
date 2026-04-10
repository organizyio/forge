package forge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// Caller issues framed RPC calls (typically a *Conn).
type Caller interface {
	Call(ctx context.Context, method string, params any) (*WireResponse, error)
}

// JobStatus is returned by the job_status RPC (matches forge-sdk JobStatus JSON).
type JobStatus struct {
	JobID    string          `json:"job_id"`
	State    string          `json:"state"`
	Progress json.RawMessage `json:"progress,omitempty"`
	Error    string          `json:"error,omitempty"`
}

// Client wraps a Caller with higher-level helpers.
type Client struct {
	conn          Caller
	JobStatusFunc func(ctx context.Context, jobID string) (JobStatus, error)
}

// NewClient builds an RPC client around conn.
func NewClient(conn Caller) *Client {
	return &Client{conn: conn}
}

// Conn returns the underlying caller (or a no-op caller if unset).
func (c *Client) Conn() Caller {
	if c == nil || c.conn == nil {
		return noOpCaller{}
	}
	return c.conn
}

// JobStatus fetches remote job status unless JobStatusFunc overrides it.
func (c *Client) JobStatus(ctx context.Context, jobID string) (JobStatus, error) {
	if c != nil && c.JobStatusFunc != nil {
		return c.JobStatusFunc(ctx, jobID)
	}
	resp, err := c.Conn().Call(ctx, "job_status", map[string]string{"job_id": jobID})
	if err != nil {
		return JobStatus{}, err
	}
	if !resp.OK {
		if resp.Error != nil {
			return JobStatus{}, resp.Error
		}
		return JobStatus{}, errors.New("job_status failed")
	}
	if resp.Payload == nil {
		return JobStatus{}, errors.New("job_status missing payload")
	}
	var out JobStatus
	if err := json.Unmarshal(*resp.Payload, &out); err != nil {
		return JobStatus{}, fmt.Errorf("decode job_status payload: %w", err)
	}
	return out, nil
}

// Ping sends a ping RPC.
func (c *Client) Ping(ctx context.Context) (*WireResponse, error) {
	return c.Conn().Call(ctx, "ping", nil)
}

// CancelJob invokes the control-plane cancel_job RPC. It returns whether the
// worker reported the job as canceled (false can mean JOB_NOT_FOUND).
func (c *Client) CancelJob(ctx context.Context, jobID string) (bool, error) {
	resp, err := c.Conn().Call(ctx, "cancel_job", map[string]string{"job_id": jobID})
	if err != nil {
		return false, err
	}
	if !resp.OK {
		if resp.Error != nil {
			return false, resp.Error
		}
		return false, errors.New("cancel_job failed")
	}
	if resp.Payload == nil {
		return false, nil
	}
	var out struct {
		Canceled bool `json:"cancelled"`
	}
	if err := json.Unmarshal(*resp.Payload, &out); err != nil {
		return false, fmt.Errorf("decode cancel_job payload: %w", err)
	}
	return out.Canceled, nil
}

// Shutdown asks the worker to shut down using the worker's default delay
// before exit (see Rust forge-sdk shutdown handler).
func (c *Client) Shutdown(ctx context.Context) (*WireResponse, error) {
	return c.Conn().Call(ctx, "shutdown", nil)
}

// ShutdownWithDelay asks the worker to shut down after delayMs milliseconds
// (clamped by the worker implementation).
func (c *Client) ShutdownWithDelay(ctx context.Context, delayMs uint64) (*WireResponse, error) {
	return c.Conn().Call(ctx, "shutdown", map[string]uint64{"delay_ms": delayMs})
}

// Close closes the underlying connection when it implements io.Closer.
func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	if closer, ok := c.conn.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}

type noOpCaller struct{}

func (noOpCaller) Call(context.Context, string, any) (*WireResponse, error) {
	return nil, errors.New("rpc caller not configured")
}
