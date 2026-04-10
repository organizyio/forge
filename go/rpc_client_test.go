package forge_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	forge "github.com/organizyio/forge/go"
)

type fakeCaller struct {
	fn func(ctx context.Context, method string, params any) (*forge.WireResponse, error)
}

func (f fakeCaller) Call(ctx context.Context, method string, params any) (*forge.WireResponse, error) {
	return f.fn(ctx, method, params)
}

func TestClient_JobStatus_Success(t *testing.T) {
	t.Parallel()
	b, _ := json.Marshal(forge.JobStatus{JobID: "j1", State: "running"})
	raw := json.RawMessage(b)
	client := forge.NewClient(fakeCaller{
		fn: func(_ context.Context, method string, params any) (*forge.WireResponse, error) {
			if method != "job_status" {
				t.Fatalf("method: %s", method)
			}
			return &forge.WireResponse{OK: true, Payload: &raw}, nil
		},
	})
	st, err := client.JobStatus(context.Background(), "j1")
	if err != nil {
		t.Fatal(err)
	}
	if st.JobID != "j1" || st.State != "running" {
		t.Fatalf("got %+v", st)
	}
}

func TestClient_JobStatus_ProgressAndError(t *testing.T) {
	t.Parallel()
	payload := map[string]any{
		"job_id":   "x",
		"state":    "running",
		"progress": map[string]any{"n": 1},
		"error":    "boom",
	}
	b, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	raw := json.RawMessage(b)
	client := forge.NewClient(fakeCaller{
		fn: func(context.Context, string, any) (*forge.WireResponse, error) {
			return &forge.WireResponse{OK: true, Payload: &raw}, nil
		},
	})
	st, err := client.JobStatus(context.Background(), "x")
	if err != nil {
		t.Fatal(err)
	}
	if st.JobID != "x" || st.State != "running" || st.Error != "boom" {
		t.Fatalf("got %+v", st)
	}
	if string(st.Progress) == "" || !json.Valid(st.Progress) {
		t.Fatalf("progress: %s", st.Progress)
	}
}

func TestClient_JobStatus_NotOK_WithError(t *testing.T) {
	t.Parallel()
	client := forge.NewClient(fakeCaller{
		fn: func(_ context.Context, method string, _ any) (*forge.WireResponse, error) {
			return &forge.WireResponse{OK: false, Error: &forge.ErrorPayload{Code: "X", Message: "nope"}}, nil
		},
	})
	_, err := client.JobStatus(context.Background(), "j1")
	if err == nil {
		t.Fatal("expected error")
	}
	var ep *forge.ErrorPayload
	if !errors.As(err, &ep) {
		t.Fatalf("want ErrorPayload, got %T", err)
	}
}

func TestClient_JobStatus_NotOK_NoErrorField(t *testing.T) {
	t.Parallel()
	client := forge.NewClient(fakeCaller{
		fn: func(context.Context, string, any) (*forge.WireResponse, error) {
			return &forge.WireResponse{OK: false}, nil
		},
	})
	_, err := client.JobStatus(context.Background(), "j1")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestClient_JobStatus_MissingPayload(t *testing.T) {
	t.Parallel()
	client := forge.NewClient(fakeCaller{
		fn: func(context.Context, string, any) (*forge.WireResponse, error) {
			return &forge.WireResponse{OK: true}, nil
		},
	})
	_, err := client.JobStatus(context.Background(), "j1")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestClient_JobStatus_BadJSON(t *testing.T) {
	t.Parallel()
	raw := json.RawMessage([]byte(`not-json`))
	client := forge.NewClient(fakeCaller{
		fn: func(context.Context, string, any) (*forge.WireResponse, error) {
			return &forge.WireResponse{OK: true, Payload: &raw}, nil
		},
	})
	_, err := client.JobStatus(context.Background(), "j1")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestClient_JobStatusFunc_Override(t *testing.T) {
	t.Parallel()
	c := &forge.Client{
		JobStatusFunc: func(ctx context.Context, jobID string) (forge.JobStatus, error) {
			return forge.JobStatus{State: "custom"}, nil
		},
	}
	st, err := c.JobStatus(context.Background(), "any")
	if err != nil || st.State != "custom" {
		t.Fatalf("got %+v %v", st, err)
	}
}

func TestClient_CancelJob_Success(t *testing.T) {
	t.Parallel()
	raw := json.RawMessage([]byte(`{"cancelled":true,"job_id":"j1"}`))
	client := forge.NewClient(fakeCaller{
		fn: func(_ context.Context, method string, params any) (*forge.WireResponse, error) {
			if method != "cancel_job" {
				t.Fatalf("method: %s", method)
			}
			m, ok := params.(map[string]string)
			if !ok || m["job_id"] != "j1" {
				t.Fatalf("params: %#v", params)
			}
			return &forge.WireResponse{OK: true, Payload: &raw}, nil
		},
	})
	ok, err := client.CancelJob(context.Background(), "j1")
	if err != nil || !ok {
		t.Fatalf("got %v %v", ok, err)
	}
}

func TestClient_CancelJob_NotOK(t *testing.T) {
	t.Parallel()
	client := forge.NewClient(fakeCaller{
		fn: func(context.Context, string, any) (*forge.WireResponse, error) {
			return &forge.WireResponse{OK: false, Error: &forge.ErrorPayload{Code: "JOB_NOT_FOUND", Message: "nope"}}, nil
		},
	})
	_, err := client.CancelJob(context.Background(), "x")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestClient_ShutdownWithDelay_Params(t *testing.T) {
	t.Parallel()
	var sawParams any
	client := forge.NewClient(fakeCaller{
		fn: func(_ context.Context, method string, params any) (*forge.WireResponse, error) {
			if method != "shutdown" {
				t.Fatalf("method: %s", method)
			}
			sawParams = params
			return &forge.WireResponse{OK: true}, nil
		},
	})
	if _, err := client.ShutdownWithDelay(context.Background(), 500); err != nil {
		t.Fatal(err)
	}
	m, ok := sawParams.(map[string]uint64)
	if !ok || m["delay_ms"] != 500 {
		t.Fatalf("params: %#v", sawParams)
	}
}

func TestClient_Conn_NoOpWhenNil(t *testing.T) {
	t.Parallel()
	c := forge.NewClient(nil)
	_, err := c.Conn().Call(context.Background(), "m", nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

type callerWithClose struct {
	fn      func(context.Context, string, any) (*forge.WireResponse, error)
	closeFn func() error
}

func (w *callerWithClose) Call(ctx context.Context, m string, p any) (*forge.WireResponse, error) {
	return w.fn(ctx, m, p)
}

func (w *callerWithClose) Close() error {
	return w.closeFn()
}

func TestClient_Close_WithCloser(t *testing.T) {
	t.Parallel()
	closed := false
	client := forge.NewClient(&callerWithClose{
		fn: func(context.Context, string, any) (*forge.WireResponse, error) {
			return &forge.WireResponse{OK: true}, nil
		},
		closeFn: func() error {
			closed = true
			return nil
		},
	})
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if !closed {
		t.Fatal("Close not called")
	}
}
