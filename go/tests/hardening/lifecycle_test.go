//go:build hardening

package hardening_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	forge "github.com/organizyio/forge/go"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestStopPreventsRestart(t *testing.T) {
	bin := os.Getenv("FORGE_MINIMAL_WORKER")
	if bin == "" {
		t.Fatal("FORGE_MINIMAL_WORKER is required")
	}
	dir, err := os.MkdirTemp("", "fg-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "w.sock")
	if runtime.GOOS == "windows" {
		socket = fmt.Sprintf(`\\.\pipe\forge-hardening-%d`, os.Getpid())
	}
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := forge.NewWorkerProcess(0, forge.WorkerConfig{BinaryPath: bin, SocketPath: socket, Encoding: forge.EncodingJSON})
	stop := func() {
		ctx, c := context.WithTimeout(context.Background(), 2*time.Second)
		defer c()
		if err := w.StopAndWait(ctx); err != nil {
			t.Error(err)
		}
	}
	defer stop()
	for i := 0; i < 3; i++ {
		if err := w.Start(parent); err != nil {
			t.Fatal(err)
		}
		if err := w.Start(parent); !errors.Is(err, forge.ErrWorkerStarted) {
			t.Fatalf("duplicate start: %v", err)
		}
		stop()
		if w.IsHealthy() {
			t.Fatal("healthy after stop")
		}
		time.Sleep(750 * time.Millisecond)
		if w.IsHealthy() {
			t.Fatal("restarted after stop")
		}
		stop()
	}
}

func TestCrashRestartsAndStopDuringBackoff(t *testing.T) {
	bin := os.Getenv("FORGE_MINIMAL_WORKER")
	if bin == "" {
		t.Fatal("FORGE_MINIMAL_WORKER is required")
	}
	dir, err := os.MkdirTemp("", "fg-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "w.sock")
	if runtime.GOOS == "windows" {
		socket = fmt.Sprintf(`\\.\pipe\forge-crash-%d`, os.Getpid())
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := forge.NewWorkerProcess(0, forge.WorkerConfig{BinaryPath: bin, SocketPath: socket, Encoding: forge.EncodingJSON})
	defer func() {
		stop, c := context.WithTimeout(context.Background(), 2*time.Second)
		defer c()
		_ = w.StopAndWait(stop)
	}()
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	health, err := w.Client().Conn().Call(ctx, "health", nil)
	if err != nil {
		t.Fatal(err)
	}
	var status struct {
		PID int `json:"pid"`
	}
	if health.Payload == nil {
		t.Fatal("missing health")
	}
	if err := json.Unmarshal(*health.Payload, &status); err != nil {
		t.Fatal(err)
	}
	process, err := os.FindProcess(status.PID)
	if err != nil {
		t.Fatal(err)
	}
	if err := process.Kill(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	restarted := false
	for time.Now().Before(deadline) {
		time.Sleep(25 * time.Millisecond)
		client := w.Client()
		if client == nil {
			continue
		}
		check, c := context.WithTimeout(ctx, 200*time.Millisecond)
		response, err := client.Conn().Call(check, "health", nil)
		c()
		if err == nil && response.Payload != nil {
			var now struct {
				PID int `json:"pid"`
			}
			_ = json.Unmarshal(*response.Payload, &now)
			if now.PID != status.PID {
				restarted = true
				break
			}
		}
	}
	if !restarted {
		t.Fatal("unexpected crash did not restart")
	}
}

func TestReliableEventsAcrossCreditWindow(t *testing.T) {
	for _, encoding := range []forge.Encoding{forge.EncodingJSON, forge.EncodingMsgpack} {
		t.Run(fmt.Sprint(encoding), func(t *testing.T) {
			bin := os.Getenv("FORGE_MINIMAL_WORKER")
			if bin == "" {
				t.Fatal("FORGE_MINIMAL_WORKER required")
			}
			dir, err := os.MkdirTemp("", "fg-")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)
			socket := filepath.Join(dir, "w.sock")
			if runtime.GOOS == "windows" {
				socket = fmt.Sprintf(`\\.\pipe\forge-events-%d-%d`, os.Getpid(), encoding)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			w := forge.NewWorkerProcess(0, forge.WorkerConfig{BinaryPath: bin, SocketPath: socket, Encoding: encoding})
			defer w.Stop(context.Background())
			if err := w.Start(ctx); err != nil {
				t.Fatal(err)
			}
			conn, ok := w.Client().Conn().(*forge.Conn)
			if !ok {
				t.Fatal("missing connection")
			}
			stream, err := conn.ConfigureReliable(ctx)
			if err != nil {
				t.Fatal(err)
			}
			response, err := conn.Call(ctx, "example_events", map[string]int{"count": 300})
			if err != nil || !response.OK {
				t.Fatalf("start: %v", err)
			}
			// Let the producer fill its credit window without committing events.
			time.Sleep(100 * time.Millisecond)
			control, stopControl := context.WithTimeout(ctx, time.Second)
			if _, err := w.Client().Ping(control); err != nil {
				stopControl()
				t.Fatalf("control blocked behind full event window: %v", err)
			}
			stopControl()
			for i := 1; i <= 300; i++ {
				select {
				case ev, ok := <-stream.Events():
					if !ok {
						t.Fatalf("closed: %v", stream.Err())
					}
					if ev.Sequence != uint64(i) {
						t.Fatal("sequence gap")
					}
					if err := stream.Commit(ctx, ev); err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			count, bytes := stream.Stats()
			if count != 0 || bytes != 0 {
				t.Fatalf("retained credits %d %d", count, bytes)
			}
		})
	}
}
