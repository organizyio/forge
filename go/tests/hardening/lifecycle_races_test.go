//go:build hardening

package hardening_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	forge "github.com/organizyio/forge/go"
)

func auditWorker(t *testing.T, onConnect func(context.Context, *forge.Client) error) *forge.WorkerProcess {
	t.Helper()
	bin := os.Getenv("FORGE_MINIMAL_WORKER")
	if bin == "" {
		t.Fatal("FORGE_MINIMAL_WORKER required")
	}
	dir, err := os.MkdirTemp("", "fg-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "w.sock")
	if runtime.GOOS == "windows" {
		socket = fmt.Sprintf(`\\.\pipe\forge-audit-%d-%d`, os.Getpid(), time.Now().UnixNano())
	}
	w := forge.NewWorkerProcess(0, forge.WorkerConfig{BinaryPath: bin, SocketPath: socket, Encoding: forge.EncodingJSON, OnConnect: onConnect})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := w.StopAndWait(ctx); err != nil {
			t.Error(err)
		}
	})
	return w
}
func auditCrash(t *testing.T, w *forge.WorkerProcess) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	r, err := w.Client().Conn().Call(ctx, "health", nil)
	if err != nil || r.Payload == nil {
		t.Fatalf("health %v", err)
	}
	var health struct {
		PID int `json:"pid"`
	}
	if err = json.Unmarshal(*r.Payload, &health); err != nil || health.PID <= 0 {
		t.Fatalf("pid %+v %v", health, err)
	}
	process, err := os.FindProcess(health.PID)
	if err != nil {
		t.Fatal(err)
	}
	if err = process.Kill(); err != nil {
		t.Fatal(err)
	}
}
func TestStopDuringInitializationAndRestartReadiness(t *testing.T) {
	for _, duringRestart := range []bool{false, true} {
		t.Run(fmt.Sprint(duringRestart), func(t *testing.T) {
			entered := make(chan struct{})
			var connections atomic.Int32
			w := auditWorker(t, func(ctx context.Context, _ *forge.Client) error {
				n := connections.Add(1)
				if (!duringRestart && n == 1) || (duringRestart && n == 2) {
					close(entered)
					<-ctx.Done()
					return ctx.Err()
				}
				return nil
			})
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			started := make(chan error, 1)
			if duringRestart {
				if err := w.Start(parent); err != nil {
					t.Fatal(err)
				}
				auditCrash(t, w)
			} else {
				go func() { started <- w.Start(parent) }()
			}
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("initialization barrier not reached")
			}
			ctx, stop := context.WithTimeout(context.Background(), 2*time.Second)
			defer stop()
			if err := w.StopAndWait(ctx); err != nil {
				t.Fatal(err)
			}
			if w.IsHealthy() {
				t.Fatal("healthy after intentional stop")
			}
			if !duringRestart {
				select {
				case err := <-started:
					if err == nil {
						t.Fatal("stopped startup succeeded")
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
		})
	}
}
func TestConcurrentStartStopAndExplicitRestart(t *testing.T) {
	w := auditWorker(t, nil)
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	const count = 8
	results := make(chan error, count)
	for n := 0; n < count; n++ {
		go func() { results <- w.Start(parent) }()
	}
	successes := 0
	for n := 0; n < count; n++ {
		err := <-results
		if err == nil {
			successes++
		} else if !errors.Is(err, forge.ErrWorkerStarted) {
			t.Fatal(err)
		}
	}
	if successes != 1 {
		t.Fatalf("successful starts %d", successes)
	}
	var wg sync.WaitGroup
	for n := 0; n < count; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
			defer stop()
			if err := w.StopAndWait(ctx); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if w.IsHealthy() {
		t.Fatal("healthy after concurrent stop")
	}
	if err := w.Start(parent); err != nil {
		t.Fatal(err)
	}
}
func TestRestartAttemptsExhaustAndLifecycleCanStartAgain(t *testing.T) {
	var connections atomic.Int32
	exhausted := make(chan struct{})
	w := auditWorker(t, func(context.Context, *forge.Client) error {
		n := connections.Add(1)
		if n > 1 && n <= 6 {
			if n == 6 {
				close(exhausted)
			}
			return errors.New("injected restart initialization failure")
		}
		return nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	auditCrash(t, w)
	select {
	case <-exhausted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// StopAndWait joins the exhausted generation; it must not retry indefinitely.
	if err := w.StopAndWait(ctx); err != nil {
		t.Fatal(err)
	}
	if connections.Load() != 6 || w.IsHealthy() {
		t.Fatalf("attempts %d healthy %v", connections.Load(), w.IsHealthy())
	}
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestStopDuringBackoffKeepsParentAliveWithoutRestart(t *testing.T) {
	var connections atomic.Int32
	w := auditWorker(t, func(context.Context, *forge.Client) error { connections.Add(1); return nil })
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := w.Start(parent); err != nil {
		t.Fatal(err)
	}
	auditCrash(t, w)
	deadline := time.Now().Add(2 * time.Second)
	for w.IsHealthy() {
		if time.Now().After(deadline) {
			t.Fatal("crash not observed")
		}
		time.Sleep(5 * time.Millisecond)
	}
	ctx, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()
	if err := w.StopAndWait(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(750 * time.Millisecond)
	if connections.Load() != 1 || w.IsHealthy() || parent.Err() != nil {
		t.Fatalf("restart after stop: connections=%d healthy=%v parent=%v", connections.Load(), w.IsHealthy(), parent.Err())
	}
}
