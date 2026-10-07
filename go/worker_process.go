package forge

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// WorkerProcess supervises a single worker subprocess and its RPC connection.
type WorkerProcess struct {
	id         int
	socketPath string
	binaryPath string
	sourceID   string
	encoding   Encoding
	logLevel   string
	log        *slog.Logger

	mu      sync.Mutex
	client  *Client
	healthy atomic.Bool
	onEvent func(*Event)
	run     *workerRun
	state   string
}

// ErrWorkerStarted means a worker lifecycle is already active.
var ErrWorkerStarted = errors.New("worker already started or stopping")

type workerRun struct {
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	stop     chan struct{}
	stopOnce sync.Once
	cmd      *exec.Cmd
	exited   chan struct{}
	client   *Client
}

// WorkerConfig configures a supervised worker process.
type WorkerConfig struct {
	BinaryPath string
	SourceID   string
	// SocketDir is joined with forge-worker-<id>.sock when SocketPath is empty.
	SocketDir string
	// SocketPath, if non-empty, is the full listen address: a Unix socket filesystem
	// path or a Windows named pipe path (e.g. \\.\pipe\forge-worker-0). When set,
	// SocketDir is ignored for the listen path.
	SocketPath string
	Encoding   Encoding
	LogLevel   string
	OnEvent    func(*Event)
	// Log receives process lifecycle messages. If nil, logs are discarded.
	Log *slog.Logger
}

// NewWorkerProcess constructs a worker supervisor for the given id and config.
func NewWorkerProcess(id int, cfg WorkerConfig) *WorkerProcess {
	socketPath := cfg.SocketPath
	if socketPath == "" {
		socketPath = filepath.Join(cfg.SocketDir, fmt.Sprintf("forge-worker-%d.sock", id))
	}
	logLevel := cfg.LogLevel
	if logLevel == "" {
		logLevel = "info"
	}
	log := cfg.Log
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	log = log.With(slog.Int("worker_id", id))
	return &WorkerProcess{
		id:         id,
		socketPath: socketPath,
		binaryPath: cfg.BinaryPath,
		sourceID:   cfg.SourceID,
		encoding:   cfg.Encoding,
		logLevel:   logLevel,
		onEvent:    cfg.OnEvent,
		log:        log,
		state:      "stopped",
	}
}

// Start launches one worker generation. Repeated starts require a completed stop.
func (w *WorkerProcess) Start(ctx context.Context) error {
	w.mu.Lock()
	if w.run != nil {
		w.mu.Unlock()
		return ErrWorkerStarted
	}
	lifetime, cancel := context.WithCancel(ctx)
	r := &workerRun{ctx: lifetime, cancel: cancel, done: make(chan struct{}), stop: make(chan struct{})}
	w.run = r
	w.state = "starting"
	w.mu.Unlock()
	if err := w.launch(r); err != nil {
		w.finish(r)
		return err
	}
	go w.supervise(r)
	return nil
}

func (w *WorkerProcess) launch(r *workerRun) error {
	w.mu.Lock()
	if w.run != r || w.state == "stopping" || r.ctx.Err() != nil {
		w.mu.Unlock()
		return context.Canceled
	}
	w.cleanupSocket()
	w.mu.Unlock()
	cmd := exec.CommandContext(r.ctx, w.binaryPath, "--socket", w.socketPath, "--source-id", w.sourceID, "--log-level", w.logLevel, "--encoding", encodingFlag(w.encoding))
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("spawn worker: %w", err)
	}
	exited := make(chan struct{})
	go func() { _ = cmd.Wait(); close(exited) }()
	fail := func(err error) error { _ = cmd.Process.Kill(); <-exited; return err }
	w.mu.Lock()
	r.cmd = cmd
	r.exited = exited
	w.mu.Unlock()
	ready, cancel := context.WithTimeout(r.ctx, 10*time.Second)
	defer cancel()
	var err error
	if isWorkerPipePath(w.socketPath) {
		err = waitForPipeReady(ready, w.socketPath, 10*time.Second)
	} else {
		err = waitForSocket(ready, w.socketPath, 10*time.Second)
	}
	if err != nil {
		return fail(fmt.Errorf("worker readiness: %w", err))
	}
	conn, err := Dial(ready, w.socketPath, w.encoding, w.onEvent)
	if err != nil {
		return fail(fmt.Errorf("connect to worker: %w", err))
	}
	client := NewClient(conn)
	if _, err = client.Ping(ready); err != nil {
		_ = client.Close()
		return fail(fmt.Errorf("worker ping: %w", err))
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.run != r || w.state == "stopping" || r.ctx.Err() != nil {
		_ = client.Close()
		return fail(context.Canceled)
	}
	r.client = client
	w.client = client
	w.state = "running"
	w.healthy.Store(true)
	return nil
}

func (w *WorkerProcess) finish(r *workerRun) {
	r.cancel()
	w.mu.Lock()
	defer w.mu.Unlock()
	if r.client != nil {
		_ = r.client.Close()
	}
	if w.run == r {
		w.healthy.Store(false)
		w.client = nil
		w.cleanupSocket()
		w.run = nil
		w.state = "stopped"
	}
	close(r.done)
}

func (w *WorkerProcess) supervise(r *workerRun) {
	defer w.finish(r)
	for {
		w.mu.Lock()
		exited := r.exited
		w.mu.Unlock()
		select {
		case <-exited:
		case <-r.stop:
			<-exited
			return
		case <-r.ctx.Done():
			<-exited
			return
		}
		w.mu.Lock()
		w.healthy.Store(false)
		if r.client != nil {
			_ = r.client.Close()
			r.client = nil
		}
		w.client = nil
		stopping := w.state == "stopping"
		if !stopping {
			w.state = "restarting"
		}
		w.mu.Unlock()
		if stopping || r.ctx.Err() != nil {
			return
		}
		restarted := false
		for i := 0; i < 5; i++ {
			timer := time.NewTimer((500 * time.Millisecond) << i)
			select {
			case <-r.stop:
				timer.Stop()
				return
			case <-r.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			if err := w.launch(r); err == nil {
				restarted = true
				break
			} else {
				w.log.Error("restart failed", "err", err, "attempt", i+1)
			}
		}
		if !restarted {
			return
		}
	}
}

// Client returns the RPC client when the worker is healthy.
func (w *WorkerProcess) Client() *Client {
	if !w.healthy.Load() {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.client
}

// Stop preserves the original API and waits within ctx for shutdown.
func (w *WorkerProcess) Stop(ctx context.Context) { _ = w.StopAndWait(ctx) }

// StopAndWait prevents restart before shutting down and reaps the process.
func (w *WorkerProcess) StopAndWait(ctx context.Context) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
	}
	w.mu.Lock()
	r := w.run
	if r == nil {
		w.mu.Unlock()
		return nil
	}
	w.state = "stopping"
	w.healthy.Store(false)
	var first bool
	r.stopOnce.Do(func() { close(r.stop); first = true })
	client := r.client
	w.mu.Unlock()
	if first {
		// Startup is canceled immediately; a running worker gets bounded graceful shutdown.
		if client == nil {
			r.cancel()
		} else {
			_, _ = client.Shutdown(ctx)
			_ = client.Close()
		}
	}
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		r.cancel()
		return ctx.Err()
	}
}

func (w *WorkerProcess) cleanupSocket() {
	if !isWorkerPipePath(w.socketPath) {
		_ = os.Remove(w.socketPath)
	}
}

// IsHealthy reports whether the worker is connected and healthy.
func (w *WorkerProcess) IsHealthy() bool { return w.healthy.Load() }

// ID returns the worker index.
func (w *WorkerProcess) ID() int { return w.id }

func waitForSocket(ctx context.Context, path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for socket %s", path)
}

func encodingFlag(e Encoding) string {
	if e == EncodingJSON {
		return "json"
	}
	return "msgpack"
}
