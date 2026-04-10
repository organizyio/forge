package forge

import (
	"context"
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
	cmd     *exec.Cmd
	client  *Client
	healthy atomic.Bool
	onEvent func(*Event)
	done    chan struct{}
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
		done:       make(chan struct{}),
	}
}

// Start launches the worker binary and connects.
func (w *WorkerProcess) Start(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.startLocked(ctx)
}

func (w *WorkerProcess) startLocked(ctx context.Context) error {
	w.cleanupSocket()
	const readyTimeout = 10 * time.Second
	cmd := exec.CommandContext(ctx, w.binaryPath, "--socket", w.socketPath, "--source-id", w.sourceID, "--log-level", w.logLevel, "--encoding", encodingFlag(w.encoding))
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("spawn worker: %w", err)
	}
	w.cmd = cmd
	var waitErr error
	if isWorkerPipePath(w.socketPath) {
		waitErr = waitForPipeReady(ctx, w.socketPath, readyTimeout)
	} else {
		waitErr = waitForSocket(ctx, w.socketPath, readyTimeout)
	}
	if waitErr != nil {
		_ = cmd.Process.Kill()
		if isWorkerPipePath(w.socketPath) {
			return fmt.Errorf("worker pipe never became ready: %w", waitErr)
		}
		return fmt.Errorf("worker socket never appeared: %w", waitErr)
	}
	conn, err := Dial(ctx, w.socketPath, w.encoding, w.onEvent)
	if err != nil {
		_ = cmd.Process.Kill()
		return fmt.Errorf("connect to worker: %w", err)
	}
	client := NewClient(conn)
	if _, err := client.Ping(ctx); err != nil {
		_ = cmd.Process.Kill()
		return fmt.Errorf("worker ping failed: %w", err)
	}
	w.client = client
	w.healthy.Store(true)
	go w.supervise(ctx)
	return nil
}

func (w *WorkerProcess) supervise(ctx context.Context) {
	cmd := w.cmd
	_ = cmd.Wait()
	w.healthy.Store(false)
	w.log.Warn("worker exited, scheduling restart")
	if ctx.Err() != nil {
		close(w.done)
		return
	}
	backoff := 500 * time.Millisecond
	for i := 0; i < 5; i++ {
		select {
		case <-ctx.Done():
			close(w.done)
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		w.mu.Lock()
		err := w.startLocked(ctx)
		w.mu.Unlock()
		if err == nil {
			w.log.Info("worker restarted successfully")
			return
		}
		w.log.Error("restart attempt failed", "err", err, "attempt", i+1)
	}
	w.log.Error("worker failed to restart after 5 attempts")
	close(w.done)
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

// Stop shuts down the worker process.
func (w *WorkerProcess) Stop(ctx context.Context) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.client != nil {
		_, _ = w.client.Shutdown(ctx)
		_ = w.client.Close()
		w.client = nil
	}
	if w.cmd != nil && w.cmd.Process != nil {
		_ = w.cmd.Process.Kill()
	}
	w.cleanupSocket()
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
