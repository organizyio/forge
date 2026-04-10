//go:build windows && integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	forge "github.com/organizyio/forge/go"
	"github.com/stretchr/testify/require"
)

func minimalWorkerBinaryWindows(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("FORGE_MINIMAL_WORKER"); p != "" {
		st, err := os.Stat(p)
		require.NoError(t, err, "FORGE_MINIMAL_WORKER")
		require.False(t, st.IsDir(), "FORGE_MINIMAL_WORKER must be a file")
		return p
	}
	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok, "runtime.Caller")
	integDir := filepath.Dir(file)
	candidate := filepath.Clean(filepath.Join(integDir, "..", "..", "..", "rust", "target", "debug", "minimal-worker.exe"))
	if st, err := os.Stat(candidate); err == nil && !st.IsDir() {
		return candidate
	}
	t.Skipf("minimal-worker not found at %s — run: (cd forge/rust && cargo build -p minimal-worker), or set FORGE_MINIMAL_WORKER", candidate)
	return ""
}

func TestWorkerProcess_NamedPipe(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}
	bin := minimalWorkerBinaryWindows(t)
	pipe := fmt.Sprintf(`\\.\pipe\forge-workerproc-test-%d`, os.Getpid())

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	wp := forge.NewWorkerProcess(0, forge.WorkerConfig{
		BinaryPath: bin,
		SourceID:   "integ",
		SocketPath: pipe,
		Encoding:   forge.EncodingJSON,
		LogLevel:   "error",
	})
	require.NoError(t, wp.Start(ctx))
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		wp.Stop(stopCtx)
	}()

	c := wp.Client()
	require.NotNil(t, c)
	_, err := c.Ping(ctx)
	require.NoError(t, err)
}
