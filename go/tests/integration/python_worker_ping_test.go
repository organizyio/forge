//go:build integration && unix

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

func minimalPythonWorker(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("FORGE_MINIMAL_PYTHON_WORKER"); p != "" {
		st, err := os.Stat(p)
		require.NoError(t, err)
		require.False(t, st.IsDir())
		return p
	}
	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)
	integDir := filepath.Dir(file)
	candidate := filepath.Clean(filepath.Join(integDir, "..", "..", "..", "python", "examples", "minimal-python-worker", "main.py"))
	st, err := os.Stat(candidate)
	if err != nil || st.IsDir() {
		t.Skipf("minimal-python-worker not found at %s", candidate)
	}
	return candidate
}

func TestPythonWorker_Ping(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}
	script := minimalPythonWorker(t)
	sock := filepath.Join(os.TempDir(), fmt.Sprintf("fpy-%d.sock", os.Getpid()))
	defer os.Remove(sock)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wp := forge.NewWorkerProcess(0, forge.WorkerConfig{
		BinaryPath: script,
		SourceID:   "integ-py",
		SocketPath: sock,
		Encoding:   forge.EncodingJSON,
		LogLevel:   "error",
	})
	require.NoError(t, wp.Start(ctx))
	defer wp.Stop(context.Background())

	client := wp.Client()
	require.NotNil(t, client)
	pingCtx, cancelPing := context.WithTimeout(ctx, 5*time.Second)
	defer cancelPing()
	resp, err := client.Ping(pingCtx)
	require.NoError(t, err)
	require.True(t, resp.OK)
}
