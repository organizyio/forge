//go:build integration && unix

package integration_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	forge "github.com/organizyio/forge/go"
	"github.com/stretchr/testify/require"
)

// minimalWorkerBinary resolves the minimal-worker executable.
// Set FORGE_MINIMAL_WORKER to override. Otherwise expects
// ../../../rust/target/debug/minimal-worker relative to this file (under forge/rust).
func minimalWorkerBinary(t *testing.T) string {
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
	// forge/go/tests/integration -> ../../../rust/target/debug/minimal-worker
	candidate := filepath.Clean(filepath.Join(integDir, "..", "..", "..", "rust", "target", "debug", "minimal-worker"))
	if st, err := os.Stat(candidate); err == nil && !st.IsDir() {
		return candidate
	}
	t.Skipf("minimal-worker not found at %s — run: (cd ../../../rust && cargo build -p minimal-worker), or set FORGE_MINIMAL_WORKER", candidate)
	return "" // unreachable when Skipf stops the test; satisfies missing-return analysis
}

func TestMinimalWorker_ShutdownWithDelay(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}
	bin := minimalWorkerBinary(t)
	sock := filepath.Join(t.TempDir(), "forge-shutdown-integ.sock")

	cmd := exec.Command(bin, "--socket", sock, "--encoding", "json", "--log-level", "error")
	require.NoError(t, cmd.Start())

	done := make(chan struct{})
	var waitErr error
	go func() {
		waitErr = cmd.Wait()
		close(done)
	}()

	defer func() {
		select {
		case <-done:
		default:
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
			<-done
		}
	}()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(sock); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	_, err := os.Stat(sock)
	require.NoError(t, err, "socket never appeared")

	ctx := context.Background()
	dialCtx, cancelDial := context.WithTimeout(ctx, 5*time.Second)
	defer cancelDial()
	conn, err := forge.Dial(dialCtx, sock, forge.EncodingJSON, nil)
	require.NoError(t, err)
	defer conn.Close()

	client := forge.NewClient(conn)
	rpcCtx, cancelRPC := context.WithTimeout(ctx, 5*time.Second)
	resp, err := client.ShutdownWithDelay(rpcCtx, 2000)
	cancelRPC()
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.True(t, resp.OK, "response: %+v", resp)
	rpcDone := time.Now()

	select {
	case <-done:
		require.NoError(t, waitErr, "worker exit")
		exitDelay := time.Since(rpcDone)
		if exitDelay < 1500*time.Millisecond {
			t.Fatalf("worker exited too soon after shutdown response (want ~2s delay_ms): %s", exitDelay)
		}
		if exitDelay >= 4*time.Second {
			t.Fatalf("worker took too long to exit: %s", exitDelay)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for worker process to exit")
	}
}
