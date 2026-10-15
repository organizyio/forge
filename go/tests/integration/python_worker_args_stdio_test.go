//go:build integration && unix

package integration_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	forge "github.com/organizyio/forge/go"
	"github.com/stretchr/testify/require"
)

func TestPythonWorker_ArgsPing(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not on PATH")
	}
	script := minimalPythonWorker(t)
	// Keep AF_UNIX path short (macOS ~104 byte limit).
	sock := filepath.Join(os.TempDir(), fmt.Sprintf("fpa-%d.sock", os.Getpid()))
	defer os.Remove(sock)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wp := forge.NewWorkerProcess(0, forge.WorkerConfig{
		BinaryPath: "python3",
		Args:       []string{script},
		SourceID:   "integ-py-args",
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

func TestPythonWorker_StdioPing(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not on PATH")
	}
	script := minimalPythonWorker(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wp := forge.NewWorkerProcess(0, forge.WorkerConfig{
		BinaryPath: "python3",
		Args:       []string{script},
		SourceID:   "integ-py-stdio",
		Encoding:   forge.EncodingJSON,
		LogLevel:   "error",
		Transport:  forge.TransportStdio,
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
