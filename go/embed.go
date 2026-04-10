package forge

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
)

// EmbeddedWorker holds a binary that has been extracted from an embed.FS to a
// temporary file on disk, ready to be passed to WorkerConfig.BinaryPath.
type EmbeddedWorker struct {
	// Path is the absolute path to the extracted executable.
	Path string
	// cleanup removes the temporary directory when called.
	cleanup func()
}

// Close removes the extracted binary and its containing directory.
// Safe to call multiple times.
func (e *EmbeddedWorker) Close() {
	if e.cleanup != nil {
		e.cleanup()
		e.cleanup = nil
	}
}

// ExtractEmbedded writes the binary at fsPath inside embedFS to a temporary
// directory, makes it executable, and returns an EmbeddedWorker whose Path
// field can be set directly on WorkerConfig.BinaryPath.
//
// The caller must call EmbeddedWorker.Close() when the worker pool shuts down
// to remove the temporary file.
//
// Typical usage in a product binary (e.g. Organizy):
//
//	//go:embed embedded/organizy-worker
//	var workerFS embed.FS
//
//	w, err := forge.ExtractEmbedded(workerFS, "embedded/organizy-worker")
//	if err != nil { ... }
//	defer w.Close()
//
//	cfg := forge.WorkerConfig{ BinaryPath: w.Path, ... }
func ExtractEmbedded(embedFS fs.ReadFileFS, fsPath string) (*EmbeddedWorker, error) {
	data, err := embedFS.ReadFile(fsPath)
	if err != nil {
		return nil, fmt.Errorf("read embedded worker %q: %w", fsPath, err)
	}

	tmpDir, err := os.MkdirTemp("", "forge-worker-*")
	if err != nil {
		return nil, fmt.Errorf("create temp dir for worker: %w", err)
	}

	binName := filepath.Base(fsPath)
	if runtime.GOOS == "windows" && filepath.Ext(binName) == "" {
		binName += ".exe"
	}

	binPath := filepath.Join(tmpDir, binName)
	if err := os.WriteFile(binPath, data, 0o755); err != nil {
		_ = os.RemoveAll(tmpDir)
		return nil, fmt.Errorf("write embedded worker to %s: %w", binPath, err)
	}

	return &EmbeddedWorker{
		Path: binPath,
		cleanup: func() {
			_ = os.RemoveAll(tmpDir)
		},
	}, nil
}
