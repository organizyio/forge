//go:build windows

package forge

import (
	"context"
	"fmt"
	"time"

	"github.com/Microsoft/go-winio"
)

func waitForPipeReady(ctx context.Context, path string, timeout time.Duration) error {
	pipePath, ok := NamedPipeDialPath(path)
	if !ok {
		return fmt.Errorf("not a named pipe path: %s", path)
	}
	deadline := time.Now().Add(timeout)
	delay := 10 * time.Millisecond
	var lastErr error
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		c, err := winio.DialPipeContext(ctx, pipePath)
		if err == nil {
			_ = c.Close()
			return nil
		}
		lastErr = err
		time.Sleep(delay)
		if delay < 100*time.Millisecond {
			delay *= 2
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("timeout")
	}
	return fmt.Errorf("pipe %s not ready after %v: %w", path, timeout, lastErr)
}
