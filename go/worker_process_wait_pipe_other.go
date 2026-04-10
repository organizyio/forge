//go:build !windows

package forge

import (
	"context"
	"fmt"
	"time"
)

func waitForPipeReady(ctx context.Context, path string, timeout time.Duration) error {
	_, _, _ = ctx, path, timeout
	return fmt.Errorf("waitForPipeReady: windows only")
}
