//go:build !windows

package forge

import (
	"context"
	"fmt"
	"net"
)

func dialWorker(ctx context.Context, addr string) (net.Conn, error) {
	var d net.Dialer
	c, err := d.DialContext(ctx, "unix", addr)
	if err != nil {
		return nil, fmt.Errorf("dial unix %s: %w", addr, err)
	}
	return c, nil
}
