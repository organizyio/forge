//go:build windows

package forge

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/Microsoft/go-winio"
)

func dialWorker(ctx context.Context, addr string) (net.Conn, error) {
	if pipePath, ok := NamedPipeDialPath(addr); ok {
		c, err := winio.DialPipeContext(ctx, pipePath)
		if err != nil {
			return nil, fmt.Errorf("dial named pipe %s: %w", pipePath, err)
		}
		return c, nil
	}
	var d net.Dialer
	c, err := d.DialContext(ctx, "unix", addr)
	if err != nil {
		return nil, fmt.Errorf("dial unix %s: %w", addr, err)
	}
	return c, nil
}

// NamedPipeDialPath returns the Win32 pipe path if addr refers to a named pipe.
// Slash forms like //./pipe/Name are normalized to \\.\pipe\Name.
func NamedPipeDialPath(addr string) (string, bool) {
	s := strings.ReplaceAll(addr, `/`, `\`)
	low := strings.ToLower(s)
	switch {
	case strings.HasPrefix(low, `\\.\pipe\`):
		return s, true
	case strings.HasPrefix(low, `\\?\pipe\`):
		return s, true
	case strings.HasPrefix(low, `\??\pipe\`):
		return s, true
	default:
		return "", false
	}
}
