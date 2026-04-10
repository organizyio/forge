//go:build !(integration && unix)

package integration_test

import "testing"

func TestIntegrationRequiresTagsAndUnix(t *testing.T) {
	t.Skip("Forge IPC integration tests: go test -tags=integration ./tests/integration/... on Unix, with minimal-worker built (see forge/Makefile sdk-go-integration)")
}
