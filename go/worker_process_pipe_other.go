//go:build !windows

package forge

func isWorkerPipePath(path string) bool {
	_ = path
	return false
}
