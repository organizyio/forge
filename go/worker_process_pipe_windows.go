//go:build windows

package forge

func isWorkerPipePath(path string) bool {
	_, ok := NamedPipeDialPath(path)
	return ok
}
