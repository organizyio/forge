package forge

import (
	"reflect"
	"testing"
)

func TestWorkerProcess_forgeFlags_socket(t *testing.T) {
	w := NewWorkerProcess(1, WorkerConfig{
		BinaryPath: "bin",
		SourceID:   "src",
		SocketPath: "/tmp/x.sock",
		Encoding:   EncodingJSON,
		LogLevel:   "error",
		Args:       []string{"script.py"},
	})
	got := append(append([]string{}, w.args...), w.forgeFlags()...)
	want := []string{"script.py", "--socket", "/tmp/x.sock", "--source-id", "src", "--log-level", "error", "--encoding", "json"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("argv=%v want %v", got, want)
	}
}

func TestWorkerProcess_forgeFlags_stdio(t *testing.T) {
	w := NewWorkerProcess(0, WorkerConfig{
		BinaryPath: "bin",
		SourceID:   "s",
		Encoding:   EncodingMsgpack,
		Transport:  TransportStdio,
	})
	got := w.forgeFlags()
	want := []string{"--stdio", "--source-id", "s", "--log-level", "info", "--encoding", "msgpack"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("argv=%v want %v", got, want)
	}
}
