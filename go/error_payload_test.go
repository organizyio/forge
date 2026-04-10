package forge_test

import (
	"testing"

	forge "github.com/organizyio/forge/go"
)

func TestErrorPayload_Error(t *testing.T) {
	t.Parallel()
	e := &forge.ErrorPayload{Code: "E_BAD", Message: "something failed", Detail: "x"}
	if got, want := e.Error(), "[E_BAD] something failed"; got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}
