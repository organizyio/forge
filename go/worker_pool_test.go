package forge

import (
	"testing"
)

func TestPool_PickHealthy(t *testing.T) {
	a := &WorkerProcess{}
	b := &WorkerProcess{}
	a.healthy.Store(false)
	b.healthy.Store(true)
	p := NewPool(PoolConfig{Workers: []*WorkerProcess{a, b}})
	got, err := p.PickHealthy()
	if err != nil || got != b {
		t.Fatalf("PickHealthy: got %v err %v want b", got, err)
	}
}

func TestPool_NextHealthy_roundRobin(t *testing.T) {
	a := &WorkerProcess{}
	b := &WorkerProcess{}
	a.healthy.Store(true)
	b.healthy.Store(true)
	p := NewPool(PoolConfig{Workers: []*WorkerProcess{a, b}})
	first, err := p.NextHealthy()
	if err != nil {
		t.Fatal(err)
	}
	second, err := p.NextHealthy()
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatalf("expected round-robin to alternate, got same worker")
	}
	third, err := p.NextHealthy()
	if err != nil {
		t.Fatal(err)
	}
	if third != first {
		t.Fatalf("expected cycle back to first")
	}
}

func TestPool_PickHealthy_none(t *testing.T) {
	p := NewPool(PoolConfig{Workers: []*WorkerProcess{{}, {}}})
	_, err := p.PickHealthy()
	if err != ErrNoHealthyWorker {
		t.Fatalf("want ErrNoHealthyWorker, got %v", err)
	}
}
