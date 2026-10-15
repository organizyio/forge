package forge

import (
	"context"
	"errors"
	"sync/atomic"
)

// ErrNoHealthyWorker is returned when no worker in the pool reports healthy.
var ErrNoHealthyWorker = errors.New("forge: no healthy worker")

// PoolConfig configures a logical pool of worker processes.
type PoolConfig struct {
	Workers []*WorkerProcess
}

// Pool holds worker processes so callers can iterate them (e.g. broadcast RPCs)
// or start/stop them as a group.
type Pool struct {
	workers []*WorkerProcess
	rr      atomic.Uint64
}

// NewPool constructs a pool from the given workers.
func NewPool(cfg PoolConfig) *Pool {
	return &Pool{workers: cfg.Workers}
}

// Workers returns the configured worker processes.
func (p *Pool) Workers() []*WorkerProcess {
	if p == nil {
		return nil
	}
	return p.workers
}

// Start calls Start on each non-nil worker in order; returns the first error.
func (p *Pool) Start(ctx context.Context) error {
	if p == nil {
		return nil
	}
	for _, w := range p.workers {
		if w == nil {
			continue
		}
		if err := w.Start(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Stop calls Stop on each non-nil worker.
func (p *Pool) Stop(ctx context.Context) {
	if p == nil {
		return
	}
	for _, w := range p.workers {
		if w != nil {
			w.Stop(ctx)
		}
	}
}

// PickHealthy returns the first healthy worker in slice order.
func (p *Pool) PickHealthy() (*WorkerProcess, error) {
	if p == nil {
		return nil, ErrNoHealthyWorker
	}
	for _, w := range p.workers {
		if w != nil && w.IsHealthy() {
			return w, nil
		}
	}
	return nil, ErrNoHealthyWorker
}

// NextHealthy returns the next healthy worker using round-robin among the pool slice.
func (p *Pool) NextHealthy() (*WorkerProcess, error) {
	if p == nil || len(p.workers) == 0 {
		return nil, ErrNoHealthyWorker
	}
	n := len(p.workers)
	start := int(p.rr.Add(1) - 1)
	for i := 0; i < n; i++ {
		w := p.workers[(start+i)%n]
		if w != nil && w.IsHealthy() {
			return w, nil
		}
	}
	return nil, ErrNoHealthyWorker
}
