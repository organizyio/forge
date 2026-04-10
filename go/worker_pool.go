package forge

import "context"

// PoolConfig configures a logical pool of worker processes.
type PoolConfig struct {
	Workers []*WorkerProcess
}

// Pool holds worker processes so callers can iterate them (e.g. broadcast RPCs)
// or start/stop them as a group.
type Pool struct {
	workers []*WorkerProcess
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
