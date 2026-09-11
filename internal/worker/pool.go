package worker

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/JustinK33/Conduit/pkg/models"
)

type JobRunner interface {
	Run(context.Context, models.Job) error
}

type Config struct {
	Concurrency     int
	QueueSize       int
	ShutdownTimeout time.Duration
}

type Pool struct {
	Runner    JobRunner
	jobs      chan models.Job
	semaphore chan struct{}
	cfg       Config
	wg        sync.WaitGroup
}

func NewPool(cfg Config, runner JobRunner) *Pool {
	return &Pool{
		Runner:    runner,
		jobs:      make(chan models.Job, cfg.QueueSize),
		semaphore: make(chan struct{}, cfg.Concurrency),
		cfg:       cfg,
	}
}

func (p *Pool) Start(ctx context.Context) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			select {
			case job, ok := <-p.jobs:
				if !ok {
					return
				}
				p.semaphore <- struct{}{}
				p.wg.Add(1)
				go func(j models.Job) {
					defer p.wg.Done()
					defer func() { <-p.semaphore }()
					defer func() {
						if r := recover(); r != nil {
							fmt.Fprintf(os.Stderr, "worker: panic in job %s: %v\n", j.ID, r)
						}
					}()
					_ = p.Runner.Run(ctx, j) // runner is responsible for its own error handling
				}(job)
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Submit returns false (without blocking) when the queue is full or ctx is done.
func (p *Pool) Submit(ctx context.Context, job models.Job) bool {
	if ctx.Err() != nil {
		return false
	}
	select {
	case p.jobs <- job:
		return true
	case <-ctx.Done():
		return false
	default:
		return false
	}
}

// SubmitBlocking waits until the queue accepts the job or ctx is cancelled.
func (p *Pool) SubmitBlocking(ctx context.Context, job models.Job) bool {
	if ctx.Err() != nil {
		return false
	}
	select {
	case p.jobs <- job:
		return true
	case <-ctx.Done():
		return false
	}
}

// Stop closes the queue and waits for in-flight work, bounded by ctx.
func (p *Pool) Stop(ctx context.Context) error {
	close(p.jobs)

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
