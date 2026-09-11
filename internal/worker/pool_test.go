package worker

import (
	"context"
	"testing"
	"time"

	"github.com/JustinK33/Conduit/pkg/models"
)

type mockRunner struct{}

func (mockRunner) Run(context.Context, models.Job) error { return nil }

func TestNewPool(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{name: "creates bounded pool", cfg: Config{Concurrency: 4, QueueSize: 32, ShutdownTimeout: time.Second}},
		{name: "small pool", cfg: Config{Concurrency: 1, QueueSize: 1, ShutdownTimeout: time.Second}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := NewPool(tc.cfg, mockRunner{})
			if p == nil {
				t.Fatal("NewPool returned nil")
			}
			if cap(p.jobs) != tc.cfg.QueueSize {
				t.Errorf("jobs channel cap = %d, want %d", cap(p.jobs), tc.cfg.QueueSize)
			}
			if cap(p.semaphore) != tc.cfg.Concurrency {
				t.Errorf("semaphore cap = %d, want %d", cap(p.semaphore), tc.cfg.Concurrency)
			}
		})
	}
}

func TestPoolStart(t *testing.T) {
	t.Run("starts and exits on context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		p := NewPool(Config{Concurrency: 2, QueueSize: 4, ShutdownTimeout: time.Second}, mockRunner{})
		p.Start(ctx)

		// Submit a job to verify the pool is processing
		job := models.Job{ID: "test-job-1", State: models.JobStatePending}
		accepted := p.Submit(ctx, job)
		if !accepted {
			t.Error("expected job to be accepted after Start")
		}

		cancel()
		err := p.Stop(context.Background())
		if err != nil {
			t.Errorf("Stop returned error: %v", err)
		}
	})
}

func TestPoolSubmit(t *testing.T) {
	t.Run("accepts job into queue", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		p := NewPool(Config{Concurrency: 2, QueueSize: 4, ShutdownTimeout: time.Second}, mockRunner{})
		p.Start(ctx)

		job := models.Job{ID: "job-1", State: models.JobStatePending}
		if accepted := p.Submit(ctx, job); !accepted {
			t.Error("Submit returned false for an available queue")
		}
	})

	t.Run("returns false for cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		p := NewPool(Config{Concurrency: 2, QueueSize: 4, ShutdownTimeout: time.Second}, mockRunner{})

		job := models.Job{ID: "job-2", State: models.JobStatePending}
		if accepted := p.Submit(ctx, job); accepted {
			t.Error("Submit should return false for a cancelled context")
		}
	})
}

func TestPoolStop(t *testing.T) {
	t.Run("shuts down gracefully within timeout", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		p := NewPool(Config{Concurrency: 2, QueueSize: 4, ShutdownTimeout: 5 * time.Second}, mockRunner{})
		p.Start(ctx)
		cancel()

		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()

		if err := p.Stop(stopCtx); err != nil {
			t.Errorf("Stop returned unexpected error: %v", err)
		}
	})
}
