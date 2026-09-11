package worker

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JustinK33/Conduit/pkg/models"
)

// noopRunner completes jobs instantly. Used to isolate pool overhead from runner cost.
type noopRunner struct{ completed int64 }

func (r *noopRunner) Run(_ context.Context, _ models.Job) error {
	atomic.AddInt64(&r.completed, 1)
	return nil
}

// syncRunner signals a WaitGroup on each completion, enabling latency measurement.
type syncRunner struct{ wg *sync.WaitGroup }

func (r *syncRunner) Run(_ context.Context, _ models.Job) error {
	r.wg.Done()
	return nil
}

// BenchmarkPoolThroughput measures end-to-end jobs/second with 16 concurrent workers.
func BenchmarkPoolThroughput(b *testing.B) {
	const (
		concurrency = 16
		queueSize   = 1024
	)
	runner := &noopRunner{}
	pool := NewPool(Config{
		Concurrency:     concurrency,
		QueueSize:       queueSize,
		ShutdownTimeout: 5 * time.Second,
	}, runner)

	ctx, cancel := context.WithCancel(context.Background())
	pool.Start(ctx)

	job := models.Job{ID: "bench"}
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for !pool.Submit(ctx, job) {
			runtime.Gosched()
		}
	}

	b.StopTimer()
	cancel()
	pool.Stop(context.Background())
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "jobs/sec")
}

// BenchmarkPoolScaling shows how throughput grows as worker count doubles.
func BenchmarkPoolScaling(b *testing.B) {
	for _, n := range []int{1, 2, 4, 8, 16, 32} {
		b.Run(fmt.Sprintf("workers-%02d", n), func(b *testing.B) {
			runner := &noopRunner{}
			pool := NewPool(Config{
				Concurrency:     n,
				QueueSize:       n * 128,
				ShutdownTimeout: 5 * time.Second,
			}, runner)

			ctx, cancel := context.WithCancel(context.Background())
			pool.Start(ctx)

			job := models.Job{ID: "bench"}
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				for !pool.Submit(ctx, job) {
					runtime.Gosched()
				}
			}

			b.StopTimer()
			cancel()
			pool.Stop(context.Background())
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "jobs/sec")
		})
	}
}

// BenchmarkPoolLatency measures round-trip time from Submit to Run completion.
func BenchmarkPoolLatency(b *testing.B) {
	var wg sync.WaitGroup
	pool := NewPool(Config{
		Concurrency:     4,
		QueueSize:       256,
		ShutdownTimeout: 5 * time.Second,
	}, &syncRunner{&wg})

	ctx, cancel := context.WithCancel(context.Background())
	pool.Start(ctx)
	defer func() {
		cancel()
		pool.Stop(context.Background())
	}()

	job := models.Job{ID: "bench"}
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		wg.Add(1)
		for !pool.Submit(ctx, job) {
			runtime.Gosched()
		}
		wg.Wait()
	}
}

// BenchmarkPoolSubmitContention measures Submit under high concurrent producer load.
func BenchmarkPoolSubmitContention(b *testing.B) {
	pool := NewPool(Config{
		Concurrency:     16,
		QueueSize:       4096,
		ShutdownTimeout: 5 * time.Second,
	}, &noopRunner{})

	ctx, cancel := context.WithCancel(context.Background())
	pool.Start(ctx)
	defer func() {
		cancel()
		pool.Stop(context.Background())
	}()

	job := models.Job{ID: "bench"}
	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for !pool.Submit(ctx, job) {
				runtime.Gosched()
			}
		}
	})
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "jobs/sec")
}
