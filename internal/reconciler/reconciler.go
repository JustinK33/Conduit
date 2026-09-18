package reconciler

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/JustinK33/Conduit/internal/store"
	"github.com/JustinK33/Conduit/pkg/models"
	"github.com/rs/zerolog"
)

type JobStore interface {
	ClaimNextJob(context.Context, time.Duration, []string) (models.Job, error)
	RequeueExpiredRunning(context.Context, int) (int, error)
	ReleaseClaim(context.Context, models.Job, string, time.Duration) error
}

type Submitter interface {
	SubmitBlocking(context.Context, models.Job) bool
}

type Config struct {
	Interval     time.Duration
	IdleInterval time.Duration
	BatchSize    int
	RunningLease time.Duration
	Queues       []string
}

type Reconciler struct {
	store     JobStore
	submitter Submitter
	log       zerolog.Logger
	cfg       Config
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	mu        sync.Mutex
	wake      chan struct{}
}

func New(cfg Config, jobStore JobStore, submitter Submitter, log zerolog.Logger) *Reconciler {
	if cfg.Interval <= 0 {
		cfg.Interval = time.Minute
	}
	if cfg.IdleInterval <= 0 {
		cfg.IdleInterval = cfg.Interval
	}
	if cfg.IdleInterval < cfg.Interval {
		cfg.IdleInterval = cfg.Interval
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.RunningLease <= 0 {
		cfg.RunningLease = 5 * time.Minute
	}
	return &Reconciler{
		store:     jobStore,
		submitter: submitter,
		log:       log,
		cfg:       cfg,
		// Depth 1: a pending wake-up already means "there is work, look now", so
		// a second one while the first is unread would ask for nothing new.
		wake: make(chan struct{}, 1),
	}
}

// Wake asks for a reconcile pass now instead of at the end of the current
// interval, and never blocks. This is the whole of what the Postgres transport
// buys: without it an enqueue onto an idle queue waits out IdleInterval, which
// is what the README's reconciler-only dispatch numbers are measuring.
//
// A spurious wake costs one claim query that finds nothing.
func (r *Reconciler) Wake() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *Reconciler) Start(ctx context.Context) {
	r.mu.Lock()
	if r.cancel != nil {
		r.mu.Unlock()
		return
	}
	childCtx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	r.mu.Unlock()

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		nextInterval := r.nextInterval(r.reconcile(childCtx))

		for {
			timer := time.NewTimer(nextInterval)
			select {
			case <-timer.C:
				nextInterval = r.nextInterval(r.reconcile(childCtx))
			case <-r.wake:
				timer.Stop()
				nextInterval = r.nextInterval(r.reconcile(childCtx))
			case <-childCtx.Done():
				timer.Stop()
				return
			}
		}
	}()
}

func (r *Reconciler) Stop() {
	r.mu.Lock()
	if r.cancel != nil {
		r.cancel()
		r.cancel = nil
	}
	r.mu.Unlock()
	r.wg.Wait()
}

func (r *Reconciler) nextInterval(hadWork bool) time.Duration {
	if hadWork {
		return r.cfg.Interval
	}
	return r.cfg.IdleInterval
}

func (r *Reconciler) reconcile(ctx context.Context) bool {
	hadWork := false
	requeued, err := r.store.RequeueExpiredRunning(ctx, r.cfg.BatchSize)
	if err != nil {
		r.log.Warn().Err(err).Msg("reconciler: stale running recovery failed")
		hadWork = true
	} else if requeued > 0 {
		r.log.Warn().Int("requeued", requeued).Msg("reconciler: recovered stale running jobs")
		hadWork = true
	}

	claimed := 0
	for claimed < r.cfg.BatchSize {
		if ctx.Err() != nil {
			return hadWork
		}

		job, err := r.store.ClaimNextJob(ctx, r.cfg.RunningLease, r.cfg.Queues)
		if errors.Is(err, store.ErrJobNotFound) {
			break
		}
		if err != nil {
			r.log.Warn().Err(err).Msg("reconciler: claim failed")
			return true
		}

		if !r.submitter.SubmitBlocking(ctx, job) {
			// SubmitBlocking waits for a free slot, so it only returns false on
			// shutdown. The job is going back for whoever starts next, and one
			// interval is enough of a delay for that.
			r.log.Warn().Str("job_id", job.ID).Msg("reconciler: worker pool rejected claimed job")
			if err := r.store.ReleaseClaim(ctx, job, "worker pool rejected claimed job", r.cfg.Interval); err != nil {
				r.log.Warn().Err(err).Str("job_id", job.ID).Msg("reconciler: release claim failed")
			}
			return true
		}
		claimed++
	}

	if claimed > 0 {
		r.log.Info().Int("claimed", claimed).Msg("reconciler: submitted pending jobs")
		hadWork = true
	}
	return hadWork
}
