package reconciler

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/JustinK33/Conduit/internal/metrics"
	"github.com/JustinK33/Conduit/internal/store"
	"github.com/JustinK33/Conduit/pkg/models"
	"github.com/rs/zerolog"
)

type JobStore interface {
	ClaimNextJob(context.Context, time.Duration, []string) (models.Job, error)
	RequeueExpiredRunning(context.Context, int) (int, error)
	ReleaseClaim(context.Context, models.Job, string, time.Duration) error
	DeleteFinished(context.Context, models.JobState, time.Time, int) (int, error)
	CountByState(context.Context) (map[models.JobState]int64, error)
}

type Submitter interface {
	SubmitBlocking(context.Context, models.Job) bool
}

// RetentionConfig decides how long finished jobs stay. Zero means keep forever,
// for both ages, because "delete nothing" has to be expressible.
type RetentionConfig struct {
	Completed time.Duration
	Dead      time.Duration
	// Interval is how often a sweep runs, not how often the reconciler ticks.
	Interval time.Duration
	// BatchSize bounds one DELETE. A sweep loops batches, so this is about how
	// long a single statement holds locks, not about how much it can remove.
	BatchSize int
}

type Config struct {
	Interval     time.Duration
	IdleInterval time.Duration
	BatchSize    int
	RunningLease time.Duration
	Queues       []string

	Retention RetentionConfig
	// BacklogInterval throttles the jobs_backlog sample. Zero uses the default;
	// negative disables it.
	BacklogInterval time.Duration
	// Metrics is optional. Without it the backlog sample is skipped, since
	// nothing would be listening.
	Metrics *metrics.Registry
}

// maxSweepBatches caps one retention pass. Without it, the first sweep of a table
// nobody ever pruned would delete for as long as it took and starve the claim
// loop that shares this goroutine. Whatever is left is still there next interval.
const maxSweepBatches = 20

type Reconciler struct {
	store     JobStore
	submitter Submitter
	log       zerolog.Logger
	cfg       Config
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	mu        sync.Mutex
	wake      chan struct{}

	// Touched only from the single goroutine Start owns, so no lock.
	lastSweep   time.Time
	lastBacklog time.Time
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
	if cfg.Retention.Interval <= 0 {
		cfg.Retention.Interval = time.Hour
	}
	if cfg.Retention.BatchSize <= 0 {
		cfg.Retention.BatchSize = 1000
	}
	if cfg.BacklogInterval == 0 {
		cfg.BacklogInterval = 10 * time.Second
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

	// Neither housekeeping pass feeds hadWork. That flag decides how soon to look
	// for jobs again, and pruning history says nothing about whether work is
	// waiting: letting it say so would hold the fast interval open over an idle
	// queue for as long as there was old history to delete.
	r.sweepRetention(ctx)
	r.sampleBacklog(ctx)

	return hadWork
}

// sweepRetention deletes finished jobs past their retention age.
//
// It lives on the reconciler's goroutine rather than on one of its own, because
// the reconciler is already the thing that keeps the table in shape - it is what
// RequeueExpiredRunning does - and an extra goroutine would add a lifecycle to
// get wrong for work that runs once an hour.
//
// The consequence is worth knowing: an instance with the reconciler disabled
// prunes nothing, so a deployment where every instance is API-only never prunes.
func (r *Reconciler) sweepRetention(ctx context.Context) {
	if r.cfg.Retention.Completed <= 0 && r.cfg.Retention.Dead <= 0 {
		return
	}
	now := time.Now().UTC()
	if !r.lastSweep.IsZero() && now.Sub(r.lastSweep) < r.cfg.Retention.Interval {
		return
	}
	r.lastSweep = now

	for _, target := range []struct {
		state models.JobState
		age   time.Duration
	}{
		{models.JobStateCompleted, r.cfg.Retention.Completed},
		{models.JobStateDead, r.cfg.Retention.Dead},
	} {
		if target.age <= 0 {
			continue
		}
		before := now.Add(-target.age)
		deleted := 0
		for i := 0; i < maxSweepBatches; i++ {
			if ctx.Err() != nil {
				return
			}
			n, err := r.store.DeleteFinished(ctx, target.state, before, r.cfg.Retention.BatchSize)
			if err != nil {
				r.log.Warn().Err(err).Str("state", string(target.state)).Msg("reconciler: retention sweep failed")
				break
			}
			deleted += n
			if n < r.cfg.Retention.BatchSize {
				break
			}
		}
		if deleted > 0 {
			r.log.Info().Int("deleted", deleted).Str("state", string(target.state)).
				Time("older_than", before).Msg("reconciler: pruned finished jobs")
		}
	}
}

// sampleBacklog publishes how many jobs sit in each state.
//
// Every instance reports the same database-wide numbers, so a dashboard has to
// aggregate with max by (state) rather than sum. That is the price of not
// electing one instance to own the sample, and it is the right trade: a gauge
// that disappears when one pod restarts is worse than one that needs the right
// aggregation.
func (r *Reconciler) sampleBacklog(ctx context.Context) {
	if r.cfg.Metrics == nil || r.cfg.BacklogInterval < 0 {
		return
	}
	now := time.Now().UTC()
	if !r.lastBacklog.IsZero() && now.Sub(r.lastBacklog) < r.cfg.BacklogInterval {
		return
	}
	r.lastBacklog = now

	counts, err := r.store.CountByState(ctx)
	if err != nil {
		r.log.Warn().Err(err).Msg("reconciler: backlog sample failed")
		return
	}
	// Report every state the gauge covers, including the ones at zero: a series
	// that vanishes when a queue drains reads as "no data" on a graph, which is
	// the same shape as a broken exporter.
	for _, state := range store.BacklogStates {
		r.cfg.Metrics.SetBacklog(string(state), float64(counts[state]))
	}
}
