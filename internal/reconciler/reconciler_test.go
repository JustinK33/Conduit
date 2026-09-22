package reconciler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/JustinK33/Conduit/internal/metrics"
	"github.com/JustinK33/Conduit/internal/store"
	"github.com/JustinK33/Conduit/pkg/models"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/rs/zerolog"
)

type fakeStore struct {
	mu             sync.Mutex
	jobs           []models.Job
	err            error
	claimed        int
	attempts       int
	leaseDuration  time.Duration
	requeued       int
	requeueErr     error
	releaseErr     error
	releasedJobID  string
	releasedReason string
	releasedAfter  time.Duration

	// Retention and the backlog sample.
	deletes    []deleteCall
	deleteErr  error
	counts     map[models.JobState]int64
	countCalls int
	countErr   error
}

type deleteCall struct {
	state  models.JobState
	before time.Time
	limit  int
}

func (fake *fakeStore) ClaimNextJob(_ context.Context, leaseDuration time.Duration, _ []string) (models.Job, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.leaseDuration = leaseDuration
	fake.attempts++
	if fake.err != nil {
		return models.Job{}, fake.err
	}
	if len(fake.jobs) == 0 {
		return models.Job{}, store.ErrJobNotFound
	}
	job := fake.jobs[0]
	fake.jobs = fake.jobs[1:]
	fake.claimed++
	return job, nil
}

func (fake *fakeStore) RequeueExpiredRunning(context.Context, int) (int, error) {
	return fake.requeued, fake.requeueErr
}

func (fake *fakeStore) ReleaseClaim(_ context.Context, job models.Job, reason string, retryAfter time.Duration) error {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.releasedJobID = job.ID
	fake.releasedReason = reason
	fake.releasedAfter = retryAfter
	return fake.releaseErr
}

func (fake *fakeStore) DeleteFinished(_ context.Context, state models.JobState, before time.Time, limit int) (int, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.deletes = append(fake.deletes, deleteCall{state, before, limit})
	if fake.deleteErr != nil {
		return 0, fake.deleteErr
	}
	// One short batch, so a sweep stops after a single call per state.
	return 1, nil
}

func (fake *fakeStore) CountByState(context.Context) (map[models.JobState]int64, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.countCalls++
	return fake.counts, fake.countErr
}

type fakeSubmitter struct {
	mu        sync.Mutex
	jobs      []models.Job
	accepted  bool
	submitted chan struct{}
}

func (fake *fakeSubmitter) SubmitBlocking(_ context.Context, job models.Job) bool {
	if !fake.accepted {
		return false
	}
	fake.mu.Lock()
	fake.jobs = append(fake.jobs, job)
	fake.mu.Unlock()
	if fake.submitted != nil {
		select {
		case fake.submitted <- struct{}{}:
		default:
		}
	}
	return true
}

func TestReconcileClaimsAndSubmitsDueJobs(t *testing.T) {
	jobStore := &fakeStore{jobs: []models.Job{
		{ID: "job-1", State: models.JobStateRunning},
		{ID: "job-2", State: models.JobStateRunning},
	}}
	submitter := &fakeSubmitter{accepted: true}
	reconciler := New(Config{BatchSize: 10}, jobStore, submitter, zerolog.Nop())

	hadWork := reconciler.reconcile(context.Background())

	if !hadWork {
		t.Fatal("hadWork = false, want true")
	}
	if jobStore.claimed != 2 {
		t.Fatalf("claimed = %d, want 2", jobStore.claimed)
	}
	if len(submitter.jobs) != 2 {
		t.Fatalf("submitted = %d, want 2", len(submitter.jobs))
	}
	if jobStore.leaseDuration != 5*time.Minute {
		t.Fatalf("lease duration = %s, want 5m", jobStore.leaseDuration)
	}
}

func TestReconcileStopsAtBatchSize(t *testing.T) {
	jobStore := &fakeStore{jobs: []models.Job{
		{ID: "job-1"},
		{ID: "job-2"},
		{ID: "job-3"},
	}}
	submitter := &fakeSubmitter{accepted: true}
	reconciler := New(Config{BatchSize: 2}, jobStore, submitter, zerolog.Nop())

	reconciler.reconcile(context.Background())

	if jobStore.claimed != 2 {
		t.Fatalf("claimed = %d, want 2", jobStore.claimed)
	}
	if len(submitter.jobs) != 2 {
		t.Fatalf("submitted = %d, want 2", len(submitter.jobs))
	}
}

func TestReconcileStopsOnClaimError(t *testing.T) {
	expectedErr := errors.New("database down")
	jobStore := &fakeStore{err: expectedErr}
	submitter := &fakeSubmitter{accepted: true}
	reconciler := New(Config{BatchSize: 10}, jobStore, submitter, zerolog.Nop())

	hadWork := reconciler.reconcile(context.Background())

	if !hadWork {
		t.Fatal("hadWork = false, want true for retryable claim error")
	}
	if len(submitter.jobs) != 0 {
		t.Fatalf("submitted = %d, want 0", len(submitter.jobs))
	}
}

func TestReconcileReturnsFalseWhenIdle(t *testing.T) {
	jobStore := &fakeStore{}
	submitter := &fakeSubmitter{accepted: true}
	reconciler := New(Config{BatchSize: 10}, jobStore, submitter, zerolog.Nop())

	hadWork := reconciler.reconcile(context.Background())

	if hadWork {
		t.Fatal("hadWork = true, want false")
	}
}

func TestNextIntervalUsesIdleIntervalWhenNoWork(t *testing.T) {
	reconciler := New(Config{
		Interval:     time.Second,
		IdleInterval: 15 * time.Second,
	}, &fakeStore{}, &fakeSubmitter{}, zerolog.Nop())

	if got := reconciler.nextInterval(true); got != time.Second {
		t.Fatalf("active interval = %s, want 1s", got)
	}
	if got := reconciler.nextInterval(false); got != 15*time.Second {
		t.Fatalf("idle interval = %s, want 15s", got)
	}
}

func TestReconcileRequeuesExpiredRunningBeforeClaim(t *testing.T) {
	jobStore := &fakeStore{
		jobs:     []models.Job{{ID: "job-1"}},
		requeued: 3,
	}
	submitter := &fakeSubmitter{accepted: true}
	reconciler := New(Config{BatchSize: 10}, jobStore, submitter, zerolog.Nop())

	reconciler.reconcile(context.Background())

	if jobStore.claimed != 1 {
		t.Fatalf("claimed = %d, want 1", jobStore.claimed)
	}
}

func TestReconcileTreatsRequeueErrorAsWork(t *testing.T) {
	jobStore := &fakeStore{requeueErr: errors.New("database unavailable")}
	submitter := &fakeSubmitter{accepted: true}
	reconciler := New(Config{BatchSize: 10}, jobStore, submitter, zerolog.Nop())

	hadWork := reconciler.reconcile(context.Background())

	if !hadWork {
		t.Fatal("hadWork = false, want true for retryable requeue error")
	}
}

func TestReconcileReleasesClaimWhenSubmitFails(t *testing.T) {
	jobStore := &fakeStore{jobs: []models.Job{{ID: "job-1", LeaseToken: "lease-1"}}}
	submitter := &fakeSubmitter{accepted: false}
	reconciler := New(Config{BatchSize: 10, Interval: time.Second}, jobStore, submitter, zerolog.Nop())

	reconciler.reconcile(context.Background())

	if jobStore.releasedJobID != "job-1" {
		t.Fatalf("released job = %q, want job-1", jobStore.releasedJobID)
	}
	if jobStore.releasedReason == "" {
		t.Fatal("expected release reason")
	}
	// A release with no delay is due again on the next tick, which is the
	// claim-release loop rather than a handover.
	if jobStore.releasedAfter <= 0 {
		t.Fatalf("released with retryAfter = %s, want a delay", jobStore.releasedAfter)
	}
}

func TestStartRunsImmediately(t *testing.T) {
	jobStore := &fakeStore{jobs: []models.Job{{ID: "job-1"}}}
	submitter := &fakeSubmitter{accepted: true, submitted: make(chan struct{}, 1)}
	reconciler := New(Config{Interval: time.Hour, BatchSize: 10}, jobStore, submitter, zerolog.Nop())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reconciler.Start(ctx)
	defer reconciler.Stop()

	select {
	case <-submitter.submitted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for immediate reconcile")
	}
}

// TestWakeReconcilesBeforeTheInterval is what the Postgres transport buys. A
// NOTIFY calls Wake, and if that did not cut the current sleep short, an enqueue
// onto an idle queue would wait out the whole interval - the several-second
// dispatch latency the README measures with the broker killed.
//
// The intervals are an hour so that a pass happening at all can only be the
// wake-up, never the timer.
func TestWakeReconcilesBeforeTheInterval(t *testing.T) {
	jobStore := &fakeStore{}
	submitter := &fakeSubmitter{accepted: true, submitted: make(chan struct{}, 1)}
	reconciler := New(Config{Interval: time.Hour, IdleInterval: time.Hour, BatchSize: 10},
		jobStore, submitter, zerolog.Nop())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reconciler.Start(ctx)
	defer reconciler.Stop()

	// The pass Start does immediately finds nothing, which puts the loop to sleep
	// for an hour. Enqueue after that, so the job can only be picked up by a wake.
	waitForClaimAttempts(t, jobStore, 1)
	jobStore.mu.Lock()
	jobStore.jobs = []models.Job{{ID: "job-1"}}
	jobStore.mu.Unlock()

	reconciler.Wake()

	select {
	case <-submitter.submitted:
	case <-time.After(2 * time.Second):
		t.Fatal("Wake did not trigger a reconcile pass")
	}
}

// TestWakeNeverBlocks covers the depth-1 buffer: a notification arriving while
// the reconciler is mid-pass must not stall the caller, which is the listener
// goroutine reading from Postgres.
func TestWakeNeverBlocks(t *testing.T) {
	reconciler := New(Config{BatchSize: 10}, &fakeStore{}, &fakeSubmitter{accepted: true}, zerolog.Nop())

	done := make(chan struct{})
	go func() {
		for range 100 {
			reconciler.Wake()
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Wake blocked with nothing reading the channel")
	}
}

// waitForClaimAttempts blocks until the fake store has been asked for a job at
// least n times, which is how a test knows the loop has finished a pass and gone
// back to sleep. Attempts rather than successful claims, because an idle pass
// still queries once and finds nothing.
func waitForClaimAttempts(t *testing.T, jobStore *fakeStore, n int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		jobStore.mu.Lock()
		attempts := jobStore.attempts
		jobStore.mu.Unlock()
		if attempts >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d claim attempts", n)
}

func TestRetentionSweep(t *testing.T) {
	t.Run("deletes each state past its own age", func(t *testing.T) {
		jobStore := &fakeStore{}
		r := New(Config{
			BatchSize: 10,
			Retention: RetentionConfig{Completed: time.Hour, Dead: 24 * time.Hour, BatchSize: 50},
		}, jobStore, &fakeSubmitter{accepted: true}, zerolog.Nop())

		before := time.Now().UTC()
		r.reconcile(context.Background())

		if len(jobStore.deletes) != 2 {
			t.Fatalf("delete calls = %d, want 2 (one per state)", len(jobStore.deletes))
		}
		byState := map[models.JobState]deleteCall{}
		for _, d := range jobStore.deletes {
			byState[d.state] = d
		}
		completed, ok := byState[models.JobStateCompleted]
		if !ok {
			t.Fatal("no delete for COMPLETED")
		}
		if completed.limit != 50 {
			t.Errorf("COMPLETED batch limit = %d, want 50", completed.limit)
		}
		// An hour of retention means a cutoff about an hour back, not now.
		if gap := before.Sub(completed.before); gap < 59*time.Minute || gap > 61*time.Minute {
			t.Errorf("COMPLETED cutoff was %s ago, want about 1h", gap)
		}
		dead, ok := byState[models.JobStateDead]
		if !ok {
			t.Fatal("no delete for DEAD")
		}
		if gap := before.Sub(dead.before); gap < 23*time.Hour || gap > 25*time.Hour {
			t.Errorf("DEAD cutoff was %s ago, want about 24h", gap)
		}
	})

	// Zero means keep forever, and it is the DEAD default. A sweep that deleted
	// dead-lettered jobs nobody asked it to delete would destroy the only copy of
	// why a job failed.
	t.Run("deletes nothing when both ages are zero", func(t *testing.T) {
		jobStore := &fakeStore{}
		r := New(Config{BatchSize: 10}, jobStore, &fakeSubmitter{accepted: true}, zerolog.Nop())

		r.reconcile(context.Background())

		if len(jobStore.deletes) != 0 {
			t.Errorf("delete calls = %d with retention off, want 0", len(jobStore.deletes))
		}
	})

	t.Run("deletes only the state with an age set", func(t *testing.T) {
		jobStore := &fakeStore{}
		r := New(Config{
			BatchSize: 10,
			Retention: RetentionConfig{Completed: time.Hour},
		}, jobStore, &fakeSubmitter{accepted: true}, zerolog.Nop())

		r.reconcile(context.Background())

		if len(jobStore.deletes) != 1 {
			t.Fatalf("delete calls = %d, want 1", len(jobStore.deletes))
		}
		if jobStore.deletes[0].state != models.JobStateCompleted {
			t.Errorf("deleted state = %s, want COMPLETED", jobStore.deletes[0].state)
		}
	})

	// The interval throttles the sweep independently of the tick, which runs once
	// a second by default. Without this an hourly sweep would run 3600 times an hour.
	t.Run("sweeps at most once per interval", func(t *testing.T) {
		jobStore := &fakeStore{}
		r := New(Config{
			BatchSize: 10,
			Retention: RetentionConfig{Completed: time.Hour, Interval: time.Hour},
		}, jobStore, &fakeSubmitter{accepted: true}, zerolog.Nop())

		r.reconcile(context.Background())
		r.reconcile(context.Background())
		r.reconcile(context.Background())

		if len(jobStore.deletes) != 1 {
			t.Errorf("delete calls = %d across three passes, want 1", len(jobStore.deletes))
		}
	})

	// Retention must not hold the fast interval open: hadWork decides how soon to
	// look for jobs again, and deleting history says nothing about waiting work.
	t.Run("does not report housekeeping as work", func(t *testing.T) {
		jobStore := &fakeStore{}
		r := New(Config{
			BatchSize: 10,
			Retention: RetentionConfig{Completed: time.Hour},
		}, jobStore, &fakeSubmitter{accepted: true}, zerolog.Nop())

		if r.reconcile(context.Background()) {
			t.Error("hadWork = true on a pass that only pruned, want false")
		}
	})
}

func TestBacklogSample(t *testing.T) {
	t.Run("publishes a gauge per state, including zeros", func(t *testing.T) {
		jobStore := &fakeStore{counts: map[models.JobState]int64{models.JobStatePending: 7}}
		reg := metrics.NewRegistry("test", "reconciler")
		if err := reg.Register(prometheus.NewRegistry()); err != nil {
			t.Fatalf("Register: %v", err)
		}
		r := New(Config{BatchSize: 10, Metrics: reg}, jobStore, &fakeSubmitter{accepted: true}, zerolog.Nop())

		r.reconcile(context.Background())

		if got := gaugeValue(t, reg.JobBacklog, "PENDING"); got != 7 {
			t.Errorf("jobs_backlog{state=PENDING} = %v, want 7", got)
		}
		// A series that vanishes when a queue drains reads as a broken exporter.
		if got := gaugeValue(t, reg.JobBacklog, "RUNNING"); got != 0 {
			t.Errorf("jobs_backlog{state=RUNNING} = %v, want 0", got)
		}
		if got := gaugeValue(t, reg.JobBacklog, "DEAD"); got != 0 {
			t.Errorf("jobs_backlog{state=DEAD} = %v, want 0", got)
		}
	})

	t.Run("samples at most once per interval", func(t *testing.T) {
		jobStore := &fakeStore{}
		reg := metrics.NewRegistry("test", "reconciler")
		if err := reg.Register(prometheus.NewRegistry()); err != nil {
			t.Fatalf("Register: %v", err)
		}
		r := New(Config{BatchSize: 10, Metrics: reg, BacklogInterval: time.Hour},
			jobStore, &fakeSubmitter{accepted: true}, zerolog.Nop())

		r.reconcile(context.Background())
		r.reconcile(context.Background())

		if jobStore.countCalls != 1 {
			t.Errorf("CountByState calls = %d across two passes, want 1", jobStore.countCalls)
		}
	})

	t.Run("skips the query when nothing is listening", func(t *testing.T) {
		jobStore := &fakeStore{}
		r := New(Config{BatchSize: 10}, jobStore, &fakeSubmitter{accepted: true}, zerolog.Nop())

		r.reconcile(context.Background())

		if jobStore.countCalls != 0 {
			t.Errorf("CountByState calls = %d without a metrics registry, want 0", jobStore.countCalls)
		}
	})
}

func gaugeValue(t *testing.T, vec *prometheus.GaugeVec, state string) float64 {
	t.Helper()
	var m dto.Metric
	g, err := vec.GetMetricWithLabelValues(state)
	if err != nil {
		t.Fatalf("GetMetricWithLabelValues(%q): %v", state, err)
	}
	if err := g.Write(&m); err != nil {
		t.Fatalf("Write: %v", err)
	}
	return m.GetGauge().GetValue()
}
