package reconciler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/JustinK33/Conduit/internal/store"
	"github.com/JustinK33/Conduit/pkg/models"
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

func (fake *fakeStore) ReleaseClaim(_ context.Context, job models.Job, reason string) error {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.releasedJobID = job.ID
	fake.releasedReason = reason
	return fake.releaseErr
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
	reconciler := New(Config{BatchSize: 10}, jobStore, submitter, zerolog.Nop())

	reconciler.reconcile(context.Background())

	if jobStore.releasedJobID != "job-1" {
		t.Fatalf("released job = %q, want job-1", jobStore.releasedJobID)
	}
	if jobStore.releasedReason == "" {
		t.Fatal("expected release reason")
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
