package reconciler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/example/conduit/internal/store"
	"github.com/example/conduit/pkg/models"
	"github.com/rs/zerolog"
)

type fakeStore struct {
	mu             sync.Mutex
	jobs           []models.Job
	err            error
	claimed        int
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
