package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/JustinK33/Conduit/internal/circuitbreaker"
	"github.com/JustinK33/Conduit/internal/lock"
	"github.com/JustinK33/Conduit/internal/metrics"
	"github.com/JustinK33/Conduit/internal/retry"
	"github.com/JustinK33/Conduit/internal/store"
	"github.com/JustinK33/Conduit/pkg/models"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

// fakeStore records the writes jobWorker still makes directly: the PENDING to
// RUNNING claim on the Kafka path, lease renewal, and claim release.
type fakeStore struct {
	store.JobStore
	updated  []models.Job
	released []release
}

func (f *fakeStore) UpdateJob(_ context.Context, job models.Job) error {
	f.updated = append(f.updated, job)
	return nil
}

func (f *fakeStore) RenewLease(context.Context, models.Job, time.Duration) error { return nil }

func (f *fakeStore) ReleaseClaim(_ context.Context, job models.Job, reason string, retryAfter time.Duration) error {
	f.released = append(f.released, release{job: job, reason: reason, retryAfter: retryAfter})
	return nil
}

type release struct {
	job        models.Job
	reason     string
	retryAfter time.Duration
}

// fakeOutcome records what the worker delegated to the service layer.
type fakeOutcome struct {
	completeCalls   int
	completeToken   string
	failCalls       int
	failToken       string
	failMsg         string
	failedPermanent bool
	failErr         error
}

func (f *fakeOutcome) Complete(_ context.Context, id, leaseToken string, _ map[string]string) (models.Job, error) {
	f.completeCalls++
	f.completeToken = leaseToken
	return models.Job{ID: id, State: models.JobStateCompleted}, nil
}

func (f *fakeOutcome) Fail(_ context.Context, _, leaseToken, errMsg string, permanent bool) (models.Job, error) {
	f.failCalls++
	f.failToken = leaseToken
	f.failMsg = errMsg
	f.failedPermanent = permanent
	return models.Job{}, f.failErr
}

// nopLocker always grants the lock. The Redlock protocol itself is covered in
// internal/lock; here it is just a gate that has to open.
type nopLocker struct{}

func (nopLocker) Acquire(_ context.Context, resource string) (lock.Lock, error) {
	return lock.Lock{Resource: resource, Value: "test"}, nil
}
func (nopLocker) Release(context.Context, lock.Lock) error { return nil }

func newTestWorker(t *testing.T, st store.JobStore, out jobOutcome, handlers map[string]TaskHandler) *jobWorker {
	t.Helper()
	reg := metrics.NewRegistry("test", "worker")
	if err := reg.Register(prometheus.NewRegistry()); err != nil {
		t.Fatalf("register metrics: %v", err)
	}
	// A threshold of 100 keeps the breaker out of the way of tests that are about
	// something else; TestBreakerIsPerTaskType builds its own with a real one.
	return &jobWorker{
		store:   st,
		outcome: out,
		lockMgr: nopLocker{},
		breakers: newTaskBreakers(circuitbreaker.Config{
			FailureThreshold: 100,
			SuccessThreshold: 1,
			OpenTimeout:      time.Second,
			HalfOpenRequests: 1,
		}, handlers),
		metrics:  reg,
		log:      zerolog.Nop(),
		handlers: handlers,
		lease:    time.Minute,
	}
}

// runningJob is a job as the reconciler delivers it: already claimed, with a
// live lease token, so jobWorker does not re-claim it.
func runningJob() models.Job {
	return models.Job{
		ID:         "job-1",
		State:      models.JobStateRunning,
		Attempt:    1,
		LeaseToken: "lease-tok",
		Task:       models.Task{Name: "test.task"},
	}
}

// The whole point of moving the retry decision into JobService is that
// jobWorker no longer makes it. These assert the delegation, not the policy:
// the worker's only job is to translate its handler's error into a bool.
func TestJobWorkerDelegatesOutcome(t *testing.T) {
	tests := []struct {
		name          string
		handlerErr    error
		wantComplete  int
		wantFail      int
		wantPermanent bool
		wantMsg       string
	}{
		{
			name:         "success completes through the service",
			wantComplete: 1,
		},
		{
			name:       "a transient error fails without demanding a permanent outcome",
			handlerErr: errors.New("upstream timed out"),
			wantFail:   1,
			wantMsg:    "upstream timed out",
		},
		{
			name:          "ErrNoRetry is how in-process code says permanent",
			handlerErr:    retry.ErrNoRetry,
			wantFail:      1,
			wantPermanent: true,
			wantMsg:       retry.ErrNoRetry.Error(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out := &fakeOutcome{}
			jw := newTestWorker(t, &fakeStore{}, out, map[string]TaskHandler{
				"test.task": func(context.Context, models.Job) error { return tc.handlerErr },
			})

			err := jw.Run(context.Background(), runningJob())
			if tc.handlerErr == nil && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.handlerErr != nil && !errors.Is(err, tc.handlerErr) {
				t.Fatalf("Run error = %v, want it to wrap %v", err, tc.handlerErr)
			}
			if out.completeCalls != tc.wantComplete {
				t.Errorf("Complete calls = %d, want %d", out.completeCalls, tc.wantComplete)
			}
			if out.failCalls != tc.wantFail {
				t.Errorf("Fail calls = %d, want %d", out.failCalls, tc.wantFail)
			}
			if out.failedPermanent != tc.wantPermanent {
				t.Errorf("permanent = %v, want %v", out.failedPermanent, tc.wantPermanent)
			}
			if tc.wantMsg != "" && out.failMsg != tc.wantMsg {
				t.Errorf("error message = %q, want %q", out.failMsg, tc.wantMsg)
			}
		})
	}
}

func TestJobWorkerFencesOnTheLeaseToken(t *testing.T) {
	t.Run("complete carries the token it was claimed with", func(t *testing.T) {
		out := &fakeOutcome{}
		jw := newTestWorker(t, &fakeStore{}, out, map[string]TaskHandler{
			"test.task": func(context.Context, models.Job) error { return nil },
		})
		if err := jw.Run(context.Background(), runningJob()); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if out.completeToken != "lease-tok" {
			t.Errorf("complete token = %q, want %q", out.completeToken, "lease-tok")
		}
	})

	t.Run("a Kafka-delivered job is claimed first and fails under its new token", func(t *testing.T) {
		fs := &fakeStore{}
		out := &fakeOutcome{}
		jw := newTestWorker(t, fs, out, map[string]TaskHandler{
			"test.task": func(context.Context, models.Job) error { return errors.New("boom") },
		})

		job := runningJob()
		job.State = models.JobStatePending
		job.LeaseToken = ""
		if err := jw.Run(context.Background(), job); err == nil {
			t.Fatal("expected the handler error to propagate")
		}

		if len(fs.updated) != 1 {
			t.Fatalf("claim writes = %d, want 1", len(fs.updated))
		}
		claimed := fs.updated[0]
		if claimed.State != models.JobStateRunning || claimed.LeaseToken == "" {
			t.Fatalf("claim wrote state %q with token %q", claimed.State, claimed.LeaseToken)
		}
		if out.failToken != claimed.LeaseToken {
			t.Errorf("failed under token %q, want the claimed token %q", out.failToken, claimed.LeaseToken)
		}
	})
}

func TestJobWorkerUnregisteredTaskIsPermanent(t *testing.T) {
	out := &fakeOutcome{}
	jw := newTestWorker(t, &fakeStore{}, out, map[string]TaskHandler{})

	if err := jw.Run(context.Background(), runningJob()); err == nil {
		t.Fatal("expected an error for an unregistered task")
	}
	if !out.failedPermanent {
		t.Error("an unregistered task name must not burn retry attempts")
	}
}

// The topic carries every job regardless of queue, so this filter is the only
// thing stopping the in-process pool from eating a remote worker's jobs and
// dead-lettering them for having no handler.
func TestKafkaJobHandlerClaimsOnlyItsQueues(t *testing.T) {
	tests := []struct {
		name   string
		queues []string
		queue  string
		want   bool
	}{
		{name: "no filter takes everything", queues: nil, queue: "remote", want: true},
		{name: "a named queue is taken", queues: []string{"default", "webhook"}, queue: "webhook", want: true},
		{name: "another worker's queue is left alone", queues: []string{"default"}, queue: "remote", want: false},
		{name: "an unset queue counts as the default", queues: []string{"default"}, queue: "", want: true},
		{name: "an unset queue is skipped when default is not ours", queues: []string{"remote"}, queue: "", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := &kafkaJobHandler{queues: tc.queues}
			if got := h.claims(tc.queue); got != tc.want {
				t.Errorf("claims(%q) with %v = %v, want %v", tc.queue, tc.queues, got, tc.want)
			}
		})
	}
}

// One dead endpoint used to stop every task type: a single process-global
// breaker meant five webhook failures opened the circuit for sql.etl too. This
// is that blast radius, so the healthy task has to still run after the broken
// one has tripped its own breaker.
func TestBreakerIsPerTaskType(t *testing.T) {
	const openTimeout = 30 * time.Second

	handlers := map[string]TaskHandler{
		"broken":  func(context.Context, models.Job) error { return errors.New("connection refused") },
		"healthy": func(context.Context, models.Job) error { return nil },
	}
	fs := &fakeStore{}
	out := &fakeOutcome{}
	jw := newTestWorker(t, fs, out, handlers)
	jw.breakers = newTaskBreakers(circuitbreaker.Config{
		FailureThreshold: 2,
		SuccessThreshold: 1,
		OpenTimeout:      openTimeout,
		HalfOpenRequests: 1,
	}, handlers)

	job := func(task string) models.Job {
		j := runningJob()
		j.Task.Name = task
		return j
	}

	// Trip the broken task's breaker.
	for i := 0; i < 2; i++ {
		if err := jw.Run(context.Background(), job("broken")); err == nil {
			t.Fatalf("run %d: expected the handler error", i)
		}
	}
	if err := jw.Run(context.Background(), job("broken")); err != nil {
		t.Fatalf("an open circuit releases the job rather than erroring: %v", err)
	}
	if len(fs.released) != 1 {
		t.Fatalf("releases = %d, want 1 once the circuit is open", len(fs.released))
	}
	// Releasing with no delay makes the job due on the reconciler's next tick,
	// which re-claims it, which releases it, for as long as the circuit is open.
	if got := fs.released[0].retryAfter; got != openTimeout {
		t.Errorf("retryAfter = %s, want the breaker's open timeout %s", got, openTimeout)
	}

	out.completeCalls = 0
	if err := jw.Run(context.Background(), job("healthy")); err != nil {
		t.Fatalf("healthy task blocked by another task's breaker: %v", err)
	}
	if out.completeCalls != 1 {
		t.Errorf("healthy Complete calls = %d, want 1", out.completeCalls)
	}
}
