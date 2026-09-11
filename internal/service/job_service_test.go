package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/JustinK33/Conduit/internal/retry"
	"github.com/JustinK33/Conduit/internal/store"
	"github.com/JustinK33/Conduit/pkg/models"
	"github.com/rs/zerolog"
)

// mockStore satisfies store.JobStore with controllable responses.
type mockStore struct {
	createErr         error
	cancelErr         error
	createdJob        models.Job
	idempotencyJob    models.Job
	idempotencyLookup error
	idempotencyCalls  int

	// Claim and the compare-and-swap methods.
	getJob       models.Job
	getJobErr    error
	claimJob     models.Job
	claimErr     error
	claimLease   time.Duration
	claimQueues  []string
	renewErr     error
	renewLease   time.Duration
	completeErr  error
	completeMeta map[string]string
	failErr      error
	failCalls    int
	failNextRun  *time.Time
	failToken    string
	failErrMsg   string
}

func (m *mockStore) CreateJob(_ context.Context, job models.Job) error {
	m.createdJob = job
	return m.createErr
}
func (m *mockStore) UpdateJob(_ context.Context, _ models.Job) error { return nil }
func (m *mockStore) GetJob(_ context.Context, _ string) (models.Job, error) {
	return m.getJob, m.getJobErr
}
func (m *mockStore) GetJobByIdempotencyKey(_ context.Context, _ string) (models.Job, error) {
	m.idempotencyCalls++
	if m.createErr == store.ErrDuplicateIdempotencyKey && m.idempotencyCalls > 1 {
		return m.idempotencyJob, nil
	}
	return m.idempotencyJob, m.idempotencyLookup
}
func (m *mockStore) CancelJob(_ context.Context, _ string) error { return m.cancelErr }
func (m *mockStore) ClaimNextJob(_ context.Context, lease time.Duration, queues []string) (models.Job, error) {
	m.claimLease = lease
	m.claimQueues = queues
	return m.claimJob, m.claimErr
}
func (m *mockStore) RenewLease(_ context.Context, _ models.Job, lease time.Duration) error {
	m.renewLease = lease
	return m.renewErr
}
func (m *mockStore) RequeueExpiredRunning(context.Context, int) (int, error) { return 0, nil }
func (m *mockStore) ReleaseClaim(context.Context, models.Job, string) error  { return nil }
func (m *mockStore) CompleteClaimedJob(_ context.Context, _, _ string, meta map[string]string) error {
	m.completeMeta = meta
	return m.completeErr
}
func (m *mockStore) FailClaimedJob(_ context.Context, _, token, errMsg string, nextRun *time.Time) error {
	m.failCalls++
	m.failToken = token
	m.failErrMsg = errMsg
	m.failNextRun = nextRun
	return m.failErr
}
func (m *mockStore) ListJobs(context.Context, store.ListFilter) ([]models.Job, string, error) {
	return nil, "", nil
}

// mockPublisher satisfies Publisher with controllable responses.
type mockPublisher struct {
	publishErr error
	mu         sync.Mutex
	published  int
	publishedC chan struct{}
}

func (m *mockPublisher) Publish(_ context.Context, _ string, _ models.Job) error {
	m.mu.Lock()
	m.published++
	m.mu.Unlock()
	if m.publishedC != nil {
		select {
		case m.publishedC <- struct{}{}:
		default:
		}
	}
	return m.publishErr
}

func (m *mockPublisher) publishedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.published
}

// testLease is the server maximum used by every test service, so a clamp is
// visible as a returned duration of exactly this.
const testLease = 2 * time.Minute

func newTestService(s *mockStore, p *mockPublisher) *JobService {
	engine := retry.NewEngine(retry.Config{
		BaseDelay:   time.Second,
		MaxDelay:    30 * time.Second,
		Multiplier:  2,
		MaxAttempts: 3,
		Jitter:      0,
	})
	return NewJobService(p, s, engine, "test-topic", testLease, zerolog.Nop())
}

func TestEnqueueSuccess(t *testing.T) {
	ms := &mockStore{}
	svc := newTestService(ms, &mockPublisher{})

	id, err := svc.Enqueue(context.Background(), models.Job{Task: models.Task{Name: "send-email"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id == "" {
		t.Fatal("expected non-empty job ID")
	}
	if ms.createdJob.ID != id {
		t.Errorf("stored job ID = %q, want %q", ms.createdJob.ID, id)
	}
	if ms.createdJob.State != models.JobStatePending {
		t.Errorf("stored job state = %q, want %q", ms.createdJob.State, models.JobStatePending)
	}
}

func TestEnqueuePreservesSuppliedID(t *testing.T) {
	ms := &mockStore{}
	svc := newTestService(ms, &mockPublisher{})

	id, err := svc.Enqueue(context.Background(), models.Job{ID: "my-id", Task: models.Task{Name: "process-image"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != "my-id" {
		t.Errorf("Enqueue returned ID %q, want %q", id, "my-id")
	}
}

func TestEnqueueStoreError(t *testing.T) {
	storeErr := errors.New("postgres: connection refused")
	ms := &mockStore{createErr: storeErr}
	svc := newTestService(ms, &mockPublisher{})

	_, err := svc.Enqueue(context.Background(), models.Job{Task: models.Task{Name: "failing-task"}})
	if err == nil {
		t.Fatal("expected error when store fails, got nil")
	}
	if !errors.Is(err, storeErr) {
		t.Errorf("error chain does not contain store error; got %v", err)
	}
}

func TestEnqueueDoesNotPublishFutureScheduledJob(t *testing.T) {
	ms := &mockStore{}
	publisher := &mockPublisher{publishedC: make(chan struct{}, 1)}
	svc := newTestService(ms, publisher)
	future := time.Now().UTC().Add(time.Hour)

	_, err := svc.Enqueue(context.Background(), models.Job{
		Task:        models.Task{Name: "send-email"},
		ScheduledAt: &future,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	select {
	case <-publisher.publishedC:
		t.Fatal("future scheduled job was published immediately")
	case <-time.After(50 * time.Millisecond):
	}
	if publisher.publishedCount() != 0 {
		t.Fatalf("published count = %d, want 0", publisher.publishedCount())
	}
}

func TestEnqueueReturnsExistingJobForIdempotencyKey(t *testing.T) {
	ms := &mockStore{
		idempotencyJob:    models.Job{ID: "existing-job"},
		idempotencyLookup: nil,
	}
	publisher := &mockPublisher{publishedC: make(chan struct{}, 1)}
	svc := newTestService(ms, publisher)

	id, err := svc.Enqueue(context.Background(), models.Job{
		IdempotencyKey: "request-1",
		Task:           models.Task{Name: "send-email"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != "existing-job" {
		t.Fatalf("id = %q, want existing-job", id)
	}
	if ms.createdJob.ID != "" {
		t.Fatalf("created new job on duplicate idempotency key: %#v", ms.createdJob)
	}
	if publisher.publishedCount() != 0 {
		t.Fatalf("published count = %d, want 0", publisher.publishedCount())
	}
}

func TestEnqueueReturnsExistingJobAfterDuplicateIdempotencyRace(t *testing.T) {
	ms := &mockStore{
		createErr:         store.ErrDuplicateIdempotencyKey,
		idempotencyJob:    models.Job{ID: "existing-job"},
		idempotencyLookup: store.ErrJobNotFound,
	}
	publisher := &mockPublisher{publishedC: make(chan struct{}, 1)}
	svc := newTestService(ms, publisher)

	id, err := svc.Enqueue(context.Background(), models.Job{
		IdempotencyKey: "request-1",
		Task:           models.Task{Name: "send-email"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != "existing-job" {
		t.Fatalf("id = %q, want existing-job", id)
	}
	if publisher.publishedCount() != 0 {
		t.Fatalf("published count = %d, want 0", publisher.publishedCount())
	}
}

func TestEnqueuePublishesDueJob(t *testing.T) {
	ms := &mockStore{}
	publisher := &mockPublisher{publishedC: make(chan struct{}, 1)}
	svc := newTestService(ms, publisher)

	_, err := svc.Enqueue(context.Background(), models.Job{Task: models.Task{Name: "send-email"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	select {
	case <-publisher.publishedC:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for due job publish")
	}
	if publisher.publishedCount() != 1 {
		t.Fatalf("published count = %d, want 1", publisher.publishedCount())
	}
}

func TestCancelSuccess(t *testing.T) {
	svc := newTestService(&mockStore{}, &mockPublisher{})

	if err := svc.Cancel(context.Background(), "job-123"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCancelError(t *testing.T) {
	cancelErr := errors.New("store: job not found")
	ms := &mockStore{cancelErr: cancelErr}
	svc := newTestService(ms, &mockPublisher{})

	err := svc.Cancel(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("expected error when cancel fails, got nil")
	}
	if !errors.Is(err, cancelErr) {
		t.Errorf("error chain does not contain cancel error; got %v", err)
	}
}

func TestClaim(t *testing.T) {
	tests := []struct {
		name        string
		queues      []string
		lease       time.Duration
		storeJob    models.Job
		storeErr    error
		wantLease   time.Duration
		wantQueues  []string
		wantErrIs   error
		wantJobID   string
		wantWrapped bool
	}{
		{
			name:       "no queue filter claims from any queue",
			lease:      time.Minute,
			storeJob:   models.Job{ID: "job-1", LeaseToken: "tok"},
			wantLease:  time.Minute,
			wantQueues: nil,
			wantJobID:  "job-1",
		},
		{
			name:       "queue filter is passed through",
			queues:     []string{"remote", "gpu"},
			lease:      time.Minute,
			storeJob:   models.Job{ID: "job-2"},
			wantLease:  time.Minute,
			wantQueues: []string{"remote", "gpu"},
			wantJobID:  "job-2",
		},
		{
			name:      "lease longer than the server maximum is clamped",
			lease:     720 * time.Hour,
			storeJob:  models.Job{ID: "job-3"},
			wantLease: testLease,
			wantJobID: "job-3",
		},
		{
			name:      "absent lease defaults to the server maximum",
			lease:     0,
			storeJob:  models.Job{ID: "job-4"},
			wantLease: testLease,
			wantJobID: "job-4",
		},
		{
			name:      "empty queue surfaces ErrJobNotFound unwrapped for a 204",
			storeErr:  store.ErrJobNotFound,
			wantErrIs: store.ErrJobNotFound,
			wantLease: testLease,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ms := &mockStore{claimJob: tc.storeJob, claimErr: tc.storeErr}
			svc := newTestService(ms, &mockPublisher{})

			job, err := svc.Claim(context.Background(), tc.queues, tc.lease)
			if tc.wantErrIs != nil {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("error = %v, want %v", err, tc.wantErrIs)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if job.ID != tc.wantJobID {
				t.Errorf("job ID = %q, want %q", job.ID, tc.wantJobID)
			}
			if ms.claimLease != tc.wantLease {
				t.Errorf("store lease = %v, want %v", ms.claimLease, tc.wantLease)
			}
			if len(ms.claimQueues) != len(tc.wantQueues) {
				t.Fatalf("store queues = %v, want %v", ms.claimQueues, tc.wantQueues)
			}
			for i := range tc.wantQueues {
				if ms.claimQueues[i] != tc.wantQueues[i] {
					t.Errorf("store queues = %v, want %v", ms.claimQueues, tc.wantQueues)
				}
			}
		})
	}
}

func TestHeartbeat(t *testing.T) {
	t.Run("returns the new expiry and clamps the request", func(t *testing.T) {
		ms := &mockStore{}
		svc := newTestService(ms, &mockPublisher{})

		before := time.Now().UTC()
		expires, err := svc.Heartbeat(context.Background(), "job-1", "tok", time.Hour)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ms.renewLease != testLease {
			t.Errorf("renew lease = %v, want %v", ms.renewLease, testLease)
		}
		if expires.Before(before.Add(testLease - time.Second)) {
			t.Errorf("expiry %v is not roughly %v in the future", expires, testLease)
		}
	})

	t.Run("a stale token becomes ErrLeaseLost", func(t *testing.T) {
		ms := &mockStore{renewErr: store.ErrInvalidTransition}
		svc := newTestService(ms, &mockPublisher{})

		if _, err := svc.Heartbeat(context.Background(), "job-1", "stale", 0); !errors.Is(err, store.ErrLeaseLost) {
			t.Fatalf("error = %v, want ErrLeaseLost", err)
		}
	})
}

func TestComplete(t *testing.T) {
	t.Run("passes metadata through", func(t *testing.T) {
		ms := &mockStore{}
		svc := newTestService(ms, &mockPublisher{})

		meta := map[string]string{"worker": "gpu-3"}
		if err := svc.Complete(context.Background(), "job-1", "tok", meta); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ms.completeMeta["worker"] != "gpu-3" {
			t.Errorf("metadata = %v, want worker=gpu-3", ms.completeMeta)
		}
	})

	t.Run("a lost lease is returned unwrapped so the API can map it to 409", func(t *testing.T) {
		ms := &mockStore{completeErr: store.ErrLeaseLost}
		svc := newTestService(ms, &mockPublisher{})

		if err := svc.Complete(context.Background(), "job-1", "stale", nil); !errors.Is(err, store.ErrLeaseLost) {
			t.Fatalf("error = %v, want ErrLeaseLost", err)
		}
	})
}

func TestFail(t *testing.T) {
	tests := []struct {
		name          string
		job           models.Job
		permanent     bool
		wantState     models.JobState
		wantScheduled bool
	}{
		{
			name:          "a transient failure within budget is rescheduled",
			job:           models.Job{ID: "job-1", State: models.JobStateRunning, Attempt: 1},
			wantState:     models.JobStatePending,
			wantScheduled: true,
		},
		{
			name:      "a permanent failure goes straight to DEAD",
			job:       models.Job{ID: "job-1", State: models.JobStateRunning, Attempt: 1},
			permanent: true,
			wantState: models.JobStateDead,
		},
		{
			name:      "an exhausted engine budget goes to DEAD",
			job:       models.Job{ID: "job-1", State: models.JobStateRunning, Attempt: 3},
			wantState: models.JobStateDead,
		},
		{
			name:          "the job's own MaxRetries wins over the engine budget",
			job:           models.Job{ID: "job-1", State: models.JobStateRunning, Attempt: 4, Task: models.Task{MaxRetries: 10}},
			wantState:     models.JobStatePending,
			wantScheduled: true,
		},
		{
			name:      "MaxRetries reached goes to DEAD",
			job:       models.Job{ID: "job-1", State: models.JobStateRunning, Attempt: 2, Task: models.Task{MaxRetries: 2}},
			wantState: models.JobStateDead,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ms := &mockStore{getJob: tc.job}
			svc := newTestService(ms, &mockPublisher{})

			job, err := svc.Fail(context.Background(), tc.job.ID, "tok", "boom", tc.permanent)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			// One write, not two. The old path went RUNNING -> FAILED -> PENDING
			// and could be interrupted between them.
			if ms.failCalls != 1 {
				t.Errorf("FailClaimedJob calls = %d, want 1", ms.failCalls)
			}
			if ms.failToken != "tok" {
				t.Errorf("fenced on token %q, want %q", ms.failToken, "tok")
			}
			if ms.failErrMsg != "boom" {
				t.Errorf("last_error = %q, want %q", ms.failErrMsg, "boom")
			}
			if tc.wantScheduled {
				if ms.failNextRun == nil {
					t.Fatal("expected a next-run time for a retry")
				}
				if !ms.failNextRun.After(time.Now().UTC()) {
					t.Errorf("next run %v is not in the future", ms.failNextRun)
				}
			} else if ms.failNextRun != nil {
				t.Errorf("next run = %v, want nil for a terminal failure", ms.failNextRun)
			}
			if job.State != tc.wantState {
				t.Errorf("state = %q, want %q", job.State, tc.wantState)
			}
			if job.State == models.JobStateFailed {
				t.Error("FAILED is unreachable by design; a retry goes straight back to PENDING")
			}
			if job.LeaseToken != "" {
				t.Error("returned job still carries a lease token")
			}
			if job.Attempt != tc.job.Attempt {
				t.Errorf("attempt = %d, want %d preserved", job.Attempt, tc.job.Attempt)
			}
		})
	}

	t.Run("a lost lease is returned unwrapped", func(t *testing.T) {
		ms := &mockStore{getJob: models.Job{ID: "job-1"}, failErr: store.ErrLeaseLost}
		svc := newTestService(ms, &mockPublisher{})

		if _, err := svc.Fail(context.Background(), "job-1", "stale", "boom", false); !errors.Is(err, store.ErrLeaseLost) {
			t.Fatalf("error = %v, want ErrLeaseLost", err)
		}
	})
}

func TestEnqueueNormalisesTheQueue(t *testing.T) {
	tests := []struct {
		name  string
		queue string
		want  string
	}{
		// task_queue routes claims now, so an unset queue has to land on the
		// same name a worker asks for rather than on ''.
		{name: "unset becomes the default queue", queue: "", want: models.DefaultQueue},
		{name: "an explicit queue is left alone", queue: "remote", want: "remote"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ms := &mockStore{}
			svc := newTestService(ms, &mockPublisher{})

			if _, err := svc.Enqueue(context.Background(), models.Job{
				Task: models.Task{Name: "send-email", Queue: tc.queue},
			}); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ms.createdJob.Task.Queue != tc.want {
				t.Errorf("stored queue = %q, want %q", ms.createdJob.Task.Queue, tc.want)
			}
		})
	}
}
