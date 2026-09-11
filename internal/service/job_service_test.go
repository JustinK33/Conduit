package service

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

// mockStore satisfies store.JobStore with controllable responses.
type mockStore struct {
	createErr         error
	cancelErr         error
	createdJob        models.Job
	idempotencyJob    models.Job
	idempotencyLookup error
	idempotencyCalls  int
}

func (m *mockStore) CreateJob(_ context.Context, job models.Job) error {
	m.createdJob = job
	return m.createErr
}
func (m *mockStore) UpdateJob(_ context.Context, _ models.Job) error        { return nil }
func (m *mockStore) GetJob(_ context.Context, _ string) (models.Job, error) { return models.Job{}, nil }
func (m *mockStore) GetJobByIdempotencyKey(_ context.Context, _ string) (models.Job, error) {
	m.idempotencyCalls++
	if m.createErr == store.ErrDuplicateIdempotencyKey && m.idempotencyCalls > 1 {
		return m.idempotencyJob, nil
	}
	return m.idempotencyJob, m.idempotencyLookup
}
func (m *mockStore) CancelJob(_ context.Context, _ string) error { return m.cancelErr }
func (m *mockStore) ClaimNextJob(_ context.Context, _ time.Duration, _ []string) (models.Job, error) {
	return models.Job{}, nil
}
func (m *mockStore) RenewLease(context.Context, models.Job, time.Duration) error { return nil }
func (m *mockStore) RequeueExpiredRunning(context.Context, int) (int, error)     { return 0, nil }
func (m *mockStore) ReleaseClaim(context.Context, models.Job, string) error      { return nil }
func (m *mockStore) CompleteClaimedJob(context.Context, string, string, map[string]string) error {
	return nil
}
func (m *mockStore) FailClaimedJob(context.Context, string, string, string, *time.Time) error {
	return nil
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

func newTestService(s *mockStore, p *mockPublisher) *JobService {
	return NewJobService(p, s, "test-topic", zerolog.Nop())
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
