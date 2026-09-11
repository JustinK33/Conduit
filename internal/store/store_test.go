package store

import (
	"context"
	"testing"
	"time"

	"github.com/JustinK33/Conduit/pkg/models"
	"github.com/jackc/pgx/v5/pgxpool"
)

type mockJobStore struct{}

func (mockJobStore) CreateJob(context.Context, models.Job) error        { return nil }
func (mockJobStore) UpdateJob(context.Context, models.Job) error        { return nil }
func (mockJobStore) GetJob(context.Context, string) (models.Job, error) { return models.Job{}, nil }
func (mockJobStore) GetJobByIdempotencyKey(context.Context, string) (models.Job, error) {
	return models.Job{}, nil
}
func (mockJobStore) CancelJob(context.Context, string) error { return nil }
func (mockJobStore) ClaimNextJob(context.Context, time.Duration, []string) (models.Job, error) {
	return models.Job{}, nil
}
func (mockJobStore) RenewLease(context.Context, models.Job, time.Duration) error { return nil }
func (mockJobStore) RequeueExpiredRunning(context.Context, int) (int, error)     { return 0, nil }
func (mockJobStore) ReleaseClaim(context.Context, models.Job, string) error      { return nil }
func (mockJobStore) CompleteClaimedJob(context.Context, string, string, map[string]string) error {
	return nil
}
func (mockJobStore) FailClaimedJob(context.Context, string, string, string, *time.Time) error {
	return nil
}
func (mockJobStore) ListJobs(context.Context, ListFilter) ([]models.Job, string, error) {
	return nil, "", nil
}

func TestNewPostgresStore(t *testing.T) {
	t.Run("captures pool and table name", func(t *testing.T) {
		s := NewPostgresStore((*pgxpool.Pool)(nil), "jobs")
		if s == nil {
			t.Fatal("NewPostgresStore returned nil")
		}
		if s.TableName != "jobs" {
			t.Errorf("TableName = %q, want %q", s.TableName, "jobs")
		}
	})
}

func TestCreateJob(t *testing.T) {
	t.Skip("requires postgres: see TestJobRoundTripWithoutIdempotencyKey for the POSTGRES_TEST_DSN setup")

	tests := []struct {
		name string
		job  models.Job
	}{
		{name: "inserts pending job", job: models.Job{State: models.JobStatePending, CreatedAt: time.Now(), UpdatedAt: time.Now()}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_ = tc.job
		})
	}
}

func TestUpdateJob(t *testing.T) {
	t.Skip("requires postgres: see TestJobRoundTripWithoutIdempotencyKey for the POSTGRES_TEST_DSN setup")
}

func TestGetJob(t *testing.T) {
	t.Skip("requires postgres: see TestJobRoundTripWithoutIdempotencyKey for the POSTGRES_TEST_DSN setup")
}

func TestCancelJob(t *testing.T) {
	t.Skip("requires postgres: see TestJobRoundTripWithoutIdempotencyKey for the POSTGRES_TEST_DSN setup")
}

func TestClaimNextJob(t *testing.T) {
	t.Skip("requires postgres: see TestJobRoundTripWithoutIdempotencyKey for the POSTGRES_TEST_DSN setup")
}

func TestCanTransition(t *testing.T) {
	tests := []struct {
		name    string
		from    models.JobState
		to      models.JobState
		allowed bool
	}{
		// Valid forward transitions
		{name: "pending to running", from: models.JobStatePending, to: models.JobStateRunning, allowed: true},
		{name: "running to completed", from: models.JobStateRunning, to: models.JobStateCompleted, allowed: true},
		{name: "running to failed", from: models.JobStateRunning, to: models.JobStateFailed, allowed: true},
		{name: "running to dead", from: models.JobStateRunning, to: models.JobStateDead, allowed: true},
		{name: "failed to pending (retry)", from: models.JobStateFailed, to: models.JobStatePending, allowed: true},
		// Dead-letter escalation from any non-terminal state
		{name: "pending to dead", from: models.JobStatePending, to: models.JobStateDead, allowed: true},
		// Invalid transitions
		{name: "completed is terminal", from: models.JobStateCompleted, to: models.JobStatePending, allowed: false},
		{name: "completed to running is invalid", from: models.JobStateCompleted, to: models.JobStateRunning, allowed: false},
		{name: "dead is terminal", from: models.JobStateDead, to: models.JobStateRunning, allowed: false},
		{name: "dead to pending is invalid", from: models.JobStateDead, to: models.JobStatePending, allowed: false},
		{name: "pending to completed is invalid", from: models.JobStatePending, to: models.JobStateCompleted, allowed: false},
		{name: "failed to running is invalid", from: models.JobStateFailed, to: models.JobStateRunning, allowed: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := CanTransition(tc.from, tc.to)
			if got != tc.allowed {
				t.Errorf("CanTransition(%s, %s) = %v, want %v", tc.from, tc.to, got, tc.allowed)
			}
		})
	}
}
