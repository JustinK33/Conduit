package store

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/example/conduit/pkg/models"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestJobRoundTripWithoutIdempotencyKey is the regression test for the bug that
// made every job enqueued without an idempotency_key unreadable: scanJob scanned
// the nullable idempotency_key and lease_token columns straight into a string,
// so GetJob returned "cannot scan NULL into *string" and ClaimNextJob failed on
// every reconciler tick. The unit tests never touched Postgres and CI's k6 load
// test only POSTs, so nothing caught it.
//
// Runs only with POSTGRES_TEST_DSN set, e.g.:
//
//	POSTGRES_HOST_PORT=5434 make up
//	POSTGRES_TEST_DSN='postgres://conduit:conduit@localhost:5434/conduit?sslmode=disable' go test ./internal/store
func TestJobRoundTripWithoutIdempotencyKey(t *testing.T) {
	dsn := os.Getenv("POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("set POSTGRES_TEST_DSN to run; see the comment above for the command")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	s := NewPostgresStore(pool, "jobs")
	id := "test-" + time.Now().UTC().Format("20060102150405.000000000")
	now := time.Now().UTC()

	// No IdempotencyKey and no LeaseToken, so both columns land as NULL.
	job := models.Job{
		ID:          id,
		Task:        models.Task{ID: id, Name: "integration-probe", Queue: "default"},
		State:       models.JobStatePending,
		ScheduledAt: &now,
		CreatedAt:   now,
	}
	if err := s.CreateJob(ctx, job); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM jobs WHERE id = $1", id)
	})

	got, err := s.GetJob(ctx, id)
	if err != nil {
		t.Fatalf("GetJob on a job with NULL idempotency_key: %v", err)
	}
	if got.IdempotencyKey != "" {
		t.Errorf("IdempotencyKey = %q, want empty for a NULL column", got.IdempotencyKey)
	}
	if got.LeaseToken != "" {
		t.Errorf("LeaseToken = %q, want empty for a NULL column", got.LeaseToken)
	}

	// ClaimNextJob shares scanJob, and this is the call the reconciler makes
	// every tick. It may claim a different due job if the table is busy, so
	// only assert that scanning succeeded.
	claimed, err := s.ClaimNextJob(ctx, time.Minute)
	if err != nil {
		t.Fatalf("ClaimNextJob: %v", err)
	}
	if claimed.LeaseToken == "" {
		t.Error("ClaimNextJob returned an empty lease token, want a generated one")
	}
	if claimed.State != models.JobStateRunning {
		t.Errorf("claimed state = %s, want RUNNING", claimed.State)
	}
}
