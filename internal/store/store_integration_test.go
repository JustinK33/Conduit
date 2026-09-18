package store

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/JustinK33/Conduit/pkg/models"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Every test here closes its pool with t.Cleanup rather than defer, because
// cleanups run after all of a test's defers: a deferred Close shuts the pool
// before the row and table cleanups can use it, and their errors are discarded,
// which is why test rows and tables used to survive every run.
//
// claimTestTable gives a test its own copy of the jobs schema, dropped when the
// test ends. Only tests that call ClaimNextJob need it: every other method here
// addresses a job by id and is unaffected by rows it did not write.
func claimTestTable(ctx context.Context, t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	name := "jobs_claim_test_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if _, err := pool.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (LIKE jobs INCLUDING DEFAULTS)", name)); err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", name))
	})
	return name
}

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
	t.Cleanup(pool.Close)

	// Its own table, because the claim below would otherwise take, and leave
	// RUNNING for a full lease, whatever unrelated job happened to be due.
	table := claimTestTable(ctx, t, pool)
	s := NewPostgresStore(pool, table)
	id := "test-" + time.Now().UTC().Format("20060102150405.000000000")
	now := time.Now().UTC()
	// Due a minute ago rather than now: scheduled_at is written from this process
	// and ClaimNextJob compares it against the database's NOW(). A Docker VM whose
	// clock has drifted behind the host - nine seconds, measured, after the host
	// slept - makes a job scheduled for "now" not yet due, and the claim below
	// fails with ErrJobNotFound for a reason that has nothing to do with the code.
	due := now.Add(-time.Minute)

	// No IdempotencyKey and no LeaseToken, so both columns land as NULL.
	job := models.Job{
		ID:          id,
		Task:        models.Task{ID: id, Name: "integration-probe", Queue: "default"},
		State:       models.JobStatePending,
		ScheduledAt: &due,
		CreatedAt:   now,
	}
	if err := s.CreateJob(ctx, job); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf("DELETE FROM %s WHERE id = $1", table), id)
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
	// every tick.
	claimed, err := s.ClaimNextJob(ctx, time.Minute, nil)
	if err != nil {
		t.Fatalf("ClaimNextJob: %v", err)
	}
	if claimed.ID != id {
		t.Fatalf("claimed %s, want the only job in the table %s", claimed.ID, id)
	}
	if claimed.LeaseToken == "" {
		t.Error("ClaimNextJob returned an empty lease token, want a generated one")
	}
	if claimed.State != models.JobStateRunning {
		t.Errorf("claimed state = %s, want RUNNING", claimed.State)
	}
}

func TestClaimNextJobQueueFilter(t *testing.T) {
	dsn := os.Getenv("POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("set POSTGRES_TEST_DSN to run")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	// A claim with no queue filter takes the oldest due row in the whole table,
	// so this test cannot share one with anything else: a leftover PENDING job
	// from a previous run or a live stack makes it claim something it never
	// created. Its own table is the only way to be deterministic.
	table := claimTestTable(ctx, t, pool)
	s := NewPostgresStore(pool, table)
	// A minute in the past, because scheduled_at is written from here and the
	// claim compares it against the database's NOW(): a Docker VM a few seconds
	// behind the host makes a job scheduled for "now" not yet due.
	now := time.Now().UTC().Add(-time.Minute)

	tests := []struct {
		name      string
		jobQueue  string
		filter    []string
		wantClaim bool
	}{
		{
			name:      "claim with matching queue filter",
			jobQueue:  "email",
			filter:    []string{"email", "sms"},
			wantClaim: true,
		},
		{
			name:      "skip job with non-matching queue filter",
			jobQueue:  "email",
			filter:    []string{"sms"},
			wantClaim: false,
		},
		{
			name:      "claim with nil filter returns any job",
			jobQueue:  "email",
			filter:    nil,
			wantClaim: true,
		},
		{
			name:      "claim with empty filter returns any job",
			jobQueue:  "email",
			filter:    []string{},
			wantClaim: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id := "test-queue-" + time.Now().UTC().Format("20060102150405.000000000")
			job := models.Job{
				ID:          id,
				Task:        models.Task{ID: id, Name: "send-email", Queue: tc.jobQueue},
				State:       models.JobStatePending,
				ScheduledAt: &now,
				CreatedAt:   now,
			}
			if err := s.CreateJob(ctx, job); err != nil {
				t.Fatalf("CreateJob: %v", err)
			}
			t.Cleanup(func() {
				_, _ = pool.Exec(context.Background(), fmt.Sprintf("DELETE FROM %s WHERE id = $1", table), id)
			})

			claimed, err := s.ClaimNextJob(ctx, time.Minute, tc.filter)
			if tc.wantClaim {
				if err != nil {
					t.Fatalf("ClaimNextJob: %v", err)
				}
				if claimed.ID != id {
					t.Errorf("claimed wrong job: got %s, want %s", claimed.ID, id)
				}
				if claimed.Task.Queue != tc.jobQueue {
					t.Errorf("claimed queue = %s, want %s", claimed.Task.Queue, tc.jobQueue)
				}
			} else {
				if err != ErrJobNotFound {
					t.Fatalf("ClaimNextJob: want ErrJobNotFound, got %v", err)
				}
			}
		})
	}
}

func TestCompleteClaimedJob(t *testing.T) {
	dsn := os.Getenv("POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("set POSTGRES_TEST_DSN to run")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	s := NewPostgresStore(pool, "jobs")
	now := time.Now().UTC()

	tests := []struct {
		name        string
		setupToken  string
		callToken   string
		initialMeta map[string]string
		meta        map[string]string
		wantErr     error
	}{
		{
			name:        "complete with correct token",
			setupToken:  "valid-token",
			callToken:   "valid-token",
			initialMeta: map[string]string{"existing": "value"},
			meta:        map[string]string{"result": "success"},
			wantErr:     nil,
		},
		{
			// A job enqueued without metadata stores JSON null, and jsonb's ||
			// concatenates a non-object as an array: [null, {...}], which does
			// not scan back into a map. The worker's result would vanish.
			name:       "complete a job that was enqueued without metadata",
			setupToken: "valid-token",
			callToken:  "valid-token",
			meta:       map[string]string{"result": "success"},
			wantErr:    nil,
		},
		{
			name:       "complete with stale token returns ErrLeaseLost",
			setupToken: "valid-token",
			callToken:  "stale-token",
			meta:       nil,
			wantErr:    ErrLeaseLost,
		},
		{
			name:       "complete with empty token returns ErrInvalidTransition",
			setupToken: "valid-token",
			callToken:  "",
			meta:       nil,
			wantErr:    ErrInvalidTransition,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id := "test-complete-" + time.Now().UTC().Format("20060102150405.000000000")
			job := models.Job{
				ID:          id,
				Task:        models.Task{ID: id, Name: "send-email", Queue: "default"},
				State:       models.JobStatePending,
				ScheduledAt: &now,
				CreatedAt:   now,
				Metadata:    tc.initialMeta,
			}
			if err := s.CreateJob(ctx, job); err != nil {
				t.Fatalf("CreateJob: %v", err)
			}
			t.Cleanup(func() {
				_, _ = pool.Exec(context.Background(), "DELETE FROM jobs WHERE id = $1", id)
			})

			// Claim the job with the setup token
			_, err := pool.Exec(ctx, `
				UPDATE jobs
				SET state = 'RUNNING',
					lease_token = $1,
					lease_expires_at = NOW() + INTERVAL '1 minute'
				WHERE id = $2
			`, tc.setupToken, id)
			if err != nil {
				t.Fatalf("setup claim: %v", err)
			}

			err = s.CompleteClaimedJob(ctx, id, tc.callToken, tc.meta)
			if err != tc.wantErr {
				t.Fatalf("CompleteClaimedJob: got error %v, want %v", err, tc.wantErr)
			}

			if tc.wantErr == nil {
				got, err := s.GetJob(ctx, id)
				if err != nil {
					t.Fatalf("GetJob: %v", err)
				}
				if got.State != models.JobStateCompleted {
					t.Errorf("state = %s, want COMPLETED", got.State)
				}
				if got.CompletedAt == nil {
					t.Error("completed_at is nil, want set")
				}
				if got.LeaseToken != "" {
					t.Errorf("lease_token = %q, want empty", got.LeaseToken)
				}
				if got.StartedAt != nil {
					t.Error("started_at should be NULL after completion")
				}
				if got.LeaseExpiresAt != nil {
					t.Error("lease_expires_at should be NULL after completion")
				}
				// Check metadata merge
				if tc.initialMeta != nil && got.Metadata["existing"] != "value" {
					t.Errorf("existing metadata lost: %v", got.Metadata)
				}
				if tc.meta != nil && got.Metadata["result"] != "success" {
					t.Errorf("new metadata not merged: %v", got.Metadata)
				}
			} else if tc.wantErr == ErrLeaseLost {
				// Verify row unchanged
				got, err := s.GetJob(ctx, id)
				if err != nil {
					t.Fatalf("GetJob: %v", err)
				}
				if got.State != models.JobStateRunning {
					t.Errorf("state = %s, want RUNNING (unchanged)", got.State)
				}
			}
		})
	}
}

func TestFailClaimedJob(t *testing.T) {
	dsn := os.Getenv("POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("set POSTGRES_TEST_DSN to run")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	s := NewPostgresStore(pool, "jobs")
	now := time.Now().UTC()
	future := now.Add(time.Hour)

	tests := []struct {
		name        string
		setupToken  string
		callToken   string
		errMsg      string
		nextRun     *time.Time
		wantErr     error
		wantState   models.JobState
		checkFields func(*testing.T, models.Job)
	}{
		{
			name:       "fail with nextRun goes to PENDING",
			setupToken: "valid-token",
			callToken:  "valid-token",
			errMsg:     "temporary failure",
			nextRun:    &future,
			wantErr:    nil,
			wantState:  models.JobStatePending,
			checkFields: func(t *testing.T, j models.Job) {
				if j.ScheduledAt == nil || j.ScheduledAt.Before(now) {
					t.Errorf("scheduled_at = %v, want future time", j.ScheduledAt)
				}
				if j.LastError != "temporary failure" {
					t.Errorf("last_error = %q, want 'temporary failure'", j.LastError)
				}
				if j.CompletedAt != nil {
					t.Error("completed_at should be NULL for PENDING")
				}
				if j.StartedAt != nil {
					t.Error("started_at should be NULL")
				}
				if j.LeaseExpiresAt != nil {
					t.Error("lease_expires_at should be NULL")
				}
				if j.LeaseToken != "" {
					t.Errorf("lease_token = %q, want empty", j.LeaseToken)
				}
			},
		},
		{
			name:       "fail with nil nextRun goes to DEAD",
			setupToken: "valid-token",
			callToken:  "valid-token",
			errMsg:     "permanent failure",
			nextRun:    nil,
			wantErr:    nil,
			wantState:  models.JobStateDead,
			checkFields: func(t *testing.T, j models.Job) {
				if j.CompletedAt == nil {
					t.Error("completed_at should be set for DEAD")
				}
				if j.LastError != "permanent failure" {
					t.Errorf("last_error = %q, want 'permanent failure'", j.LastError)
				}
				if j.StartedAt != nil {
					t.Error("started_at should be NULL")
				}
				if j.LeaseExpiresAt != nil {
					t.Error("lease_expires_at should be NULL")
				}
				if j.LeaseToken != "" {
					t.Errorf("lease_token = %q, want empty", j.LeaseToken)
				}
			},
		},
		{
			name:       "fail with stale token returns ErrLeaseLost",
			setupToken: "valid-token",
			callToken:  "stale-token",
			errMsg:     "error",
			nextRun:    &future,
			wantErr:    ErrLeaseLost,
			wantState:  models.JobStateRunning,
			checkFields: func(t *testing.T, j models.Job) {
				if j.LeaseToken != "valid-token" {
					t.Errorf("lease_token = %q, want valid-token (unchanged)", j.LeaseToken)
				}
			},
		},
		{
			name:       "fail with empty token returns ErrInvalidTransition",
			setupToken: "valid-token",
			callToken:  "",
			errMsg:     "error",
			nextRun:    nil,
			wantErr:    ErrInvalidTransition,
			wantState:  models.JobStateRunning,
			checkFields: func(t *testing.T, j models.Job) {
				if j.LeaseToken != "valid-token" {
					t.Errorf("lease_token = %q, want valid-token (unchanged)", j.LeaseToken)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id := "test-fail-" + time.Now().UTC().Format("20060102150405.000000000")
			job := models.Job{
				ID:          id,
				Task:        models.Task{ID: id, Name: "send-email", Queue: "default"},
				State:       models.JobStatePending,
				ScheduledAt: &now,
				CreatedAt:   now,
				Attempt:     1,
			}
			if err := s.CreateJob(ctx, job); err != nil {
				t.Fatalf("CreateJob: %v", err)
			}
			t.Cleanup(func() {
				_, _ = pool.Exec(context.Background(), "DELETE FROM jobs WHERE id = $1", id)
			})

			// Claim the job with the setup token
			_, err := pool.Exec(ctx, `
				UPDATE jobs
				SET state = 'RUNNING',
					lease_token = $1,
					lease_expires_at = NOW() + INTERVAL '1 minute',
					started_at = NOW()
				WHERE id = $2
			`, tc.setupToken, id)
			if err != nil {
				t.Fatalf("setup claim: %v", err)
			}

			err = s.FailClaimedJob(ctx, id, tc.callToken, tc.errMsg, tc.nextRun)
			if err != tc.wantErr {
				t.Fatalf("FailClaimedJob: got error %v, want %v", err, tc.wantErr)
			}

			got, err := s.GetJob(ctx, id)
			if err != nil {
				t.Fatalf("GetJob: %v", err)
			}
			if got.State != tc.wantState {
				t.Errorf("state = %s, want %s", got.State, tc.wantState)
			}
			if got.Attempt != 1 {
				t.Errorf("attempt = %d, want 1 (unchanged)", got.Attempt)
			}
			if tc.checkFields != nil {
				tc.checkFields(t, got)
			}
		})
	}
}

// A released job keeps its attempt and its PENDING state either way, so
// scheduled_at is the only column that separates a correct release from the
// claim-release loop: with it untouched the reconciler re-claims the job on its
// very next tick, and a caller that keeps refusing spins as fast as it can poll.
func TestReleaseClaimDefersTheJob(t *testing.T) {
	dsn := os.Getenv("POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("set POSTGRES_TEST_DSN to run")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	// Claims here are unfiltered, so they take the oldest due row in the table:
	// its own table is what stops a live stack's reconciler from claiming this
	// job first. See TestClaimNextJobQueueFilter.
	s := NewPostgresStore(pool, claimTestTable(ctx, t, pool))
	now := time.Now().UTC()
	// Due a minute ago, not now: scheduled_at comes from this process and
	// ClaimNextJob compares it against the database's NOW(), so a Docker VM whose
	// clock has drifted a few seconds behind the host makes a job scheduled for
	// "now" not yet due. Caught that way once, as a claim that found nothing.
	due := now.Add(-time.Minute)
	id := "test-release-" + now.Format("20060102150405.000000000")

	if err := s.CreateJob(ctx, models.Job{
		ID:          id,
		Task:        models.Task{ID: id, Name: "webhook", Queue: "default"},
		State:       models.JobStatePending,
		ScheduledAt: &due,
		CreatedAt:   now,
	}); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	job, err := s.ClaimNextJob(ctx, time.Minute, nil)
	if err != nil {
		t.Fatalf("ClaimNextJob: %v", err)
	}

	const retryAfter = 30 * time.Second
	released := time.Now().UTC()
	if err := s.ReleaseClaim(ctx, job, "circuit open", retryAfter); err != nil {
		t.Fatalf("ReleaseClaim: %v", err)
	}

	got, err := s.GetJob(ctx, id)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.State != models.JobStatePending {
		t.Errorf("state = %s, want PENDING", got.State)
	}
	// ClaimNextJob took it to 1 and nothing ran, so the refusal must not spend
	// the retry budget: a job that meets an open circuit twice would otherwise
	// reach MaxRetries having executed once.
	if got.Attempt != 0 {
		t.Errorf("attempt = %d, want 0: a release that ran nothing must give the attempt back", got.Attempt)
	}
	if got.ScheduledAt == nil {
		t.Fatal("scheduled_at is NULL, so the job is due immediately")
	}
	if !got.ScheduledAt.After(released) {
		t.Errorf("scheduled_at = %s, want after %s: a release with no delay is the claim-release loop", got.ScheduledAt, released)
	}
	// Allow a second of slack for the round trip, but not a whole different unit.
	if latest := released.Add(retryAfter + time.Second); got.ScheduledAt.After(latest) {
		t.Errorf("scheduled_at = %s, want no later than %s", got.ScheduledAt, latest)
	}
	if got.LeaseToken != "" {
		t.Errorf("lease_token = %q, want empty", got.LeaseToken)
	}
}
