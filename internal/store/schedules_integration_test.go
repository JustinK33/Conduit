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

// testPool is the setup the tests in store_integration_test.go each write out by
// hand. New tests use this instead; the older ones are left alone.
func testPool(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("set POSTGRES_TEST_DSN to run; see store_integration_test.go for the command")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	return ctx, pool
}

// scheduleTestTable is claimTestTable for schedules: a throwaway copy of the
// schema so a live instance's scheduler cannot advance rows out from under the
// test, and so the unique name constraint cannot collide across runs.
//
// INCLUDING INDEXES, unlike claimTestTable, because the unique index on name is
// the thing TestCreateScheduleRejectsADuplicateName is testing: without it the
// copy accepts two schedules by the same name and the test passes on a table
// that does not resemble the real one. Copied indexes get generated names, so
// concurrent copies do not collide.
func scheduleTestTable(ctx context.Context, t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	name := "schedules_test_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if _, err := pool.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (LIKE schedules INCLUDING DEFAULTS INCLUDING INDEXES)", name)); err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", name))
	})
	return name
}

func TestDueSchedules(t *testing.T) {
	ctx, pool := testPool(t)
	s := NewScheduleStore(pool, scheduleTestTable(ctx, t, pool))

	// Past and future relative to the database's NOW(), which is what the query
	// compares against: a Docker VM whose clock has drifted behind the host makes
	// "now" not yet due. Minutes, not seconds, so drift cannot decide the test.
	past := time.Now().UTC().Add(-time.Hour)
	future := time.Now().UTC().Add(time.Hour)

	mustCreate := func(name string, enabled bool, next time.Time) models.Schedule {
		t.Helper()
		sched, err := s.CreateSchedule(ctx, models.Schedule{
			Name:      name,
			Cron:      "* * * * *",
			Task:      models.Task{Name: "webhook"},
			Enabled:   enabled,
			NextRunAt: next,
		})
		if err != nil {
			t.Fatalf("CreateSchedule(%s): %v", name, err)
		}
		return sched
	}

	due := mustCreate("due", true, past)
	mustCreate("disabled-but-due", false, past)
	mustCreate("not-yet-due", true, future)

	got, err := s.DueSchedules(ctx, time.Now().UTC(), 10)
	if err != nil {
		t.Fatalf("DueSchedules: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("due = %d schedules, want 1: %#v", len(got), got)
	}
	if got[0].ID != due.ID {
		t.Errorf("due schedule = %q, want %q", got[0].Name, due.Name)
	}
	// The row has to come back whole, because the scheduler enqueues straight from
	// it: a task name lost here is a job that fails at dispatch.
	if got[0].Task.Name != "webhook" || got[0].Task.Queue != models.DefaultQueue {
		t.Errorf("task = %+v, want webhook on the default queue", got[0].Task)
	}

	// The fire budget is a LIMIT, not a suggestion.
	mustCreate("also-due", true, past)
	got, err = s.DueSchedules(ctx, time.Now().UTC(), 1)
	if err != nil {
		t.Fatalf("DueSchedules: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("due with limit 1 = %d schedules, want 1", len(got))
	}
}

// TestAdvanceScheduleIsConditional is the multi-replica story in one test. Ten
// instances poll the same due row and all ten try to advance it; the observed
// next_run_at in the WHERE clause is what makes exactly one of them win.
func TestAdvanceScheduleIsConditional(t *testing.T) {
	ctx, pool := testPool(t)
	s := NewScheduleStore(pool, scheduleTestTable(ctx, t, pool))

	fireAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	sched, err := s.CreateSchedule(ctx, models.Schedule{
		Name:      "hourly",
		Cron:      "0 * * * *",
		Task:      models.Task{Name: "sql.etl"},
		Enabled:   true,
		NextRunAt: fireAt,
	})
	if err != nil {
		t.Fatalf("CreateSchedule: %v", err)
	}
	next := fireAt.Add(time.Hour)

	won, err := s.AdvanceSchedule(ctx, sched.ID, fireAt, next, "job-1")
	if err != nil {
		t.Fatalf("AdvanceSchedule: %v", err)
	}
	if !won {
		t.Fatal("the first advance lost the race with nobody")
	}

	// The second replica read the same fireAt, so its UPDATE matches nothing.
	won, err = s.AdvanceSchedule(ctx, sched.ID, fireAt, next, "job-2")
	if err != nil {
		t.Fatalf("AdvanceSchedule (stale): %v", err)
	}
	if won {
		t.Error("a stale observed next_run_at advanced the schedule, so every replica would fire")
	}

	list, err := s.ListSchedules(ctx)
	if err != nil {
		t.Fatalf("ListSchedules: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("schedules = %d, want 1", len(list))
	}
	if !list[0].NextRunAt.Equal(next) {
		t.Errorf("next_run_at = %s, want %s", list[0].NextRunAt, next)
	}
	// The loser must not have overwritten the winner's job id.
	if list[0].LastJobID != "job-1" {
		t.Errorf("last_job_id = %q, want job-1", list[0].LastJobID)
	}
	if list[0].LastRunAt == nil {
		t.Error("last_run_at is NULL after a fire")
	}
}

func TestCreateScheduleRejectsADuplicateName(t *testing.T) {
	ctx, pool := testPool(t)
	s := NewScheduleStore(pool, scheduleTestTable(ctx, t, pool))

	sched := models.Schedule{
		Name:      "nightly",
		Cron:      "0 3 * * *",
		Task:      models.Task{Name: "sql.etl"},
		Enabled:   true,
		NextRunAt: time.Now().UTC().Add(time.Hour),
	}
	if _, err := s.CreateSchedule(ctx, sched); err != nil {
		t.Fatalf("CreateSchedule: %v", err)
	}

	// A fresh ID, same name: the name is the unique one, so that a retried create
	// cannot quietly produce two schedules firing the same task.
	if _, err := s.CreateSchedule(ctx, sched); err != ErrDuplicateSchedule {
		t.Fatalf("second CreateSchedule: got %v, want ErrDuplicateSchedule", err)
	}
}

func TestDeleteScheduleReportsAMiss(t *testing.T) {
	ctx, pool := testPool(t)
	s := NewScheduleStore(pool, scheduleTestTable(ctx, t, pool))

	if err := s.DeleteSchedule(ctx, "no-such-schedule"); err != ErrScheduleNotFound {
		t.Fatalf("DeleteSchedule: got %v, want ErrScheduleNotFound", err)
	}
}

func TestDeleteFinished(t *testing.T) {
	ctx, pool := testPool(t)
	// Its own table: the delete is by state and age, not by id, so it would take
	// a live stack's history with it.
	table := claimTestTable(ctx, t, pool)
	s := NewPostgresStore(pool, table)

	now := time.Now().UTC()
	old := now.Add(-48 * time.Hour)

	create := func(id string, state models.JobState, completedAt *time.Time) {
		t.Helper()
		if err := s.CreateJob(ctx, models.Job{
			ID:          id,
			Task:        models.Task{ID: id, Name: "webhook", Queue: "default"},
			State:       models.JobStatePending,
			ScheduledAt: &now,
			CreatedAt:   now,
		}); err != nil {
			t.Fatalf("CreateJob(%s): %v", id, err)
		}
		if state == models.JobStatePending {
			return
		}
		if _, err := pool.Exec(ctx, fmt.Sprintf(
			"UPDATE %s SET state = $1, completed_at = $2 WHERE id = $3", table),
			state, completedAt, id); err != nil {
			t.Fatalf("set state on %s: %v", id, err)
		}
	}

	create("old-completed", models.JobStateCompleted, &old)
	create("fresh-completed", models.JobStateCompleted, &now)
	create("old-dead", models.JobStateDead, &old)
	create("pending", models.JobStatePending, nil)

	deleted, err := s.DeleteFinished(ctx, models.JobStateCompleted, now.Add(-time.Hour), 100)
	if err != nil {
		t.Fatalf("DeleteFinished: %v", err)
	}
	if deleted != 1 {
		t.Errorf("deleted = %d, want 1", deleted)
	}
	if _, err := s.GetJob(ctx, "old-completed"); err != ErrJobNotFound {
		t.Errorf("old COMPLETED job survived the sweep: %v", err)
	}
	// Age, state, and nothing else. A sweep that took the fresh job would lose
	// history somebody is still reading; one that took DEAD or PENDING would lose
	// the investigation or the work itself.
	for _, id := range []string{"fresh-completed", "old-dead", "pending"} {
		if _, err := s.GetJob(ctx, id); err != nil {
			t.Errorf("GetJob(%s) after sweep: %v", id, err)
		}
	}

	// The limit bounds one statement, so a table with a million rows to prune
	// cannot hold a single lock over all of them.
	if _, err := s.DeleteFinished(ctx, models.JobStateDead, now, 1); err != nil {
		t.Fatalf("DeleteFinished(DEAD): %v", err)
	}
	if _, err := s.GetJob(ctx, "old-dead"); err != ErrJobNotFound {
		t.Errorf("old DEAD job survived an explicit DEAD sweep: %v", err)
	}

	// A non-terminal state is a caller bug, not a no-op: PENDING rows are the work.
	if _, err := s.DeleteFinished(ctx, models.JobStatePending, now, 100); err == nil {
		t.Error("DeleteFinished(PENDING) returned no error, want a refusal")
	}
}

func TestRequeueDeadJob(t *testing.T) {
	ctx, pool := testPool(t)
	table := claimTestTable(ctx, t, pool)
	s := NewPostgresStore(pool, table)
	now := time.Now().UTC()

	setup := func(id string, state models.JobState) {
		t.Helper()
		if err := s.CreateJob(ctx, models.Job{
			ID:          id,
			Task:        models.Task{ID: id, Name: "webhook", Queue: "default"},
			State:       models.JobStatePending,
			ScheduledAt: &now,
			CreatedAt:   now,
			Attempt:     3,
		}); err != nil {
			t.Fatalf("CreateJob(%s): %v", id, err)
		}
		if _, err := pool.Exec(ctx, fmt.Sprintf(`
			UPDATE %s SET state = $1, completed_at = NOW(), last_error = 'boom',
				lease_token = 'stale', lease_expires_at = NOW()
			WHERE id = $2`, table), state, id); err != nil {
			t.Fatalf("set state on %s: %v", id, err)
		}
	}

	setup("dead-job", models.JobStateDead)
	setup("completed-job", models.JobStateCompleted)

	if err := s.RequeueDeadJob(ctx, "dead-job"); err != nil {
		t.Fatalf("RequeueDeadJob: %v", err)
	}
	got, err := s.GetJob(ctx, "dead-job")
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.State != models.JobStatePending {
		t.Errorf("state = %s, want PENDING", got.State)
	}
	// A fresh budget is the point: a requeue with attempt still at MaxRetries
	// dead-letters again on the first failure.
	if got.Attempt != 0 {
		t.Errorf("attempt = %d, want 0", got.Attempt)
	}
	if got.CompletedAt != nil {
		t.Error("completed_at should be NULL again, or retention would prune a live job")
	}
	if got.LeaseToken != "" {
		t.Errorf("lease_token = %q, want empty so the job can be claimed", got.LeaseToken)
	}
	if got.LeaseExpiresAt != nil {
		t.Error("lease_expires_at should be NULL")
	}
	if got.ScheduledAt == nil {
		t.Error("scheduled_at is NULL, so the job is not due")
	}

	// A COMPLETED job already succeeded, and a RUNNING one has a worker holding
	// its lease. The WHERE state = 'DEAD' guard is what the API's 409 rests on.
	if err := s.RequeueDeadJob(ctx, "completed-job"); err != ErrInvalidTransition {
		t.Fatalf("RequeueDeadJob(COMPLETED): got %v, want ErrInvalidTransition", err)
	}
	if err := s.RequeueDeadJob(ctx, "no-such-job"); err != ErrJobNotFound {
		t.Fatalf("RequeueDeadJob(missing): got %v, want ErrJobNotFound", err)
	}
}

func TestCountByState(t *testing.T) {
	ctx, pool := testPool(t)
	table := claimTestTable(ctx, t, pool)
	s := NewPostgresStore(pool, table)
	now := time.Now().UTC()

	for id, state := range map[string]models.JobState{
		"p1": models.JobStatePending,
		"p2": models.JobStatePending,
		"r1": models.JobStateRunning,
		"c1": models.JobStateCompleted,
	} {
		if err := s.CreateJob(ctx, models.Job{
			ID:          id,
			Task:        models.Task{ID: id, Name: "webhook", Queue: "default"},
			State:       models.JobStatePending,
			ScheduledAt: &now,
			CreatedAt:   now,
		}); err != nil {
			t.Fatalf("CreateJob(%s): %v", id, err)
		}
		if _, err := pool.Exec(ctx, fmt.Sprintf("UPDATE %s SET state = $1 WHERE id = $2", table), state, id); err != nil {
			t.Fatalf("set state on %s: %v", id, err)
		}
	}

	counts, err := s.CountByState(ctx)
	if err != nil {
		t.Fatalf("CountByState: %v", err)
	}
	if counts[models.JobStatePending] != 2 {
		t.Errorf("PENDING = %d, want 2", counts[models.JobStatePending])
	}
	if counts[models.JobStateRunning] != 1 {
		t.Errorf("RUNNING = %d, want 1", counts[models.JobStateRunning])
	}
	// A drained bucket has to report zero rather than vanish, or the gauge reads
	// as a broken exporter.
	if got, ok := counts[models.JobStateDead]; !ok || got != 0 {
		t.Errorf("DEAD = %d (present %v), want 0 and present", got, ok)
	}
}
