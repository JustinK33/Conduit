package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/JustinK33/Conduit/pkg/models"
	"github.com/rs/zerolog"
)

// fakeStore is a ScheduleStore over a slice, with AdvanceSchedule enforcing the
// same optimistic-concurrency rule Postgres does: the observed next_run_at has
// to still be the current one.
type fakeStore struct {
	mu         sync.Mutex
	schedules  []models.Schedule
	dueErr     error
	advanceErr error

	dueCalls int
	advances []advanceCall
}

type advanceCall struct {
	id           string
	observedNext time.Time
	next         time.Time
	lastJobID    string
}

func (f *fakeStore) DueSchedules(_ context.Context, now time.Time, limit int) ([]models.Schedule, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dueCalls++
	if f.dueErr != nil {
		return nil, f.dueErr
	}
	due := []models.Schedule{}
	for _, s := range f.schedules {
		if s.Enabled && !s.NextRunAt.After(now) && len(due) < limit {
			due = append(due, s)
		}
	}
	return due, nil
}

func (f *fakeStore) AdvanceSchedule(_ context.Context, id string, observedNext, next time.Time, lastJobID string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.advanceErr != nil {
		return false, f.advanceErr
	}
	f.advances = append(f.advances, advanceCall{id, observedNext, next, lastJobID})
	for i, s := range f.schedules {
		if s.ID != id {
			continue
		}
		if !s.NextRunAt.Equal(observedNext) {
			return false, nil
		}
		f.schedules[i].NextRunAt = next
		f.schedules[i].LastJobID = lastJobID
		return true, nil
	}
	return false, nil
}

type fakeEnqueuer struct {
	mu   sync.Mutex
	jobs []models.Job
	id   string
	err  error
}

func (f *fakeEnqueuer) Enqueue(_ context.Context, job models.Job) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return "", f.err
	}
	f.jobs = append(f.jobs, job)
	if f.id == "" {
		return "job-1", nil
	}
	return f.id, nil
}

func newTestScheduler(store ScheduleStore, enq Enqueuer, now time.Time) *Scheduler {
	s := New(Config{TickInterval: time.Hour, FireBudget: 10}, store, enq, zerolog.Nop())
	s.now = func() time.Time { return now }
	return s
}

// minutely is the every-minute schedule, which makes fire instants easy to name.
func minutely(t *testing.T) Schedule {
	t.Helper()
	sched, err := Parse("* * * * *")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return sched
}

func TestNew(t *testing.T) {
	tests := []struct {
		name     string
		cfg      Config
		wantTick time.Duration
		wantFire int
	}{
		{name: "keeps explicit values", cfg: Config{TickInterval: time.Second, FireBudget: 3}, wantTick: time.Second, wantFire: 3},
		{name: "defaults a zero tick interval", cfg: Config{}, wantTick: 30 * time.Second, wantFire: 5},
		{name: "defaults a negative fire budget", cfg: Config{TickInterval: time.Minute, FireBudget: -1}, wantTick: time.Minute, wantFire: 5},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := New(tc.cfg, nil, nil, zerolog.Nop())
			if s == nil {
				t.Fatal("New returned nil")
			}
			if s.cfg.TickInterval != tc.wantTick {
				t.Errorf("TickInterval = %v, want %v", s.cfg.TickInterval, tc.wantTick)
			}
			if s.cfg.FireBudget != tc.wantFire {
				t.Errorf("FireBudget = %d, want %d", s.cfg.FireBudget, tc.wantFire)
			}
		})
	}
}

func TestSchedulerFires(t *testing.T) {
	t.Run("enqueues with the fire instant as the idempotency key and advances", func(t *testing.T) {
		fireAt := time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)
		now := fireAt.Add(10 * time.Second)

		store := &fakeStore{schedules: []models.Schedule{{
			ID:        "sch-1",
			Name:      "nightly",
			Cron:      "* * * * *",
			Task:      models.Task{Name: "sql.etl", Queue: "default"},
			Enabled:   true,
			NextRunAt: fireAt,
		}}}
		enq := &fakeEnqueuer{id: "job-42"}

		newTestScheduler(store, enq, now).dispatch(context.Background())

		if len(enq.jobs) != 1 {
			t.Fatalf("enqueued %d jobs, want 1", len(enq.jobs))
		}
		wantKey := fmt.Sprintf("sched:sch-1:%d", fireAt.Unix())
		if got := enq.jobs[0].IdempotencyKey; got != wantKey {
			t.Errorf("idempotency key = %q, want %q", got, wantKey)
		}
		if got := enq.jobs[0].Task.Name; got != "sql.etl" {
			t.Errorf("task name = %q, want sql.etl", got)
		}
		if got := enq.jobs[0].Metadata["conduit.schedule"]; got != "nightly" {
			t.Errorf("metadata conduit.schedule = %q, want nightly", got)
		}
		if enq.jobs[0].ScheduledAt == nil || !enq.jobs[0].ScheduledAt.Equal(fireAt) {
			t.Errorf("scheduled_at = %v, want %v", enq.jobs[0].ScheduledAt, fireAt)
		}

		if len(store.advances) != 1 {
			t.Fatalf("advance calls = %d, want 1", len(store.advances))
		}
		adv := store.advances[0]
		if !adv.observedNext.Equal(fireAt) {
			t.Errorf("observedNext = %v, want %v", adv.observedNext, fireAt)
		}
		if want := fireAt.Add(time.Minute); !adv.next.Equal(want) {
			t.Errorf("next = %v, want %v", adv.next, want)
		}
		if adv.lastJobID != "job-42" {
			t.Errorf("lastJobID = %q, want job-42", adv.lastJobID)
		}
	})

	t.Run("skips missed occurrences instead of replaying them", func(t *testing.T) {
		fireAt := time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)
		now := fireAt.Add(3 * time.Hour)

		store := &fakeStore{schedules: []models.Schedule{{
			ID: "sch-1", Name: "hourly", Cron: "* * * * *",
			Enabled: true, NextRunAt: fireAt,
		}}}
		enq := &fakeEnqueuer{}

		newTestScheduler(store, enq, now).dispatch(context.Background())

		if len(enq.jobs) != 1 {
			t.Fatalf("enqueued %d jobs, want 1 - a catch-up must not replay", len(enq.jobs))
		}
		if want := now.Add(time.Minute); !store.advances[0].next.Equal(want) {
			t.Errorf("next = %v, want %v (first instant after now)", store.advances[0].next, want)
		}
	})

	t.Run("does not fire a disabled schedule", func(t *testing.T) {
		now := time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)
		store := &fakeStore{schedules: []models.Schedule{{
			ID: "sch-1", Cron: "* * * * *", Enabled: false, NextRunAt: now.Add(-time.Hour),
		}}}
		enq := &fakeEnqueuer{}

		newTestScheduler(store, enq, now).dispatch(context.Background())

		if len(enq.jobs) != 0 {
			t.Errorf("enqueued %d jobs from a disabled schedule, want 0", len(enq.jobs))
		}
	})

	t.Run("does not fire a schedule whose instant has not arrived", func(t *testing.T) {
		now := time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)
		store := &fakeStore{schedules: []models.Schedule{{
			ID: "sch-1", Cron: "* * * * *", Enabled: true, NextRunAt: now.Add(time.Minute),
		}}}
		enq := &fakeEnqueuer{}

		newTestScheduler(store, enq, now).dispatch(context.Background())

		if len(enq.jobs) != 0 {
			t.Errorf("enqueued %d jobs before the fire instant, want 0", len(enq.jobs))
		}
	})

	t.Run("does not advance when the enqueue fails", func(t *testing.T) {
		now := time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)
		store := &fakeStore{schedules: []models.Schedule{{
			ID: "sch-1", Cron: "* * * * *", Enabled: true, NextRunAt: now,
		}}}
		enq := &fakeEnqueuer{err: errors.New("postgres is down")}

		newTestScheduler(store, enq, now).dispatch(context.Background())

		if len(store.advances) != 0 {
			t.Errorf("advanced %d schedules after a failed enqueue, want 0", len(store.advances))
		}
		if !store.schedules[0].NextRunAt.Equal(now) {
			t.Error("next_run_at moved after a failed enqueue; the fire would be lost")
		}
	})

	t.Run("does not fire a schedule with an unparseable cron", func(t *testing.T) {
		now := time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)
		store := &fakeStore{schedules: []models.Schedule{{
			ID: "sch-1", Cron: "not a cron", Enabled: true, NextRunAt: now,
		}}}
		enq := &fakeEnqueuer{}

		newTestScheduler(store, enq, now).dispatch(context.Background())

		if len(enq.jobs) != 0 {
			t.Errorf("enqueued %d jobs from an unparseable cron, want 0", len(enq.jobs))
		}
	})
}

// TestSchedulerReplicaRace is the multi-instance claim: N schedulers all see the
// same due schedule, all enqueue the same idempotency key, and exactly one wins
// the advance.
func TestSchedulerReplicaRace(t *testing.T) {
	fireAt := time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)
	store := &fakeStore{schedules: []models.Schedule{{
		ID: "sch-1", Cron: "* * * * *", Enabled: true, NextRunAt: fireAt,
	}}}
	enq := &fakeEnqueuer{id: "job-42"}

	// All three read the schedule as due before any of them advances it, which is
	// what happens on three replicas polling the same row on the same tick.
	due, err := store.DueSchedules(context.Background(), fireAt, 10)
	if err != nil {
		t.Fatalf("DueSchedules: %v", err)
	}
	if len(due) != 1 {
		t.Fatalf("due = %d, want 1", len(due))
	}
	for i := 0; i < 3; i++ {
		newTestScheduler(store, enq, fireAt).fire(context.Background(), due[0], fireAt)
	}

	// All three enqueue - that is what the idempotency key is for, and the store
	// collapses them into one job.
	if len(enq.jobs) != 3 {
		t.Fatalf("enqueue calls = %d, want 3", len(enq.jobs))
	}
	keys := map[string]bool{}
	for _, j := range enq.jobs {
		keys[j.IdempotencyKey] = true
	}
	if len(keys) != 1 {
		t.Errorf("distinct idempotency keys = %d, want 1: %v", len(keys), keys)
	}
	if want := fireAt.Add(time.Minute); !store.schedules[0].NextRunAt.Equal(want) {
		t.Errorf("next_run_at = %v, want %v advanced exactly once", store.schedules[0].NextRunAt, want)
	}
}

func TestNextAfter(t *testing.T) {
	base := time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)
	tests := []struct {
		name        string
		from, now   time.Time
		wantNext    time.Time
		wantSkipped int
	}{
		{name: "on time", from: base, now: base, wantNext: base.Add(time.Minute)},
		{name: "one minute late", from: base, now: base.Add(90 * time.Second), wantNext: base.Add(2 * time.Minute), wantSkipped: 1},
		{name: "an hour late", from: base, now: base.Add(time.Hour), wantNext: base.Add(61 * time.Minute), wantSkipped: 60},
	}

	cron := minutely(t)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			next, skipped := nextAfter(cron, tc.from, tc.now)
			if !next.Equal(tc.wantNext) {
				t.Errorf("next = %v, want %v", next, tc.wantNext)
			}
			if skipped != tc.wantSkipped {
				t.Errorf("skipped = %d, want %d", skipped, tc.wantSkipped)
			}
		})
	}
}

func TestSchedulerStart(t *testing.T) {
	t.Run("ticks and stops without panic", func(t *testing.T) {
		store := &fakeStore{}
		s := New(Config{TickInterval: 20 * time.Millisecond}, store, &fakeEnqueuer{}, zerolog.Nop())

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		s.Start(ctx)
		<-ctx.Done()
		s.Stop()

		store.mu.Lock()
		defer store.mu.Unlock()
		if store.dueCalls == 0 {
			t.Error("scheduler never polled for due schedules")
		}
	})
}

func TestSchedulerStop(t *testing.T) {
	t.Run("stop is safe to call before start", func(t *testing.T) {
		s := New(Config{TickInterval: time.Second}, &fakeStore{}, &fakeEnqueuer{}, zerolog.Nop())
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Stop panicked: %v", r)
			}
		}()
		s.Stop()
	})

	t.Run("stop is safe to call twice", func(t *testing.T) {
		s := New(Config{TickInterval: 50 * time.Millisecond}, &fakeStore{}, &fakeEnqueuer{}, zerolog.Nop())
		ctx, cancel := context.WithCancel(context.Background())
		s.Start(ctx)
		cancel()
		s.Stop()

		defer func() {
			if r := recover(); r != nil {
				t.Errorf("second Stop panicked: %v", r)
			}
		}()
		s.Stop()
	})
}
