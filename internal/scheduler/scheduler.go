package scheduler

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/JustinK33/Conduit/pkg/models"
	"github.com/rs/zerolog"
)

// Schedule is a parsed cron expression.
type Schedule interface {
	Next(time.Time) time.Time
}

// ScheduleStore is the subset of store.ScheduleStore the loop needs.
type ScheduleStore interface {
	DueSchedules(ctx context.Context, now time.Time, limit int) ([]models.Schedule, error)
	AdvanceSchedule(ctx context.Context, id string, observedNext, next time.Time, lastJobID string) (bool, error)
}

// Enqueuer is satisfied by *service.JobService. Firing a schedule goes through
// the same door as POST /api/jobs, so a scheduled run gets the idempotency
// check, the queue defaulting, and the Kafka nudge for free - and, crucially,
// the same answer when two replicas fire the same instant.
type Enqueuer interface {
	Enqueue(ctx context.Context, job models.Job) (string, error)
}

// IdempotencyPrefix namespaces the keys the scheduler mints. Callers should not
// use it for their own enqueues: a collision would make Conduit treat a hand
// enqueue as a schedule fire that already happened, and silently skip the fire.
const IdempotencyPrefix = "sched:"

type Config struct {
	TickInterval time.Duration
	// FireBudget caps how many schedules one tick fires. It bounds the work a
	// tick can do, not the number of schedules that exist: anything over budget
	// is still due on the next tick, because DueSchedules orders by next_run_at.
	FireBudget int
}

type Scheduler struct {
	schedules ScheduleStore
	enqueuer  Enqueuer
	log       zerolog.Logger
	cfg       Config

	now     func() time.Time
	running bool
	mu      sync.Mutex
	wg      sync.WaitGroup
	cancel  context.CancelFunc
}

func New(cfg Config, schedules ScheduleStore, enqueuer Enqueuer, log zerolog.Logger) *Scheduler {
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = 30 * time.Second
	}
	if cfg.FireBudget <= 0 {
		cfg.FireBudget = 5
	}
	return &Scheduler{
		schedules: schedules,
		enqueuer:  enqueuer,
		log:       log,
		cfg:       cfg,
		now:       time.Now,
	}
}

func (s *Scheduler) Start(ctx context.Context) {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return
	}
	childCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.running = true
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(s.cfg.TickInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.dispatch(childCtx)
			case <-childCtx.Done():
				return
			}
		}
	}()
}

// Stop halts the scheduler loop. Safe to call before Start or multiple times.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	if s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}
	s.running = false
	s.mu.Unlock()
	s.wg.Wait()
}

// dispatch fires every schedule whose instant has arrived.
//
// Serially, in one goroutine, because firing is two round trips to Postgres and
// no execution: the work itself happens in a worker after the job is enqueued.
// A goroutine per fire would buy nothing and make FireBudget a concurrency limit
// nobody asked for.
func (s *Scheduler) dispatch(ctx context.Context) {
	if s.schedules == nil || s.enqueuer == nil {
		return
	}
	now := s.now().UTC()

	due, err := s.schedules.DueSchedules(ctx, now, s.cfg.FireBudget)
	if err != nil {
		s.log.Error().Err(err).Msg("scheduler: failed to read due schedules")
		return
	}
	for _, sched := range due {
		if ctx.Err() != nil {
			return
		}
		s.fire(ctx, sched, now)
	}
}

// fire enqueues one run of a schedule and then advances it.
//
// The order matters both ways round. Enqueue first, so a crash in between
// re-fires the same instant on the next tick, where the idempotency key absorbs
// the duplicate: at worst one extra no-op enqueue. Advance last and
// conditionally, so of N replicas that all saw this schedule as due, exactly one
// wins the advance and the rest find their observed next_run_at gone.
func (s *Scheduler) fire(ctx context.Context, sched models.Schedule, now time.Time) {
	log := s.log.With().Str("schedule_id", sched.ID).Str("schedule", sched.Name).Logger()

	cron, err := Parse(sched.Cron)
	if err != nil {
		// Unreachable through the API, which parses before insert. Reachable by
		// hand-editing the table, and disabling the row is better than failing
		// this way every tick forever.
		log.Error().Err(err).Str("cron", sched.Cron).Msg("scheduler: schedule has an unparseable cron expression; not firing")
		return
	}

	fireAt := sched.NextRunAt.UTC()
	next, skipped := nextAfter(cron, fireAt, now)
	if next.IsZero() {
		log.Error().Str("cron", sched.Cron).Msg("scheduler: cron expression has no next occurrence; not firing")
		return
	}

	job := models.Job{
		// The fire instant, not the wall clock, is what makes this key stable
		// across replicas: they all read the same next_run_at.
		IdempotencyKey: fmt.Sprintf("%s%s:%d", IdempotencyPrefix, sched.ID, fireAt.Unix()),
		Task:           sched.Task,
		ScheduledAt:    &fireAt,
		Metadata: map[string]string{
			"conduit.schedule_id": sched.ID,
			"conduit.schedule":    sched.Name,
		},
	}

	jobID, err := s.enqueuer.Enqueue(ctx, job)
	if err != nil {
		// Do not advance. The schedule stays due and the next tick tries again.
		log.Error().Err(err).Msg("scheduler: failed to enqueue scheduled run")
		return
	}

	advanced, err := s.schedules.AdvanceSchedule(ctx, sched.ID, sched.NextRunAt, next, jobID)
	if err != nil {
		log.Error().Err(err).Msg("scheduler: failed to advance schedule")
		return
	}
	if !advanced {
		// Another replica got there first. Normal, and the common case once more
		// than one instance is running, so debug rather than warn.
		log.Debug().Msg("scheduler: another instance advanced this schedule")
		return
	}

	event := log.Info().Str("job_id", jobID).Time("fired_for", fireAt).Time("next_run_at", next)
	if skipped > 0 {
		event = event.Int("skipped_occurrences", skipped)
	}
	event.Msg("scheduler: fired schedule")
}

// nextAfter returns the first occurrence strictly after now, and how many
// occurrences between from and now it stepped over.
//
// Missed occurrences are skipped, not replayed. A deployment that was down for a
// day should not wake up and run twenty-four hourly aggregates back to back;
// almost nobody who writes a recurring job wants the backlog, and the ones who
// do want it want it ordered and rate-limited, which is a different feature.
func nextAfter(cron Schedule, from, now time.Time) (time.Time, int) {
	// ponytail: one Next call per missed occurrence. A minutely schedule left a
	// year behind costs ~500k calls at ~250 ns, so well under a second, once.
	// Solve arithmetically only if a pathological expression ever shows up.
	skipped := 0
	next := cron.Next(from)
	for !next.IsZero() && !next.After(now) {
		next = cron.Next(next)
		skipped++
	}
	return next, skipped
}

// Parse parses a standard 5-field cron expression: minute hour dom month dow.
// Exported so the API can reject an invalid expression at create time rather
// than at fire time.
func Parse(expr string) (Schedule, error) {
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return nil, fmt.Errorf("expected 5 fields, got %d", len(fields))
	}

	var cs cronSchedule

	if bits, err := parseField(fields[0], 0, 59); err != nil {
		return nil, fmt.Errorf("minute: %w", err)
	} else {
		copy(cs.minutes[:], bits)
	}
	if bits, err := parseField(fields[1], 0, 23); err != nil {
		return nil, fmt.Errorf("hour: %w", err)
	} else {
		copy(cs.hours[:], bits)
	}
	if bits, err := parseField(fields[2], 1, 31); err != nil {
		return nil, fmt.Errorf("day-of-month: %w", err)
	} else {
		copy(cs.doms[:], bits)
	}
	if bits, err := parseField(fields[3], 1, 12); err != nil {
		return nil, fmt.Errorf("month: %w", err)
	} else {
		copy(cs.months[:], bits)
	}
	if bits, err := parseField(fields[4], 0, 6); err != nil {
		return nil, fmt.Errorf("day-of-week: %w", err)
	} else {
		copy(cs.dows[:], bits)
	}

	return &cs, nil
}

// parseField converts a single cron field expression into a boolean array.
// The returned slice has length (max-min+1); index 0 maps to min.
func parseField(expr string, min, max int) ([]bool, error) {
	result := make([]bool, max-min+1)

	for _, part := range strings.Split(expr, ",") {
		part = strings.TrimSpace(part)
		switch {
		case part == "*":
			for i := range result {
				result[i] = true
			}

		case strings.HasPrefix(part, "*/"):
			n, err := strconv.Atoi(part[2:])
			if err != nil || n <= 0 {
				return nil, fmt.Errorf("invalid step %q", part)
			}
			for i := 0; i <= max-min; i += n {
				result[i] = true
			}

		case strings.Contains(part, "-"):
			bounds := strings.SplitN(part, "-", 2)
			lo, err1 := strconv.Atoi(bounds[0])
			hi, err2 := strconv.Atoi(bounds[1])
			if err1 != nil || err2 != nil || lo < min || hi > max || lo > hi {
				return nil, fmt.Errorf("invalid range %q", part)
			}
			for i := lo; i <= hi; i++ {
				result[i-min] = true
			}

		default:
			n, err := strconv.Atoi(part)
			if err != nil || n < min || n > max {
				return nil, fmt.Errorf("invalid value %q (allowed %d-%d)", part, min, max)
			}
			result[n-min] = true
		}
	}

	return result, nil
}

// cronSchedule holds pre-computed membership sets for each cron field.
type cronSchedule struct {
	minutes [60]bool // 0-59
	hours   [24]bool // 0-23
	doms    [31]bool // index 0 = day 1
	months  [12]bool // index 0 = January
	dows    [7]bool  // 0 = Sunday
}

// Next returns the earliest time strictly after `from` that satisfies the schedule.
// Returns the zero time if no match is found within four years.
func (c *cronSchedule) Next(from time.Time) time.Time {
	t := from.Add(time.Minute).Truncate(time.Minute)
	limit := from.Add(4 * 365 * 24 * time.Hour)

	for t.Before(limit) {
		if !c.months[int(t.Month())-1] {
			t = time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, t.Location())
			continue
		}
		if day := t.Day(); day > 31 || !c.doms[day-1] || !c.dows[int(t.Weekday())] {
			t = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, t.Location())
			continue
		}
		if !c.hours[t.Hour()] {
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, 0, 0, 0, t.Location())
			continue
		}
		if !c.minutes[t.Minute()] {
			t = t.Add(time.Minute)
			continue
		}
		return t
	}
	return time.Time{}
}
