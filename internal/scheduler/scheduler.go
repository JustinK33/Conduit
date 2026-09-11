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

type Schedule interface {
	Next(time.Time) time.Time
}

type JobFunc func(context.Context, models.Job) error

type Entry struct {
	Name    string
	Cron    string
	Job     models.Job
	Handler JobFunc
	Enabled bool
}

type Scheduler struct {
	entries      map[string]Entry
	schedules    map[string]Schedule
	lastRun      map[string]time.Time
	now          func() time.Time
	tickInterval time.Duration
	running      bool
	mu           sync.Mutex
	wg           sync.WaitGroup
	cancel       context.CancelFunc
}

func New(tickInterval time.Duration) *Scheduler {
	if tickInterval <= 0 {
		tickInterval = time.Minute
	}
	return &Scheduler{
		entries:      make(map[string]Entry),
		schedules:    make(map[string]Schedule),
		lastRun:      make(map[string]time.Time),
		now:          time.Now,
		tickInterval: tickInterval,
	}
}

// Register adds or replaces an entry. Cron expression is validated up front.
func (s *Scheduler) Register(entry Entry) error {
	if entry.Name == "" {
		return fmt.Errorf("scheduler: entry name is required")
	}
	sched, err := parseCron(entry.Cron)
	if err != nil {
		return fmt.Errorf("scheduler: invalid cron %q: %w", entry.Cron, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[entry.Name] = entry
	s.schedules[entry.Name] = sched
	return nil
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
		ticker := time.NewTicker(s.tickInterval)
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

// Stop halts the scheduler loop and waits for any dispatched jobs to finish.
// Safe to call before Start or multiple times.
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

// dispatch fires any entries whose next scheduled time is at or before now.
// It takes a snapshot of entries under the lock to avoid holding it during dispatch.
func (s *Scheduler) dispatch(ctx context.Context) {
	now := s.now()

	type candidate struct {
		entry   Entry
		sched   Schedule
		lastRun time.Time
	}

	s.mu.Lock()
	candidates := make([]candidate, 0, len(s.entries))
	for name, entry := range s.entries {
		candidates = append(candidates, candidate{
			entry:   entry,
			sched:   s.schedules[name],
			lastRun: s.lastRun[name],
		})
	}
	s.mu.Unlock()

	for _, c := range candidates {
		if !c.entry.Enabled || c.sched == nil {
			continue
		}
		next := c.sched.Next(c.lastRun)
		if next.IsZero() || now.Before(next) {
			continue
		}

		s.mu.Lock()
		s.lastRun[c.entry.Name] = now
		s.mu.Unlock()

		s.wg.Add(1)
		go func(e Entry) {
			defer s.wg.Done()
			if e.Handler == nil {
				return
			}
			if err := e.Handler(ctx, e.Job); err != nil {
				zerolog.Ctx(ctx).Error().Err(err).Str("entry", e.Name).Msg("scheduled job failed")
			}
		}(c.entry)
	}
}

// parseCron parses a standard 5-field cron expression: minute hour dom month dow.
func parseCron(expr string) (Schedule, error) {
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
				return nil, fmt.Errorf("invalid value %q (allowed %d–%d)", part, min, max)
			}
			result[n-min] = true
		}
	}

	return result, nil
}

// cronSchedule holds pre-computed membership sets for each cron field.
type cronSchedule struct {
	minutes [60]bool // 0–59
	hours   [24]bool // 0–23
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
