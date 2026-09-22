package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/JustinK33/Conduit/pkg/models"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

var (
	ErrScheduleNotFound  = errors.New("store: schedule not found")
	ErrDuplicateSchedule = errors.New("store: schedule name already exists")
)

// ScheduleStore owns the schedules table. A separate type rather than more
// methods on PostgresStore, because the two tables have separate lifecycles and
// nothing needs them in one transaction: firing a schedule writes a job through
// the service layer, not through here.
type ScheduleStore struct {
	Pool      *pgxpool.Pool
	TableName string
}

// DefaultSchedulesTable is the table migrations/004 creates. TableName is a
// field rather than a constant for the same reason PostgresStore's is: an
// integration test gets its own copy of the schema so a live instance's
// reconciler cannot claim rows out from under it.
const DefaultSchedulesTable = "schedules"

func NewScheduleStore(pool *pgxpool.Pool, tableName string) *ScheduleStore {
	if tableName == "" {
		tableName = DefaultSchedulesTable
	}
	return &ScheduleStore{Pool: pool, TableName: tableName}
}

const scheduleColumns = `id, name, cron_expr,
	task_name, task_queue, task_payload, task_max_retries, task_timeout_ns, task_metadata,
	enabled, next_run_at, last_run_at, last_job_id, created_at, updated_at`

// CreateSchedule inserts a schedule and returns it as stored, with the id and
// timestamps filled in. The caller supplies the cron expression and the first
// NextRunAt; minting the id here keeps it in one place, the way JobService.Enqueue
// does for jobs.
func (s *ScheduleStore) CreateSchedule(ctx context.Context, sched models.Schedule) (models.Schedule, error) {
	metaBytes, err := json.Marshal(sched.Task.Metadata)
	if err != nil {
		return models.Schedule{}, fmt.Errorf("store: marshal task metadata: %w", err)
	}

	if sched.ID == "" {
		id, err := newID()
		if err != nil {
			return models.Schedule{}, fmt.Errorf("store: generate schedule id: %w", err)
		}
		sched.ID = id
	}
	if sched.Task.Queue == "" {
		sched.Task.Queue = models.DefaultQueue
	}

	now := time.Now().UTC()
	if sched.CreatedAt.IsZero() {
		sched.CreatedAt = now
	}
	sched.UpdatedAt = now

	query := fmt.Sprintf(`
		INSERT INTO %s (
			id, name, cron_expr,
			task_name, task_queue, task_payload, task_max_retries, task_timeout_ns, task_metadata,
			enabled, next_run_at, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, s.TableName)

	_, err = s.Pool.Exec(ctx, query,
		sched.ID,
		sched.Name,
		sched.Cron,
		sched.Task.Name,
		sched.Task.Queue,
		[]byte(sched.Task.Payload),
		sched.Task.MaxRetries,
		time.Duration(sched.Task.Timeout).Nanoseconds(),
		metaBytes,
		sched.Enabled,
		sched.NextRunAt,
		sched.CreatedAt,
		sched.UpdatedAt,
	)
	if isUniqueViolation(err) {
		return models.Schedule{}, ErrDuplicateSchedule
	}
	if err != nil {
		return models.Schedule{}, err
	}
	return sched, nil
}

func (s *ScheduleStore) ListSchedules(ctx context.Context) ([]models.Schedule, error) {
	query := fmt.Sprintf(`SELECT %s FROM %s ORDER BY name ASC`, scheduleColumns, s.TableName)

	rows, err := s.Pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	schedules := []models.Schedule{}
	for rows.Next() {
		sched, err := scanSchedule(ctx, rows)
		if err != nil {
			return nil, err
		}
		schedules = append(schedules, sched)
	}
	return schedules, rows.Err()
}

func (s *ScheduleStore) DeleteSchedule(ctx context.Context, id string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE id = $1`, s.TableName)
	tag, err := s.Pool.Exec(ctx, query, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrScheduleNotFound
	}
	return nil
}

// DueSchedules returns enabled schedules whose fire instant has arrived, oldest
// first. Every instance runs this every tick and they all see the same rows:
// nothing is claimed here, because a schedule is not work, it is a decision
// about work. AdvanceSchedule is where the race is settled.
func (s *ScheduleStore) DueSchedules(ctx context.Context, now time.Time, limit int) ([]models.Schedule, error) {
	if limit <= 0 {
		return nil, nil
	}
	query := fmt.Sprintf(`
		SELECT %s FROM %s
		WHERE enabled AND next_run_at <= $1
		ORDER BY next_run_at ASC
		LIMIT $2`, scheduleColumns, s.TableName)

	rows, err := s.Pool.Query(ctx, query, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	due := []models.Schedule{}
	for rows.Next() {
		sched, err := scanSchedule(ctx, rows)
		if err != nil {
			return nil, err
		}
		due = append(due, sched)
	}
	return due, rows.Err()
}

// AdvanceSchedule moves a fired schedule to its next instant, conditional on
// next_run_at still holding the value the caller read.
//
// That condition is the whole of the multi-instance story. Every replica polls
// DueSchedules, so all of them see the same due schedule and all of them try to
// advance it; exactly one matches the observed value and the rest affect zero
// rows and stop. No leader election, no advisory lock, and no coordination
// beyond one UPDATE.
//
// It returns false rather than an error when it loses, because losing is the
// normal case on every instance but one and is not worth a log line at warn.
func (s *ScheduleStore) AdvanceSchedule(ctx context.Context, id string, observedNext, next time.Time, lastJobID string) (bool, error) {
	query := fmt.Sprintf(`
		UPDATE %s
		SET next_run_at = $1,
			last_run_at = NOW(),
			last_job_id = $2,
			updated_at = NOW()
		WHERE id = $3
		  AND next_run_at = $4`, s.TableName)

	tag, err := s.Pool.Exec(ctx, query, next, nullableString(lastJobID), id, observedNext)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func scanSchedule(ctx context.Context, row pgx.Row) (models.Schedule, error) {
	var (
		sched     models.Schedule
		payload   []byte
		metaJSON  []byte
		timeoutNS int64
		lastJobID *string
	)

	err := row.Scan(
		&sched.ID,
		&sched.Name,
		&sched.Cron,
		&sched.Task.Name,
		&sched.Task.Queue,
		&payload,
		&sched.Task.MaxRetries,
		&timeoutNS,
		&metaJSON,
		&sched.Enabled,
		&sched.NextRunAt,
		&sched.LastRunAt,
		&lastJobID,
		&sched.CreatedAt,
		&sched.UpdatedAt,
	)
	if err != nil {
		return models.Schedule{}, err
	}

	if lastJobID != nil {
		sched.LastJobID = *lastJobID
	}
	sched.Task.Payload = payload
	sched.Task.Timeout = models.Duration(timeoutNS)
	if len(metaJSON) > 0 {
		if err := json.Unmarshal(metaJSON, &sched.Task.Metadata); err != nil {
			zerolog.Ctx(ctx).Warn().Err(err).Str("schedule_id", sched.ID).Msg("store: failed to unmarshal schedule task metadata")
		}
	}
	return sched, nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// newID mints a random UUID v4. Same shape as the job ids JobService generates,
// so one id in a log line does not read differently from another.
func newID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b)
	return fmt.Sprintf("%s-%s-%s-%s-%s", h[0:8], h[8:12], h[12:16], h[16:20], h[20:]), nil
}
